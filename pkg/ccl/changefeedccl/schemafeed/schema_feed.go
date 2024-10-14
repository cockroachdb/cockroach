// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package schemafeed provides SchemaFeed, which can be used to track schema
// updates.
package schemafeed

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedvalidators"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Ideally we could have a centralized worker which reads the
// table descriptors instead of polling from each changefeed. This wouldn't be
// too hard. Each registered queue would have a start time. You'd scan from the
// earliest and just ingest the relevant descriptors.

// TableEvent represents a change to a table descriptor.
type TableEvent struct {
	Before, After catalog.TableDescriptor
}

// Timestamp refers to the ModificationTime of the After table descriptor.
func (e TableEvent) Timestamp() hlc.Timestamp {
	return e.After.GetModificationTime()
}

// leaseAcquirer is an interface containing the methods on *lease.Manager used
// by the schema feed.
type leaseAcquirer interface {
	Acquire(ctx context.Context, timestamp hlc.Timestamp, id descpb.ID) (lease.LeasedDescriptor, error)
	AcquireFreshestFromStore(ctx context.Context, id descpb.ID) error
	// TODO(yang): Investigate whether the codec can be stored in the schema feed itself.
	Codec() keys.SQLCodec
}

// SchemaFeed is a stream of events corresponding the relevant set of
// descriptors.
type SchemaFeed interface {
	// Run synchronously runs the SchemaFeed. It should be invoked before any
	// calls to Peek or Pop.
	Run(ctx context.Context) error

	// Peek returns events occurring up to atOrBefore.
	Peek(ctx context.Context, atOrBefore hlc.Timestamp) (events []TableEvent, err error)
	// Pop returns events occurring up to atOrBefore and removes them from the feed.
	Pop(ctx context.Context, atOrBefore hlc.Timestamp) (events []TableEvent, err error)
}

// New creates a SchemaFeed tracking 'targets' and emitting specified 'events'.
//
// initialFrontier is the earliest timestamp for which updates should be emitted.
// NB: When clients want to create a changefeed which has a resolved timestamp
// of ts1, they care about write which occur at ts1.Next() and later but they
// should scan the tables as of ts1. This is important so that writes which
// change the table at ts1.Next() are emitted as an event.
func New(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	events changefeedbase.SchemaChangeEventClass,
	targets changefeedbase.Targets,
	initialFrontier hlc.Timestamp,
	metrics *Metrics,
	tolerances changefeedbase.CanHandle,
) SchemaFeed {
	m := &schemaFeed{
		filter:          schemaChangeEventFilters[events],
		db:              cfg.DB,
		clock:           cfg.DB.KV().Clock(),
		settings:        cfg.Settings,
		targets:         targets,
		leaseMgr:        cfg.LeaseManager.(*lease.Manager),
		metrics:         metrics,
		tolerances:      tolerances,
		initialFrontier: initialFrontier,
	}
	m.mu.previousTableVersion = make(map[descpb.ID]catalog.TableDescriptor)
	m.mu.typeDeps = typeDependencyTracker{deps: make(map[descpb.ID][]descpb.ID)}
	return m
}

// schemaFeed tracks changes to a set of tables and exports them as a queue of
// events. The queue allows clients to provide a timestamp at or before which
// all events must be seen by the time Peek or Pop returns. This allows clients
// to ensure that all table events which precede some rangefeed event are seen
// before propagating that rangefeed event.
//
// Internally, two timestamps are tracked. The frontier is the latest
// timestamp such that every version of a TableDescriptor has met a provided
// invariant (via `validateFn`). An error timestamp is also kept, which is the
// earliest timestamp where at least one table doesn't meet the invariant.
type schemaFeed struct {
	filter          tableEventFilter
	db              descs.DB
	clock           *hlc.Clock
	settings        *cluster.Settings
	targets         changefeedbase.Targets
	metrics         *Metrics
	tolerances      changefeedbase.CanHandle
	initialFrontier hlc.Timestamp

	// TODO(ajwerner): Should this live underneath the FilterFunc?
	// Should there be another function to decide whether to update the
	// lease manager?
	leaseMgr leaseAcquirer

	mu struct {
		syncutil.Mutex

		// started is used to prevent running a schema feed more than once.
		started bool

		// ts keeps track of the schema feed frontier and error timestamps.
		ts timestampBarrier

		// events is a sorted list of table events which have not been popped and
		// are at or earlier than the current frontier.
		// TODO(yang): Refactor this into a struct and extract out all the logic.
		events []TableEvent

		// previousTableVersion is a map from tableID to the most recent version
		// of the table descriptor seen by the poller. This is needed to determine
		// when a backfilling mutation has successfully completed - this can only
		// be determining by comparing a version to the previous version.
		previousTableVersion map[descpb.ID]catalog.TableDescriptor

		// typeDeps tracks dependencies from target tables to user defined types
		// that they use.
		typeDeps typeDependencyTracker

		// pollingPaused, if set, pauses the polling background work.
		// Polling can be paused if all tables are locked from schema changes because
		// we know no table events will occur.
		pollingPaused bool
	}
}

type typeDependencyTracker struct {
	deps map[descpb.ID][]descpb.ID
}

func (t *typeDependencyTracker) addDependency(typeID, tableID descpb.ID) {
	deps, ok := t.deps[typeID]
	if !ok {
		t.deps[typeID] = []descpb.ID{tableID}
	} else {
		// Check if we already contain this dependency. If so, noop.
		for _, dep := range deps {
			if dep == tableID {
				return
			}
		}
		t.deps[typeID] = append(deps, tableID)
	}
}

func (t *typeDependencyTracker) removeDependency(typeID, tableID descpb.ID) {
	deps, ok := t.deps[typeID]
	if !ok {
		return
	}
	for i := range deps {
		if deps[i] == tableID {
			deps = append(deps[:i], deps[i+1:]...)
			break
		}
	}
	if len(deps) == 0 {
		delete(t.deps, typeID)
	} else {
		t.deps[typeID] = deps
	}
}

func (t *typeDependencyTracker) purgeTable(tbl catalog.TableDescriptor) {
	for _, col := range tbl.UserDefinedTypeColumns() {
		id := typedesc.UserDefinedTypeOIDToID(col.GetType().Oid())
		t.removeDependency(id, tbl.GetID())
	}
}

func (t *typeDependencyTracker) ingestTable(tbl catalog.TableDescriptor) {
	for _, col := range tbl.UserDefinedTypeColumns() {
		id := typedesc.UserDefinedTypeOIDToID(col.GetType().Oid())
		t.addDependency(id, tbl.GetID())
	}
}

func (t *typeDependencyTracker) containsType(id descpb.ID) bool {
	_, ok := t.deps[id]
	return ok
}

func (tf *schemaFeed) pollingPaused() bool {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	return tf.mu.pollingPaused
}

// init does all necessary initialization work.
// It should be called only once when running the feed.
func (tf *schemaFeed) init() error {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	if tf.mu.started {
		return errors.AssertionFailedf("SchemaFeed started more than once")
	}
	tf.mu.started = true
	return nil
}

// Run will run the SchemaFeed. It is an error to run a feed more than once.
func (tf *schemaFeed) Run(ctx context.Context) error {
	if err := tf.init(); err != nil {
		return err
	}

	// Fetch the table descs as of the initial frontier and prime the table
	// history with them. This addresses #41694 where we'd skip the rest of a
	// backfill if the changefeed was paused/unpaused during it. The bug was that
	// the changefeed wouldn't notice the table descriptor had changed (and thus
	// we were in the backfill state) when it restarted.
	if err := tf.primeInitialTableDescs(ctx); err != nil {
		return err
	}
	// We want to initialize the table history which will pull the initial version
	// and then begin polling.
	//
	// TODO(ajwerner): As written the polling will add table events forever.
	// If there are a ton of table events we'll buffer them all in RAM. There are
	// cases where this might be problematic. It could be mitigated with some
	// memory monitoring. Probably better is to not poll eagerly but only poll if
	// we don't have an event.
	//
	// After we add some sort of locking to prevent schema changes we should also
	// only poll if we don't have a lease.
	return tf.periodicallyMaybePollTableHistory(ctx)
}

func (tf *schemaFeed) primeInitialTableDescs(ctx context.Context) error {
	var initialDescs []catalog.Descriptor

	initialTableDescsFn := func(
		ctx context.Context, txn descs.Txn,
	) error {
		descriptors := txn.Descriptors()
		initialDescs = initialDescs[:0]
		if err := txn.KV().SetFixedTimestamp(ctx, tf.initialFrontier); err != nil {
			return err
		}
		// Note that all targets are currently guaranteed to be tables.
		return tf.targets.EachTableID(func(id descpb.ID) error {
			tableDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
			if err != nil {
				return err
			}
			initialDescs = append(initialDescs, tableDesc)
			return nil
		})
	}

	if err := tf.db.DescsTxn(ctx, initialTableDescsFn); err != nil {
		return err
	}

	func() {
		tf.mu.Lock()
		defer tf.mu.Unlock()
		// Register all types used by the initial set of tables.
		for _, desc := range initialDescs {
			tbl := desc.(catalog.TableDescriptor)
			tf.mu.typeDeps.ingestTable(tbl)
		}
	}()

	return tf.ingestDescriptors(ctx, hlc.Timestamp{}, tf.initialFrontier, initialDescs, tf.validateDescriptor)
}

// periodicallyMaybePollTableHistory periodically polls all versions of watched
// tables from `system.descriptor` table between `tf.frontier` and now, if it
// is not paused. This is the general mechanism schemaFeed use to know all watched
// table versions at or before a particular timestamp, internally tracked by
// `tf.frontier`, so it can answer Peek/Pop(atOrBefore).
//
// Note: such a spinning loop incurs a lot of read traffic in the cluster (think:
// many nodes and many changefeeds) but for the vast, vast majority of the time,
// the polling answer is "no table has changed during this interval" because
// schema changes are rare. To mitigate that and still be able to answer
// Peek/Pop(atOrBefore), we can "lock" the table to prevent schema changes, so
// the periodic polling can be paused and Peek/Pop will always just return nil.
// Read commentary in peekOrPop for more explanations.
func (tf *schemaFeed) periodicallyMaybePollTableHistory(ctx context.Context) error {
	for {
		if !tf.pollingPaused() {
			if err := tf.updateTableHistory(ctx, tf.clock.Now()); err != nil {
				return err
			}
			if tf.metrics != nil {
				tf.metrics.TableHistoryScans.Inc(1)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(changefeedbase.TableDescriptorPollInterval.Get(&tf.settings.SV)):
		}
	}
}

// updateTableHistory attempts to advance `frontier` to `endTS` by fetching
// descriptor versions from the current `frontier` to `endTS`.
func (tf *schemaFeed) updateTableHistory(ctx context.Context, endTS hlc.Timestamp) error {
	startTS := func() hlc.Timestamp {
		tf.mu.Lock()
		defer tf.mu.Unlock()
		return tf.mu.ts.frontier
	}()

	if endTS.LessEq(startTS) {
		return nil
	}
	descs, err := tf.fetchDescriptorVersions(ctx, startTS, endTS)
	if err != nil {
		return err
	}
	return tf.ingestDescriptors(ctx, startTS, endTS, descs, tf.validateDescriptor)
}

// Peek returns all events which have not been popped which happen at or
// before the passed timestamp.
func (tf *schemaFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []TableEvent, err error) {
	return tf.peekOrPop(ctx, atOrBefore, false /* pop */)
}

// Pop pops events from the EventQueue.
func (tf *schemaFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []TableEvent, err error) {
	return tf.peekOrPop(ctx, atOrBefore, true /* pop */)
}

func (tf *schemaFeed) peekOrPop(
	ctx context.Context, atOrBefore hlc.Timestamp, pop bool,
) (events []TableEvent, err error) {
	// Routinely check whether to pause or resume polling.
	if err = tf.pauseOrResumePolling(ctx, atOrBefore); err != nil {
		return nil, err
	}
	if err = tf.waitForTS(ctx, atOrBefore); err != nil {
		return nil, err
	}
	tf.mu.Lock()
	defer tf.mu.Unlock()
	i := sort.Search(len(tf.mu.events), func(i int) bool {
		return !tf.mu.events[i].Timestamp().LessEq(atOrBefore)
	})
	events = tf.mu.events[:i]
	if pop {
		tf.mu.events = tf.mu.events[i:]
	}
	return events, nil
}

// pauseOrResumePolling pauses or resumes the periodic table history scan
// performed by the schema feed (polling) based on whether all target tables
// are "locked" from schema changes.
//
// There are two cases:
//
//  1. If atOrBefore <= tf.frontier, then we can try and determine if it's
//     safe to pause polling as of tf.frontier based on whether all target
//     tables are schema-locked at that point.
//
//  2. Otherwise, atOrBefore > tf.frontier, in which case we also need to
//     check whether all target tables still have the same schema version
//     at atOrBefore. If so, we can safely bump tf.frontier up to atOrBefore
//     and (continue to) pause polling.
//
// Note that we continue to update the tf.frontier so that we know the
// timestamp at which we should resume polling from once any of the target
// tables are no longer schema-locked. Another reason to keep tf.frontier
// updated is so that we do not attempt to acquire a lease at a very old timestamp.
//
// Technical details about leasing:
// We know that the lease manager guarantees a two-version invariant:
//
//	leaseManager.Acquire(t, ts) returns a descriptor of `t` whose version is valid
//	for SQL activities at timestamp `ts`. This version is either the "canonical"
//	version of `t` at `ts` (newest), or its predecessor version (second-newest).
//
// Let `ld1` be the leased table descriptor at tf.frontier.
// Let `ld2` be the leased descriptor at atOrBefore.
//
// If both `ld1` and `ld2` are of the same version and are both "schema_locked",
// then it's safe to report "there's no table events in (tf.frontier, atOrBefore]",
// because of the following case analysis:
//
//   - ld1 canonical, ld2 canonical: no table events because `t` remains the same
//     from `tf.frontier` to `atOrBefore`
//
//     atOrBefore-------------------------------v
//     tf.frontier---------v
//     ----------v1--------|--------------------|--------------------
//     ld1-------^
//     ld2-------^
//
//   - ld1 canonical, ld2 predecessor: a schema change happened in (tf.frontier, atOrBefore].
//     But the only schema change allowed on a locked table is to unlock
//     it so `ld2` will be exactly the same as the canonical version except
//     for the locked-bit. We can ignore/omit such an "uninteresting" table event
//     as it will be filtered out by the schema feed anyway.
//
//     atOrBefore-------------------------------v
//     tf.frontier---------v
//     ----------v1--------|----------------v2--|--------------------
//     ld1-------^
//     ld2-------^
//
//   - ld1 predecessor, ld2 predecessor: no table events because `t` remains the same
//     from `tf.frontier` to `atOrBefore`.
//
//     atOrBefore-------------------------------v
//     tf.frontier---------v
//     ----------v1----v2--|--------------------|--------------------
//     ld1-------^
//     ld2-------^
//
//   - ld1 predecessor, ld2 canonical: impossible as it would imply that somehow
//     that the newest descriptor before atOrBefore, which is later than tf.frontier,
//     is the same as the second-newest descriptor before tf.frontier. This cannot
//     be possible within a single timeline.
//
//     atOrBefore-------------------------------v
//     tf.frontier---------v
//     ----------v1----v2--|--------------------|--------------------
//     ld1-------^
//     ----------v1--------|--------------------|--------------------
//     ld2-------^
func (tf *schemaFeed) pauseOrResumePolling(ctx context.Context, atOrBefore hlc.Timestamp) error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	frontier := tf.mu.ts.frontier

	// Fast path.
	if tf.mu.pollingPaused && atOrBefore.LessEq(frontier) {
		return nil
	}

	// Always assume we need to resume polling until we've proven otherwise.
	tf.mu.pollingPaused = false

	if canPausePolling, err := tf.targets.EachTableIDWithBool(func(id descpb.ID) (bool, error) {
		// Check if target table is schema-locked at the current frontier.
		ld1, err := tf.leaseMgr.Acquire(ctx, frontier, id)
		if err != nil {
			return false, err
		}
		defer ld1.Release(ctx)
		desc1 := ld1.Underlying().(catalog.TableDescriptor)
		if !desc1.IsSchemaLocked() {
			if log.V(2) {
				log.Infof(ctx, "desc %d not schema-locked at frontier %s", desc1.GetID(), frontier)
			}
			return false, nil
		}

		if atOrBefore.LessEq(frontier) {
			return true, nil
		}

		// Check if target table remains at the same version at atOrBefore.
		ld2, err := tf.leaseMgr.Acquire(ctx, atOrBefore, id)
		if err != nil {
			return false, err
		}
		defer ld2.Release(ctx)
		desc2 := ld2.Underlying().(catalog.TableDescriptor)
		if desc1.GetVersion() != desc2.GetVersion() {
			if log.V(1) {
				log.Infof(ctx,
					"desc %d version changed from version %d to %d between frontier %s and atOrBefore %s",
					desc1.GetID(), desc1.GetVersion(), desc2.GetVersion(), frontier, atOrBefore)
			}
			return false, nil
		}
		return true, nil
	}); !canPausePolling || err != nil {
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			// If a table is dropped and causes Acquire to fail, we mark it as a
			// terminal error, so that we don't retry, and let the changefeed job
			// handle this error.
			return changefeedbase.WithTerminalError(err)
		}
		// We swallow any non-terminal errors so that the slow path can be tried
		// after we resume polling.
		if log.V(1) {
			log.Infof(ctx, "got a non-terminal error while checking if polling can be paused: %s", err)
		}
		return nil
	}

	tf.mu.pollingPaused = true
	if !frontier.Less(atOrBefore) {
		return nil
	}
	return tf.mu.ts.advanceFrontier(atOrBefore)
}

// waitForTS blocks until the given timestamp is less than or equal to the
// frontier, greater than or equal to error timestamp, or the context is
// canceled. In the error timestamp case, the relevant error is returned.
//
// If called twice with the same timestamp, two different errors may be returned
// (since the error timestamp can recede). However, the return for a given
// timestamp will never switch from nil to an error or vice-versa (assuming that
// `validateFn` is deterministic and the ingested descriptors are read
// transactionally).
func (tf *schemaFeed) waitForTS(ctx context.Context, ts hlc.Timestamp) error {
	if log.V(1) {
		log.Infof(ctx, "waiting for frontier to reach %s", ts)
	}

	waitCh, needToWait := func() (<-chan error, bool) {
		tf.mu.Lock()
		defer tf.mu.Unlock()
		return tf.mu.ts.wait(ts)
	}()

	start := timeutil.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-waitCh:
		if needToWait {
			waited := timeutil.Since(start)
			if log.V(1) {
				log.Infof(ctx, "waited %s for frontier to reach %s: err=%v", waited, ts, err)
			}
			if tf.metrics != nil {
				tf.metrics.TableMetadataNanos.Inc(waited.Nanoseconds())
			}
		} else {
			if log.V(1) {
				log.Infof(ctx, "fastpath taken when waiting for %s", ts)
			}
		}
		return err
	}
}

// descLess orders descriptors by (modificationTime, id).
func descLess(a, b catalog.Descriptor) bool {
	aTime, bTime := a.GetModificationTime(), b.GetModificationTime()
	if aTime.Equal(bTime) {
		return a.GetID() < b.GetID()
	}
	return aTime.Less(bTime)
}

// ingestDescriptors checks the given descriptors against the invariant check
// function and adjusts the frontier or error timestamp appropriately. It is
// required that the descriptors represent a transactional kv read between the
// two given timestamps.
//
// TODO(yang): Consider refactoring to a validator interface type.
// validateFn is exposed for testing, in production it is tf.validateDescriptor.
func (tf *schemaFeed) ingestDescriptors(
	ctx context.Context,
	startTS, endTS hlc.Timestamp,
	descs []catalog.Descriptor,
	validateFn func(ctx context.Context, earliestTsBeingIngested hlc.Timestamp, desc catalog.Descriptor) error,
) error {
	sort.Slice(descs, func(i, j int) bool { return descLess(descs[i], descs[j]) })
	var validateErr error
	for _, desc := range descs {
		if err := validateFn(ctx, startTS, desc); validateErr == nil {
			validateErr = err
		}
	}
	return tf.adjustTimestamps(startTS, endTS, validateErr)
}

// adjustTimestamps adjusts the frontier or error timestamp appropriately.
func (tf *schemaFeed) adjustTimestamps(startTS, endTS hlc.Timestamp, validateErr error) error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if validateErr != nil {
		if err := tf.mu.ts.setError(endTS, validateErr); err != nil {
			return errors.Wrapf(err, "failed to set error with error timestamp %s", endTS)
		}
		return validateErr
	}

	if frontier := tf.mu.ts.frontier; frontier.Less(startTS) {
		return errors.Errorf(`gap between %s and %s`, frontier, startTS)
	}
	return tf.mu.ts.advanceFrontier(endTS)
}

func (e TableEvent) String() string {
	return formatEvent(e)
}

func formatDesc(desc catalog.TableDescriptor) string {
	return fmt.Sprintf("%d:%d@%v", desc.GetID(), desc.GetVersion(), desc.GetModificationTime())
}

func formatEvent(e TableEvent) string {
	return fmt.Sprintf("%v->%v", formatDesc(e.Before), formatDesc(e.After))
}

func (tf *schemaFeed) validateDescriptor(
	ctx context.Context, earliestTsBeingIngested hlc.Timestamp, desc catalog.Descriptor,
) error {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	switch desc := desc.(type) {
	case catalog.TypeDescriptor:
		if !tf.mu.typeDeps.containsType(desc.GetID()) {
			return nil
		}
		// If a interesting type changed, then we just want to force the lease
		// manager to acquire the freshest version of the type.
		if err := tf.leaseMgr.AcquireFreshestFromStore(ctx, desc.GetID()); err != nil {
			err = errors.Wrapf(err, "could not acquire type descriptor %d lease", desc.GetID())
			if errors.Is(err, catalog.ErrDescriptorDropped) { // That's pretty fatal.
				err = changefeedbase.WithTerminalError(err)
			}
			return err
		}
		return nil
	case catalog.TableDescriptor:
		if err := changefeedvalidators.ValidateTable(tf.targets, desc, tf.tolerances); err != nil {
			return err
		}
		log.VEventf(ctx, 1, "validate %v", formatDesc(desc))
		if lastVersion, ok := tf.mu.previousTableVersion[desc.GetID()]; ok {
			// NB: Writes can occur to a table
			if desc.GetModificationTime().LessEq(lastVersion.GetModificationTime()) {
				return nil
			}

			// To avoid race conditions with the lease manager, at this point we force
			// the manager to acquire the freshest descriptor of this table from the
			// store. In normal operation, the lease manager returns the newest
			// descriptor it knows about for the timestamp, assuming it's still
			// allowed; without this explicit load, the lease manager might therefore
			// return the previous version of the table, which is still technically
			// allowed by the schema change system.
			if err := tf.leaseMgr.AcquireFreshestFromStore(ctx, desc.GetID()); err != nil {
				return err
			}

			// Purge the old version of the table from the type mapping.
			tf.mu.typeDeps.purgeTable(lastVersion)

			e := TableEvent{
				Before: lastVersion,
				After:  desc,
			}
			shouldFilter, err := tf.filter.shouldFilter(ctx, e, tf.targets)
			log.VEventf(ctx, 1, "validate shouldFilter %v %v", formatEvent(e), shouldFilter)
			if err != nil {
				return changefeedbase.WithTerminalError(err)
			}
			if !shouldFilter {
				// Only sort the tail of the events from earliestTsBeingIngested.
				// The head could already have been handed out and sorting is not
				// stable.
				idxToSort := sort.Search(len(tf.mu.events), func(i int) bool {
					return !tf.mu.events[i].After.GetModificationTime().Less(earliestTsBeingIngested)
				})
				tf.mu.events = append(tf.mu.events, e)
				toSort := tf.mu.events[idxToSort:]
				sort.Slice(toSort, func(i, j int) bool {
					return descLess(toSort[i].After, toSort[j].After)
				})
			}
		}
		// Add the types used by the table into the dependency tracker.
		tf.mu.typeDeps.ingestTable(desc)
		tf.mu.previousTableVersion[desc.GetID()] = desc
		return nil
	default:
		return errors.AssertionFailedf("unexpected descriptor type %T", desc)
	}
}

var highPriorityAfter = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.schema_feed.read_with_priority_after",
	"retry with high priority if we were not able to read descriptors for too long; 0 disables",
	time.Minute,
	settings.WithPublic)

// sendExportRequestWithPriorityOverride uses KV API Export() to dump all kv pairs
// whose key falls under `span` and whose mvcc timestamp falls within [startTs, endTS].
//
// If this KV call didn't succeed after `changefeed.schema_feed.read_with_priority_after`,
// we cancel the call and retry the call with high priority.
func sendExportRequestWithPriorityOverride(
	ctx context.Context,
	st *cluster.Settings,
	sender kv.Sender,
	span roachpb.Span,
	startTS, endTS hlc.Timestamp,
) (kvpb.Response, error) {
	header := kvpb.Header{
		Timestamp:                   endTS,
		ReturnElasticCPUResumeSpans: true,
	}
	req := &kvpb.ExportRequest{
		RequestHeader: kvpb.RequestHeaderFromSpan(span),
		StartTime:     startTS,
		MVCCFilter:    kvpb.MVCCFilter_All,
	}

	sendRequest := func(ctx context.Context) (kvpb.Response, error) {
		resp, pErr := kv.SendWrappedWith(ctx, sender, header, req)
		if pErr != nil {
			err := pErr.GoError()
			return nil, errors.Wrapf(err, `fetching changes for %s`, span)
		}
		return resp, nil
	}

	priorityAfter := highPriorityAfter.Get(&st.SV)
	if priorityAfter == 0 {
		return sendRequest(ctx)
	}

	var resp kvpb.Response
	err := timeutil.RunWithTimeout(
		ctx, "schema-feed", priorityAfter,
		func(ctx context.Context) error {
			var err error
			resp, err = sendRequest(ctx)
			return err
		},
	)
	if err == nil {
		return resp, nil
	}
	if errors.HasType(err, (*timeutil.TimeoutError)(nil)) {
		header.UserPriority = roachpb.MaxUserPriority
		return sendRequest(ctx)
	}
	return nil, err
}

// fetchDescriptorVersions makes a KV API call to fetch all watched descriptors
// versions with mvcc timestamp in (startTS, endTS].
// TODO(yang): Consider refactoring this logic into an interface so we can mock
// it in tests.
func (tf *schemaFeed) fetchDescriptorVersions(
	ctx context.Context, startTS, endTS hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, `fetching table descs (%s,%s]`, startTS, endTS)
	}
	codec := tf.leaseMgr.Codec()
	start := timeutil.Now()
	span := roachpb.Span{Key: codec.TablePrefix(keys.DescriptorTableID)}
	span.EndKey = span.Key.PrefixEnd()

	tf.mu.Lock()
	defer tf.mu.Unlock()

	var descriptors []catalog.Descriptor
	for {
		res, err := sendExportRequestWithPriorityOverride(
			ctx, tf.settings, tf.db.KV().NonTransactionalSender(), span, startTS, endTS)
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, `fetched table descs (%s,%s] took %s err=%s`, startTS, endTS, timeutil.Since(start), err)
		}
		if err != nil {
			return nil, err
		}

		found := errors.New(``)
		exportResp := res.(*kvpb.ExportResponse)
		for _, file := range exportResp.Files {
			if err := func() error {
				it, err := storage.NewMemSSTIterator(file.SST, false /* verify */, storage.IterOptions{
					// NB: We assume there will be no MVCC range tombstones here.
					KeyTypes:   storage.IterKeyTypePointsOnly,
					LowerBound: keys.MinKey,
					UpperBound: keys.MaxKey,
				})
				if err != nil {
					return err
				}
				defer it.Close()
				for it.SeekGE(storage.NilKey); ; it.Next() {
					if ok, err := it.Valid(); err != nil {
						return err
					} else if !ok {
						return nil
					}
					k := it.UnsafeKey()
					remaining, _, _, err := codec.DecodeIndexPrefix(k.Key)
					if err != nil {
						return err
					}
					_, id, err := encoding.DecodeUvarintAscending(remaining)
					if err != nil {
						return err
					}
					var origName changefeedbase.StatementTimeName
					isTable, _ := tf.targets.EachHavingTableID(descpb.ID(id), func(t changefeedbase.Target) error {
						origName = t.StatementTimeName
						return found // sentinel error to break the loop
					})
					isType := tf.mu.typeDeps.containsType(descpb.ID(id))
					// Check if the descriptor is an interesting table or type.
					if !(isTable || isType) {
						// Uninteresting descriptor.
						continue
					}

					unsafeValue, err := it.UnsafeValue()
					if err != nil {
						return err
					}

					if len(unsafeValue) == 0 {
						if isType {
							return changefeedbase.WithTerminalError(
								errors.Wrapf(catalog.ErrDescriptorDropped, "type descriptor %d dropped", id))
						}

						name := origName
						if name == "" {
							name = changefeedbase.StatementTimeName(fmt.Sprintf("desc(%d)", id))
						}
						return changefeedbase.WithTerminalError(
							errors.Wrapf(catalog.ErrDescriptorDropped, `table "%v"[%d] was dropped or truncated`, name, id))
					}

					// Unmarshal the descriptor.
					value := roachpb.Value{RawBytes: unsafeValue, Timestamp: k.Timestamp}
					b, err := descbuilder.FromSerializedValue(&value)
					if err != nil {
						return err
					}
					if b != nil && (b.DescriptorType() == catalog.Table || b.DescriptorType() == catalog.Type) {
						descriptors = append(descriptors, b.BuildImmutable())
					}
				}
			}(); err != nil {
				return nil, err
			}
		}

		if exportResp.ResumeSpan == nil {
			break
		}
		span.Key = exportResp.ResumeSpan.Key
	}

	return descriptors, nil
}

type doNothingSchemaFeed struct{}

// Run does nothing until the context is canceled.
func (f doNothingSchemaFeed) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Peek implements SchemaFeed
func (f doNothingSchemaFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []TableEvent, err error) {
	return nil, nil
}

// Pop implements SchemaFeed
func (f doNothingSchemaFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []TableEvent, err error) {
	return nil, nil
}

// DoNothingSchemaFeed is the SchemaFeed implementation that does nothing.
var DoNothingSchemaFeed SchemaFeed = &doNothingSchemaFeed{}
