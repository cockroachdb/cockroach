// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
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

// New creates SchemaFeed tracking 'targets' and emitting specified 'events'.
//
// initialHighwater is the timestamp after which events should occur.
// NB: When clients want to create a changefeed which has a resolved timestamp
// of ts1, they care about write which occur at ts1.Next() and later but they
// should scan the tables as of ts1. This is important so that writes which
// change the table at ts1.Next() are emitted as an event.
func New(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	events changefeedbase.SchemaChangeEventClass,
	targets changefeedbase.Targets,
	initialHighwater hlc.Timestamp,
	metrics *Metrics,
	tolerances changefeedbase.CanHandle,
) SchemaFeed {
	m := &schemaFeed{
		filter:     schemaChangeEventFilters[events],
		db:         cfg.DB,
		clock:      cfg.DB.KV().Clock(),
		settings:   cfg.Settings,
		targets:    targets,
		leaseMgr:   cfg.LeaseManager.(*lease.Manager),
		metrics:    metrics,
		tolerances: tolerances,
	}
	m.mu.previousTableVersion = make(map[descpb.ID]catalog.TableDescriptor)
	m.mu.highWater = initialHighwater
	m.mu.typeDeps = typeDependencyTracker{deps: make(map[descpb.ID][]descpb.ID)}
	return m
}

// SchemaFeed tracks changes to a set of tables and exports them as a queue of
// events. The queue allows clients to provide a timestamp at or before which
// all events must be seen by the time Peek or Pop returns. This allows clients
// to ensure that all table events which precede some rangefeed event are seen
// before propagating that rangefeed event.
//
// Internally, two timestamps are tracked. The high-water is the highest
// timestamp such that every version of a TableDescriptor has met a provided
// invariant (via `validateFn`). An error timestamp is also kept, which is the
// lowest timestamp where at least one table doesn't meet the invariant.
type schemaFeed struct {
	filter     tableEventFilter
	db         descs.DB
	clock      *hlc.Clock
	settings   *cluster.Settings
	targets    changefeedbase.Targets
	metrics    *Metrics
	tolerances changefeedbase.CanHandle

	// TODO(ajwerner): Should this live underneath the FilterFunc?
	// Should there be another function to decide whether to update the
	// lease manager?
	leaseMgr *lease.Manager

	mu struct {
		syncutil.Mutex

		// started is used to prevent running a schema feed more than once.
		started bool

		// the highest known valid timestamp
		highWater hlc.Timestamp

		// the lowest known invalid timestamp
		errTS hlc.Timestamp

		// the error associated with errTS
		err error

		// callers waiting on a timestamp to be resolved as valid or invalid
		waiters []tableHistoryWaiter

		// events is a sorted list of table events which have not been popped and
		// are at or below highWater.
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

		// The following two maps are memoization to help avoid map allocation
		// on a hot path. It is by nature implementation details and should only
		// be concerned by implementer of method pauseOrResumePolling.
		allTableVersions1 map[descpb.ID]descpb.DescriptorVersion
		allTableVersions2 map[descpb.ID]descpb.DescriptorVersion
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

type tableHistoryWaiter struct {
	ts    hlc.Timestamp
	errCh chan error
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

	// Fetch the table descs as of the initial highWater and prime the table
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
	initialTableDescTs := tf.highWater()
	var initialDescs []catalog.Descriptor

	initialTableDescsFn := func(
		ctx context.Context, txn descs.Txn,
	) error {
		descriptors := txn.Descriptors()
		initialDescs = initialDescs[:0]
		if err := txn.KV().SetFixedTimestamp(ctx, initialTableDescTs); err != nil {
			return err
		}
		// Note that all targets are currently guaranteed to be tables.
		return tf.targets.EachTableID(func(id descpb.ID) error {
			tableDesc, err := descriptors.ByID(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
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

	return tf.ingestDescriptors(ctx, hlc.Timestamp{}, initialTableDescTs, initialDescs, tf.validateDescriptor)
}

// periodicallyMaybePollTableHistory periodically polls all versions of watched
// tables from `system.descriptor` table between `tf.highWater` and now, if it
// is not paused. This is the general mechanism schemaFeed use to know all watched
// table versions at or before a particular timestamp, internally tracked by
// `tf.highWater`, so it can answer Peek/Pop(atOrBefore).
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

// updateTableHistory attempts to advance `high_water` to `endTS` by fetching
// descriptor versions from `high_water` to `endTS`.
func (tf *schemaFeed) updateTableHistory(ctx context.Context, endTS hlc.Timestamp) error {
	startTS := tf.highWater()
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
	// Routinely check to pause or resume polling. If it decides to pause polling,
	// then `atOrBefore` will be updated to one that requires no waiting.
	atOrBefore, err = tf.pauseOrResumePolling(ctx, atOrBefore)
	if err != nil {
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
	if i == -1 {
		i = 0
	}
	events = tf.mu.events[:i]
	if pop {
		tf.mu.events = tf.mu.events[i:]
	}
	return events, nil
}

// pauseOrResumePolling pauses or resumes the periodic table history scan
// performed by the schema feed, based on whether all tables are "locked"
// from schema changes.
// Either way, it returns a timestamp `ts` that is ready to be called with
// peekOrPop(ts).
//
// Namely, if it decides to pause the polling (meaning it has confirmed that
// all tables are locked), then it returns `tf.highWater` because
// there is no table events in (tf.highWater, atOrBefore], and we just need to
// peekOrPop at `tf.highWater`, which requires no waiting.
// If it decides to resume the polling (meaning it has confirmed that not all
// tables are locked), then it returns the same `atOrBefore` so we can fall back
// and rely on peekOkPop(atOrBefore) to give us the answer.
//
// Technical details:
// The way it confirms that there is no table events in (tf.highWater, atOrBefore]
// is to acquire a lease of the table at `tf.highWater` (call it `ld1`) and
// at `atOrBefore` (call it `ld2`).
// The lease manager guarantees the following invariant:
//
//	leaseManager.Acquire(t, ts) returns a descriptor of `t` whose version is valid
//	for SQL activities at timestamp `ts`. This version is either the "canonical"
//	version of `t` at `ts`, or its predecessor version.
//
// Now, if both `ld1` and `ld2` are of the same version and are both "schema_locked",
// then it's safe to report "there's no table events in (tf.highWater, atOrBefore]",
// because
//   - ld1 canonical, ld2 canonical: no table events bc `t` remains the same
//     from `tf.highWater` to `atOrBefore`
//   - ld1 canonical, ld2 predecessor: a schema change happened in (tf.highWater, atOrBefore].
//     But the only schema change allowed on a locked table is to unlock
//     it so `ld2` will be exactly the same as the canonical version except
//     for the locked-bit. We can ignore/omit such an "uninteresting" table event
//     as it will be filtered by the schema feed anyway.
//   - ld1 predecessor, ld2 predecessor: no schema change bc `t` remains the same
//     from `tf.highWater` to `atOrBefore`.
//   - ld1 predecessor, ld2 canonical: impossible (how can it be that `t` is
//     unlocked at tf.highWater but locked at atOrBefore with the same version?).
func (tf *schemaFeed) pauseOrResumePolling(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (hlc.Timestamp, error) {
	// areAllLeasedTablesSchemaLockedAt returns true if all leased tables are
	// schema locked at timestamp `ts`.
	// It also updates input `versions` to record those table versions at `ts`.
	areAllLeasedTablesSchemaLockedAt := func(
		ts hlc.Timestamp, versions map[descpb.ID]descpb.DescriptorVersion,
	) (bool, error) {
		allWatchedTableSchemaLocked := true
		err := tf.targets.EachTableID(func(id descpb.ID) error {
			ld, err := tf.leaseMgr.Acquire(ctx, ts, id)
			if err != nil {
				return err
			}
			defer ld.Release(ctx)
			if !ld.Underlying().(catalog.TableDescriptor).IsSchemaLocked() {
				allWatchedTableSchemaLocked = false
				return iterutil.StopIteration()
			}
			versions[id] = ld.Underlying().(catalog.TableDescriptor).GetVersion()
			return nil
		})
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			// If a table is dropped and cause Acquire to fail, we mark it as terminal
			// error, so we don't retry and let the changefeed job handle this error.
			err = changefeedbase.WithTerminalError(err)
		}
		return allWatchedTableSchemaLocked, err
	}

	tf.mu.Lock()
	defer tf.mu.Unlock()
	if atOrBefore.LessEq(tf.mu.highWater) {
		// `atOrBefore` warrants a fast path already, with polling paused or not.
		return atOrBefore, nil
	}

	if tf.mu.allTableVersions1 == nil {
		tf.mu.allTableVersions1 = make(map[descpb.ID]descpb.DescriptorVersion)
		tf.mu.allTableVersions2 = make(map[descpb.ID]descpb.DescriptorVersion)
	}

	// Always start with a stance to resume polling until we've proved otherwise.
	tf.mu.pollingPaused = false
	if ok, err := areAllLeasedTablesSchemaLockedAt(tf.mu.highWater, tf.mu.allTableVersions1); err != nil || !ok {
		return atOrBefore, err
	}
	if ok, err := areAllLeasedTablesSchemaLockedAt(atOrBefore, tf.mu.allTableVersions2); err != nil || !ok {
		return atOrBefore, err
	}
	if len(tf.mu.allTableVersions1) != len(tf.mu.allTableVersions2) {
		return atOrBefore, nil
	}
	for id, version := range tf.mu.allTableVersions1 {
		if version != tf.mu.allTableVersions2[id] {
			return atOrBefore, nil
		}
	}
	tf.mu.pollingPaused = true
	return tf.mu.highWater, nil
}

// highWater returns the current high-water timestamp.
func (tf *schemaFeed) highWater() hlc.Timestamp {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	highWater := tf.mu.highWater
	return highWater
}

// waitForTS blocks until the given timestamp is less than or equal to the
// high-water or error timestamp. In the latter case, the error is returned.
//
// If called twice with the same timestamp, two different errors may be returned
// (since the error timestamp can recede). However, the return for a given
// timestamp will never switch from nil to an error or vice-versa (assuming that
// `validateFn` is deterministic and the ingested descriptors are read
// transactionally).
func (tf *schemaFeed) waitForTS(ctx context.Context, ts hlc.Timestamp) error {
	waitCh, feedErr := tf.tryWaitForTS(ts)
	if feedErr != nil {
		return feedErr
	}

	if waitCh == nil {
		if log.V(1) {
			log.Infof(ctx, "fastpath for %s", ts)
		}
		return nil
	}

	if log.V(1) {
		log.Infof(ctx, "waiting for %s highwater", ts)
	}
	start := timeutil.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-waitCh:
		if log.V(1) {
			log.Infof(ctx, "waited %s for %s highwater: err=%v", timeutil.Since(start), ts, err)
		}
		if tf.metrics != nil {
			tf.metrics.TableMetadataNanos.Inc(timeutil.Since(start).Nanoseconds())
		}
		return err
	}
}

// tryWaitForTS is a fast path for waitForTS.  Returns non-nil channel
// if the fast path not available.
func (tf *schemaFeed) tryWaitForTS(ts hlc.Timestamp) (chan error, error) {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if !tf.mu.errTS.IsEmpty() && tf.mu.errTS.LessEq(ts) {
		// Schema feed error occurred.
		return nil, tf.mu.err
	}

	if ts.LessEq(tf.mu.highWater) {
		return nil, nil // Fast path.
	}

	// non-fastPath is when we need to prove the invariant holds from [`high_water`, `ts].
	waitCh := make(chan error, 1)
	tf.mu.waiters = append(tf.mu.waiters, tableHistoryWaiter{ts: ts, errCh: waitCh})
	return waitCh, nil
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
// function and adjusts the high-water or error timestamp appropriately. It is
// required that the descriptors represent a transactional kv read between the
// two given timestamps.
//
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

// adjustTimestamps adjusts the high-water or error timestamp appropriately.
func (tf *schemaFeed) adjustTimestamps(startTS, endTS hlc.Timestamp, validateErr error) error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if validateErr != nil {
		// don't care about startTS in the invalid case
		if tf.mu.errTS.IsEmpty() || endTS.Less(tf.mu.errTS) {
			tf.mu.errTS = endTS
			tf.mu.err = validateErr
			newWaiters := make([]tableHistoryWaiter, 0, len(tf.mu.waiters))
			for _, w := range tf.mu.waiters {
				if w.ts.Less(tf.mu.errTS) {
					newWaiters = append(newWaiters, w)
					continue
				}
				w.errCh <- validateErr
			}
			tf.mu.waiters = newWaiters
		}
		return validateErr
	}

	if tf.mu.highWater.Less(startTS) {
		return errors.Errorf(`gap between %s and %s`, tf.mu.highWater, startTS)
	}
	if tf.mu.highWater.Less(endTS) {
		tf.mu.highWater = endTS
		newWaiters := make([]tableHistoryWaiter, 0, len(tf.mu.waiters))
		for _, w := range tf.mu.waiters {
			if tf.mu.highWater.Less(w.ts) {
				newWaiters = append(newWaiters, w)
				continue
			}
			w.errCh <- nil
		}
		tf.mu.waiters = newWaiters
	}
	return nil
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
