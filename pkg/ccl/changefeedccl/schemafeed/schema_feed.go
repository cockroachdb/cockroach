// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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
	targets []jobspb.ChangefeedTargetSpecification,
	initialHighwater hlc.Timestamp,
	metrics *Metrics,
	changefeedOpts map[string]string,
) SchemaFeed {
	m := &schemaFeed{
		filter:            schemaChangeEventFilters[events],
		db:                cfg.DB,
		clock:             cfg.DB.Clock(),
		settings:          cfg.Settings,
		targets:           targets,
		leaseMgr:          cfg.LeaseManager.(*lease.Manager),
		ie:                cfg.SessionBoundInternalExecutorFactory(ctx, &sessiondata.SessionData{}),
		collectionFactory: cfg.CollectionFactory,
		metrics:           metrics,
		changefeedOpts:    changefeedOpts,
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
	filter         tableEventFilter
	db             *kv.DB
	clock          *hlc.Clock
	settings       *cluster.Settings
	targets        []jobspb.ChangefeedTargetSpecification
	ie             sqlutil.InternalExecutor
	metrics        *Metrics
	changefeedOpts map[string]string

	// TODO(ajwerner): Should this live underneath the FilterFunc?
	// Should there be another function to decide whether to update the
	// lease manager?
	leaseMgr          *lease.Manager
	collectionFactory *descs.CollectionFactory

	mu struct {
		syncutil.Mutex

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

func (t *typeDependencyTracker) purgeTable(tbl catalog.TableDescriptor) error {
	for _, col := range tbl.UserDefinedTypeColumns() {
		id, err := typedesc.UserDefinedTypeOIDToID(col.GetType().Oid())
		if err != nil {
			return err
		}
		t.removeDependency(id, tbl.GetID())
	}

	return nil
}

func (t *typeDependencyTracker) ingestTable(tbl catalog.TableDescriptor) error {
	for _, col := range tbl.UserDefinedTypeColumns() {
		id, err := typedesc.UserDefinedTypeOIDToID(col.GetType().Oid())
		if err != nil {
			return err
		}
		t.addDependency(id, tbl.GetID())
	}
	return nil
}

func (t *typeDependencyTracker) containsType(id descpb.ID) bool {
	_, ok := t.deps[id]
	return ok
}

type tableHistoryWaiter struct {
	ts    hlc.Timestamp
	errCh chan error
}

func (tf *schemaFeed) markStarted() error {
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
	if err := tf.markStarted(); err != nil {
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
	return tf.pollTableHistory(ctx)
}

func (tf *schemaFeed) primeInitialTableDescs(ctx context.Context) error {
	tf.mu.Lock()
	initialTableDescTs := tf.mu.highWater
	tf.mu.Unlock()
	var initialDescs []catalog.Descriptor
	initialTableDescsFn := func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		initialDescs = initialDescs[:0]
		if err := txn.SetFixedTimestamp(ctx, initialTableDescTs); err != nil {
			return err
		}
		// Note that all targets are currently guaranteed to be tables.
		for _, table := range tf.targets {
			flags := tree.ObjectLookupFlagsWithRequired()
			flags.AvoidLeased = true
			tableDesc, err := descriptors.GetImmutableTableByID(ctx, txn, table.TableID, flags)
			if err != nil {
				return err
			}
			initialDescs = append(initialDescs, tableDesc)
		}
		return nil
	}

	if err := tf.collectionFactory.Txn(
		ctx, tf.ie, tf.db, initialTableDescsFn,
	); err != nil {
		return err
	}

	tf.mu.Lock()
	// Register all types used by the initial set of tables.
	for _, desc := range initialDescs {
		tbl := desc.(catalog.TableDescriptor)
		if err := tf.mu.typeDeps.ingestTable(tbl); err != nil {
			tf.mu.Unlock()
			return err
		}
	}
	tf.mu.Unlock()

	return tf.ingestDescriptors(ctx, hlc.Timestamp{}, initialTableDescTs, initialDescs, tf.validateDescriptor)
}

func (tf *schemaFeed) pollTableHistory(ctx context.Context) error {
	for {
		if err := tf.updateTableHistory(ctx, tf.clock.Now()); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(changefeedbase.TableDescriptorPollInterval.Get(&tf.settings.SV)):
		}
	}
}

func (tf *schemaFeed) updateTableHistory(ctx context.Context, endTS hlc.Timestamp) error {
	startTS := tf.highWater()
	if endTS.LessEq(startTS) {
		return nil
	}
	descs, err := tf.fetchDescriptorVersions(ctx, tf.leaseMgr.Codec(), tf.db, startTS, endTS)
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

// highWater returns the current high-water timestamp.
func (tf *schemaFeed) highWater() hlc.Timestamp {
	tf.mu.Lock()
	highWater := tf.mu.highWater
	tf.mu.Unlock()
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
	var errCh chan error

	tf.mu.Lock()
	highWater := tf.mu.highWater
	var err error
	if !tf.mu.errTS.IsEmpty() && tf.mu.errTS.LessEq(ts) {
		err = tf.mu.err
	}
	fastPath := err != nil || ts.LessEq(highWater)
	if !fastPath {
		errCh = make(chan error, 1)
		tf.mu.waiters = append(tf.mu.waiters, tableHistoryWaiter{ts: ts, errCh: errCh})
	}
	tf.mu.Unlock()
	if fastPath {
		if log.V(1) {
			log.Infof(ctx, "fastpath for %s: %v", ts, err)
		}
		return err
	}

	if log.V(1) {
		log.Infof(ctx, "waiting for %s highwater", ts)
	}
	start := timeutil.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if log.V(1) {
			log.Infof(ctx, "waited %s for %s highwater: %v", timeutil.Since(start), ts, err)
		}
		if tf.metrics != nil {
			tf.metrics.TableMetadataNanos.Inc(timeutil.Since(start).Nanoseconds())
		}
		return err
	}
}

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
		return tf.leaseMgr.AcquireFreshestFromStore(ctx, desc.GetID())
	case catalog.TableDescriptor:
		if err := changefeedbase.ValidateTable(tf.targets, desc, tf.changefeedOpts); err != nil {
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
			if err := tf.mu.typeDeps.purgeTable(lastVersion); err != nil {
				return err
			}

			e := TableEvent{
				Before: lastVersion,
				After:  desc,
			}
			shouldFilter, err := tf.filter.shouldFilter(ctx, e)
			log.VEventf(ctx, 1, "validate shouldFilter %v %v", formatEvent(e), shouldFilter)
			if err != nil {
				return err
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
		if err := tf.mu.typeDeps.ingestTable(desc); err != nil {
			return err
		}
		tf.mu.previousTableVersion[desc.GetID()] = desc
		return nil
	default:
		return errors.AssertionFailedf("unexpected descriptor type %T", desc)
	}
}

func (tf *schemaFeed) fetchDescriptorVersions(
	ctx context.Context, codec keys.SQLCodec, db *kv.DB, startTS, endTS hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	if log.V(2) {
		log.Infof(ctx, `fetching table descs (%s,%s]`, startTS, endTS)
	}
	start := timeutil.Now()
	span := roachpb.Span{Key: codec.TablePrefix(keys.DescriptorTableID)}
	span.EndKey = span.Key.PrefixEnd()
	header := roachpb.Header{Timestamp: endTS}
	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
		StartTime:     startTS,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
	}
	res, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
	if log.V(2) {
		log.Infof(ctx, `fetched table descs (%s,%s] took %s`, startTS, endTS, timeutil.Since(start))
	}
	if pErr != nil {
		err := pErr.GoError()
		return nil, errors.Wrapf(err, `fetching changes for %s`, span)
	}

	tf.mu.Lock()
	defer tf.mu.Unlock()

	var descriptors []catalog.Descriptor
	for _, file := range res.(*roachpb.ExportResponse).Files {
		if err := func() error {
			it, err := storage.NewMemSSTIterator(file.SST, false /* verify */)
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
				var origName string
				var isTable bool
				for _, cts := range tf.targets {
					if cts.TableID == descpb.ID(id) {
						origName = cts.StatementTimeName
						isTable = true
						break
					}
				}
				isType := tf.mu.typeDeps.containsType(descpb.ID(id))
				// Check if the descriptor is an interesting table or type.
				if !(isTable || isType) {
					// Uninteresting descriptor.
					continue
				}

				unsafeValue := it.UnsafeValue()
				if unsafeValue == nil {
					name := origName
					if name == "" {
						name = fmt.Sprintf("desc(%d)", id)
					}
					return errors.Errorf(`"%v" was dropped or truncated`, name)
				}

				// Unmarshal the descriptor.
				value := roachpb.Value{RawBytes: unsafeValue}
				var desc descpb.Descriptor
				if err := value.GetProto(&desc); err != nil {
					return err
				}

				b := descbuilder.NewBuilderWithMVCCTimestamp(&desc, k.Timestamp)
				if b != nil && (b.DescriptorType() == catalog.Table || b.DescriptorType() == catalog.Type) {
					descriptors = append(descriptors, b.BuildImmutable())
				}
			}
		}(); err != nil {
			return nil, err
		}
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
