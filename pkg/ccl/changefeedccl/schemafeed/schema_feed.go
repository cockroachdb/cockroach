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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	Before, After *sqlbase.TableDescriptor
}

// Timestamp refers to the ModificationTime of the After table descriptor.
func (e TableEvent) Timestamp() hlc.Timestamp {
	return e.After.ModificationTime
}

// Config configures a SchemaFeed.
type Config struct {
	DB       *kv.DB
	Clock    *hlc.Clock
	Settings *cluster.Settings
	Targets  jobspb.ChangefeedTargets

	// SchemaChangeEvents controls the class of events which are emitted by this
	// SchemaFeed.
	SchemaChangeEvents changefeedbase.SchemaChangeEventClass

	// InitialHighWater is the timestamp after which events should occur.
	//
	// NB: When clients want to create a changefeed which has a resolved timestamp
	// of ts1, they care about write which occur at ts1.Next() and later but they
	// should scan the tables as of ts1. This is important so that writes which
	// change the table at ts1.Next() are emitted as an event.
	InitialHighWater hlc.Timestamp

	// LeaseManager is used to ensure that when an event is emitted that at a higher
	// level it is ensured that the right table descriptor will be used for the
	// event if this lease manager is used.
	//
	// TODO(ajwerner): Should this live underneath the FilterFunc?
	// Should there be another function to decide whether to update the
	// lease manager?
	LeaseManager *lease.Manager
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
type SchemaFeed struct {
	filter   tableEventFilter
	db       *kv.DB
	clock    *hlc.Clock
	settings *cluster.Settings
	targets  jobspb.ChangefeedTargets
	leaseMgr *lease.Manager
	mu       struct {
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
		// when a backilling mutation has successfully completed - this can only
		// be determining by comparing a version to the previous version.
		previousTableVersion map[sqlbase.ID]*sqlbase.TableDescriptor
	}
}

type tableHistoryWaiter struct {
	ts    hlc.Timestamp
	errCh chan error
}

// New creates SchemaFeed with the given Config.
func New(cfg Config) *SchemaFeed {
	// TODO(ajwerner): validate config.
	m := &SchemaFeed{
		filter:   schemaChangeEventFilters[cfg.SchemaChangeEvents],
		db:       cfg.DB,
		clock:    cfg.Clock,
		settings: cfg.Settings,
		targets:  cfg.Targets,
		leaseMgr: cfg.LeaseManager,
	}
	m.mu.previousTableVersion = make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	m.mu.highWater = cfg.InitialHighWater
	return m
}

func (tf *SchemaFeed) markStarted() error {
	tf.mu.Lock()
	defer tf.mu.Unlock()
	if tf.mu.started {
		return errors.AssertionFailedf("SchemaFeed started more than once")
	}
	tf.mu.started = true
	return nil
}

// Run will run the SchemaFeed. It is an error to run a feed more than once.
func (tf *SchemaFeed) Run(ctx context.Context) error {
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

func (tf *SchemaFeed) primeInitialTableDescs(ctx context.Context) error {
	tf.mu.Lock()
	initialTableDescTs := tf.mu.highWater
	tf.mu.Unlock()
	var initialDescs []*sqlbase.TableDescriptor
	initialTableDescsFn := func(ctx context.Context, txn *kv.Txn) error {
		initialDescs = initialDescs[:0]
		txn.SetFixedTimestamp(ctx, initialTableDescTs)
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range tf.targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, keys.SystemSQLCodec, tableID)
			if err != nil {
				return err
			}
			initialDescs = append(initialDescs, tableDesc)
		}
		return nil
	}
	if err := tf.db.Txn(ctx, initialTableDescsFn); err != nil {
		return err
	}
	return tf.ingestDescriptors(ctx, hlc.Timestamp{}, initialTableDescTs, initialDescs, tf.validateTable)
}

func (tf *SchemaFeed) pollTableHistory(ctx context.Context) error {
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

func (tf *SchemaFeed) updateTableHistory(ctx context.Context, endTS hlc.Timestamp) error {
	startTS := tf.highWater()
	if endTS.LessEq(startTS) {
		return nil
	}
	descs, err := fetchTableDescriptorVersions(ctx, tf.db, startTS, endTS, tf.targets)
	if err != nil {
		return err
	}
	return tf.ingestDescriptors(ctx, startTS, endTS, descs, tf.validateTable)
}

// Peek returns all events which have not been popped which happen at or
// before the passed timestamp.
func (tf *SchemaFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []TableEvent, err error) {

	return tf.peekOrPop(ctx, atOrBefore, false /* pop */)
}

// Pop pops events from the EventQueue.
func (tf *SchemaFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []TableEvent, err error) {
	return tf.peekOrPop(ctx, atOrBefore, true /* pop */)
}

func (tf *SchemaFeed) peekOrPop(
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
func (tf *SchemaFeed) highWater() hlc.Timestamp {
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
func (tf *SchemaFeed) waitForTS(ctx context.Context, ts hlc.Timestamp) error {
	var errCh chan error

	tf.mu.Lock()
	highWater := tf.mu.highWater
	var err error
	if tf.mu.errTS != (hlc.Timestamp{}) && tf.mu.errTS.LessEq(ts) {
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
		return err
	}
}

func descLess(a, b *sqlbase.TableDescriptor) bool {
	if a.ModificationTime.Equal(b.ModificationTime) {
		return a.ID < b.ID
	}
	return a.ModificationTime.Less(b.ModificationTime)
}

// ingestDescriptors checks the given descriptors against the invariant check
// function and adjusts the high-water or error timestamp appropriately. It is
// required that the descriptors represent a transactional kv read between the
// two given timestamps.
//
// validateFn is exposed for testing, in production it is tf.validateTable.
func (tf *SchemaFeed) ingestDescriptors(
	ctx context.Context,
	startTS, endTS hlc.Timestamp,
	descs []*sqlbase.TableDescriptor,
	validateFn func(ctx context.Context, desc *sqlbase.TableDescriptor) error,
) error {
	sort.Slice(descs, func(i, j int) bool { return descLess(descs[i], descs[j]) })
	var validateErr error
	for _, desc := range descs {
		if err := validateFn(ctx, desc); validateErr == nil {
			validateErr = err
		}
	}
	return tf.adjustTimestamps(startTS, endTS, validateErr)
}

// adjustTimestamps adjusts the high-water or error timestamp appropriately.
func (tf *SchemaFeed) adjustTimestamps(startTS, endTS hlc.Timestamp, validateErr error) error {
	tf.mu.Lock()
	defer tf.mu.Unlock()

	if validateErr != nil {
		// don't care about startTS in the invalid case
		if tf.mu.errTS == (hlc.Timestamp{}) || endTS.Less(tf.mu.errTS) {
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

func formatDesc(desc *sqlbase.TableDescriptor) string {
	return fmt.Sprintf("%d:%d@%v", desc.ID, desc.Version, desc.ModificationTime)
}

func formatEvent(e TableEvent) string {
	return fmt.Sprintf("%v->%v", formatDesc(e.Before), formatDesc(e.After))
}

func (tf *SchemaFeed) validateTable(ctx context.Context, desc *sqlbase.TableDescriptor) error {
	if err := changefeedbase.ValidateTable(tf.targets, desc); err != nil {
		return err
	}
	tf.mu.Lock()
	defer tf.mu.Unlock()
	log.Infof(ctx, "validate %v", formatDesc(desc))
	if lastVersion, ok := tf.mu.previousTableVersion[desc.ID]; ok {
		// NB: Writes can occur to a table
		if desc.ModificationTime.LessEq(lastVersion.ModificationTime) {
			return nil
		}

		// To avoid race conditions with the lease manager, at this point we force
		// the manager to acquire the freshest descriptor of this table from the
		// store. In normal operation, the lease manager returns the newest
		// descriptor it knows about for the timestamp, assuming it's still
		// allowed; without this explicit load, the lease manager might therefore
		// return the previous version of the table, which is still technically
		// allowed by the schema change system.
		if err := tf.leaseMgr.AcquireFreshestFromStore(ctx, desc.ID); err != nil {
			return err
		}

		e := TableEvent{
			Before: lastVersion,
			After:  desc,
		}
		shouldFilter, err := tf.filter.shouldFilter(ctx, e)
		log.Infof(ctx, "validate shouldFilter %v %v", formatEvent(e), shouldFilter)
		if err != nil {
			return err
		}
		if !shouldFilter {
			tf.mu.events = append(tf.mu.events, e)
			sort.Slice(tf.mu.events, func(i, j int) bool {
				return descLess(tf.mu.events[i].After, tf.mu.events[j].After)
			})
		}
	}
	tf.mu.previousTableVersion[desc.ID] = desc
	return nil
}

func fetchTableDescriptorVersions(
	ctx context.Context, db *kv.DB, startTS, endTS hlc.Timestamp, targets jobspb.ChangefeedTargets,
) ([]*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, `fetching table descs (%s,%s]`, startTS, endTS)
	}
	start := timeutil.Now()
	span := roachpb.Span{Key: keys.TODOSQLCodec.TablePrefix(keys.DescriptorTableID)}
	span.EndKey = span.Key.PrefixEnd()
	header := roachpb.Header{Timestamp: endTS}
	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
		StartTime:     startTS,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
		OmitChecksum:  true,
	}
	res, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
	if log.V(2) {
		log.Infof(ctx, `fetched table descs (%s,%s] took %s`, startTS, endTS, timeutil.Since(start))
	}
	if pErr != nil {
		err := pErr.GoError()
		return nil, errors.Wrapf(err, `fetching changes for %s`, span)
	}

	var tableDescs []*sqlbase.TableDescriptor
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
				remaining, _, _, err := keys.TODOSQLCodec.DecodeIndexPrefix(k.Key)
				if err != nil {
					return err
				}
				_, tableID, err := encoding.DecodeUvarintAscending(remaining)
				if err != nil {
					return err
				}
				origName, ok := targets[sqlbase.ID(tableID)]
				if !ok {
					// Uninteresting table.
					continue
				}
				unsafeValue := it.UnsafeValue()
				if unsafeValue == nil {
					return errors.Errorf(`"%v" was dropped or truncated`, origName)
				}
				value := roachpb.Value{RawBytes: unsafeValue}
				var desc sqlbase.Descriptor
				if err := value.GetProto(&desc); err != nil {
					return err
				}
				if tableDesc := desc.Table(k.Timestamp); tableDesc != nil {
					tableDescs = append(tableDescs, tableDesc)
				}
			}
		}(); err != nil {
			return nil, err
		}
	}
	return tableDescs, nil
}
