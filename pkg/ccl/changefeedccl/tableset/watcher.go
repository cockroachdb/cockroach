// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tableset implements a tableset watcher.
package tableset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/sync/errgroup"
)

// Filter is a filter for a tableset.
type Filter struct {
	// DatabaseID is the ID of the database to watch. It is mandatory.
	DatabaseID  descpb.ID
	TableFilter jobspb.FilterList
}

func NewFilterFromChangefeedDetails(details jobspb.ChangefeedDetails) (Filter, error) {
	if len(details.TargetSpecifications) != 1 {
		return Filter{}, errors.AssertionFailedf("expected one database target specification")
	}
	spec := details.TargetSpecifications[0]
	dbDescID := spec.DescID
	if dbDescID == 0 {
		return Filter{}, errors.AssertionFailedf("database descriptor ID is 0")
	}
	filterList := spec.FilterList
	if filterList == nil {
		return Filter{}, errors.AssertionFailedf("filter list is nil")
	}

	return Filter{DatabaseID: dbDescID, TableFilter: *filterList}, nil
}

func (f Filter) String() string {
	return fmt.Sprintf("Filter{database_id=%d, table_filter=%v}", f.DatabaseID, f.TableFilter)
}

func (f Filter) includes(tableInfo Table) bool {
	if tableInfo.ParentID != f.DatabaseID {
		return false
	}
	return f.TableFilter.Matches(tableInfo.FQName)
}

type Table struct {
	descpb.NameInfo
	ID     descpb.ID
	FQName string
}

func (t Table) size() int64 {
	return int64(t.NameInfo.Size()) + 8
}

func (t Table) String() string {
	return fmt.Sprintf("Table{parent_id=%d, parent_schema_id=%d, name=%s, id=%d}",
		t.NameInfo.ParentID, t.NameInfo.ParentSchemaID, t.NameInfo.Name, t.ID)
}

type TableAdd struct {
	Table Table
	AsOf  hlc.Timestamp
}

func (d TableAdd) size() int64 {
	return d.Table.size() + int64(d.AsOf.Size())
}

func (d TableAdd) String() string {
	return fmt.Sprintf("TableAdd{Table: %s, AsOf: %s}", d.Table.String(), d.AsOf.String())
}

func compareTableDiffsByTS(a, b TableAdd) int {
	return a.AsOf.Compare(b.AsOf)
}

type TableAdds []TableAdd

func (d TableAdds) Frontier() hlc.Timestamp {
	if len(d) == 0 {
		return hlc.Timestamp{}
	}
	return d[0].AsOf
}

// Watcher watches a tableset and buffers table diffs. It will notify waiters
// when the resolved timestamp is advanced.
type Watcher struct {
	filter  Filter
	dbName  tree.Name
	id      int64
	execCfg *sql.ExecutorConfig
	mon     *mon.BytesMonitor
	acc     mon.BoundAccount
	started bool

	// exitedWithErr is a pointer to the error that caused the watcher to exit.
	// If the *pointer* is nil, the watcher is still alive. If it is not nil,
	// the watcher has exited with that error, which as of writing may not be
	// nil.
	exitedWithErr atomic.Pointer[error]

	// lastPoppedFrontier is the last frontier timestamp that was passed to
	// PopUnchangedUpTo(). It's used to detect invalid uses of
	// PopUnchangedUpTo(). It's not perfect but it's better than nothing.
	lastPoppedFrontier hlc.Timestamp

	mu struct {
		syncutil.Mutex
		tableAdds []TableAdd
		resolved  hlc.Timestamp
		// NOTE: only one waiter per timestamp is supported.
		// TODO: in reality we only need to support one waiter period.
		resolvedWaiters map[hlc.Timestamp]chan struct{}
	}

	// schemaNameCache is a cache of schema names by ID.
	schemaNameCache struct {
		syncutil.Mutex
		*cache.UnorderedCache
	}
}

// NewWatcher creates a new watcher.
func NewWatcher(
	ctx context.Context, filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id int64,
) (*Watcher, error) {
	// Resolve database name.
	var dbName tree.Name
	err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, filter.DatabaseID)
		if err != nil {
			return err
		}
		dbName = tree.Name(dbDesc.GetName())
		return nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "getting database name for filter %s", filter)
	}
	w := &Watcher{
		filter:  filter,
		dbName:  dbName,
		execCfg: execCfg,
		mon:     mon,
		acc:     mon.MakeBoundAccount(), // Closed at the end of Start()
		id:      id,
	}
	w.mu.resolvedWaiters = make(map[hlc.Timestamp]chan struct{})
	return w, nil
}

// Start starts the watcher. It will not return until the watcher is shut down
// due to an error or context cancellation. It may only be run once.
func (w *Watcher) Start(ctx context.Context, initialTS hlc.Timestamp) (retErr error) {
	if w.started {
		return errors.AssertionFailedf("watcher already started")
	}
	w.started = true
	defer func() {
		w.exitedWithErr.Store(&retErr)
	}()

	ctx = logtags.AddTag(ctx, "tableset.watcher.id", fmt.Sprintf("%d", w.id))
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	const cacheSize = 1024 * 1024
	if err := w.acc.Grow(ctx, int64(cacheSize)); err != nil {
		return errors.Wrapf(err, "failed to allocate %d bytes for schema name cache", cacheSize)
	}
	w.schemaNameCache.UnorderedCache = cache.NewUnorderedCache(cache.Config{
		Policy:      cache.CacheLRU,
		ShouldEvict: func(size int, _ any, _ any) bool { return size > cacheSize },
	})
	defer w.acc.Close(ctx)

	log.Changefeed.Infof(ctx, "starting watcher with filter=%s, initialTS=%s", w.filter, initialTS)

	errCh := make(chan error, 1)
	setErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
	}

	var cfTargets changefeedbase.Targets
	cfTargets.Add(changefeedbase.Target{
		DescID:            systemschema.NamespaceTable.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(systemschema.NamespaceTable.GetName()),
		Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
	})
	dec, err := cdcevent.NewEventDecoder(ctx, w.execCfg, cfTargets, false, false)
	if err != nil {
		return err
	}

	// Buffering goroutine.
	incomingTableAdds := make(chan TableAdd)
	incomingResolveds := make(chan hlc.Timestamp)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		ctx, sp := tracing.ChildSpan(egCtx, "tableset.watcher.buffer-loop")
		defer sp.Finish()

		// Don't set this to initialTS because we're likely to do a catchup scan which may emit older resolveds.
		var curResolved hlc.Timestamp

		bufferedTableDiffs := make(map[hlc.Timestamp]map[TableAdd]struct{})
		for {
			select {
			// Buffer incoming tables between resolved messages.
			case diff := <-incomingTableAdds:
				if diff.AsOf.Less(curResolved) {
					// TODO: it's unclear to me if this is possible/expected. If it is, allow it.
					return errors.AssertionFailedf("diff %s is before current resolved %s", diff, curResolved)
				}
				if diff.AsOf.Less(initialTS) {
					if log.V(2) {
						log.Changefeed.Infof(ctx, "diff %s is before initialTS %s; skipping", diff, initialTS)
					}
					continue
				}

				if _, ok := bufferedTableDiffs[curResolved]; !ok {
					bufferedTableDiffs[curResolved] = make(map[TableAdd]struct{})
				}

				// mem accounting
				if err := w.acc.Grow(ctx, diff.size()); err != nil {
					return errors.Wrapf(err, "failed to allocated %d bytes from monitor", diff.size())
				}

				bufferedTableDiffs[curResolved][diff] = struct{}{}
			// Flush buffered tables when we receive a resolved message.
			case resolved := <-incomingResolveds:
				if log.V(2) {
					log.Changefeed.Infof(ctx, "resolved: %s; %d tables buffered", resolved, len(bufferedTableDiffs[curResolved]))
				}

				if resolved.Less(curResolved) {
					return errors.AssertionFailedf("resolved %s is less than current resolved %s", resolved, curResolved)
				}
				// The diffs/tables are already deduped, so we just need to sort them by timestamp.
				// Using a btree could be more efficient for some use cases, but this is probably faster for the low volume we expect, and simpler.
				diffs := slices.SortedFunc(maps.Keys(bufferedTableDiffs[curResolved]), compareTableDiffsByTS)

				if err := func() error {
					w.mu.Lock()
					defer w.mu.Unlock()
					w.mu.tableAdds = append(w.mu.tableAdds, diffs...)
					// TODO: may be worth using a btree here also to avoid the sort-dedupe
					w.mu.tableAdds = dedupeAndSortDiffs(w.mu.tableAdds)

					// Update resolved and notify waiters.
					return w.updateResolvedLocked(ctx, resolved)
				}(); err != nil {
					return err
				}

				delete(bufferedTableDiffs, curResolved)

				curResolved = resolved
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Rangefeed setup.
	onValue := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		setErr(func() error {
			var table Table
			var err error
			var ok bool

			if kv.Value.IsPresent() {
				table, ok, err = w.kvToTable(ctx, roachpb.KeyValue{Key: kv.Key, Value: kv.Value}, dec)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}

			if log.V(2) {
				log.Changefeed.Infof(ctx, "onValue: %s", table)
			}

			tableIncluded := w.filter.includes(table)
			if !tableIncluded {
				return nil
			}
			add := TableAdd{Table: table, AsOf: kv.Value.Timestamp}
			select {
			case incomingTableAdds <- add:
			case <-egCtx.Done():
				return egCtx.Err()
			}
			return nil
		}())
	}

	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("tableset.watcher", fmt.Sprintf("id=%d", w.id)),
		rangefeed.WithMemoryMonitor(w.mon),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			if log.V(2) {
				log.Changefeed.Infof(ctx, "onCheckpoint: chk=%s", checkpoint)
			}
			// This can happen when done catching up; ignore it.
			if checkpoint.ResolvedTS.IsEmpty() {
				return
			}
			select {
			case incomingResolveds <- checkpoint.ResolvedTS:
			case <-egCtx.Done():
			}
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) { setErr(err) }),
		rangefeed.WithDiff(true),
		// We make the id negative so that it's not a valid job id.
		// TODO: a more elegant solution
		rangefeed.WithConsumerID(mkConsumerID(w.id)),
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
	}

	// Start rangefeed.
	rf := w.execCfg.RangeFeedFactory.New(
		fmt.Sprintf("tableset.watcher.id=%v", w.id), initialTS, onValue, opts...,
	)
	defer rf.Close()

	watchSpans := roachpb.Spans{systemschema.NamespaceTable.TableSpan(w.execCfg.Codec)}
	frontier, err := span.MakeFrontier(watchSpans...)
	if err != nil {
		return err
	}
	for _, span := range watchSpans {
		if _, err := frontier.Forward(span, initialTS); err != nil {
			return err
		}
	}
	frontier = span.MakeConcurrentFrontier(frontier)
	defer frontier.Release()

	if err := rf.StartFromFrontier(ctx, frontier); err != nil {
		return err
	}

	log.Changefeed.Infof(ctx, "rangefeed started")

	// Wait for shutdown due to error or context cancellation.
	select {
	case err := <-errCh:
		cancel(err)
		err = errors.Join(err, eg.Wait())
		log.Changefeed.Warningf(ctx, "watcher %d shutting down due to error: %v (%s)", w.id, err, context.Cause(ctx))
		return err
	case <-ctx.Done():
		err = errors.Join(ctx.Err(), eg.Wait())
		log.Changefeed.Warningf(ctx, "watcher %d shutting down due to context cancellation: %v (%s)", w.id, err, context.Cause(ctx))
		return err
	}
}

var errRepeatedPopCall = errors.AssertionFailedf(
	"PopUpTo called with non-advancing timestamp: upTo is less than or equal to lastPoppedFrontier; this indicates a bug in the caller",
)

// TODO(#156780): return an error if the database is dropped, to alert the caller that we're never going to get any more diffs.

// PopUpTo returns true added tables between when it was last called (or the
// initialTS) and the given timestamp [inclusive, exclusive). It discards data
// older than the last call to it
func (w *Watcher) PopUpTo(ctx context.Context, upTo hlc.Timestamp) (added TableAdds, err error) {
	if errPtr := w.exitedWithErr.Load(); errPtr != nil {
		return nil, errors.Wrapf(*errPtr, "watcher %d has exited with error", w.id)
	}

	// Check that the frontier is advancing.
	if upTo.Compare(w.lastPoppedFrontier) <= 0 {
		return nil, errors.Wrapf(errRepeatedPopCall, "upTo=%s, lastPoppedFrontier=%s", upTo, w.lastPoppedFrontier)
	}
	w.lastPoppedFrontier = upTo

	// TODO: we technically dont need to wait for resolved here if there already
	// are diffs buffered > upTo. But this seems tricky to implement rn.

	added, err = w.popAddedTablesUpTo(ctx, upTo)
	if err != nil {
		return nil, err
	}
	return added, nil
}

func (w *Watcher) popAddedTablesUpTo(ctx context.Context, upTo hlc.Timestamp) (TableAdds, error) {
	if err := w.maybeWaitForResolved(ctx, upTo); err != nil {
		return nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if buildutil.CrdbTestBuild {
		sorted := slices.IsSortedFunc(w.mu.tableAdds, func(a, b TableAdd) int {
			return a.AsOf.Compare(b.AsOf)
		})
		if !sorted {
			return nil, errors.AssertionFailedf("adds are not sorted")
		}
	}

	upToIdx, _ := slices.BinarySearchFunc(w.mu.tableAdds, upTo, func(add TableAdd, ts hlc.Timestamp) int {
		return add.AsOf.Compare(ts)
	})

	adds := w.mu.tableAdds[:upToIdx]
	w.mu.tableAdds = w.mu.tableAdds[upToIdx:]

	// Mem accounting.
	sz := int64(0)
	for _, add := range adds {
		sz += add.size()
	}
	w.acc.Shrink(ctx, sz)

	return slices.Clone(adds), nil
}

// maybeWaitForResolved waits for the resolved timestamp to be greater than or
// equal to the given timestamp. If the resolved timestamp is already greater
// than or equal to the given timestamp, it returns immediately.
func (w *Watcher) maybeWaitForResolved(ctx context.Context, ts hlc.Timestamp) error {
	start := crtime.NowMono()
	if log.V(2) {
		log.Changefeed.Infof(ctx, "maybeWaitForResolved(%s) start", ts)
	}

	maybeWaiter := func() chan struct{} {
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.mu.resolved.Compare(ts) >= 0 {
			if log.V(2) {
				log.Changefeed.Infof(ctx, "maybeWaitForResolved(%s) already resolved", ts)
			}
			return nil
		}
		return w.addWaiterLocked(ts)
	}()

	if maybeWaiter == nil {
		return nil
	}

	select {
	case <-maybeWaiter:
		if log.V(2) {
			log.Changefeed.Infof(ctx, "maybeWaitForResolved(%s) done waiting in %s", ts, start.Elapsed())
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Watcher) updateResolvedLocked(ctx context.Context, ts hlc.Timestamp) error {
	if ts.Compare(w.mu.resolved) < 0 {
		return errors.AssertionFailedf("resolved %s is less than current resolved %s", ts, w.mu.resolved)
	}

	// The lag seems to be about 3s, coinciding with the default value of kv.closed_timestamp.target_duration.
	// This is probably fine since the changefeed data will have the same lag anyway.
	if log.V(2) {
		log.Changefeed.Infof(ctx, "updateResolved@%d %s -> %s", timeutil.Now().Unix(), w.mu.resolved, ts)
	}
	w.mu.resolved = ts
	for wts, waiter := range w.mu.resolvedWaiters {
		if wts.Compare(ts) <= 0 {
			delete(w.mu.resolvedWaiters, wts)
			close(waiter)
		}
	}
	return nil
}

func (w *Watcher) addWaiterLocked(ts hlc.Timestamp) chan struct{} {
	waiter := make(chan struct{})
	w.mu.resolvedWaiters[ts] = waiter
	return waiter
}

func (w *Watcher) kvToTable(
	ctx context.Context, kv roachpb.KeyValue, dec cdcevent.Decoder,
) (_ Table, ok bool, _ error) {
	if log.V(2) {
		log.Changefeed.Infof(ctx, "decoding %s@%s", kv.Key, kv.Value.Timestamp)
	}

	row, err := dec.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return Table{}, false, err
	}

	if log.V(2) {
		log.Changefeed.Infof(ctx, "row: %s", row.DebugString())
	}

	if log.V(2) {
		log.Changefeed.Infof(ctx, "row: %s", row.DebugString())
	}

	var tableID descpb.ID
	idCol, err := row.DatumNamed("id")
	if err != nil {
		return Table{}, false, err
	}
	err = idCol.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		tableID = descpb.ID(tree.MustBeDInt(d))
		return nil
	})
	if err != nil {
		return Table{}, false, err
	}

	nameInfo, err := catalogkeys.DecodeNameMetadataKey(w.execCfg.Codec, kv.Key)
	if err != nil {
		return Table{}, false, err
	}

	// Tables always have a parent ID and schema ID. If this doesn't, it's not a table.
	if nameInfo.ParentID == 0 || nameInfo.ParentSchemaID == 0 {
		return Table{}, false, nil
	}

	schemaName, err := w.getSchemaName(ctx, nameInfo.ParentSchemaID)
	if err != nil {
		// Ignore errors from dropped schemas.
		if errors.Is(err, catalog.ErrDescriptorNotFound) || errors.Is(err, catalog.ErrDescriptorDropped) {
			return Table{}, false, nil
		}
		return Table{}, false, errors.Wrapf(err, "getting schema name for table %+#v", nameInfo)
	}

	fqn := tree.NewTableNameWithSchema(w.dbName, schemaName, tree.Name(nameInfo.Name))
	table := Table{
		NameInfo: nameInfo,
		ID:       tableID,
		FQName:   fqn.FQString(),
	}

	return table, true, nil
}

func (w *Watcher) getSchemaName(ctx context.Context, schemaID descpb.ID) (tree.Name, error) {
	w.schemaNameCache.Lock()
	defer w.schemaNameCache.Unlock()

	if schemaName, ok := w.schemaNameCache.Get(schemaID); ok {
		return schemaName.(tree.Name), nil
	}

	var schemaName tree.Name
	err := w.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		schemaDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, schemaID)
		if err != nil {
			return err
		}
		schemaName = tree.Name(schemaDesc.GetName())
		return nil
	})
	if err != nil {
		return "", errors.Wrapf(err, "getting schema name with id %d", schemaID)
	}
	w.schemaNameCache.Add(schemaID, schemaName)
	return schemaName, nil
}

func mkConsumerID(id int64) int64 {
	if id > 0 {
		return -id
	}
	return id
}

func dedupeAndSortDiffs(diffs []TableAdd) []TableAdd {
	seen := make(map[TableAdd]struct{})
	for _, diff := range diffs {
		seen[diff] = struct{}{}
	}
	return slices.SortedFunc(maps.Keys(seen), compareTableDiffsByTS)
}
