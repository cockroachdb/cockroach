// Package tableset implements a tableset watcher.
package tableset

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/sync/errgroup"
)

// Filter is a filter for a tableset.
type Filter struct {
	DatabaseID    descpb.ID
	IncludeTables []string
	ExcludeTables []string
}

func (f Filter) String() string {
	return fmt.Sprintf("database_id=%d, include_tables=%v, exclude_tables=%v", f.DatabaseID, f.IncludeTables, f.ExcludeTables)
}

func (f Filter) includes(tableInfo Table) bool {
	if f.DatabaseID != 0 && tableInfo.ParentID != f.DatabaseID {
		return false
	}
	if len(f.ExcludeTables) > 0 {
		return !slices.Contains(f.ExcludeTables, tableInfo.Name)
	} else if len(f.IncludeTables) > 0 {
		return slices.Contains(f.IncludeTables, tableInfo.Name)
	} else if len(f.IncludeTables) == 0 && len(f.ExcludeTables) == 0 {
		return true
	} else {
		panic("invalid filter: todo make this an error or smth")
	}
}

type Table struct {
	descpb.NameInfo
	ID descpb.ID
}

func (t Table) size() int64 {
	return int64(t.NameInfo.Size()) + 8
}

func (t Table) String() string {
	return fmt.Sprintf("Table{parent_id=%d, parent_schema_id=%d, name=%s, id=%d}", t.NameInfo.ParentID, t.NameInfo.ParentSchemaID, t.NameInfo.Name, t.ID)
}

type TableDiff struct {
	Added   Table
	Deleted Table
	AsOf    hlc.Timestamp
}

func (d TableDiff) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "TableDiff{")
	if d.Added.ID != 0 {
		fmt.Fprintf(&b, "added=%s, ", d.Added)
	}
	if d.Deleted.ID != 0 {
		fmt.Fprintf(&b, "deleted=%s, ", d.Deleted)
	}
	fmt.Fprintf(&b, "as_of=%s}", d.AsOf)
	return b.String()
}

func (d TableDiff) size() int64 {
	return int64(d.Added.size()) + int64(d.Deleted.size()) + int64(d.AsOf.Size())
}

func compareTableDiffsByTS(a, b TableDiff) int {
	return a.AsOf.Compare(b.AsOf)
}

type TableSet struct {
	Tables []Table
	AsOf   hlc.Timestamp
}

// Watcher watches a tableset and buffers table diffs. It will notify waiters
// when the resolved timestamp is advanced.
type Watcher struct {
	filter Filter

	id      int64
	execCfg *sql.ExecutorConfig
	mon     *mon.BytesMonitor
	acc     mon.BoundAccount // set on Start

	state struct {
		mu              syncutil.Mutex
		tableDiffs      []TableDiff
		resolved        hlc.Timestamp
		resolvedWaiters map[hlc.Timestamp]chan struct{}
	}
}

// NewWatcher creates a new watcher.
func NewWatcher(filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id int64) *Watcher {
	w := &Watcher{
		filter:  filter,
		execCfg: execCfg,
		mon:     mon,
		id:      id,
	}
	w.state.resolvedWaiters = make(map[hlc.Timestamp]chan struct{})
	return w
}

// Start starts the watcher. It will not return until the watcher is shut down
// due to an error or context cancellation. It may only be run once.
func (w *Watcher) Start(ctx context.Context, initialTS hlc.Timestamp) (retErr error) {
	ctx = logtags.AddTag(ctx, "tableset.watcher.filter", w.filter.String())
	ctx = logtags.AddTag(ctx, "tableset.watcher.id", fmt.Sprintf("%d", w.id))
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(retErr)

	log.Infof(ctx, "starting watcher with filter %s", w.filter)

	w.acc = w.mon.MakeBoundAccount()
	defer w.acc.Close(ctx)

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
		TableID:           systemschema.NamespaceTable.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(systemschema.NamespaceTable.GetName()),
		Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
	})
	dec, err := cdcevent.NewEventDecoder(ctx, w.execCfg, cfTargets, false, false)
	if err != nil {
		return err
	}

	// Buffering goroutine.
	incomingTableDiffs := make(chan TableDiff)
	incomingResolveds := make(chan hlc.Timestamp)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		ctx, sp := tracing.ChildSpan(ctx, "tableset.watcher.buffer-loop")
		defer sp.Finish()

		func() {
			w.state.mu.Lock()
			defer w.state.mu.Unlock()
			w.state.resolved = initialTS
		}()

		curResolved := initialTS
		bufferedTableDiffs := make(map[hlc.Timestamp]map[TableDiff]struct{})
		for {
			select {
			// Buffer incoming tables between resolved messages.
			case diff := <-incomingTableDiffs:
				if diff.AsOf.Less(curResolved) {
					log.Infof(ctx, "diff %s is before current resolved %s; skipping", diff, curResolved)
					continue
				}
				if _, ok := bufferedTableDiffs[curResolved]; !ok {
					bufferedTableDiffs[curResolved] = make(map[TableDiff]struct{})
				}

				// mem accounting
				if err := w.acc.Grow(ctx, diff.size()); err != nil {
					return errors.Wrapf(err, "failed to allocated %d bytes from monitor", diff.size())
				}

				bufferedTableDiffs[curResolved][diff] = struct{}{}
			// Flush buffered tables when we receive a resolved message.
			case resolved := <-incomingResolveds:
				if log.V(2) {
					log.Infof(ctx, "resolved: %s; %d tables buffered", resolved, len(bufferedTableDiffs[curResolved]))
				}

				if resolved.Less(curResolved) {
					return errors.AssertionFailedf("resolved %s is less than current resolved %s", resolved, curResolved)
				}
				// The diffs/tables are already deduped, so we just need to sort them by timestamp.
				// Using a btree could be more efficient for some use cases, but this is probably faster for the low volume we expect, and simpler.
				diffs := slices.SortedFunc(maps.Keys(bufferedTableDiffs[curResolved]), compareTableDiffsByTS)

				if err := func() error {
					w.state.mu.Lock()
					defer w.state.mu.Unlock()
					w.state.tableDiffs = append(w.state.tableDiffs, diffs...)
					// TODO: may be worth using a btree here also to avoid the sort-dedupe
					w.state.tableDiffs = dedupeAndSortDiffs(w.state.tableDiffs)

					// Update resolved and notify waiters.
					return w.updateResolvedLocked(ctx, resolved)
				}(); err != nil {
					return err
				}

				delete(bufferedTableDiffs, curResolved)

				curResolved = resolved
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
	})

	// rangefeed setup
	onValue := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		setErr(func() error {
			var table, prevTable Table
			var err error

			if kv.Value.IsPresent() {
				table, err = w.kvToTable(ctx, roachpb.KeyValue{Key: kv.Key, Value: kv.Value}, dec)
				if err != nil {
					return err
				}
			}

			if kv.PrevValue.IsPresent() {
				kvpb := roachpb.KeyValue{Key: kv.Key, Value: kv.PrevValue}
				kvpb.Value.Timestamp = kv.Value.Timestamp
				prevTable, err = w.kvToTable(ctx, kvpb, dec)
				if err != nil {
					return err
				}
			}

			if log.V(2) {
				log.Infof(ctx, "onValue: %s; prev: %s", table, prevTable)
			}

			if !(w.filter.includes(table) || (kv.PrevValue.IsPresent() && w.filter.includes(prevTable))) {
				return nil
			}

			added := !kv.PrevValue.IsPresent()
			dropped := kv.PrevValue.IsPresent() && !kv.Value.IsPresent()
			// NOTE: a rename looks like a drop then an add, because the name is part of the key.
			// They will be in the same txn.

			var diff TableDiff
			if added {
				diff = TableDiff{Added: table, AsOf: kv.Value.Timestamp}
			} else if dropped {
				diff = TableDiff{Deleted: prevTable, AsOf: kv.Value.Timestamp}
			} else {
				log.Warningf(ctx, "onValue:unexpected table state: table=%s, prev=%s", table, prevTable)
				return nil
			}
			select {
			case incomingTableDiffs <- diff:
			case <-egCtx.Done():
				return egCtx.Err()
			}
			return nil
		}())
	}

	watchSpans := roachpb.Spans{systemschema.NamespaceTable.TableSpan(w.execCfg.Codec)}
	frontier, err := span.MakeFrontier(watchSpans...)
	if err != nil {
		return err
	}
	for _, span := range watchSpans {
		frontier.Forward(span, initialTS)
	}
	frontier = span.MakeConcurrentFrontier(frontier)
	defer frontier.Release()

	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("tableset.watcher", fmt.Sprintf("id=%d", w.id)),
		rangefeed.WithMemoryMonitor(w.mon),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			advanced, err := frontier.Forward(checkpoint.Span, checkpoint.ResolvedTS)
			if err != nil {
				setErr(errors.Wrapf(err, "failed to forward frontier"))
			}
			if advanced {
				select {
				case incomingResolveds <- checkpoint.ResolvedTS:
				case <-egCtx.Done():
					return
				}
			}
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) { setErr(err) }),
		rangefeed.WithFrontierQuantized(1 * time.Second), // TODO: why does it not work without this?
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

	if err := rf.StartFromFrontier(ctx, frontier); err != nil {
		return err
	}

	log.Infof(ctx, "rangefeed started")

	// Wait for shutdown due to error or context cancellation.
	select {
	case err := <-errCh:
		log.Warningf(ctx, "watcher %d shutting down due to error: %v", w.id, err)
		cancel(err)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UnchangedUpTo returns true if the tableset is unchanged between when it (or
// Pop()) was last called (or the initialTS) and the given timestamp. It's a
// convenience wrapper around Pop().
func (w *Watcher) UnchangedUpTo(ctx context.Context, upTo hlc.Timestamp) (bool, error) {
	diffs, err := w.Pop(ctx, upTo)
	if err != nil {
		return false, err
	}
	return len(diffs) == 0, nil
}

// Pop returns the table diffs between the last call to Pop() (or the initial
// timestamp) and the given timestamp.
func (w *Watcher) Pop(ctx context.Context, upTo hlc.Timestamp) ([]TableDiff, error) {
	if err := w.maybeWaitForResolved(ctx, upTo); err != nil {
		return nil, err
	}

	w.state.mu.Lock()
	defer w.state.mu.Unlock()

	upToIdx, found := slices.BinarySearchFunc(w.state.tableDiffs, upTo, func(diff TableDiff, ts hlc.Timestamp) int {
		return diff.AsOf.Compare(ts)
	})
	if found && upToIdx > 0 {
		upToIdx--
	}

	if buildutil.CrdbTestBuild {
		sorted := slices.IsSortedFunc(w.state.tableDiffs, func(a, b TableDiff) int {
			return a.AsOf.Compare(b.AsOf)
		})
		if !sorted {
			return nil, errors.AssertionFailedf("diffs are not sorted")
		}
	}

	diffs := w.state.tableDiffs[:upToIdx]
	w.state.tableDiffs = w.state.tableDiffs[upToIdx:]

	// Mem accounting.
	sz := int64(0)
	for _, diff := range diffs {
		sz += diff.size()
	}
	w.acc.Shrink(ctx, sz)

	return slices.Clone(diffs), nil
}

// TableSetAt returns the tableset at the given timestamp. This is just a
// convenience method and does not mutate the watcher.
func (w *Watcher) TableSetAt(ctx context.Context, at hlc.Timestamp) (TableSet, error) {
	tableSet := TableSet{AsOf: at}
	err := w.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, at); err != nil {
			return err
		}
		getter := txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get()
		dbDesc, err := getter.Database(ctx, w.filter.DatabaseID)
		if err != nil {
			return err
		}
		tables, err := getter.GetAllTablesInDatabase(ctx, txn.KV(), dbDesc)
		if err != nil {
			return err
		}
		err = tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
			tableDesc := desc.(catalog.TableDescriptor)
			table := Table{
				NameInfo: descpb.NameInfo{
					ParentID:       tableDesc.GetParentID(),
					ParentSchemaID: tableDesc.GetParentSchemaID(),
					Name:           tableDesc.GetName(),
				},
				ID: tableDesc.GetID(),
			}
			if !w.filter.includes(table) {
				return nil
			}
			tableSet.Tables = append(tableSet.Tables, table)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return TableSet{}, err
	}

	return tableSet, nil
}

func (w *Watcher) maybeWaitForResolved(ctx context.Context, ts hlc.Timestamp) error {
	start := timeutil.Now()
	if log.V(2) {
		log.Infof(ctx, "maybeWaitForResolved(%s) start", ts)
	}

	w.state.mu.Lock()
	defer w.state.mu.Unlock()

	if w.state.resolved.Compare(ts) >= 0 {
		if log.V(2) {
			log.Infof(ctx, "maybeWaitForResolved(%s) already resolved", ts)
		}
		return nil
	}

	waiter := w.addWaiterLocked(ts)

	w.state.mu.Unlock()
	defer w.state.mu.Lock()

	select {
	case <-waiter:
		if log.V(2) {
			log.Infof(ctx, "maybeWaitForResolved(%s) done waiting in %s", ts, time.Since(start))
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Watcher) updateResolvedLocked(ctx context.Context, ts hlc.Timestamp) error {
	if ts.Compare(w.state.resolved) < 0 {
		return errors.AssertionFailedf("resolved %s is less than current resolved %s", ts, w.state.resolved)
	}

	// the lag seems to be about 3s, coinciding with the default value of kv.closed_timestamp.target_duration.
	// this is probably fine since the changefeed data will have the same lag anyway.
	if log.V(2) {
		log.Infof(ctx, "updateResolved@%d %s -> %s", timeutil.Now().Unix(), w.state.resolved, ts)
	}
	w.state.resolved = ts
	for wts, waiter := range w.state.resolvedWaiters {
		if wts.Compare(ts) <= 0 {
			delete(w.state.resolvedWaiters, wts)
			close(waiter)
		}
	}
	return nil
}

func (w *Watcher) addWaiterLocked(ts hlc.Timestamp) chan struct{} {
	waiter := make(chan struct{})
	w.state.resolvedWaiters[ts] = waiter
	return waiter
}

func (w *Watcher) kvToTable(ctx context.Context, kv roachpb.KeyValue, dec cdcevent.Decoder) (Table, error) {
	if log.V(2) {
		log.Infof(ctx, "kvToTable: %s, %s", kv.Key, kv.Value.Timestamp)
	}

	row, err := dec.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return Table{}, err
	}

	var tableID descpb.ID
	err = row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		switch col.Name {
		case "id":
			tableID = descpb.ID(tree.MustBeDInt(d))
		}
		return nil
	})
	if err != nil {
		return Table{}, err
	}

	nameInfo, err := catalogkeys.DecodeNameMetadataKey(w.execCfg.Codec, kv.Key)
	if err != nil {
		return Table{}, err
	}

	table := Table{
		NameInfo: nameInfo,
		ID:       tableID,
	}

	return table, nil
}

func mkConsumerID(id int64) int64 {
	if id > 0 {
		return -id
	}
	return id
}

func dedupeAndSortDiffs(diffs []TableDiff) []TableDiff {
	seen := make(map[TableDiff]struct{})
	for _, diff := range diffs {
		seen[diff] = struct{}{}
	}
	return slices.SortedFunc(maps.Keys(seen), compareTableDiffsByTS)
}
