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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/sync/errgroup"
)

type Filter struct {
	DatabaseID    descpb.ID
	SchemaID      descpb.ID
	IncludeTables []string
	ExcludeTables []string
}

func (f Filter) String() string {
	return fmt.Sprintf("database_id=%d, schema_id=%d, include_tables=%v, exclude_tables=%v", f.DatabaseID, f.SchemaID, f.IncludeTables, f.ExcludeTables)
}

func (f Filter) Includes(tableInfo Table) bool {
	if f.DatabaseID != 0 && tableInfo.ParentID != f.DatabaseID {
		return false
	}
	if f.SchemaID != 0 && tableInfo.ParentSchemaID != f.SchemaID {
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
	ID    descpb.ID
	State descpb.DescriptorState
	AsOf  hlc.Timestamp
}

func (t Table) String() string {
	return fmt.Sprintf("Table{parent_id=%d, parent_schema_id=%d, name=%s, id=%d, as_of=%s, state=%s}", t.NameInfo.ParentID, t.NameInfo.ParentSchemaID, t.NameInfo.Name, t.ID, t.AsOf, t.State)
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

type TableSet struct {
	Tables []Table
	AsOf   hlc.Timestamp
}

type Watcher struct {
	filter Filter

	id      int64
	mon     *mon.BytesMonitor
	execCfg *sql.ExecutorConfig

	state struct {
		mu              syncutil.Mutex
		tableDiffs      []TableDiff
		resolved        hlc.Timestamp
		resolvedWaiters map[hlc.Timestamp]chan struct{}
	}
}

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

func (w *Watcher) Start(ctx context.Context, initialTS hlc.Timestamp) error {
	ctx = logtags.AddTag(ctx, "tableset.watcher.filter", w.filter)
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()

	fmt.Printf("starting watcher %#v with filter %s\n", w.id, w.filter)

	acc := w.mon.MakeBoundAccount()
	defer acc.Close(ctx)

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
		TableID:           systemschema.DescriptorTable.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(systemschema.DescriptorTable.GetName()),
		Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
	})
	dec, err := cdcevent.NewEventDecoder(ctx, w.execCfg, cfTargets, false, false)
	if err != nil {
		return err
	}

	// actually do we need to worry about resolveds?
	// we just want to know when our tableset changes. we need to do reordering and deduping for sure but..
	// the core question we need to answer is - is this tableset-timestamp still valid since the last time i checked?
	// to answer that we need to keep tableset changes between those times

	// TODO: i bet deletes don't work because descriptors don't get dropped until schema change gc happens...
	// so does that mean we have to watch system.descriptor instead and look at liveness?

	/// buffer & dedupe
	// callback channels:
	// pushed to when we've finished the initial scan. todo: how to use this
	finishedInitialScan := make(chan struct{})
	// pushed to when we've received a table
	incomingTableDiffs := make(chan TableDiff)
	// pushed to when we've received a resolved
	incomingResolveds := make(chan hlc.Timestamp)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		func() {
			w.state.mu.Lock()
			defer w.state.mu.Unlock()
			w.state.resolved = initialTS
		}()
		curResolved := initialTS
		bufferedTableDiffs := make(map[hlc.Timestamp]map[TableDiff]struct{})
		for {
			select {
			// buffer incoming tables between resolveds
			case table := <-incomingTableDiffs:
				fmt.Printf("(buffering) diff: %s\n", table)
				if table.AsOf.Less(curResolved) {
					fmt.Printf("(buffering) diff %s is before current resolved %s; skipping\n", table, curResolved)
					continue
				}
				if _, ok := bufferedTableDiffs[curResolved]; !ok {
					bufferedTableDiffs[curResolved] = make(map[TableDiff]struct{})
				}

				// mem accounting
				// TODO: shrink me on pop
				sz := int64(table.Added.Size()) + int64(table.Deleted.Size()) + int64(table.AsOf.Size())
				if err := acc.Grow(ctx, sz); err != nil {
					return errors.Wrapf(err, "failed to allocated %d bytes from monitor", sz)
				}

				bufferedTableDiffs[curResolved][table] = struct{}{}
			// flush buffered tables when we receive a resolved
			case resolved := <-incomingResolveds:
				fmt.Printf("(buffering) resolved: %s; %d tables buffered\n", resolved, len(bufferedTableDiffs[curResolved]))
				if resolved.Less(curResolved) {
					return errors.AssertionFailedf("resolved %s is less than current resolved %s", resolved, curResolved)
				}
				// the diffs/tables are already deduped, so we just need to sort them by timestamp
				diffs := slices.SortedFunc(maps.Keys(bufferedTableDiffs[curResolved]), func(a, b TableDiff) int {
					return a.AsOf.Compare(b.AsOf)
				})

				if err := func() error {
					w.state.mu.Lock()
					defer w.state.mu.Unlock()
					w.state.tableDiffs = append(w.state.tableDiffs, diffs...)

					// update resolved and notify waiters
					return w.updateResolvedLocked(resolved)
				}(); err != nil {
					return err
				}

				delete(bufferedTableDiffs, curResolved)

				curResolved = resolved
				// TODO: save progress at this timestamp?
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
	})

	// rangefeed setup

	// called from initial scans and maybe other places (catchups?)
	onValues := func(ctx context.Context, values []kv.KeyValue) {
		setErr(func() error {
			for _, kv := range values {
				// don't think this can happen
				if !kv.Value.IsPresent() {
					fmt.Printf("(onValues) no value for key %s\n", kv.Key)
					continue
				}
				kvpb := roachpb.KeyValue{Key: kv.Key, Value: *kv.Value}
				table, err := kvToTable(ctx, kvpb, dec)
				if err != nil {
					return err
				}
				// TODO: why am i not seeing foo_initial anymore?
				fmt.Printf("(onValues) table: %s; public: %t\n", table, table.State == descpb.DescriptorState_PUBLIC)
				if !w.filter.Includes(table) {
					continue
				}
				fmt.Printf("(onValues) matching table: %s\n", table)
				// TODO: clean up this logic after figuring it out in onValue. maybe just make this fn call onValue?
				var diff TableDiff
				if table.State == descpb.DescriptorState_PUBLIC {
					diff = TableDiff{Added: table, Deleted: Table{}, AsOf: kv.Value.Timestamp}
				} else {
					diff = TableDiff{Deleted: table, AsOf: kv.Value.Timestamp}
				}
				select {
				case incomingTableDiffs <- diff:
				case <-egCtx.Done():
					return egCtx.Err()
				}
			}
			return nil
		}())
	}

	// called with ordinary rangefeed values
	onValue := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		setErr(func() error { // prevvalue is zero on table add?
			if !kv.Value.IsPresent() {
				// schema change gc cleaning up dropped tables? prob ok to ignore
				fmt.Printf("(onValue) no value for key %s\n", kv.Key)
				return nil
			}

			table, err := kvToTable(ctx, roachpb.KeyValue{Key: kv.Key, Value: kv.Value}, dec)
			if err != nil {
				return err
			}

			var prevTable Table
			if kv.PrevValue.IsPresent() {
				kvpb := roachpb.KeyValue{Key: kv.Key, Value: kv.PrevValue}
				kvpb.Value.Timestamp = kv.Value.Timestamp
				prevTable, err = kvToTable(ctx, kvpb, dec)
				if err != nil {
					return err
				}
			}

			fmt.Printf("(onValue) table: %s; prev: %s\n", table, prevTable)

			// TODO: is this right re renames? think so..
			//if !(w.filter.Includes(table) || (kv.PrevValue.IsPresent() && w.filter.Includes(prevTable))) {
			if !(w.filter.Includes(table) || (kv.PrevValue.IsPresent() && w.filter.Includes(prevTable))) {
				return nil
			}
			// TODO: a drop can be more than one diff, but that's ok? we see multiple diffs with removed but they should be contiguous.
			// we can see that tableDesc.DeclarativeSchemaChangerState == nil on the second/last one if we want to dedupe.
			// or if prev is also dropped

			// new table: prev is zero
			// dropped table: prev is not zero and dropped
			// TODO: others (offline, importing, add?)...?
			added := !kv.PrevValue.IsPresent()
			dropped := kv.PrevValue.IsPresent() && table.State != descpb.DescriptorState_PUBLIC
			renamed := kv.PrevValue.IsPresent() && prevTable.Name != table.Name
			// NOTE: drops seem to happen in 2 stages some times with the declarative schema changer. here we'lll lose the second one, is that ok? like, probably...

			var diff TableDiff
			if added {
				diff = TableDiff{Added: table, Deleted: Table{}, AsOf: kv.Value.Timestamp}
			} else if dropped {
				diff = TableDiff{Deleted: table, AsOf: kv.Value.Timestamp}
			} else if renamed {
				diff = TableDiff{Deleted: prevTable, Added: table, AsOf: kv.Value.Timestamp}
			} else {
				// TODO: classify these states and return errors
				// - a normal schema change eg column add
				// - ?
				fmt.Printf("unexpected table state: %s\n", table)
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

	watchSpans := roachpb.Spans{systemschema.DescriptorTable.TableSpan(w.execCfg.Codec)}
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
		rangefeed.WithFrontierQuantized(1 * time.Second),
		rangefeed.WithOnValues(onValues),
		rangefeed.WithDiff(true),
		rangefeed.WithConsumerID(w.id), // TODO: do we need some magic non-job-id value?
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
		rangefeed.WithInitialScan(func(ctx context.Context) {
			fmt.Printf("initial scan done\n")
			close(finishedInitialScan)
		}),
		rangefeed.WithRowTimestampInInitialScan(true),
	}

	// Start rangefeed.
	rf := w.execCfg.RangeFeedFactory.New(
		fmt.Sprintf("tableset.watcher.id=%v", w.id), initialTS, onValue, opts...,
	)
	defer rf.Close()

	fmt.Printf("starting rangefeed\n")

	if err := rf.StartFromFrontier(ctx, frontier); err != nil {
		return err
	}

	fmt.Printf("rangefeed started\n")

	// wait for shutdown due to error or context cancellation
	select {
	case err := <-errCh:
		fmt.Printf("shutting down due to error: %v\n", err)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TODO: can we return a more helpful thing? what does the caller really care about? basically just that the diff is empty or nonempty, so this is fine but overkill?

func (w *Watcher) PeekDiffs(ctx context.Context, from, to hlc.Timestamp) ([]TableDiff, error) {
	w.state.mu.Lock()
	defer w.state.mu.Unlock()

	// TODO: this isnt quite right -- if we panic between the unlock and the lock, we'll do a double unlock and panic again
	if w.state.resolved.Compare(to) < 0 {
		// wait
		start := timeutil.Now()
		fmt.Printf("peekdiffs(%s, %s) will wait\n", from, to)
		waiter := w.addWaiterLocked(to)
		w.state.mu.Unlock()
		select {
		case <-waiter:
			fmt.Printf("peekdiffs(%s, %s) done waiting in %s\n", from, to, time.Since(start))
		case <-ctx.Done():
			w.state.mu.Lock()
			return nil, ctx.Err()
		}
		w.state.mu.Lock()
	}

	diffs := w.state.tableDiffs

	if buildutil.CrdbTestBuild {
		sorted := slices.IsSortedFunc(diffs, func(a, b TableDiff) int {
			return a.AsOf.Compare(b.AsOf)
		})
		if !sorted {
			return nil, errors.AssertionFailedf("diffs are not sorted")
		}
	}

	// returns the earliest position where target is found, or the position where target would appear in the sort order
	cmp := func(diff TableDiff, ts hlc.Timestamp) int {
		return diff.AsOf.Compare(ts)
	}
	start, _ := slices.BinarySearchFunc(diffs, from, cmp)
	end, _ := slices.BinarySearchFunc(diffs, to, cmp)

	diffs = diffs[start:end]

	return slices.Clone(diffs), nil
}

func (w *Watcher) PopDiffs(from, to hlc.Timestamp) ([]TableDiff, error) {
	return nil, nil
}

func (w *Watcher) updateResolvedLocked(ts hlc.Timestamp) error {
	if ts.Compare(w.state.resolved) < 0 {
		return errors.AssertionFailedf("resolved %s is less than current resolved %s", ts, w.state.resolved)
	}

	// the lag seems to be about 3s, coinciding with the default value of kv.closed_timestamp.target_duration.
	// this is probably fine since the changefeed data will have the same lag anyway.
	fmt.Printf("updateResolved@%d %s -> %s\n", timeutil.Now().Unix(), w.state.resolved, ts)
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

// check that my tableset is unchanged from a to b ts
// -> would have to store an unbounded number of tablesets/diffs
// -> pop them out

// example usage:
// startup
// changefeed hits t1; is my tableset still valid at t1?
//   - peek -> yes -> continue; pop on commit t1 (can clean up diffs up to t1)
//   - peek -> no; there was a change since t0/last time you checked -> restart from t0

func kvToTable(ctx context.Context, kv roachpb.KeyValue, dec cdcevent.Decoder) (Table, error) {
	fmt.Printf("kvToTable: %s, %s\n", kv.Key, kv.Value.Timestamp)
	row, err := dec.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return Table{}, err
	}

	var tableID descpb.ID
	var descriptor descpb.Descriptor
	err = row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		switch col.Name {
		case "id":
			tableID = descpb.ID(tree.MustBeDInt(d))
		case "descriptor":
			bs := tree.MustBeDBytes(d)
			if err := descriptor.Unmarshal(bs.UnsafeBytes()); err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unexpected column: %s", col.Name)
		}
		return nil
	})
	if err != nil {
		return Table{}, err
	}
	tableDesc := descriptor.GetTable()

	table := Table{
		NameInfo: descpb.NameInfo{
			ParentID: tableDesc.GetParentID(),
			// parent schema id isn't on here...? doesnt matter right now but could be an issue later
			Name: tableDesc.GetName(),
		},
		ID:    tableID,
		State: tableDesc.State,
		AsOf:  kv.Value.Timestamp,
	}

	return table, nil
}

// prettyRow := func(row cdcevent.Row) string {
// 	var b strings.Builder
// 	fmt.Fprintf(&b, "Row{")
// 	err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
// 		fmt.Fprintf(&b, "%s: %+v, ", col.Name, d)
// 		return nil
// 	})
// 	if err != nil {
// 		return "err: " + err.Error()
// 	}
// 	fmt.Fprintf(&b, "}")
// 	return b.String()
// }

// prettyKey := func(key roachpb.Key) string {
// 	return catalogkeys.PrettyKey(nil, key, -1)
// }

// unused; saved for reference
func applyDiffs(curTableSet TableSet, diffs []TableDiff) (TableSet, error) {
	for _, diff := range diffs {
		curTableSet.AsOf = diff.AsOf
		if diff.Added.ID != 0 {
			curTableSet.Tables = append(curTableSet.Tables, diff.Added)
		}
		if diff.Deleted.ID != 0 {
			// remove deleted table from curTableSet
			for i, table := range curTableSet.Tables {
				if table.ID == diff.Deleted.ID {
					curTableSet.Tables = append(curTableSet.Tables[:i], curTableSet.Tables[i+1:]...)
				}
			}
		}
		fmt.Printf("applied diff %s to curTableSet; now %s\n", diff, curTableSet)
	}
	return curTableSet, nil
}
