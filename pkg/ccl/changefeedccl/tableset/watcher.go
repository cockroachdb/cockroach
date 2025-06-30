// will come from pb in reality
package tableset

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	if f.DatabaseID != 0 && tableInfo.NameInfo.ParentID != f.DatabaseID {
		return false
	}
	if f.SchemaID != 0 && tableInfo.NameInfo.ParentSchemaID != f.SchemaID {
		return false
	}
	if len(f.ExcludeTables) > 0 {
		for _, exclude := range f.ExcludeTables {
			if tableInfo.NameInfo.Name == exclude {
				return false
			}
		}
		return true
	} else if len(f.IncludeTables) > 0 {
		for _, include := range f.IncludeTables {
			if tableInfo.NameInfo.Name == include {
				return true
			}
		}
		return false
	} else if len(f.IncludeTables) == 0 && len(f.ExcludeTables) == 0 {
		return true
	} else {
		panic("invalid filter: todo make this an error or smth")
	}
}

type Table struct {
	descpb.NameInfo
	ID   descpb.ID
	AsOf hlc.Timestamp
}

func (t Table) String() string {
	return fmt.Sprintf("Table{parent_id=%d, parent_schema_id=%d, name=%s, id=%d, as_of=%s}", t.NameInfo.ParentID, t.NameInfo.ParentSchemaID, t.NameInfo.Name, t.ID, t.AsOf)
}

type TableDiff struct {
	Added   Table
	Deleted Table
	AsOf    hlc.Timestamp
}

func (d TableDiff) String() string {
	return fmt.Sprintf("TableDiff{added=%s, deleted=%s, as_of=%s}", d.Added, d.Deleted, d.AsOf)
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

	tablesets struct {
		mu syncutil.Mutex
		// ordered; todo: better ds
		sets []TableSet
	}
}

func NewWatcher(filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id int64) *Watcher {
	return &Watcher{filter: filter, execCfg: execCfg, mon: mon, id: id}
}

func (w *Watcher) Start(ctx context.Context, initialTS hlc.Timestamp) error {
	ctx = logtags.AddTag(ctx, "tableset.watcher.filter", w.filter)
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()

	fmt.Printf("starting watcher %s with filter %s\n", w.id, w.filter)

	acc := w.mon.MakeBoundAccount()

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

	// actually do we need to worry about resolveds?
	// we just want to know when our tableset changes. we need to do reordering and deduping for sure but..
	// the core question we need to answer is - is this tableset-timestamp still valid since the last time i checked?
	// to answer that we need to keep tableset changes between those times

	// TODO: i bet deletes don't work because descriptors don't get dropped until schema change gc happens...
	// so does that mean we have to watch system.descriptor instead and look at liveness?

	/// buffer & dedupe
	dedupedTableDiffs := make(chan TableDiff)
	// callback channels:
	// pushed to when we've finished the initial scan. todo: how to use this
	finishedInitialScan := make(chan struct{})
	// pushed to when we've received a table
	incomingTableDiffs := make(chan TableDiff)
	// pushed to when we've received a resolved
	incomingResolveds := make(chan hlc.Timestamp)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(dedupedTableDiffs)
		curResolved := initialTS
		bufferedTableDiffs := make(map[hlc.Timestamp]map[TableDiff]struct{})
		for {
			select {
			// buffer incoming tables between resolveds
			case table := <-incomingTableDiffs:
				fmt.Printf("(incoming) diff: %s\n", table)
				if table.AsOf.Less(curResolved) {
					fmt.Printf("(incoming) diff %s is before current resolved %s; skipping\n", table, curResolved)
					continue
				}
				if _, ok := bufferedTableDiffs[curResolved]; !ok {
					bufferedTableDiffs[curResolved] = make(map[TableDiff]struct{})
				}

				// mem accounting
				if err := acc.Grow(ctx, int64(table.Added.Size())); err != nil {
					return errors.Wrapf(err, "failed to allocated %d bytes from monitor", table.Added.Size())
				}

				bufferedTableDiffs[curResolved][table] = struct{}{}
			// flush buffered tables when we receive a resolved
			case resolved := <-incomingResolveds:
				fmt.Printf("(incoming) resolved: %s; %d tables buffered\n", resolved, len(bufferedTableDiffs[curResolved]))
				if resolved.Less(curResolved) {
					return errors.AssertionFailedf("resolved %s is less than current resolved %s", resolved, curResolved)
				}
				for diff := range bufferedTableDiffs[curResolved] {
					select {
					case dedupedTableDiffs <- diff:
					case <-egCtx.Done():
						return egCtx.Err()
					}
				}
				delete(bufferedTableDiffs, curResolved)
				// mem accounting
				acc.Shrink(ctx, 0) // TODO

				curResolved = resolved
				// TODO: save progress at this timestamp?
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
	})

	eg.Go(func() error {
		curTableSet := TableSet{AsOf: initialTS}
		// TODO: have to apply these in order / make sure they're in order already
		for diff := range dedupedTableDiffs {
			fmt.Printf("applying diff %s to curTableSet %s\n", diff, curTableSet)
			// apply diff to curTableSet
			curTableSet.Tables = append(curTableSet.Tables, diff.Added)
			curTableSet.AsOf = diff.AsOf
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
		return nil
	})

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
				table, err := kvToTable(ctx, kvpb, dec, w)
				if err != nil {
					return err
				}
				// TODO: why am i not seeing foo_initial anymore?
				fmt.Printf("(onValues) table: %s\n", table)
				if !w.filter.Includes(table) {
					continue
				}
				fmt.Printf("(onValues) matching table: %s\n", table)
				select {
				case incomingTableDiffs <- TableDiff{Added: table, Deleted: Table{}, AsOf: kv.Value.Timestamp}:
				case <-egCtx.Done():
					return egCtx.Err()
				}
			}
			return nil
		}())
	}

	// called with ordinary rangefeed values
	onValue := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		setErr(func() error {
			var table, prevTable Table
			var pbkv roachpb.KeyValue
			var err error
			if kv.Value.IsPresent() {
				pbkv = roachpb.KeyValue{Key: kv.Key, Value: kv.Value}
				table, err = kvToTable(ctx, pbkv, dec, w)
				if err != nil {
					return err
				}
			}
			if kv.PrevValue.IsPresent() {
				pbkv = roachpb.KeyValue{Key: kv.Key, Value: kv.PrevValue}
				pbkv.Value.Timestamp = kv.Value.Timestamp.Prev() // TODO: is this right?
				prevTable, err = kvToTable(ctx, pbkv, dec, w)
				if err != nil {
					return err
				}
			}
			fmt.Printf("(onValue) table: %s, prevTable: %s\n", table, prevTable)
			if !(w.filter.Includes(table) || (kv.PrevValue.IsPresent() && w.filter.Includes(prevTable))) {
				return nil
			}
			select {
			case incomingTableDiffs <- TableDiff{Added: table, Deleted: prevTable, AsOf: kv.Value.Timestamp}:
			case <-egCtx.Done():
				return egCtx.Err()
			}
			return nil
		}())
	}

	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("job", fmt.Sprintf("id=%s", w.id)),
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
		fmt.Sprintf("tableset.watcher.id=%s", w.id), initialTS, onValue, opts...,
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

func (w *Watcher) Pop() (TableSet, error) {
	return TableSet{}, nil
}

// TODO: saw this error
// error decoding key /NamespaceTable/30/1/100/101/"foo_0"/4/1@0,0 (hex_kv: 0a0ea689eced12666f6f5f3000018c8912021200): getDescriptorsFromStoreForInterval: lower bound cannot be empty
func kvToTable(ctx context.Context, kv roachpb.KeyValue, dec cdcevent.Decoder, w *Watcher) (Table, error) {
	fmt.Printf("kvToTable: %s\n", kv)
	row, err := dec.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return Table{}, err
	}

	// decode the row into the table id, and the key into name info
	var tableId descpb.ID
	row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		if col.Name == "id" {
			tableId = descpb.ID(tree.MustBeDInt(d))
		}
		return nil
	})

	nameInfo, err := catalogkeys.DecodeNameMetadataKey(w.execCfg.Codec, kv.Key)
	if err != nil {
		fmt.Printf("failed to decode namespace key: %v\n", err)
		return Table{}, err
	}

	return Table{
		NameInfo: nameInfo,
		ID:       tableId,
		AsOf:     kv.Value.Timestamp,
	}, nil
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
