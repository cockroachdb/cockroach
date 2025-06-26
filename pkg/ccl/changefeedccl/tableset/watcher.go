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
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
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
	ID descpb.ID
}

func (t Table) String() string {
	return fmt.Sprintf("parent_id=%d, parent_schema_id=%d, name=%s, id=%d", t.NameInfo.ParentID, t.NameInfo.ParentSchemaID, t.NameInfo.Name, t.ID)
}

type TableSet struct {
	Set  map[Table]struct{}
	AsOf hlc.Timestamp
}

type Watcher struct {
	filter Filter

	id      string
	mon     *mon.BytesMonitor
	execCfg *sql.ExecutorConfig

	tablesets []TableSet
}

func NewWatcher(filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id string) *Watcher {
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

	// TODO: should really be on system.descriptor too/instead

	var cfTargets changefeedbase.Targets
	// TODO: not getting any data except id when i specify that fam
	// cfTargets.Add(changefeedbase.Target{
	// 	TableID:           systemschema.NamespaceTable.TableDescriptor.GetID(),
	// 	StatementTimeName: changefeedbase.StatementTimeName(systemschema.NamespaceTable.TableDescriptor.TableDesc().Name),
	// 	Type:              jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
	// 	FamilyName:        "fam_4_id",
	// })
	cfTargets.Add(changefeedbase.Target{
		TableID:           systemschema.NamespaceTable.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(systemschema.NamespaceTable.GetName()),
		Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
	})
	dec, err := cdcevent.NewEventDecoder(ctx, w.execCfg, cfTargets, false, false)
	if err != nil {
		return err
	}

	// called from initial scans and maybe other places (catchups?)
	onValues := func(ctx context.Context, values []kv.KeyValue) {
		setErr(func() error {
			for _, kv := range values {
				if !kv.Value.IsPresent() {
					continue // ?
				}
				kvpb := roachpb.KeyValue{Key: kv.Key, Value: *kv.Value}
				table, err := kvToTable(ctx, kvpb, dec, w)
				if err != nil {
					return err
				}
				if !w.filter.Includes(table) {
					continue
				}
				fmt.Printf("(scan?) table: %s\n", table)
			}
			return nil
		}())
	}

	// called with ordinary rangefeed values
	onValue := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		setErr(func() error {
			if !kv.Value.IsPresent() {
				return nil
			}
			pbkv := roachpb.KeyValue{Key: kv.Key, Value: kv.Value}
			table, err := kvToTable(ctx, pbkv, dec, w)
			if err != nil {
				return err
			}
			if !w.filter.Includes(table) {
				return nil
			}
			fmt.Printf("(onValue) table: %s\n", table)
			return nil
		}())
	}

	// Common rangefeed options.
	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("job", fmt.Sprintf("id=%s", w.id)),
		rangefeed.WithMemoryMonitor(w.mon),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) { setErr(err) }),
		rangefeed.WithFrontierQuantized(1 * time.Second),
		rangefeed.WithOnValues(onValues),
		rangefeed.WithDiff(true),
		rangefeed.WithConsumerID(int64(42)),
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
		rangefeed.WithInitialScan(func(ctx context.Context) {
			fmt.Printf("initial scan done\n")
		}),
	}

	// // find family to watch
	// var indexID descpb.IndexID
	// for _, family := range systemschema.NamespaceTable.TableDescriptor.TableDesc().Families {
	// 	if family.Name == "fam_4_id" {
	// 		indexID = descpb.IndexID(family.ID)
	// 		break
	// 	}
	// }
	// watchSpans := roachpb.Spans{systemschema.NamespaceTable.IndexSpan(w.execCfg.Codec, indexID)}
	watchSpans := roachpb.Spans{systemschema.NamespaceTable.TableSpan(w.execCfg.Codec)}

	frontier, err := span.MakeFrontier(watchSpans...)
	if err != nil {
		return err
	}
	frontier = span.MakeConcurrentFrontier(frontier)
	defer frontier.Release()

	// TODO: is this our buffer size? do we need this?
	if err := acc.Grow(ctx, 1024); err != nil {
		return errors.Wrapf(err, "failed to allocated %d bytes from monitor", 1024)
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

func (w *Watcher) OnChange(cb func(tableSet TableSet)) {}

func (w *Watcher) ValidAsOf(tableSet TableSet, ts hlc.Timestamp) bool {
	return false
}

func (w *Watcher) Close() {}

func kvToTable(ctx context.Context, kv roachpb.KeyValue, dec cdcevent.Decoder, w *Watcher) (Table, error) {
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
