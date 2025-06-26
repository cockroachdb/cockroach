// will come from pb in reality
package tableset

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
)

type Filter struct {
	IncludeTables []string
	ExcludeTables []string
}

func (f Filter) Includes(table catalog.TableDescriptor) bool {
	if len(f.ExcludeTables) > 0 {
		for _, exclude := range f.ExcludeTables {
			if table.GetName() == exclude {
				return false
			}
		}
		return true
	} else if len(f.IncludeTables) > 0 {
		for _, include := range f.IncludeTables {
			if table.GetName() == include {
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
	Name string
	ID   descpb.ID
}

func (t Table) String() string {
	return fmt.Sprintf("%s.%d", t.Name, t.ID)
}

type TableSet struct {
	Set  map[Table]struct{}
	AsOf hlc.Timestamp
}

type Watcher struct {
	filter     Filter
	databaseID descpb.ID

	id      string
	mon     *mon.BytesMonitor
	execCfg *sql.ExecutorConfig

	tablesets []TableSet
}

func NewWatcher(databaseID descpb.ID, filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id string) *Watcher {
	return &Watcher{databaseID: databaseID, filter: filter, execCfg: execCfg, mon: mon, id: id}
}

func (w *Watcher) Run(ctx context.Context, initialTS hlc.Timestamp) error {
	ctx = logtags.AddTag(ctx, "changefeed.tableset.watcher.filter", w.filter)
	ctx = logtags.AddTag(ctx, "changefeed.tableset.watcher.id", w.id)
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()

	return nil
}

// do we need a peek and pop like the schemafeed? we don't want to store schema changes forever..
// maybe we only need to store between calls?
// changes between the last call and now, delete the old ones.

// is this all we need?
func (w *Watcher) TableSet(ctx context.Context, asOf hlc.Timestamp) (TableSet, error) {
	ctx = logtags.AddTag(ctx, "changefeed.tableset.watcher.filter", w.filter)
	ctx = logtags.AddTag(ctx, "changefeed.tableset.watcher.id", w.id)
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.tableset")
	defer sp.Finish()

	tableSet := TableSet{
		Set:  make(map[Table]struct{}),
		AsOf: asOf,
	}
	err := w.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, asOf); err != nil {
			return err
		}
		dbDesc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get().Database(ctx, w.databaseID)
		if err != nil {
			return err
		}
		tables, err := txn.Descriptors().GetAllTablesInDatabase(ctx, txn.KV(), dbDesc)
		if err != nil {
			return err
		}
		tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
			tableDesc, err := catalog.AsTableDescriptor(desc)
			if err != nil {
				return err
			}
			if w.filter.Includes(tableDesc) {
				table := Table{
					Name: tableDesc.GetName(),
					ID:   tableDesc.GetID(),
				}
				tableSet.Set[table] = struct{}{}
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return TableSet{}, err
	}
	return tableSet, nil
}
