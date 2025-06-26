// will come from pb in reality
package tableset

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
)

type Filter struct {
	DB            string
	Schema        string
	IncludeTables []string
	ExcludeTables []string
}

type Table struct {
	DB     string
	Schema string
	Name   string
	ID     descpb.ID
}

func (t Table) String() string {
	return fmt.Sprintf("%s.%s.%s", t.DB, t.Schema, t.Name)
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
}

func NewWatcher(filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id string) *Watcher {
	return &Watcher{filter: filter, execCfg: execCfg, mon: mon, id: id}
}

func (w *Watcher) Start(ctx context.Context, initialTS hlc.Timestamp) error {
	ctx = logtags.AddTag(ctx, "tableset.watcher.filter", w.filter)
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()

	return nil
}
