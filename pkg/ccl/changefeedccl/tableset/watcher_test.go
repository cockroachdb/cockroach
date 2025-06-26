package tableset

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestTablesetDebug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeName("test-mm"),
		Limit:     1024 * 1024,
		Increment: 128,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(1024*2024))
	defer mm.Stop(ctx)

	var dbId descpb.ID
	require.NoError(t, execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbs, err := txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		if err != nil {
			return err
		}
		dbs = dbs.FilterByNames([]descpb.NameInfo{{Name: "defaultdb"}})
		dbs.ForEachDescriptor(func(desc catalog.Descriptor) error {
			dbId = desc.GetID()
			return nil
		})
		return nil
	}))

	filter := Filter{
		DatabaseID:    dbId,
		ExcludeTables: []string{"exclude_me"},
	}
	watcher := NewWatcher(filter, &execCfg, mm, "testwatcher1")

	eg, ctx := errgroup.WithContext(ctx)

	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	eg.Go(func() error {
		return watcher.Start(ctx, ts)
	})

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	eg.Go(func() error {
		for i := 0; ; i++ {
			select {
			case <-ticker.C:
				db.Exec(fmt.Sprintf("create table foo_%d (id int primary key)", i))
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	time.AfterFunc(1*time.Minute, func() {
		cancel()
	})

	require.NoError(t, eg.Wait())
}
