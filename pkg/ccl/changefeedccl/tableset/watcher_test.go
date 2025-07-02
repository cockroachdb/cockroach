package tableset

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
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

	// TODO: we still don't see these. may have to do a separate initial scan (desc query)
	db.Exec("create table foo_initial (id int primary key)")
	db.Exec("create table bar_initial (id int primary key)")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeName("test-mm"),
		Limit:     1024 * 1024,
		Increment: 128,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(1024*2024))
	defer mm.Stop(ctx)

	var dbID descpb.ID
	require.NoError(t, execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbs, err := txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		if err != nil {
			return err
		}
		dbs = dbs.FilterByNames([]descpb.NameInfo{{Name: "defaultdb"}})
		dbs.ForEachDescriptor(func(desc catalog.Descriptor) error {
			dbID = desc.GetID()
			return nil
		})
		return nil
	}))

	filter := Filter{
		DatabaseID:    dbID,
		ExcludeTables: []string{"exclude_me", "exclude_me_also"},
	}
	watcher := NewWatcher(filter, &execCfg, mm, 42)

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

				if i%2 == 0 {
					db.Exec("drop table if exists exclude_me")
					db.Exec("drop table if exists foober")

					// db.Exec("alter table foo_initial add column bar int default 42")
					// db.Exec("alter table foo_initial drop column bar")

					db.Exec("rename table bar_initial to exclude_me_also")
					db.Exec("rename table foo_0 to boo_0")
				} else {
					db.Exec("create table if not exists exclude_me (id int primary key)")
					db.Exec("create table if not exists foober (id int primary key)")

					db.Exec("rename table exclude_me_also to bar_initial")
					db.Exec("rename table boo_0 to foo_0")
				}

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	var numQueries atomic.Int64
	eg.Go(func() error {
		for ctx.Err() == nil {
			time.Sleep(time.Second)
			eventTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			diffs, err := watcher.Pop(ctx, eventTS)
			if err != nil {
				return err
			}

			require.True(t, slices.IsSortedFunc(diffs, func(a, b TableDiff) int {
				return a.AsOf.Compare(b.AsOf)
			}))

			for _, diff := range diffs {
				require.True(t, diff.AsOf.Compare(eventTS) <= 0, "diff.AsOf.Compare(eventTs) <= 0")
			}

			for _, diff := range diffs {
				require.NotEqual(t, diff.Added.Name, "exclude_me")
				require.NotEqual(t, diff.Deleted.Name, "exclude_me")
			}

			fmt.Printf("popped diffs up to %s:\n", eventTS)
			for _, diff := range diffs {
				fmt.Printf("  %s\n", diff)
			}
			numQueries.Add(1)
		}
		return nil
	})

	time.AfterFunc(1*time.Minute, func() {
		cancel()
	})

	require.ErrorIs(t, eg.Wait(), context.Canceled)
	require.Greater(t, numQueries.Load(), int64(0))
}

func TestTablesetMoreRealisticUsageTODO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
}
