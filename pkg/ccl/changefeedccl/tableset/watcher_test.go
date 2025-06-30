package tableset

import (
	"context"
	"fmt"
	"slices"
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
		ExcludeTables: []string{"exclude_me"},
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
				} else {
					db.Exec("create table if not exists exclude_me (id int primary key)")
					db.Exec("create table if not exists foober (id int primary key)")
				}

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	eg.Go(func() error {
		curTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		for ctx.Err() == nil {
			time.Sleep(time.Second)
			newTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			diffs, err := watcher.PeekDiffs(ctx, curTS, newTS)
			require.NoError(t, err)

			require.True(t, slices.IsSortedFunc(diffs, func(a, b TableDiff) int {
				return a.AsOf.Compare(b.AsOf)
			}))

			if len(diffs) > 0 {
				require.True(t, diffs[0].AsOf.Compare(curTS) >= 0, "diffs[0].AsOf.Compare(curTS) >= 0")
				require.True(t, diffs[len(diffs)-1].AsOf.Compare(newTS) <= 0, "diffs[len(diffs)-1].AsOf.Compare(newTS) <= 0")
			}

			fmt.Printf("peeked diffs from %s to %s:\n", curTS, newTS)
			for _, diff := range diffs {
				fmt.Printf("  %s\n", diff)
			}
			curTS = newTS
		}
		return nil
	})

	time.AfterFunc(1*time.Minute, func() {
		cancel()
	})

	require.ErrorIs(t, eg.Wait(), context.Canceled)
}
