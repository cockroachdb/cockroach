// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tableset_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/tableset"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestTablesetBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, sdb, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sdb)
	db.Exec(t, "SELECT crdb_internal.set_vmodule('watcher=100')")

	// setup the watcher & its deps
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeName("test-mm"),
		Limit:     1024 * 1024 * 1024,
		Increment: 128,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(1024*2024))
	defer mm.Stop(ctx)

	dbID := getDatabaseID(t, ctx, &execCfg, "defaultdb")
	filter := tableset.Filter{
		DatabaseID: dbID,
		TableFilter: jobspb.FilterList{
			FilterType: tree.ExcludeFilter,
			Tables: map[string]pbtypes.Empty{
				"defaultdb.public.exclude_me":      {},
				"defaultdb.public.exclude_me_also": {},
			},
		},
	}
	filterWithIncludes := tableset.Filter{
		DatabaseID: dbID,
		TableFilter: jobspb.FilterList{
			FilterType: tree.IncludeFilter,
			Tables: map[string]pbtypes.Empty{
				"defaultdb.public.foo":    {},
				"defaultdb.public.bar":    {},
				"defaultdb.public.baz":    {},
				"defaultdb.public.foober": {},
			},
		},
	}

	testutils.RunValues(t, "filter", []tableset.Filter{filter, filterWithIncludes}, func(t *testing.T, filter tableset.Filter) {
		spawn := func(t *testing.T, initialTS hlc.Timestamp) (watcher *tableset.Watcher, shutdown func()) {
			var err error
			ctx, spawnCancel := context.WithCancel(ctx)

			watcher, err = tableset.NewWatcher(ctx, filter, &execCfg, mm, 42)
			require.NoError(t, err)
			eg, ctx := errgroup.WithContext(ctx)

			eg.Go(func() error {
				return watcher.Start(ctx, initialTS)
			})

			return watcher, func() {
				spawnCancel()
				require.ErrorIs(t, eg.Wait(), context.Canceled)
			}
		}
		cleanup := func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo")
			db.Exec(t, "DROP TABLE IF EXISTS bar")
			db.Exec(t, "DROP TABLE IF EXISTS baz")
			db.Exec(t, "DROP TABLE IF EXISTS exclude_me")
			db.Exec(t, "DROP TABLE IF EXISTS exclude_me_also")
			db.Exec(t, "DROP TABLE IF EXISTS foober")
			db.Exec(t, "DROP SCHEMA IF EXISTS foo")
			db.Exec(t, "DROP DATABASE IF EXISTS bar")
		}

		mkTable := func(name string) {
			db.Exec(t, fmt.Sprintf("CREATE TABLE %s (id int primary key)", name))
		}

		t.Run("no changes", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})

		t.Run("unrelated schema changes on a watched table", func(t *testing.T) {
			defer cleanup()
			mkTable("foo")

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE foo ADD COLUMN bar int default 42")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)

			db.Exec(t, "ALTER TABLE foo DROP COLUMN bar")

			adds, err = watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})

		t.Run("create & drop ignored table", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("exclude_me")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)

			db.Exec(t, "DROP TABLE exclude_me")

			adds, err = watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})

		t.Run("add watched table", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("foo")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, adds, 1)
			assert.Equal(t, "foo", adds[0].Table.Name)
		})

		t.Run("drop watched table", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("foo")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, adds, 1)
			assert.Equal(t, "foo", adds[0].Table.Name)

			db.Exec(t, "DROP TABLE foo")

			adds, err = watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})

		t.Run("multiple updates", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("foo")
			mkTable("bar")
			mkTable("baz")

			db.Exec(t, "DROP TABLE foo")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(adds), 3)
			// should contain foo, bar, baz add and foo drop
			assertContainsFunc(t, adds, func(add tableset.TableAdd) bool {
				return add.Table.Name == "foo"
			})
			assertContainsFunc(t, adds, func(add tableset.TableAdd) bool {
				return add.Table.Name == "bar"
			})
			assertContainsFunc(t, adds, func(add tableset.TableAdd) bool {
				return add.Table.Name == "baz"
			})
		})
		t.Run("rename watched table", func(t *testing.T) {
			defer cleanup()

			mkTable("foo")

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE foo RENAME TO bar")

			// NOTE: caller needs to know that this is a rename by checking table ids.
			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, adds, 1)
			assertContainsFunc(t, adds, func(add tableset.TableAdd) bool {
				return add.Table.Name == "bar"
			})
		})
		t.Run("rename ignored table", func(t *testing.T) {
			defer cleanup()
			mkTable("exclude_me")

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE exclude_me RENAME TO exclude_me_also")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})
		t.Run("rename watched table to ignored table", func(t *testing.T) {
			defer cleanup()
			mkTable("foo")

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE foo RENAME TO exclude_me")

			// NOTE: this means that we'll keep watching the old table forever. This aligns with existing changefeed behavior.
			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})
		t.Run("rename ignored table to watched table", func(t *testing.T) {
			defer cleanup()
			mkTable("exclude_me")

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE exclude_me RENAME TO foo")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, adds, 1)
			assert.Equal(t, "foo", adds[0].Table.Name)
		})
		t.Run("schemas and databases ignored", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(t, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "CREATE SCHEMA foo")
			db.Exec(t, "CREATE DATABASE bar")

			adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, adds)
		})
	})
}

func TestTablesetNonIdempotentProtection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, sdb, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sdb)
	db.Exec(t, "SELECT crdb_internal.set_vmodule('watcher=100')")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeName("test-mm"),
		Limit:     1024 * 1024,
		Increment: 128,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(1024*2024))
	defer mm.Stop(ctx)

	dbID := getDatabaseID(t, ctx, &execCfg, "defaultdb")
	filter := tableset.Filter{DatabaseID: dbID}
	watcher, err := tableset.NewWatcher(ctx, filter, &execCfg, mm, 42)
	require.NoError(t, err)

	db.Exec(t, "CREATE TABLE foo (id int primary key)")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return watcher.Start(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	})

	ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	_, err = watcher.PopUpTo(ctx, ts)
	require.NoError(t, err)

	_, err = watcher.PopUpTo(ctx, ts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "PopUpTo called with non-advancing timestamp")
}

func TestWatcherDropDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, sdb, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sdb)
	db.Exec(t, "SELECT crdb_internal.set_vmodule('watcher=100')")

	// setup the watcher & its deps
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeName("test-mm"),
		Limit:     1024 * 1024,
		Increment: 128,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(1024*2024))
	defer mm.Stop(ctx)

	dbID := getDatabaseID(t, ctx, &execCfg, "defaultdb")
	filter := tableset.Filter{DatabaseID: dbID}
	watcher, err := tableset.NewWatcher(ctx, filter, &execCfg, mm, 42)
	require.NoError(t, err)

	db.Exec(t, "CREATE TABLE foo (id int primary key)")
	db.Exec(t, "CREATE TABLE bar (id int primary key)")

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return watcher.Start(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	})

	db.Exec(t, "DROP DATABASE defaultdb")

	adds, err := watcher.PopUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	require.NoError(t, err)
	assert.Empty(t, adds)

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func getDatabaseID(
	t *testing.T, ctx context.Context, execCfg *sql.ExecutorConfig, name string,
) descpb.ID {
	var dbID descpb.ID
	require.NoError(t, execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbs, err := txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		if err != nil {
			return err
		}
		dbs = dbs.FilterByNames([]descpb.NameInfo{{Name: name}})
		require.NoError(t, dbs.ForEachDescriptor(func(desc catalog.Descriptor) error {
			dbID = desc.GetID()
			return nil
		}))
		return nil
	}))
	return dbID
}

func assertContainsFunc(
	t *testing.T, adds []tableset.TableAdd, f func(add tableset.TableAdd) bool,
) {
	assert.Greater(t, slices.IndexFunc(adds, f), -1)
}
