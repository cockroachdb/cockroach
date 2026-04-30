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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		Limit:     1024 * 1024,
		Increment: 128,
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(1024*2024))
	defer mm.Stop(ctx)

	dbID := getDatabaseID(t, ctx, &execCfg, "defaultdb")
	filter := tableset.Filter{
		DatabaseID:    dbID,
		ExcludeTables: map[string]struct{}{"exclude_me": {}, "exclude_me_also": {}},
	}
	filterWithIncludes := tableset.Filter{
		DatabaseID:    dbID,
		IncludeTables: map[string]struct{}{"foo": {}, "bar": {}, "baz": {}, "foober": {}},
	}

	testutils.RunValues(t, "filter", []tableset.Filter{filter, filterWithIncludes}, func(t *testing.T, filter tableset.Filter) {
		spawn := func(initialTS hlc.Timestamp) (watcher *tableset.Watcher, shutdown func()) {
			ctx, spawnCancel := context.WithCancel(ctx)

			watcher = tableset.NewWatcher(filter, &execCfg, mm, 42)
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
		}

		mkTable := func(name string) {
			db.Exec(t, fmt.Sprintf("CREATE TABLE %s (id int primary key)", name))
		}

		t.Run("no changes", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, diffs)
			assert.True(t, unchanged)
		})

		t.Run("unrelated schema changes on a watched table", func(t *testing.T) {
			defer cleanup()
			mkTable("foo")

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE foo ADD COLUMN bar int default 42")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, diffs)
			assert.True(t, unchanged)

			db.Exec(t, "ALTER TABLE foo DROP COLUMN bar")

			unchanged, diffs, err = watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, diffs)
			assert.True(t, unchanged)
		})

		t.Run("create & drop ignored table", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()
			db.Exec(t, "CREATE SCHEMA foo")
			mkTable("foo.exclude_me")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.NotEmpty(t, diffs)
			assert.False(t, unchanged)

			db.Exec(t, "DROP TABLE foo.exclude_me")

			unchanged, diffs, err = watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.NotEmpty(t, diffs)
			assert.False(t, unchanged)
		})

		t.Run("add watched table", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("foo")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, diffs, 1)
			assert.Equal(t, "foo", diffs[0].Added.Name)
			assert.Zero(t, diffs[0].Dropped.Name)
			assert.False(t, unchanged)
		})

		t.Run("drop watched table", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("foo")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, diffs, 1)
			assert.Equal(t, "foo", diffs[0].Added.Name)
			assert.Zero(t, diffs[0].Dropped.Name)
			assert.False(t, unchanged)

			db.Exec(t, "DROP TABLE foo")

			unchanged, diffs, err = watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Len(t, diffs, 1)
			assert.Equalf(t, "foo", diffs[0].Dropped.Name, "got %+v", diffs)
			assert.Zero(t, diffs[0].Added.Name)
			assert.False(t, unchanged)
		})

		t.Run("multiple updates", func(t *testing.T) {
			defer cleanup()

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			mkTable("foo")
			mkTable("bar")
			mkTable("baz")

			db.Exec(t, "DROP TABLE foo")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(diffs), 3)
			assert.False(t, unchanged)
			// should contain foo, bar, baz add and foo drop
			assertContainsFunc(t, diffs, func(diff tableset.TableDiff) bool {
				return diff.Added.Name == "foo"
			})
			assertContainsFunc(t, diffs, func(diff tableset.TableDiff) bool {
				return diff.Added.Name == "bar"
			})
			assertContainsFunc(t, diffs, func(diff tableset.TableDiff) bool {
				return diff.Added.Name == "baz"
			})
			assertContainsFunc(t, diffs, func(diff tableset.TableDiff) bool {
				return diff.Dropped.Name == "foo"
			})
		})
		t.Run("rename watched table", func(t *testing.T) {
			defer cleanup()

			mkTable("foo")

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE foo RENAME TO bar")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			// a rename looks like a drop then an add.
			// they SHOULD always be in the same txn, i think.
			// NOTE: we don't actually need to restart the changefeed if we see this.. but we have no way to know that.
			assert.Len(t, diffs, 2)
			assert.False(t, unchanged)
			assertContainsFunc(t, diffs, func(diff tableset.TableDiff) bool {
				return diff.Added.Name == "bar"
			})
			assertContainsFunc(t, diffs, func(diff tableset.TableDiff) bool {
				return diff.Dropped.Name == "foo"
			})
		})
		t.Run("rename ignored table", func(t *testing.T) {
			defer cleanup()
			mkTable("exclude_me")

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE exclude_me RENAME TO exclude_me_also")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			assert.Empty(t, diffs)
			assert.True(t, unchanged)
		})
		t.Run("rename watched table to ignored table", func(t *testing.T) {
			defer cleanup()
			mkTable("foo")

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE foo RENAME TO exclude_me")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			// a rename looks like a drop then an add. since we ignore the new name, we only see the drop.
			assert.Len(t, diffs, 1)
			assert.Zero(t, diffs[0].Added.Name)
			assert.Equalf(t, "foo", diffs[0].Dropped.Name, "got %+v", diffs)
			assert.False(t, unchanged)
		})
		t.Run("rename ignored table to watched table", func(t *testing.T) {
			defer cleanup()
			mkTable("exclude_me")

			watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			defer shutdown()

			db.Exec(t, "ALTER TABLE exclude_me RENAME TO foo")

			unchanged, diffs, err := watcher.PopUnchangedUpTo(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
			require.NoError(t, err)
			// a rename looks like a drop then an add. since we ignore the original table, we only see the add.
			assert.Len(t, diffs, 1)
			assert.Zero(t, diffs[0].Dropped.Name)
			assert.Equal(t, "foo", diffs[0].Added.Name)
			assert.False(t, unchanged)
		})
		// NOTE(#issue): offline tables are not supported currently.
	})
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
	t *testing.T, diffs []tableset.TableDiff, f func(diff tableset.TableDiff) bool,
) {
	assert.Greater(t, slices.IndexFunc(diffs, f), -1)
}
