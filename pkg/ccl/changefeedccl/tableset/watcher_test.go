package tableset

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
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
	db.Exec(t, "SELECT crdb_internal.set_vmodule('watcher=2')")

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
	filter := Filter{
		DatabaseID:    dbID,
		ExcludeTables: []string{"exclude_me", "exclude_me_also"},
	}

	spawn := func(initialTS hlc.Timestamp) (watcher *Watcher, shutdown func()) {
		ctx, spawnCancel := context.WithCancel(ctx)

		watcher = NewWatcher(filter, &execCfg, mm, 42)
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
		db.Exec(t, "drop table if exists foo")
		db.Exec(t, "drop table if exists bar")
		db.Exec(t, "drop table if exists baz")
		db.Exec(t, "drop table if exists exclude_me")
		db.Exec(t, "drop table if exists exclude_me_also")
		db.Exec(t, "drop table if exists foober")
	}

	mkTable := func(name string) {
		db.Exec(t, fmt.Sprintf("create table %s (id int primary key)", name))
	}

	t.Run("no changes", func(t *testing.T) {
		defer cleanup()

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Empty(t, diffs)
	})

	t.Run("unrelated schema changes", func(t *testing.T) {
		defer cleanup()
		mkTable("foo_e")

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		db.Exec(t, "alter table foo_e add column bar int default 42")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Empty(t, diffs)

		db.Exec(t, "alter table foo_e drop column bar")

		diffs, err = watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Empty(t, diffs)
	})

	t.Run("create & drop ignored table", func(t *testing.T) {
		defer cleanup()

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		mkTable("exclude_me")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Empty(t, diffs)

		db.Exec(t, "drop table exclude_me")

		diffs, err = watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Empty(t, diffs)
	})

	t.Run("add watched table", func(t *testing.T) {
		defer cleanup()

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		mkTable("foo")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "foo", diffs[0].Added.Name)
		assert.Zero(t, diffs[0].Deleted.Name)
	})

	t.Run("drop watched table", func(t *testing.T) {
		defer cleanup()

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		mkTable("foo")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "foo", diffs[0].Added.Name)
		assert.Zero(t, diffs[0].Deleted.Name)

		db.Exec(t, "drop table foo")

		diffs, err = watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Len(t, diffs, 1)
		assert.Equalf(t, "foo", diffs[0].Deleted.Name, "got %+v", diffs)
		assert.Zero(t, diffs[0].Added.Name)
	})

	t.Run("multiple updates", func(t *testing.T) {
		defer cleanup()

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		mkTable("foo")
		mkTable("bar")
		mkTable("baz")

		db.Exec(t, "drop table foo")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(diffs), 3)
		// should contain foo, bar, baz add and foo drop
		assertContainsFunc(t, diffs, func(diff TableDiff) bool {
			return diff.Added.Name == "foo"
		})
		assertContainsFunc(t, diffs, func(diff TableDiff) bool {
			return diff.Added.Name == "bar"
		})
		assertContainsFunc(t, diffs, func(diff TableDiff) bool {
			return diff.Added.Name == "baz"
		})
		assertContainsFunc(t, diffs, func(diff TableDiff) bool {
			return diff.Deleted.Name == "foo"
		})
	})
	t.Run("rename watched table", func(t *testing.T) {
		defer cleanup()

		mkTable("foo")

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		db.Exec(t, "alter table foo rename to bar")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		// a rename looks like a drop then an add.
		// they SHOULD always be in the same txn, i think.
		// NOTE: we don't actually need to restart the changefeed if we see this.. but we have no way to know that.
		assert.Len(t, diffs, 2)
		assertContainsFunc(t, diffs, func(diff TableDiff) bool {
			return diff.Added.Name == "bar"
		})
		assertContainsFunc(t, diffs, func(diff TableDiff) bool {
			return diff.Deleted.Name == "foo"
		})
	})
	t.Run("rename ignored table", func(t *testing.T) {
		defer cleanup()
		mkTable("exclude_me")

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		db.Exec(t, "alter table exclude_me rename to exclude_me_also")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Empty(t, diffs)
	})
	t.Run("rename watched table to ignored table", func(t *testing.T) {
		defer cleanup()
		mkTable("foo")

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		db.Exec(t, "alter table foo rename to exclude_me")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		// a rename looks like a drop then an add. since we ignore the new name, we only see the drop.
		assert.Len(t, diffs, 1)
		assert.Zero(t, diffs[0].Added.Name)
		assert.Equalf(t, "foo", diffs[0].Deleted.Name, "got %+v", diffs)
	})
	t.Run("rename ignored table to watched table", func(t *testing.T) {
		defer cleanup()
		mkTable("exclude_me")

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		db.Exec(t, "alter table exclude_me rename to foo")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		// a rename looks like a drop then an add. since we ignore the original table, we only see the add.
		assert.Len(t, diffs, 1)
		assert.Zero(t, diffs[0].Deleted.Name)
		assert.Equal(t, "foo", diffs[0].Added.Name)
	})

	// offline stuff
	t.Run("watched table goes offline", func(t *testing.T) {
		t.Skip("tableset doesn't support offline tables currently")

		defer cleanup()
		mkTable("foo_import_1") // NOTE: offline tables can't be dropped, so we never clean this up.

		watcher, shutdown := spawn(hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		defer shutdown()

		// TODO: is there a cleaner way to do this?
		mkBlockForeverFile(t, s.ExternalIODir(), "foo_import_1.csv")
		_, err := sdb.ExecContext(ctx, "import into foo_import_1 (id) CSV DATA ('nodelocal://self/foo_import_1.csv') WITH DETACHED")
		require.NoError(t, err)

		waitForTableState(t, db, "foo_import_1", "OFFLINE")

		diffs, err := watcher.Pop(ctx, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
		require.NoError(t, err)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "foo_import_1", diffs[0].Deleted.Name)
		assert.Zero(t, diffs[0].Added.Name)
	})

	t.Run("watched offline table goes online", func(t *testing.T) {
		t.Skip("tableset doesn't support offline tables currently")
	})
}

func getDatabaseID(t *testing.T, ctx context.Context, execCfg *sql.ExecutorConfig, name string) descpb.ID {
	var dbID descpb.ID
	require.NoError(t, execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		dbs, err := txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		if err != nil {
			return err
		}
		dbs = dbs.FilterByNames([]descpb.NameInfo{{Name: name}})
		dbs.ForEachDescriptor(func(desc catalog.Descriptor) error {
			dbID = desc.GetID()
			return nil
		})
		return nil
	}))
	return dbID
}

func assertContainsFunc(t *testing.T, diffs []TableDiff, f func(diff TableDiff) bool) {
	assert.Greater(t, slices.IndexFunc(diffs, f), -1)
}

func waitForTableState(t *testing.T, db *sqlutils.SQLRunner, tableName string, targetState string) {
	for state := "ONLINE"; state != targetState; {
		db.QueryRow(t, fmt.Sprintf(`with descs as (select id, crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) as desc from system.descriptor)
		select coalesce("desc"->'table'->>'state', 'ONLINE') from descs where "desc"->'table'->>'name' = '%s'`, tableName)).Scan(&state)
		time.Sleep(time.Second)
	}
}

func mkBlockForeverFile(t *testing.T, dir, name string) {
	filePath := path.Join(dir, name)
	err := exec.Command("mkfifo", filePath).Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(filePath)
	})
}
