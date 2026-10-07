// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestEnumVersionSkewFromDistributedGatewayFlow verifies that queries don't
// fail with "comparison of two different versions of enum", and that prepared
// statements planned against an older enum version are reparsed, when the
// gateway flow of a distributed plan updates the plan's *types.T objects in
// place with a newer version of the enum (see cockroachlabs/cockroach#12711).
//
// The gateway flow hydrates its specs, which alias the plan's *types.T objects,
// with a fresh descriptor collection. In a read-committed transaction that
// collection can resolve a newer enum version than the one the plan was built
// against. Another session can then reuse a memo built at the old version and
// compare its histogram bounds against values at the new version.
func TestEnumVersionSkewFromDistributedGatewayFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test exercises the relaxation of the assertion for adjacent enum
	// versions, which test builds disable by default.
	defer tree.TestingRelaxEnumVersionSkewAssertion()()

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase:       "test",
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
	sqlDB.Exec(t, "CREATE DATABASE test")
	sqlDB.Exec(t, "CREATE TYPE te AS ENUM ('a', 'b', 'c')")
	sqlDB.Exec(t, "CREATE TABLE t (k INT PRIMARY KEY, e te)")
	sqlDB.Exec(t, `INSERT INTO t SELECT i, (ARRAY['a', 'b', 'c'])[(i % 3) + 1]::te
		FROM generate_series(0, 29) AS g(i)`)
	sqlDB.Exec(t, "ALTER TABLE t SPLIT AT VALUES (10), (20)")
	sqlDB.Exec(t, fmt.Sprintf(
		"ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 10), (ARRAY[%d], 20)",
		tc.Server(0).GetFirstStoreID(),
		tc.Server(1).GetFirstStoreID(),
		tc.Server(2).GetFirstStoreID(),
	))
	// The histogram on e holds the enum values at the version of the memo.
	sqlDB.Exec(t, "ANALYZE t")

	run := func(
		t *testing.T, round int, iso gosql.IsolationLevel, distsql string, expectReparse bool,
	) {
		// Use distinct statement text in each round so that it doesn't reuse a
		// memo cached by an earlier round.
		query := fmt.Sprintf("SELECT count(*) AS n%d FROM t WHERE e = $1", round)
		var n int

		// The victim builds the memo at the current enum version.
		victim, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = victim.Close() }()
		victimStmt, err := victim.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer func() { _ = victimStmt.Close() }()
		require.NoError(t, victimStmt.QueryRowContext(ctx, "a").Scan(&n))
		require.Equal(t, 10, n)

		// The poisoner's transaction leases the current enum version.
		poisoner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = poisoner.Close() }()
		_, err = poisoner.ExecContext(ctx, "SET distsql = "+distsql)
		require.NoError(t, err)
		tx, err := poisoner.BeginTx(ctx, &gosql.TxOptions{Isolation: iso})
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		poisonerStmt, err := tx.PrepareContext(ctx, query)
		require.NoError(t, err)
		require.NoError(t, poisonerStmt.QueryRowContext(ctx, "a").Scan(&n))
		require.Equal(t, 10, n)

		// Publish a new enum version with a read-only value. The ALTER itself
		// can't finish until the poisoner's transaction releases its lease on
		// the old version, so it runs in the background.
		newValue := fmt.Sprintf("r%d", round)
		alterDone := make(chan error, 1)
		go func() {
			_, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TYPE te ADD VALUE '%s'", newValue))
			alterDone <- err
		}()
		alterWaited := false
		defer func() {
			if !alterWaited {
				_ = tx.Rollback()
				<-alterDone
			}
		}()
		testutils.SucceedsSoon(t, func() error {
			var s string
			err := db.QueryRow(fmt.Sprintf("SELECT '%s'::te", newValue)).Scan(&s)
			if err == nil {
				return errors.New("new enum version not visible yet")
			}
			if !testutils.IsError(err, "enum value is not yet public") {
				return errors.Wrap(err, "new enum version not visible yet")
			}
			return nil
		})

		// The poisoner still plans against the old version, while its gateway
		// flow resolves the new one.
		require.NoError(t, poisonerStmt.QueryRowContext(ctx, "a").Scan(&n))
		require.Equal(t, 10, n)

		// The victim executes at the new version. Its prepared statement was
		// planned against the old version, so it should be reparsed.
		_, err = victim.ExecContext(ctx, "SET tracing = on")
		require.NoError(t, err)
		require.NoError(t, victimStmt.QueryRowContext(ctx, "a").Scan(&n))
		require.Equal(t, 10, n)
		_, err = victim.ExecContext(ctx, "SET tracing = off")
		require.NoError(t, err)
		var reparsed bool
		require.NoError(t, victim.QueryRowContext(ctx, `SELECT count(*) > 0 FROM [SHOW TRACE FOR SESSION]
			WHERE message LIKE 'reparsing prepared statement%'`).Scan(&reparsed))
		require.Equal(t, expectReparse, reparsed)

		require.NoError(t, tx.Commit())
		alterWaited = true
		require.NoError(t, <-alterDone)
	}

	// The read-committed distributed case must run first: once the enum has
	// changed, the table statistics cache re-stamps its histograms with the
	// shared column type, which would mask the comparison error. The next two
	// cases are controls: a serializable transaction resolves the old version
	// in the fresh collection too, and a local plan doesn't create one.
	t.Run("read_committed/distributed", func(t *testing.T) {
		run(t, 1, gosql.LevelReadCommitted, "always", true /* expectReparse */)
	})
	t.Run("serializable/distributed", func(t *testing.T) {
		run(t, 2, gosql.LevelSerializable, "always", true /* expectReparse */)
	})
	t.Run("read_committed/local", func(t *testing.T) {
		run(t, 3, gosql.LevelReadCommitted, "off", true /* expectReparse */)
	})
	// Without tracking the planned-against versions, the in-place update makes
	// the victim's prepared statement look up-to-date.
	t.Run("read_committed/distributed/planned_versions_disabled", func(t *testing.T) {
		const setting = "sql.opt.use_planned_udt_versions_for_staleness_checks.enabled"
		sqlDB.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = false", setting))
		defer sqlDB.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", setting))
		run(t, 4, gosql.LevelReadCommitted, "always", false /* expectReparse */)
	})
}

// TestEnumVersionSkewWithRenamedValue verifies that a memo built before a
// single-version swap of two enum names doesn't return incorrect results when
// it's reused after the gateway flow of a distributed plan updates the shared
// *types.T objects in place (see TestEnumVersionSkewFromDistributedGatewayFlow).
//
// The reused memo looks up e2 = 'a' by the physical representation that 'a'
// had when the memo was built, which now belongs to 'b'. Comparing the
// placeholder value for e against the histogram on e only involves 'c', which
// means the same thing in both versions, so the comparison must still fail
// because other values were renamed.
func TestEnumVersionSkewWithRenamedValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test exercises the relaxation of the assertion for adjacent enum
	// versions, which test builds disable by default.
	defer tree.TestingRelaxEnumVersionSkewAssertion()()

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			UseDatabase:       "test",
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
	sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_partial_collection.enabled = false")
	sqlDB.Exec(t, "CREATE DATABASE test")

	// run swaps the names of 'a' and 'b' of a fresh enum type while the victim
	// has a prepared statement planned against the old version, and it returns
	// the result of the victim's statement after the swap as well as whether
	// the statement was reparsed.
	run := func(t *testing.T, round int) (n int, label string, reparsed bool, _ error) {
		typ, tab := fmt.Sprintf("te%d", round), fmt.Sprintf("t%d", round)
		sqlDB.Exec(t, fmt.Sprintf("CREATE TYPE %s AS ENUM ('a', 'b', 'c')", typ))
		sqlDB.Exec(t, fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, e %s, e2 %s)", tab, typ, typ))
		// e is always 'c'. e2 is 'a' in 10 rows and 'b' in 20 rows.
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO %s SELECT i, 'c', IF(i %% 3 = 0, 'a', 'b')::%s
			FROM generate_series(0, 29) AS g(i)`, tab, typ))
		sqlDB.Exec(t, fmt.Sprintf("ALTER TABLE %s SPLIT AT VALUES (10), (20)", tab))
		sqlDB.Exec(t, fmt.Sprintf(
			"ALTER TABLE %s EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 10), (ARRAY[%d], 20)",
			tab,
			tc.Server(0).GetFirstStoreID(),
			tc.Server(1).GetFirstStoreID(),
			tc.Server(2).GetFirstStoreID(),
		))
		// Only e has a histogram, and it only contains 'c'.
		sqlDB.Exec(t, fmt.Sprintf("CREATE STATISTICS s ON e FROM %s", tab))

		query := fmt.Sprintf("SELECT count(*), min(e2::STRING) FROM %s WHERE e = $1 AND e2 = 'a'", tab)

		// The victim builds the memo at the current enum version.
		victim, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = victim.Close() }()
		victimStmt, err := victim.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer func() { _ = victimStmt.Close() }()
		require.NoError(t, victimStmt.QueryRowContext(ctx, "c").Scan(&n, &label))
		require.Equal(t, 10, n)
		require.Equal(t, "a", label)

		// The poisoner's transaction leases the current enum version.
		poisoner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() { _ = poisoner.Close() }()
		_, err = poisoner.ExecContext(ctx, "SET distsql = always")
		require.NoError(t, err)
		tx, err := poisoner.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		poisonerStmt, err := tx.PrepareContext(ctx, query)
		require.NoError(t, err)
		require.NoError(t, poisonerStmt.QueryRowContext(ctx, "c").Scan(&n, &label))

		// Swap the names of 'a' and 'b' in a single version, which only the
		// legacy schema changer does (the declarative one stages renames across
		// several versions). The swap can't finish until the poisoner's
		// transaction releases its lease on the old version, so it runs in the
		// background.
		alterDone := make(chan error, 1)
		go func() {
			alterDone <- func() error {
				conn, err := db.Conn(ctx)
				if err != nil {
					return err
				}
				defer func() { _ = conn.Close() }()
				for _, stmt := range []string{
					"SET use_declarative_schema_changer = off",
					"SET autocommit_before_ddl = false",
					"BEGIN",
					fmt.Sprintf("ALTER TYPE %s RENAME VALUE 'a' TO 'tmp'", typ),
					fmt.Sprintf("ALTER TYPE %s RENAME VALUE 'b' TO 'a'", typ),
					fmt.Sprintf("ALTER TYPE %s RENAME VALUE 'tmp' TO 'b'", typ),
					"COMMIT",
				} {
					if _, err := conn.ExecContext(ctx, stmt); err != nil {
						return err
					}
				}
				return nil
			}()
		}()
		alterWaited := false
		defer func() {
			if !alterWaited {
				_ = tx.Rollback()
				<-alterDone
			}
		}()
		testutils.SucceedsSoon(t, func() error {
			var first string
			if err := db.QueryRow(fmt.Sprintf("SELECT enum_first(NULL::%s)::STRING", typ)).Scan(&first); err != nil {
				return err
			}
			if first != "b" {
				return errors.Newf("swapped names not visible yet: first value is %s", first)
			}
			return nil
		})

		// The poisoner still plans against the old version, while its gateway
		// flow resolves the new one and updates the shared types in place.
		require.NoError(t, poisonerStmt.QueryRowContext(ctx, "c").Scan(&n, &label))

		_, err = victim.ExecContext(ctx, "SET tracing = on")
		require.NoError(t, err)
		victimErr := victimStmt.QueryRowContext(ctx, "c").Scan(&n, &label)
		_, err = victim.ExecContext(ctx, "SET tracing = off")
		require.NoError(t, err)
		require.NoError(t, victim.QueryRowContext(ctx, `SELECT count(*) > 0 FROM [SHOW TRACE FOR SESSION]
			WHERE message LIKE 'reparsing prepared statement%'`).Scan(&reparsed))

		require.NoError(t, tx.Commit())
		alterWaited = true
		require.NoError(t, <-alterDone)
		return n, label, reparsed, victimErr
	}

	// The victim's prepared statement was planned against the old version, so
	// it's reparsed and returns the rows that are now 'a'.
	t.Run("planned_versions_enabled", func(t *testing.T) {
		n, label, reparsed, err := run(t, 1)
		require.NoError(t, err)
		require.True(t, reparsed)
		require.Equal(t, 20, n)
		require.Equal(t, "a", label)
	})
	// Without tracking the planned-against versions, the in-place update makes
	// the victim's memo look up-to-date, so it's reused, and the comparison of
	// the placeholder value against the histogram must fail rather than let
	// the memo return the rows that are now 'b'.
	t.Run("planned_versions_disabled", func(t *testing.T) {
		const setting = "sql.opt.use_planned_udt_versions_for_staleness_checks.enabled"
		sqlDB.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = false", setting))
		defer sqlDB.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", setting))
		_, _, reparsed, err := run(t, 2)
		require.ErrorContains(t, err, "comparison of two different versions of enum")
		require.False(t, reparsed)
	})
}
