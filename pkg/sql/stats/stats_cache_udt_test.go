// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCacheEnumVersionChange verifies that an enum type version bump only
// re-reads the referencing tables' statistics when a value was dropped.
func TestCacheEnumVersionChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "many schema changes and stats collections")
	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	// USE is per-session, so every statement must run on one connection.
	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	r := sqlutils.MakeSQLRunner(conn)
	sc := s.ExecutorConfig().(sql.ExecutorConfig).TableStatsCache

	// Background stats writes would invalidate entries, and internal queries on
	// system tables would consult the cache, polluting the counts.
	r.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	r.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_partial_collection.enabled = false`)
	r.Exec(t, `SET CLUSTER SETTING sql.stats.system_tables.enabled = false`)
	r.Exec(t, `SET CLUSTER SETTING sql.stats.enum_type_rehydration.enabled = true`)

	const numTables = 3
	const usdRows, eurRows, gbpRows = 4, 3, 2

	waitForSchemaChanges := func() {
		r.CheckQueryResultsRetry(t, `
SELECT job_id, status, error, description FROM [SHOW JOBS]
WHERE job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE', 'TYPEDESC SCHEMA CHANGE')
AND status != 'succeeded'`, [][]string{})
	}

	setup := func(t *testing.T, db string) {
		r.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, db))
		r.Exec(t, fmt.Sprintf(`USE %s`, db))
		r.Exec(t, `CREATE TYPE cur AS ENUM ('usd', 'eur', 'gbp')`)
		for i := range numTables {
			r.Exec(t, fmt.Sprintf(`CREATE TABLE t%d (k INT PRIMARY KEY, c cur, INDEX (c))`, i))
			r.Exec(t, fmt.Sprintf(`INSERT INTO t%d
SELECT i, 'usd' FROM generate_series(1, %d) AS g(i)
UNION ALL SELECT i + %d, 'eur' FROM generate_series(1, %d) AS g(i)
UNION ALL SELECT i + %d, 'gbp' FROM generate_series(1, %d) AS g(i)`,
				i, usdRows, usdRows, eurRows, usdRows+eurRows, gbpRows))
			r.Exec(t, fmt.Sprintf(`CREATE STATISTICS s FROM t%d`, i))
		}
		waitForSchemaChanges()
	}

	// probe's query text is identical across calls so that the plan cache
	// decides whether the stats cache is consulted.
	probe := func(t *testing.T, value string) {
		for i := range numTables {
			r.Exec(t, fmt.Sprintf(`SELECT k FROM t%d WHERE c = '%s'`, i, value))
		}
	}

	estimatedRowsRE := regexp.MustCompile(`estimated row count: (\d+)`)
	estimatedRows := func(t *testing.T, value string) int {
		rows := r.QueryStr(t, fmt.Sprintf(`EXPLAIN SELECT k FROM t0 WHERE c = '%s'`, value))
		for _, row := range rows {
			if m := estimatedRowsRE.FindStringSubmatch(row[0]); m != nil {
				n, err := strconv.Atoi(m[1])
				require.NoError(t, err)
				return n
			}
		}
		t.Fatalf("no row estimate in EXPLAIN output: %v", rows)
		return 0
	}

	// warm probes until the cache is warm for every table. The rangefeed can
	// invalidate an entry shortly after CREATE STATISTICS.
	warm := func(t *testing.T) {
		testutils.SucceedsSoon(t, func() error {
			before := sc.Metrics().Misses.Count()
			probe(t, "usd")
			if n := sc.Metrics().Misses.Count() - before; n != 0 {
				return errors.Newf("cache not yet warm: %d misses", n)
			}
			return nil
		})
	}

	// probeMisses returns the stats cache misses and lookups of a probe. Callers
	// check lookups to prove the cache was consulted rather than bypassed by a
	// cached plan.
	probeMisses := func(t *testing.T) (misses, lookups int64) {
		before := sc.Metrics().Misses.Count()
		hitsBefore := sc.Metrics().Hits.Count()
		probe(t, "usd")
		misses = sc.Metrics().Misses.Count() - before
		hits := sc.Metrics().Hits.Count() - hitsBefore
		t.Logf("misses=%d hits=%d", misses, hits)
		return misses, misses + hits
	}

	missesDuring := func(t *testing.T, change string) (misses, lookups int64) {
		warm(t)
		r.Exec(t, change)
		waitForSchemaChanges()
		return probeMisses(t)
	}

	typeChange := func(t *testing.T, change string, wantMisses int64) {
		misses, lookups := missesDuring(t, change)
		require.GreaterOrEqual(t, lookups, int64(numTables), "probe did not consult the cache")
		require.Equal(t, wantMisses, misses)
	}

	t.Run("add value", func(t *testing.T) {
		setup(t, "add_value")
		typeChange(t, `ALTER TYPE cur ADD VALUE 'jpy'`, 0)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
	})

	t.Run("rename value", func(t *testing.T) {
		setup(t, "rename_value")
		typeChange(t, `ALTER TYPE cur RENAME VALUE 'eur' TO 'euro'`, 0)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
		require.Equal(t, eurRows, estimatedRows(t, "euro"))
	})

	t.Run("rename type", func(t *testing.T) {
		setup(t, "rename_type")
		typeChange(t, `ALTER TYPE cur RENAME TO currency`, 0)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
	})

	t.Run("new referencing table", func(t *testing.T) {
		setup(t, "new_table")
		typeChange(t, `CREATE TABLE extra (k INT PRIMARY KEY, c cur)`, 0)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
	})

	t.Run("drop value", func(t *testing.T) {
		setup(t, "drop_value")
		for i := range numTables {
			r.Exec(t, fmt.Sprintf(`DELETE FROM t%d WHERE c = 'gbp'`, i))
		}
		// The drop is a two-step job, so a refill decoded against the
		// intermediate version can be evicted a second time.
		misses, _ := missesDuring(t, `ALTER TYPE cur DROP VALUE 'gbp'`)
		require.GreaterOrEqual(t, misses, int64(numTables))
	})

	t.Run("rehydration disabled", func(t *testing.T) {
		setup(t, "disabled")
		r.Exec(t, `SET CLUSTER SETTING sql.stats.enum_type_rehydration.enabled = false`)
		defer r.Exec(t, `SET CLUSTER SETTING sql.stats.enum_type_rehydration.enabled = true`)
		typeChange(t, `ALTER TYPE cur ADD VALUE 'jpy'`, numTables)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
	})

	// olderCaller pins a transaction's lease on the type at the current
	// version, then bumps the type and refills the cache from other sessions
	// so that the transaction's next lookup lags the cache. It returns that
	// lookup's misses and estimate for 'eur'.
	olderCaller := func(t *testing.T, db string) (misses int64, eurEstimate int) {
		setup(t, db)
		warm(t)
		r.Exec(t, `BEGIN`)
		open := true
		var alterDone chan struct{}
		var alterErr error
		defer func() {
			// The ALTER cannot finish until the transaction releases its lease.
			if open {
				r.Exec(t, `ROLLBACK`)
			}
			if alterDone != nil {
				<-alterDone
			}
		}()
		probe(t, "usd")

		// The schema change statement waits for its job, which waits for the
		// pinned lease, but the new version, with 'jpy' read-only, is published
		// before that wait. Other sessions lease it once the lease manager
		// notices, which the cast observes.
		alterDone = make(chan struct{})
		go func() {
			defer close(alterDone)
			_, alterErr = sqlDB.ExecContext(ctx, fmt.Sprintf(`ALTER TYPE %s.cur ADD VALUE 'jpy'`, db))
		}()
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.ExecContext(ctx, fmt.Sprintf(`SELECT 'jpy'::%s.cur`, db))
			if err == nil {
				return errors.New("new value is already public")
			}
			if !strings.Contains(err.Error(), "not yet public") {
				return errors.Wrap(err, "new type version not yet leased")
			}
			return nil
		})
		_, err := sqlDB.ExecContext(ctx, fmt.Sprintf(`SELECT k FROM %s.t0 WHERE c = 'usd'`, db))
		require.NoError(t, err)

		// A new query text forces a plan, and so a lookup, at the pinned version.
		before := sc.Metrics().Misses.Count()
		eurEstimate = estimatedRows(t, "eur")
		misses = sc.Metrics().Misses.Count() - before
		t.Logf("older caller: misses=%d", misses)
		r.Exec(t, `COMMIT`)
		open = false
		<-alterDone
		require.NoError(t, alterErr)
		waitForSchemaChanges()
		return misses, eurEstimate
	}

	t.Run("older caller", func(t *testing.T) {
		misses, eurEstimate := olderCaller(t, "older_caller")
		require.Equal(t, int64(0), misses)
		require.Equal(t, eurRows, eurEstimate)
	})

	t.Run("older caller, rehydration disabled", func(t *testing.T) {
		r.Exec(t, `SET CLUSTER SETTING sql.stats.enum_type_rehydration.enabled = false`)
		defer r.Exec(t, `SET CLUSTER SETTING sql.stats.enum_type_rehydration.enabled = true`)
		misses, eurEstimate := olderCaller(t, "older_caller_disabled")
		require.GreaterOrEqual(t, misses, int64(1))
		require.Equal(t, eurRows, eurEstimate)
	})

	// The type version seen inside the transaction is never committed. The
	// entry is re-stamped to it and back without a re-read.
	t.Run("uncommitted type", func(t *testing.T) {
		setup(t, "uncommitted")
		warm(t)
		r.Exec(t, `BEGIN`)
		r.Exec(t, `ALTER TYPE cur ADD VALUE 'jpy'`)
		misses, lookups := probeMisses(t)
		require.GreaterOrEqual(t, lookups, int64(numTables), "probe did not consult the cache")
		require.Equal(t, int64(0), misses)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
		r.Exec(t, `ROLLBACK`)
		misses, lookups = probeMisses(t)
		require.GreaterOrEqual(t, lookups, int64(numTables), "probe did not consult the cache")
		require.Equal(t, int64(0), misses)
	})

	t.Run("table change control", func(t *testing.T) {
		setup(t, "control")
		// Only t0's plan is invalidated, so only t0 is looked up.
		misses, lookups := missesDuring(t, `ALTER TABLE t0 ADD COLUMN z INT`)
		require.GreaterOrEqual(t, lookups, int64(1), "probe did not consult the cache")
		require.Equal(t, int64(0), misses)
		require.Equal(t, usdRows, estimatedRows(t, "usd"))
	})
}
