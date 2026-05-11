// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgbench

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		scale       int
		batchSize   int
		mode        string
		expectedErr string
	}{
		{name: "defaults", scale: defaultScale, batchSize: defaultBatchSize, mode: defaultMode},
		{name: "tpcb mode", scale: 1, batchSize: 1, mode: modeTPCB},
		{name: "select-only mode", scale: 1, batchSize: 1, mode: modeSelectOnly},
		{name: "simple-update mode", scale: 1, batchSize: 1, mode: modeSimpleUpdate},
		{name: "scale zero rejected", scale: 0, batchSize: 1, mode: modeTPCB,
			expectedErr: "scale must be >= 1"},
		{name: "negative scale rejected", scale: -1, batchSize: 1, mode: modeTPCB,
			expectedErr: "scale must be >= 1"},
		{name: "zero batch rejected", scale: 1, batchSize: 0, mode: modeTPCB,
			expectedErr: "batch-size must be > 0"},
		{name: "unknown mode rejected", scale: 1, batchSize: 1, mode: "oltp",
			expectedErr: "invalid mode"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := &pgbench{scale: tc.scale, batchSize: tc.batchSize, mode: tc.mode}
			err := g.Hooks().Validate()
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCheckConsistency exercises the consistency check directly against a
// hand-built fixture so the assertion logic can be tested without paying for
// a full scale=1 init.
func TestCheckConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE test`)

	// Build a fixture matching scale=1 cardinalities: branches=1, tellers=10,
	// accounts=100000, history empty.
	setup := func(t *testing.T) {
		t.Helper()
		sqlDB.Exec(t, `DROP TABLE IF EXISTS pgbench_history, pgbench_accounts, pgbench_tellers, pgbench_branches`)
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE pgbench_branches %s`, branchesSchema))
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE pgbench_tellers %s`, tellersSchema))
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE pgbench_accounts %s`, accountsSchema))
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE pgbench_history %s`, historySchema))
		sqlDB.Exec(t, `INSERT INTO pgbench_branches (bid, bbalance) VALUES (1, 0)`)
		sqlDB.Exec(t, `INSERT INTO pgbench_tellers (tid, bid, tbalance)
			SELECT g, (g - 1) // 10 + 1, 0 FROM generate_series(1, 10) AS g`)
		sqlDB.Exec(t, `INSERT INTO pgbench_accounts (aid, bid, abalance)
			SELECT g, (g - 1) // 100000 + 1, 0 FROM generate_series(1, 100000) AS g`)
	}

	gen := &pgbench{scale: 1}
	hooks := gen.Hooks()

	t.Run("happy path: all sums zero", func(t *testing.T) {
		setup(t)
		require.NoError(t, hooks.CheckConsistency(ctx, db))
	})

	t.Run("balance sums match a non-zero offset", func(t *testing.T) {
		setup(t)
		// Apply a +7 to one of each balance and to history so all sums are 7.
		sqlDB.Exec(t, `UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1`)
		sqlDB.Exec(t, `UPDATE pgbench_branches SET bbalance = 7 WHERE bid = 1`)
		sqlDB.Exec(t, `UPDATE pgbench_tellers SET tbalance = 7 WHERE tid = 1`)
		sqlDB.Exec(t, `INSERT INTO pgbench_history (tid, bid, aid, delta) VALUES (1, 1, 1, 7)`)
		require.NoError(t, hooks.CheckConsistency(ctx, db))
	})

	t.Run("missing history row detected", func(t *testing.T) {
		setup(t)
		sqlDB.Exec(t, `UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1`)
		sqlDB.Exec(t, `UPDATE pgbench_branches SET bbalance = 7 WHERE bid = 1`)
		sqlDB.Exec(t, `UPDATE pgbench_tellers SET tbalance = 7 WHERE tid = 1`)
		// No history insert — history sum (0) won't match the others (7).
		err := hooks.CheckConsistency(ctx, db)
		require.ErrorContains(t, err, "balance invariant violated")
	})

	t.Run("row deletion detected", func(t *testing.T) {
		setup(t)
		sqlDB.Exec(t, `DELETE FROM pgbench_tellers WHERE tid = 5`)
		err := hooks.CheckConsistency(ctx, db)
		require.ErrorContains(t, err, "row count for pgbench_tellers")
	})
}

// TestPgbenchEndToEnd exercises the full workload: schema creation, initial
// data load via the framework, running a batch of transactions through the
// worker, and verifying the consistency check still passes.
func TestPgbenchEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE test`)

	tests := []struct {
		name string
		mode string
	}{
		{name: "tpcb", mode: modeTPCB},
		{name: "simple-update", mode: modeSimpleUpdate},
		{name: "select-only", mode: modeSelectOnly},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB.Exec(t, `DROP TABLE IF EXISTS pgbench_history, pgbench_accounts, pgbench_tellers, pgbench_branches`)

			gen := workload.FromFlags(pgbenchMeta,
				`--scale=1`, `--batch-size=10000`, `--mode=`+tc.mode, `--concurrency=2`)
			loader := workloadsql.InsertsDataLoader{BatchSize: 1000, Concurrency: 4}
			_, err := workloadsql.Setup(ctx, db, gen, loader)
			require.NoError(t, err)

			pgURL, urlCleanup := srv.PGUrl(t,
				serverutils.User(username.RootUser),
				serverutils.DBName("test"),
			)
			defer urlCleanup()

			reg := histogram.NewRegistry(time.Minute, pgbenchMeta.Name)
			ql, err := gen.(workload.Opser).Ops(ctx, []string{pgURL.String()}, reg)
			require.NoError(t, err)
			defer func() {
				if ql.Close != nil {
					require.NoError(t, ql.Close(ctx))
				}
			}()

			// Run a small batch of operations across all worker funcs.
			const opsPerWorker = 25
			for _, fn := range ql.WorkerFns {
				for i := 0; i < opsPerWorker; i++ {
					require.NoError(t, fn(ctx))
				}
			}

			require.NoError(t, gen.(workload.Hookser).Hooks().CheckConsistency(ctx, db))
		})
	}
}
