// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"context"
	gOSQL "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// These tests focus on write-flush mechanics (e.g., DeleteRange-induced flush,
// buffer budget-triggered flush) independent of any specific implementation.

// DeleteRange flush correctness: final state should reflect all mutations.
func TestTxnWriteFlush_DeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS t_bw_dr (k INT PRIMARY KEY)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_dr"); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "INSERT INTO t_bw_dr VALUES (1)")
				return err
			},
		},
		Workload: WorkloadSpec{Templates: []TxnTemplate{
			{
				Name: "ins-delrange",
				Steps: Steps(
					Exec("INSERT INTO t_bw_dr VALUES (2)"),
					// Force a range delete that should trigger a flush in the write path.
					Exec("DELETE FROM t_bw_dr WHERE k > 0 AND k < 10"),
				),
				Retry:  RetrySerializable,
				Weight: 1,
			},
		}},
		Invariants: []Invariant{{
			Name: "empty-after-flush",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var c int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_bw_dr").Scan(&c); err != nil {
					return err
				}
				if c != 0 {
					return fmt.Errorf("count=%d, want=0", c)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// Buffer budget overflow triggers a flush; subsequent operations still commit.
func TestTxnWriteFlush_Budget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS t_bw_budget (k INT PRIMARY KEY, v STRING)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_budget"); err != nil {
					return err
				}
				// Set a tiny budget to force a flush during the txn.
				_, err := db.ExecContext(ctx, "SET CLUSTER SETTING kv.transaction.write_buffering.max_buffer_size = '1KiB'")
				return err
			},
		},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "overflow",
			Steps: Steps(
				Exec("INSERT INTO t_bw_budget VALUES (1, repeat('x', 700))"),
				Exec("INSERT INTO t_bw_budget VALUES (2, repeat('y', 700))"),
				// After flush, continue to update to exercise post-flush behavior.
				Exec("UPDATE t_bw_budget SET v = 'z' WHERE k = 1"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "two-rows-present",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var c int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_bw_budget").Scan(&c); err != nil {
					return err
				}
				if c != 2 {
					return fmt.Errorf("count=%d, want=2", c)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}
