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

// These tests exercise general transactional visibility and RYOW semantics
// (not specific to buffered write flush triggers).

// Savepoint rollbacks with mid-txn mutations.
func TestBufferedWrites_SavepointRollbacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{DDL: []string{
			"CREATE TABLE IF NOT EXISTS t_bw_sp (k INT PRIMARY KEY, v INT)",
		}, Init: func(ctx context.Context, db *gOSQL.DB) error {
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_sp")
			return err
		}},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "savepoints",
			Steps: Steps(
				Exec("INSERT INTO t_bw_sp VALUES (1,100),(2,200),(3,300)"),
				Exec("SAVEPOINT s1"),
				Exec("INSERT INTO t_bw_sp VALUES (4,400),(5,500),(6,600)"),
				Exec("SAVEPOINT s2"),
				Exec("INSERT INTO t_bw_sp VALUES (7,700),(8,800),(9,900)"),
				// Throw in some deletes before rolling back.
				Exec("DELETE FROM t_bw_sp WHERE k IN (1,2,3)"),
				Exec("ROLLBACK TO SAVEPOINT s2"),
				Exec("ROLLBACK TO SAVEPOINT s1"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "only-first-three",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var c int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_bw_sp").Scan(&c); err != nil {
					return err
				}
				if c != 3 {
					return fmt.Errorf("count=%d, want=3", c)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// Insert-delete-insert roundtrip within a single txn.
func TestBufferedWrites_InsertDeleteInsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{DDL: []string{
			"CREATE TABLE IF NOT EXISTS t_bw_idi (k INT PRIMARY KEY)",
		}, Init: func(ctx context.Context, db *gOSQL.DB) error {
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_idi")
			return err
		}},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "idi",
			Steps: Steps(
				Exec("INSERT INTO t_bw_idi VALUES (1)"),
				Exec("DELETE FROM t_bw_idi WHERE k = 1"),
				Exec("INSERT INTO t_bw_idi VALUES (1)"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "present-once",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var c int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_bw_idi WHERE k = 1").Scan(&c); err != nil {
					return err
				}
				if c != 1 {
					return fmt.Errorf("k=1 count=%d, want=1", c)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// Secondary index read-your-own-writes within a single txn.
func TestBufferedWrites_SecondaryIndexRYOW(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{DDL: []string{
			"CREATE TABLE IF NOT EXISTS t_bw_si (k INT PRIMARY KEY, v INT NOT NULL)",
			"CREATE INDEX IF NOT EXISTS t_bw_si_v_idx ON t_bw_si (v)",
		}, Init: func(ctx context.Context, db *gOSQL.DB) error {
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_si")
			return err
		}},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "si-ryow",
			Steps: Steps(
				Exec("INSERT INTO t_bw_si VALUES (1,7)"),
				// Verify the insert is visible through the secondary index in the same txn.
				QueryRow("SELECT 1 FROM t_bw_si WHERE v = 7"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "one-row",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var c int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_bw_si WHERE v = 7").Scan(&c); err != nil {
					return err
				}
				if c != 1 {
					return fmt.Errorf("count=%d, want=1", c)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// Foreign key reference created and read within a txn.
func TestBufferedWrites_FKWithinTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{DDL: []string{
			"CREATE TABLE IF NOT EXISTS t_bw_p (id INT PRIMARY KEY)",
			"CREATE TABLE IF NOT EXISTS t_bw_c (id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES t_bw_p(id))",
		}, Init: func(ctx context.Context, db *gOSQL.DB) error {
			// Truncate both tables in a single statement to satisfy FK constraints.
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_c, t_bw_p")
			return err
		}},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "fk-ryow",
			Steps: Steps(
				Exec("INSERT INTO t_bw_p VALUES (1)"),
				Exec("INSERT INTO t_bw_c VALUES (10, 1)"),
				// Ensure the child can see the parent via a join predicate in the same txn.
				QueryRow("SELECT 1 FROM t_bw_c JOIN t_bw_p ON (t_bw_c.parent_id = t_bw_p.id) WHERE t_bw_c.id = 10"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "child-exists",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var c int
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t_bw_c WHERE id = 10 AND parent_id = 1").Scan(&c); err != nil {
					return err
				}
				if c != 1 {
					return fmt.Errorf("child count=%d, want=1", c)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// Unique index after delete+reinsert with same unique key should succeed.
func TestBufferedWrites_UniqueAfterDeleteReinsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{DDL: []string{
			"CREATE TABLE IF NOT EXISTS t_bw_u (k INT PRIMARY KEY, u INT UNIQUE NOT NULL)",
		}, Init: func(ctx context.Context, db *gOSQL.DB) error {
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_u")
			return err
		}},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "unique-delete-reinsert",
			Steps: Steps(
				Exec("INSERT INTO t_bw_u VALUES (1, 100)"),
				Exec("DELETE FROM t_bw_u WHERE k = 1"),
				// Reinsert same unique key u=100 under a different primary key.
				Exec("INSERT INTO t_bw_u VALUES (2, 100)"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "only-new-row",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var k, u int
				if err := db.QueryRowContext(ctx, "SELECT k, u FROM t_bw_u WHERE u = 100").Scan(&k, &u); err != nil {
					return err
				}
				if k != 2 || u != 100 {
					return fmt.Errorf("got (k,u)=(%d,%d), want (2,100)", k, u)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// Update then read in same txn (RYOW on updates).
func TestBufferedWrites_UpdateThenReadRYOW(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{DDL: []string{
			"CREATE TABLE IF NOT EXISTS t_bw_up (k INT PRIMARY KEY, v INT NOT NULL)",
		}, Init: func(ctx context.Context, db *gOSQL.DB) error {
			if _, err := db.ExecContext(ctx, "TRUNCATE TABLE t_bw_up"); err != nil {
				return err
			}
			_, err := db.ExecContext(ctx, "INSERT INTO t_bw_up VALUES (1, 5)")
			return err
		}},
		Workload: WorkloadSpec{Templates: []TxnTemplate{{
			Name: "update-then-read",
			Steps: Steps(
				Exec("UPDATE t_bw_up SET v = 6 WHERE k = 1"),
				// Must observe v=6 within the same txn; selecting 1 row is enough to fail if not visible
				QueryRow("SELECT 1 FROM t_bw_up WHERE k = 1 AND v = 6"),
			),
			Retry:  RetrySerializable,
			Weight: 1,
		}}},
		Invariants: []Invariant{{
			Name: "updated",
			Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
				var v int
				if err := db.QueryRowContext(ctx, "SELECT v FROM t_bw_up WHERE k = 1").Scan(&v); err != nil {
					return err
				}
				if v != 6 {
					return fmt.Errorf("v=%d, want=6", v)
				}
				return nil
			},
		}},
		RunConfig: RunConfig{Concurrency: 1, Iterations: 1},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}
