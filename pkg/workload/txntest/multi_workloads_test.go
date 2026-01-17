// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"context"
	gOSQL "database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// 1) KV read-your-own-writes and aggregate reads.
func TestKVReadYourOwnWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS kv_rw (id INT PRIMARY KEY, v INT NOT NULL)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE kv_rw"); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "INSERT INTO kv_rw SELECT g, 0 FROM generate_series(1, 100) AS g")
				return err
			},
			Database: "defaultdb",
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "write-then-read",
					Steps: Steps(
						QueryRow("SELECT id, v FROM kv_rw ORDER BY random() LIMIT 1").Capture("id", "v"),
						Exec("UPDATE kv_rw SET v = v + 1 WHERE id = $1", Var("id")),
						Sleep(2*time.Millisecond),
						QueryRow("SELECT v FROM kv_rw WHERE id = $1", Var("id")).Capture("v2"),
					),
					Retry:  RetrySerializable,
					Weight: 2,
				},
				{
					Name: "agg-read",
					Steps: Steps(
						QueryRow("SELECT count(*), COALESCE(sum(v), 0) FROM kv_rw").Capture("cnt", "sumv"),
						Sleep(1*time.Millisecond),
						QueryRow("SELECT max(v) FROM kv_rw").Capture("maxv"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "kv-count-constant",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var cnt int
					if err := db.QueryRowContext(ctx, "SELECT count(*) FROM kv_rw").Scan(&cnt); err != nil {
						return err
					}
					if cnt != 100 {
						return fmt.Errorf("count=%d, want=100", cnt)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 2) Classic write-skew style toggling of on-call doctors for a single day.
func TestDoctorsWriteSkewPrevention(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS doctors_ws (id INT PRIMARY KEY, day INT NOT NULL, oncall BOOL NOT NULL)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE doctors_ws"); err != nil {
					return err
				}
				// Seed day=1 with two on-call doctors.
				if _, err := db.ExecContext(ctx, "INSERT INTO doctors_ws (id, day, oncall) VALUES (1, 1, true), (2, 1, true)"); err != nil {
					return err
				}
				return nil
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "go-offcall",
					Steps: Steps(
						QueryRow("SELECT count(*) FROM doctors_ws WHERE day = 1 AND oncall").Capture("num_on"),
						Sleep(2*time.Millisecond),
						Exec("UPDATE doctors_ws SET oncall = false WHERE id IN (SELECT id FROM doctors_ws WHERE day = 1 AND oncall ORDER BY id LIMIT 1)"),
					),
					Retry:  RetrySerializable,
					Weight: 2,
				},
				{
					Name: "go-oncall",
					Steps: Steps(
						Exec("UPDATE doctors_ws SET oncall = true WHERE id IN (SELECT id FROM doctors_ws WHERE day = 1 AND NOT oncall ORDER BY id LIMIT 1)"),
						Sleep(2*time.Millisecond),
						QueryRow("SELECT count(*) FROM doctors_ws WHERE day = 1 AND oncall").Capture("num_on2"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "oncall-count-bounds",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var num int
					if err := db.QueryRowContext(ctx, "SELECT count(*) FROM doctors_ws WHERE day = 1 AND oncall").Scan(&num); err != nil {
						return err
					}
					if num < 0 || num > 2 {
						return fmt.Errorf("num_oncall=%d, want in [0,2]", num)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 3) Reservation system: prevent overlapping intervals per room using range predicates.
func TestReservationsNoOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS reservations_ws (id INT PRIMARY KEY DEFAULT unique_rowid(), room INT NOT NULL, start_ts INT NOT NULL, end_ts INT NOT NULL, CHECK (start_ts < end_ts))",
				"CREATE INDEX IF NOT EXISTS reservations_ws_room_start_end_idx ON reservations_ws (room, start_ts, end_ts)",
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "check-then-insert",
					Steps: Steps(
						// Insert only if no conflicting interval exists, using a scalar subquery predicate.
						Exec("INSERT INTO reservations_ws (room, start_ts, end_ts) SELECT $1, $2, $3 WHERE 0 = (SELECT count(*) FROM reservations_ws WHERE room = $1 AND NOT ($3 <= start_ts OR end_ts <= $2))", Var("room"), Var("start"), Var("end")),
						Sleep(1*time.Millisecond),
						// Read a non-trivial aggregate for pressure.
						QueryRow("SELECT COALESCE(sum(end_ts - start_ts), 0) FROM reservations_ws WHERE room = $1", Var("room")).Capture("tot_span"),
					),
					ParamGen: func(rng RNG, _ Vars) Bindings {
						r := rng.(*rand.Rand)
						room := r.Intn(5) + 1
						start := r.Intn(1000)
						length := r.Intn(10) + 1
						end := start + length
						return Bindings{"room": room, "start": start, "end": end}
					},
					Retry:  RetrySerializable,
					Weight: 3,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "no-overlaps",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var conflicts int
					q := `
						SELECT count(*) FROM reservations_ws r1
						JOIN reservations_ws r2
						  ON r1.room = r2.room AND r1.id < r2.id
						WHERE NOT (r1.end_ts <= r2.start_ts OR r2.end_ts <= r1.start_ts)
					`
					if err := db.QueryRowContext(ctx, q).Scan(&conflicts); err != nil {
						return err
					}
					if conflicts != 0 {
						return fmt.Errorf("found %d overlapping reservations", conflicts)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 4) Conflicting update order to stress deadlock/retry paths while preserving equality.
func TestDeadlockUpdateOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS pair_ws (id INT PRIMARY KEY, v INT NOT NULL)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE pair_ws"); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "INSERT INTO pair_ws (id, v) VALUES (1, 0), (2, 0)")
				return err
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "inc-1-then-2",
					Steps: Steps(
						Exec("UPDATE pair_ws SET v = v + 1 WHERE id = 1"),
						Sleep(2*time.Millisecond),
						Exec("UPDATE pair_ws SET v = v + 1 WHERE id = 2"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
				{
					Name: "inc-2-then-1",
					Steps: Steps(
						Exec("UPDATE pair_ws SET v = v + 1 WHERE id = 2"),
						Sleep(2*time.Millisecond),
						Exec("UPDATE pair_ws SET v = v + 1 WHERE id = 1"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "pair-equal",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var a, b int
					if err := db.QueryRowContext(ctx, "SELECT v FROM pair_ws WHERE id = 1").Scan(&a); err != nil {
						return err
					}
					if err := db.QueryRowContext(ctx, "SELECT v FROM pair_ws WHERE id = 2").Scan(&b); err != nil {
						return err
					}
					if a != b {
						return fmt.Errorf("pair diverged: %d vs %d", a, b)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 5) Parent/child with FK and cascading deletes; ensure no orphans.
func TestParentChildFKConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS parents_ws (id INT PRIMARY KEY)",
				"CREATE TABLE IF NOT EXISTS children_ws (id INT PRIMARY KEY DEFAULT unique_rowid(), parent_id INT NOT NULL REFERENCES parents_ws(id) ON DELETE CASCADE)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				// Truncate both tables in one statement to satisfy FK constraints on TRUNCATE.
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE children_ws, parents_ws"); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "INSERT INTO parents_ws SELECT g FROM generate_series(1, 50) AS g")
				return err
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "insert-child-then-read",
					Steps: Steps(
						QueryRow("SELECT COALESCE((SELECT id FROM parents_ws ORDER BY random() LIMIT 1), -1)").Capture("pid"),
						Exec("INSERT INTO children_ws(parent_id) SELECT $1 WHERE EXISTS (SELECT 1 FROM parents_ws WHERE id = $1)", Var("pid")),
						Sleep(1*time.Millisecond),
						QueryRow("SELECT count(*) FROM children_ws WHERE parent_id = $1", Var("pid")).Capture("num_children"),
					),
					Retry:  RetrySerializable,
					Weight: 3,
				},
				{
					Name: "delete-parent-then-check",
					Steps: Steps(
						QueryRow("SELECT COALESCE((SELECT id FROM parents_ws ORDER BY random() LIMIT 1), -1)").Capture("pid2"),
						Exec("DELETE FROM parents_ws WHERE id = $1", Var("pid2")),
						Sleep(1*time.Millisecond),
						QueryRow("SELECT count(*) FROM children_ws WHERE parent_id = $1", Var("pid2")).Capture("num_after"),
					),
					Retry:  RetrySerializable,
					Weight: 2,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "no-orphan-children",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var orphans int
					q := `SELECT count(*) FROM children_ws c LEFT JOIN parents_ws p ON c.parent_id = p.id WHERE p.id IS NULL`
					if err := db.QueryRowContext(ctx, q).Scan(&orphans); err != nil {
						return err
					}
					if orphans != 0 {
						return fmt.Errorf("found %d orphan children", orphans)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 6) Bank transfer with a ledger table to exercise multi-table transactional semantics.
func TestBankTransferWithLedger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS accounts_ws (id INT PRIMARY KEY, balance INT NOT NULL)",
				"CREATE TABLE IF NOT EXISTS ledger_ws (id INT PRIMARY KEY DEFAULT unique_rowid(), src INT NOT NULL, dst INT NOT NULL, amt INT NOT NULL)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE ledger_ws"); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE accounts_ws"); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "INSERT INTO accounts_ws SELECT g, 100 FROM generate_series(1, 100) AS g")
				return err
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "transfer-ledger",
					Steps: Steps(
						QueryRow("SELECT id, balance FROM accounts_ws ORDER BY random() LIMIT 1").Capture("aID", "aBal"),
						QueryRow("SELECT id, balance FROM accounts_ws ORDER BY random() LIMIT 1").Capture("bID", "bBal"),
						Exec("UPDATE accounts_ws SET balance = balance - $1 WHERE id = $2", Var("amt"), Var("aID")),
						Exec("UPDATE accounts_ws SET balance = balance + $1 WHERE id = $2", Var("amt"), Var("bID")),
						Exec("INSERT INTO ledger_ws (src, dst, amt) VALUES ($1, $2, $3)", Var("aID"), Var("bID"), Var("amt")),
					),
					ParamGen: func(rng RNG, _ Vars) Bindings {
						r, _ := rng.(*rand.Rand)
						if r == nil {
							r = rand.New(rand.NewSource(time.Now().UnixNano()))
						}
						return Bindings{"amt": r.Intn(5) + 1}
					},
					Retry:  RetrySerializable,
					Weight: 3,
				},
				{
					Name: "audit-read",
					Steps: Steps(
						QueryRow("SELECT COALESCE(sum(balance), 0) FROM accounts_ws").Capture("sum_bal"),
						QueryRow("SELECT COALESCE(count(*), 0) FROM ledger_ws").Capture("num_ledger"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "sum-constant",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var sum int
					if err := db.QueryRowContext(ctx, "SELECT sum(balance) FROM accounts_ws").Scan(&sum); err != nil {
						return err
					}
					if sum != 100*100 {
						return fmt.Errorf("sum=%d, want=%d", sum, 100*100)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 7) Inventory/orders with conditional updates to avoid negative stock.
func TestInventoryOrdersConditional(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS items_ws (id INT PRIMARY KEY, stock INT NOT NULL)",
				"CREATE TABLE IF NOT EXISTS orders_ws (id INT PRIMARY KEY DEFAULT unique_rowid(), item INT NOT NULL REFERENCES items_ws(id), qty INT NOT NULL)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				// Truncate both tables in a single statement to respect FK constraints on TRUNCATE.
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE orders_ws, items_ws"); err != nil {
					return err
				}
				_, err := db.ExecContext(ctx, "INSERT INTO items_ws SELECT g, 100 FROM generate_series(1, 20) AS g")
				return err
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "place-order",
					Steps: Steps(
						QueryRow("SELECT id, stock FROM items_ws ORDER BY random() LIMIT 1").Capture("item", "stock"),
						Exec("UPDATE items_ws SET stock = stock - $1 WHERE id = $2 AND stock >= $1", Var("qty"), Var("item")),
						Sleep(1*time.Millisecond),
						Exec("INSERT INTO orders_ws(item, qty) SELECT $1, $2 WHERE EXISTS (SELECT 1 FROM items_ws WHERE id = $1)", Var("item"), Var("qty")),
						QueryRow("SELECT COALESCE(sum(qty), 0) FROM orders_ws WHERE item = $1", Var("item")).Capture("sumqty"),
					),
					ParamGen: func(rng RNG, _ Vars) Bindings {
						r := rng.(*rand.Rand)
						return Bindings{"qty": r.Intn(5) + 1}
					},
					Retry:  RetrySerializable,
					Weight: 3,
				},
				{
					Name: "restock-and-read",
					Steps: Steps(
						QueryRow("SELECT id FROM items_ws ORDER BY random() LIMIT 1").Capture("item2"),
						Exec("UPDATE items_ws SET stock = stock + 1 WHERE id = $1", Var("item2")),
						QueryRow("SELECT stock FROM items_ws WHERE id = $1", Var("item2")).Capture("stock2"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "no-negative-stock",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var neg int
					if err := db.QueryRowContext(ctx, "SELECT count(*) FROM items_ws WHERE stock < 0").Scan(&neg); err != nil {
						return err
					}
					if neg != 0 {
						return fmt.Errorf("found %d items with negative stock", neg)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 8) Users with secondary index and group aggregates; ensure row count invariant.
func TestUsersGroupAggregate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS users_ws (id INT PRIMARY KEY DEFAULT unique_rowid(), balance INT NOT NULL, group_id INT NOT NULL)",
				"CREATE INDEX IF NOT EXISTS users_ws_group_idx ON users_ws (group_id)",
			},
			Init: func(ctx context.Context, db *gOSQL.DB) error {
				if _, err := db.ExecContext(ctx, "TRUNCATE TABLE users_ws"); err != nil {
					return err
				}
				// Seed 200 users spread across 10 groups.
				_, err := db.ExecContext(ctx, "INSERT INTO users_ws SELECT unique_rowid(), 0, (g % 10) + 1 FROM generate_series(1, 200) AS g")
				return err
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "move-group",
					Steps: Steps(
						QueryRow("SELECT id, group_id FROM users_ws ORDER BY random() LIMIT 1").Capture("uid", "gid"),
						Exec("UPDATE users_ws SET group_id = 1 + (group_id % 10) WHERE id = $1", Var("uid")),
						QueryRow("SELECT group_id, count(*) FROM users_ws GROUP BY group_id ORDER BY count(*) DESC LIMIT 1").Capture("top_gid", "top_count"),
					),
					Retry:  RetrySerializable,
					Weight: 2,
				},
				{
					Name: "adjust-balance",
					Steps: Steps(
						QueryRow("SELECT id, balance FROM users_ws ORDER BY random() LIMIT 1").Capture("uid2", "bal"),
						Exec("UPDATE users_ws SET balance = balance + $1 WHERE id = $2", Var("delta"), Var("uid2")),
						QueryRow("SELECT avg(balance) FROM users_ws").Capture("avg_bal"),
					),
					ParamGen: func(rng RNG, _ Vars) Bindings {
						r := rng.(*rand.Rand)
						d := r.Intn(11) - 5 // -5..+5
						if d == 0 {
							d = 1
						}
						return Bindings{"delta": d}
					},
					Retry:  RetrySerializable,
					Weight: 2,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "users-count-constant",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var cnt int
					if err := db.QueryRowContext(ctx, "SELECT count(*) FROM users_ws").Scan(&cnt); err != nil {
						return err
					}
					if cnt != 200 {
						return fmt.Errorf("count=%d, want=200", cnt)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}

// 9) JSON document manipulations with read-your-own-writes within a transaction.
func TestJSONDocsReadYourOwnWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS docs_ws (id INT PRIMARY KEY DEFAULT unique_rowid(), j JSONB NOT NULL)",
			},
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "insert-then-query-json",
					Steps: Steps(
						Exec("INSERT INTO docs_ws (j) VALUES ('{"+"\"a\""+": 1}')"),
						Sleep(1*time.Millisecond),
						QueryRow("SELECT (j->>'a')::INT FROM docs_ws ORDER BY id DESC LIMIT 1").Capture("a"),
						Exec("UPDATE docs_ws SET j = jsonb_set(j, '{b}', '2'::jsonb) WHERE id IN (SELECT id FROM docs_ws ORDER BY id DESC LIMIT 1)"),
					),
					Retry:  RetrySerializable,
					Weight: 3,
				},
				{
					Name: "read-agg-json",
					Steps: Steps(
						QueryRow("SELECT count(*), COALESCE(sum((j->>'a')::INT), 0) FROM docs_ws").Capture("cnt", "suma"),
					),
					Retry:  RetrySerializable,
					Weight: 1,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "docs-nonempty",
				Fn: func(ctx context.Context, db *gOSQL.DB, _ History) error {
					var cnt int
					if err := db.QueryRowContext(ctx, "SELECT count(*) FROM docs_ws").Scan(&cnt); err != nil {
						return err
					}
					if cnt == 0 {
						return fmt.Errorf("no docs inserted")
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{Concurrency: 64, Iterations: 2},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}
