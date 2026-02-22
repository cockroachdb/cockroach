// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBankTransferTriad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	spec := TestSpec{
		Schema: SchemaSpec{
			DDL: []string{
				"CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
			},
			Init: func(ctx context.Context, db *gosql.DB) error {
				_, err := db.ExecContext(ctx, "INSERT INTO accounts SELECT g, 100 FROM generate_series(1, 100) AS g")
				return err
			},
			Database: "defaultdb",
		},
		Workload: WorkloadSpec{
			Templates: []TxnTemplate{
				{
					Name: "transfer",
					Steps: Steps(
						QueryRow("SELECT id, balance FROM accounts ORDER BY random() LIMIT 1").Capture("aID", "aBal"),
						QueryRow("SELECT id, balance FROM accounts ORDER BY random() LIMIT 1").Capture("bID", "bBal"),
						Exec("UPDATE accounts SET balance = balance - $1 WHERE id = $2", Var("amt"), Var("aID")),
						Exec("UPDATE accounts SET balance = balance + $1 WHERE id = $2", Var("amt"), Var("bID")),
					),
					ParamGen: func(rng RNG, vars Vars) Bindings {
						// Use 1..5 inclusive
						r, _ := rng.(*rand.Rand)
						if r == nil {
							r = rand.New(rand.NewSource(time.Now().UnixNano()))
						}
						return Bindings{"amt": r.Intn(5) + 1}
					},
					Weight: 1,
					Retry:  RetrySerializable,
				},
			},
		},
		Invariants: []Invariant{
			{
				Name: "sum-constant",
				Fn: func(ctx context.Context, db *gosql.DB, _ History) error {
					var sum int
					if err := db.QueryRowContext(ctx, "SELECT sum(balance) FROM accounts").Scan(&sum); err != nil {
						return err
					}
					if sum != 100*100 {
						return fmt.Errorf("sum=%d, want=%d", sum, 100*100)
					}
					return nil
				},
			},
		},
		RunConfig: RunConfig{
			Concurrency: 64,
			Iterations:  2,
		},
	}

	require.NoError(t, Run(ctx, t, spec, 1))
}
