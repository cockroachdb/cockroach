// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func TestValidations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)

		t.Run("bank", func(t *testing.T) {
			ctx := context.Background()
			const numRows, numRanges, payloadBytes, maxTransfer = 10, 10, 10, 999
			gen := bank.FromConfig(numRows, payloadBytes, numRanges)
			if _, err := workload.Setup(ctx, db, gen, 0, 0); err != nil {
				t.Fatal(err)
			}

			bank := f.Feed(t, `CREATE CHANGEFEED FOR bank WITH updated, resolved`)
			defer bank.Close(t)

			var done int64
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				for {
					if atomic.LoadInt64(&done) > 0 {
						return nil
					}

					// TODO(dan): This bit is copied from the bank workload. It's
					// currently much easier to do this than to use the real Ops,
					// which is silly. Fixme.
					from := rand.Intn(numRows)
					to := rand.Intn(numRows)
					for from == to {
						to = rand.Intn(numRows)
					}
					amount := rand.Intn(maxTransfer)
					if _, err := db.Exec(`UPDATE bank
					SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
					WHERE id IN ($1, $2)
				`, from, to, amount); err != nil {
						return err
					}
				}
			})

			const requestedResolved = 5
			var numResolved, rowsSinceResolved int

			v := Validators{
				NewOrderValidator(`bank`),
				// TODO(mrtracy): Disabled by #30902. Re-enabling is tracked by #31110.
				// NewFingerprintValidator(db, `bank`, `fprint`, bank.Partitions()),
			}
			sqlDB.Exec(t, `CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)`)
			for {
				_, partition, key, value, resolved, ok := bank.Next(t)
				if !ok {
					t.Fatal(`expected more rows`)
				} else if key != nil {
					updated, _, err := ParseJSONValueTimestamps(value)
					if err != nil {
						t.Fatal(err)
					}
					v.NoteRow(partition, string(key), string(value), updated)
					rowsSinceResolved++
				} else if resolved != nil {
					_, resolved, err := ParseJSONValueTimestamps(resolved)
					if err != nil {
						t.Fatal(err)
					}
					if rowsSinceResolved > 0 || true {
						if err := v.NoteResolved(partition, resolved); err != nil {
							t.Fatal(err)
						}
						numResolved++
						if numResolved > requestedResolved {
							atomic.StoreInt64(&done, 1)
							break
						}
					}
					rowsSinceResolved = 0
				}
			}
			for _, f := range v.Failures() {
				t.Error(f)
			}

			if err := g.Wait(); err != nil {
				t.Errorf(`%+v`, err)
			}
		})
	}
	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}
