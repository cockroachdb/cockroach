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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/stretchr/testify/require"
)

func TestValidations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		// Poller-based tests get checkpoints every 10 milliseconds, whereas
		// rangefeed tests get on at most every 200 milliseconds, and there is
		// also a short lead time for rangefeed-based feeds before the
		// first checkpoint. This means that a much larger number of transfers
		// happens, and the validator gets overwhelmed by fingerprints.
		slowWrite := PushEnabled.Get(&f.Server().ClusterSettings().SV)
		sqlDB := sqlutils.MakeSQLRunner(db)

		t.Run("bank", func(t *testing.T) {
			ctx := context.Background()
			const numRows, numRanges, payloadBytes, maxTransfer = 10, 10, 10, 999
			gen := bank.FromConfig(numRows, payloadBytes, numRanges)
			if _, err := workload.Setup(ctx, db, gen, 0, 0); err != nil {
				t.Fatal(err)
			}

			bankFeed := f.Feed(t, `CREATE CHANGEFEED FOR bank WITH updated, resolved`)
			defer bankFeed.Close(t)

			var done int64
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				for {
					if atomic.LoadInt64(&done) > 0 {
						return nil
					}

					if err := randomBankTransfer(numRows, maxTransfer, db); err != nil {
						return err
					}

					if slowWrite {
						time.Sleep(100 * time.Millisecond)
					}
				}
			})

			const requestedResolved = 7
			var numResolved, rowsSinceResolved int

			v := Validators{
				NewOrderValidator(`bank`),
				NewFingerprintValidator(db, `bank`, `fprint`, bankFeed.Partitions()),
			}
			sqlDB.Exec(t, `CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)`)
			for {
				_, partition, key, value, resolved, ok := bankFeed.Next(t)
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
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

func TestCatchupScanOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		t.Run("bank", func(t *testing.T) {
			ctx := context.Background()
			const numRows, numRanges, payloadBytes, maxTransfer = 10, 10, 10, 999
			gen := bank.FromConfig(numRows, payloadBytes, numRanges)
			if _, err := workload.Setup(ctx, db, gen, 0, 0); err != nil {
				t.Fatal(err)
			}

			var nowString string
			require.NoError(t, db.QueryRow("SELECT cluster_logical_timestamp()").Scan(&nowString))

			existingChangeCount := 50
			for i := 0; i < existingChangeCount; i++ {
				if err := randomBankTransfer(numRows, maxTransfer, db); err != nil {
					t.Fatal(err)
				}
			}

			bankFeed := f.Feed(t, `CREATE CHANGEFEED FOR bank WITH updated, cursor=$1`, nowString)
			defer bankFeed.Close(t)

			var done int64
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				for {
					if atomic.LoadInt64(&done) > 0 {
						return nil
					}

					if err := randomBankTransfer(numRows, maxTransfer, db); err != nil {
						return err
					}
				}
			})

			v := NewOrderValidator(`bank`)
			seenChanges := 0
			for {
				_, partition, key, value, _, ok := bankFeed.Next(t)
				if !ok {
					t.Fatal(`expected more rows`)
				} else if key != nil {
					updated, _, err := ParseJSONValueTimestamps(value)
					if err != nil {
						t.Fatal(err)
					}
					v.NoteRow(partition, string(key), string(value), updated)
					seenChanges++
					if seenChanges >= 200 {
						atomic.StoreInt64(&done, 1)
						break
					}
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
	t.Run(`poller`, pollerTest(sinklessTest, testFn))
}

// TODO(dan): This bit is copied from the bank workload. It's
// currently much easier to do this than to use the real Ops,
// which is silly. Fixme.
func randomBankTransfer(numRows, maxTransfer int, db *gosql.DB) error {
	from := rand.Intn(numRows)
	to := rand.Intn(numRows)
	for from == to {
		to = rand.Intn(numRows)
	}
	amount := rand.Intn(maxTransfer)
	_, err := db.Exec(`UPDATE bank
					SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
					WHERE id IN ($1, $2)
				`, from, to, amount)
	return err
}
