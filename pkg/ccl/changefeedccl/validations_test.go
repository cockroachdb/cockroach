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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/stretchr/testify/require"
)

func TestCatchupScanOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		t.Run("bank", func(t *testing.T) {
			ctx := context.Background()
			const numRows, numRanges, payloadBytes, maxTransfer = 10, 10, 10, 999
			gen := bank.FromConfig(numRows, numRows, payloadBytes, numRanges)
			var l workloadsql.InsertsDataLoader
			if _, err := workloadsql.Setup(ctx, db, gen, l); err != nil {
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

			bankFeed := feed(t, f, `CREATE CHANGEFEED FOR bank WITH updated, cursor=$1`, nowString)
			defer closeFeed(t, bankFeed)

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

			v := cdctest.NewOrderValidator(`bank`)
			seenChanges := 0
			for {
				m, err := bankFeed.Next()
				if err != nil {
					t.Fatal(err)
				} else if len(m.Key) > 0 || len(m.Value) > 0 {
					updated, _, err := cdctest.ParseJSONValueTimestamps(m.Value)
					if err != nil {
						t.Fatal(err)
					}
					err = v.NoteRow(m.Partition, string(m.Key), string(m.Value), updated)
					if err != nil {
						t.Fatal(err)
					}
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
	// Tenant tests skipped because of:
	// validations_test.go:40: executing ALTER TABLE bank SPLIT AT
	// VALUES (5): pq: unimplemented: operation is unsupported in
	// multi-tenancy mode
	t.Run(`sinkless`, sinklessTest(testFn, feedTestNoTenants))
	t.Run(`enterprise`, enterpriseTest(testFn))
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
