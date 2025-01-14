// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

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

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		t.Run("bank", func(t *testing.T) {
			ctx := context.Background()
			const numRows, numRanges, payloadBytes, maxTransfer = 10, 10, 10, 999
			gen := bank.FromConfig(numRows, numRows, payloadBytes, numRanges)
			var l workloadsql.InsertsDataLoader
			if _, err := workloadsql.Setup(ctx, s.DB, gen, l); err != nil {
				t.Fatal(err)
			}

			var nowString string
			require.NoError(t, s.DB.QueryRow("SELECT cluster_logical_timestamp()").Scan(&nowString))

			var numOfTrans atomic.Int32
			numOfTrans.Store(1)
			existingChangeCount := 50
			prevTime := time.Now()
			for i := 0; i < existingChangeCount; i++ {
				if err := randomBankTransfer(numRows, maxTransfer, s.DB); err != nil {
					t.Fatal(err)
				}
				t.Logf("time taken to make the %d-th bank transfer: %v", numOfTrans.Load(), time.Since(prevTime))
				numOfTrans.Add(1)
				prevTime = time.Now()
			}

			bankFeed := feed(t, f, `CREATE CHANGEFEED FOR bank WITH updated, cursor=$1`, nowString)
			defer closeFeed(t, bankFeed)

			var done int64
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				prevTimeTransfer := time.Now()
				for {
					if atomic.LoadInt64(&done) > 0 {
						return nil
					}
					if err := randomBankTransfer(numRows, maxTransfer, s.DB); err != nil {
						return err
					}
					t.Logf("time taken to make the %d-th bank transfer: %v",
						numOfTrans.Load(), time.Since(prevTimeTransfer))
					numOfTrans.Add(1)
					prevTimeTransfer = time.Now()
				}
			})

			v := cdctest.NewOrderValidator(`bank`)
			seenChanges := 0
			prevTimeCDC := time.Now()
			for {
				m, err := bankFeed.Next()
				if err != nil {
					t.Fatal(err)
				} else if len(m.Key) > 0 || len(m.Value) > 0 {
					updated, _, err := cdctest.ParseJSONValueTimestamps(m.Value)
					if err != nil {
						t.Fatal(err)
					}
					err = v.NoteRow(m.Partition, string(m.Key), string(m.Value), updated, m.Topic)
					if err != nil {
						t.Fatal(err)
					}
					seenChanges++
					t.Logf("key: %s, value: %s\n", m.Key, m.Value)
					t.Logf("time taken to see the %d-th changefeed change: %v", seenChanges, time.Since(prevTimeCDC))
					prevTimeCDC = time.Now()
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
	// Tenant tests disabled because ALTER TABLE .. SPLIT is not
	// supported with cluster virtualization:
	//
	// nemeses_test.go:39: pq: unimplemented: operation is unsupported inside virtual clusters
	//
	// TODO(knz): This seems incorrect, see issue #109417.
	cdcTest(t, testFn, feedTestNoTenants)
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
