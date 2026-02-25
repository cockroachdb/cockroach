// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func createRandomTransactions(
	transactions int, transactionSize int, keyspace int, readFraction float64,
) []Transaction {
	result := make([]Transaction, transactions)
	for i := range transactions {
		locks := make([]Lock, transactionSize)
		for j := range locks {
			locks[j] = Lock{
				Hash:   LockHash(rand.Int63n(int64(keyspace))),
				IsRead: rand.Float64() < readFraction,
			}
		}
		result[i] = Transaction{
			CommitTime: hlc.Timestamp{WallTime: int64(i + 1)},
			Locks:      locks,
		}
	}
	return result
}

func BenchmarkScheduler_Schedule(b *testing.B) {
	for _, txnSize := range []int{1, 10, 50, 1000} {
		b.Run(fmt.Sprintf("size=%d", txnSize), func(b *testing.B) {
			transactions := createRandomTransactions(b.N/txnSize, txnSize, 10000, 0.8)
			scratch := make([]hlc.Timestamp, 0, 100)
			scheduler := NewScheduler(1024 * 1024)
			b.ResetTimer()
			for _, txn := range transactions {
				scratch, _ = scheduler.Schedule(txn, scratch)
			}
		})
	}
}
