// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func createRandomTransactions(
	transactions int, transactionSize int, keyspace int, readFraction float64,
) []Transaction {
	result := make([]Transaction, transactions)
	for i := range transactions {
		seen := make(map[txnlock.LockHash]struct{}, transactionSize)
		locks := make([]txnlock.Lock, 0, transactionSize)
		for len(locks) < transactionSize {
			h := txnlock.LockHash(rand.Int63n(int64(keyspace)))
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			locks = append(locks, txnlock.Lock{
				Hash: h,
				Read: rand.Float64() < readFraction,
			})
		}
		result[i] = Transaction{
			CommitTime: hlc.Timestamp{WallTime: int64(i + 1)},
			Locks:      locks,
		}
	}
	return result
}

func BenchmarkScheduler_Schedule(b *testing.B) {
	for _, keyspace := range []int{1000, 10000, 100000} {
		for _, txnSize := range []int{1, 10, 50, 1000} {
			b.Run(fmt.Sprintf("keyspace=%d/size=%d", keyspace, txnSize), func(b *testing.B) {
				transactions := createRandomTransactions(b.N/txnSize, txnSize, keyspace, 0.8)
				dependenciesBuffer := make([]hlc.Timestamp, 0, 100)
				scheduler := NewScheduler(1024 * 1024)
				b.ResetTimer()
				for _, txn := range transactions {
					dependenciesBuffer, _ = scheduler.Schedule(txn, dependenciesBuffer)
				}
			})
		}
	}
}
