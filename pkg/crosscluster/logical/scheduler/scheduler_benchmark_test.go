package scheduler

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func CreateRandomTransactions(transactions int, transactionSize int, keyspace int, readFraction float64) []Transaction {
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
	transactions := CreateRandomTransactions(b.N, 1, 10000, 0.8)
	scratch := make([]hlc.Timestamp, 0, 100)
	scheduler := NewScheduler(10000)
	b.ResetTimer()
	for _, txn := range transactions {
		scratch, _ = scheduler.Schedule(txn, scratch)
	}
}
