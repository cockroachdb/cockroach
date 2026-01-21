// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnwriter"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// testWriter is a test implementation of txnwriter.TransactionWriter that
// adds a random delay and records applied transactions to a shared log.
type testWriter struct {
	t   *testing.T
	rng *rand.Rand
	mu  struct {
		syncutil.Mutex
		log []hlc.Timestamp
	}
}

func (w *testWriter) ApplyBatch(
	ctx context.Context, txns []ldrdecoder.Transaction,
) ([]txnwriter.ApplyResult, error) {
	delay := time.Duration(w.rng.Int63n(int64(10 * time.Microsecond)))
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	results := make([]txnwriter.ApplyResult, len(txns))
	for i, txn := range txns {
		w.t.Logf("applied txn at %s", txn.Timestamp)
		w.mu.log = append(w.mu.log, txn.Timestamp)
		results[i] = txnwriter.ApplyResult{AppliedRows: len(txn.WriteSet)}
	}
	return results, nil
}

func (w *testWriter) Close(ctx context.Context) {}

// txnNode represents a transaction with its dependencies.
type txnNode struct {
	ts   hlc.Timestamp
	deps []hlc.Timestamp
}

// generateRandomDAG creates a random DAG of transactions where each transaction
// can depend on earlier transactions.
func generateRandomDAG(rng *rand.Rand, numTxns int, maxDeps int) []txnNode {
	nodes := make([]txnNode, numTxns)
	for i := range nodes {
		nodes[i].ts = hlc.Timestamp{WallTime: int64(i + 1)}
		// Each transaction can depend on up to maxDeps earlier transactions.
		numDeps := min(rng.Intn(maxDeps+1), i)
		deps := rng.Perm(i)[:numDeps]
		for _, dep := range deps {
			nodes[i].deps = append(nodes[i].deps, nodes[dep].ts)
		}
	}
	return nodes
}

func logDAG(t *testing.T, dag []txnNode) {
	t.Log("transaction dependency graph:")
	for _, node := range dag {
		t.Logf("  %s depends on %v", node.ts, node.deps)
	}
}

// checkApplyOrder verifies that all transactions were applied after their
// dependencies.
func checkApplyOrder(t *testing.T, dag []txnNode, applied []hlc.Timestamp) {
	appliedAt := make(map[hlc.Timestamp]int)
	for i, ts := range applied {
		appliedAt[ts] = i
	}

	deps := make(map[hlc.Timestamp][]hlc.Timestamp)
	for _, node := range dag {
		deps[node.ts] = node.deps
	}

	for ts, order := range appliedAt {
		for _, dep := range deps[ts] {
			depOrder, ok := appliedAt[dep]
			require.True(t, ok, "dependency %s of %s was never applied", dep, ts)
			require.Less(t, depOrder, order,
				"dependency %s (position %d) applied after %s (position %d)",
				dep, depOrder, ts, order)
		}
	}
}

func TestTxnApplier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()
	dag := generateRandomDAG(rng, 100, rng.Intn(10))
	logDAG(t, dag)

	writer := &testWriter{t: t, rng: rng}
	numWriters := max(1, rng.Intn(len(dag)))
	writers := make([]txnwriter.TransactionWriter, numWriters)
	for i := range writers {
		writers[i] = writer
	}

	input := make(chan ScheduledTransaction, len(dag))
	for _, node := range dag {
		input <- ScheduledTransaction{
			Transaction:  ldrdecoder.Transaction{Timestamp: node.ts},
			Dependencies: node.deps,
		}
	}

	applier := NewApplier(input, writers)
	defer applier.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		return applier.Run(ctx, input, applier.Frontier())
	})

	// Consume frontier updates and validate monotonicity.
	lastTs := dag[len(dag)-1].ts
	group.GoCtx(func(ctx context.Context) error {
		var prev hlc.Timestamp
		for ts := range applier.Frontier() {
			if prev.IsSet() && !prev.Less(ts) {
				return errors.Newf("frontier regressed: %s -> %s", prev, ts)
			}
			prev = ts
			t.Logf("frontier advanced to %s", ts)
			if ts.Equal(lastTs) {
				cancel()
				return nil
			}
		}
		return errors.New("frontier channel closed before reaching final transaction")
	})

	err := group.Wait()
	require.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected error: %v", err)

	writer.mu.Lock()
	applied := append([]hlc.Timestamp(nil), writer.mu.log...)
	writer.mu.Unlock()

	require.Equal(t, len(dag), len(applied), "not all transactions were applied")
	checkApplyOrder(t, dag, applied)
}
