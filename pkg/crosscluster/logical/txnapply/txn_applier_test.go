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
	delay := time.Duration(w.rng.Int63n(int64(100 * time.Microsecond)))
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

// txnNode represents a transaction with its dependencies and an optional
// EventHorizon that must be met before the transaction can be applied.
type txnNode struct {
	ts           hlc.Timestamp
	deps         []hlc.Timestamp
	eventHorizon hlc.Timestamp
}

// txnOpt is an option for constructing a txnNode via the txn helper.
type txnOpt interface{ add(*txnNode) }

type depsOpt []int64

func (d depsOpt) add(n *txnNode) {
	for _, w := range d {
		n.deps = append(n.deps, hlc.Timestamp{WallTime: w})
	}
}

func deps(wallTimes ...int64) depsOpt { return depsOpt(wallTimes) }

type horizonOpt int64

func (h horizonOpt) add(n *txnNode) {
	n.eventHorizon = hlc.Timestamp{WallTime: int64(h)}
}

func horizon(wallTime int64) horizonOpt { return horizonOpt(wallTime) }

func txn(wallTime int64, opts ...txnOpt) txnNode {
	n := txnNode{ts: hlc.Timestamp{WallTime: wallTime}}
	for _, o := range opts {
		o.add(&n)
	}
	return n
}

// generateRandomDAG creates a random DAG of transactions where each transaction
// can depend on earlier transactions. Two invariants are maintained:
//
//  1. Each txn's eventHorizon is strictly less than the timestamp of its
//     earliest dependency.
//  2. EventHorizon values are monotonically non-decreasing with respect to
//     transaction timestamps: if txn.ts > txn2.ts then
//     txn.EventHorizon >= txn2.EventHorizon.
func generateRandomDAG(rng *rand.Rand, numTxns int, maxDeps int) []txnNode {
	nodes := make([]txnNode, numTxns)
	var maxHorizonWallTime int64
	for i := range nodes {
		nodes[i].ts = hlc.Timestamp{WallTime: int64(i + 1)}

		horizonRange := int64(i) - maxHorizonWallTime + 1
		horizonWallTime := maxHorizonWallTime + int64(rng.Intn(int(horizonRange)))
		nodes[i].eventHorizon = hlc.Timestamp{WallTime: horizonWallTime}
		maxHorizonWallTime = horizonWallTime

		// Dependencies are restricted to transactions with timestamps
		// strictly greater than the event horizon.
		availableStart := int(horizonWallTime)
		availableCount := i - availableStart
		if availableCount > 0 {
			numDeps := min(rng.Intn(maxDeps+1), availableCount)
			perm := rng.Perm(availableCount)[:numDeps]
			for _, p := range perm {
				nodes[i].deps = append(nodes[i].deps, nodes[availableStart+p].ts)
			}
		}
	}
	return nodes
}

func logDAG(t *testing.T, dag []txnNode) {
	t.Log("transaction dependency graph:")
	for _, node := range dag {
		if node.eventHorizon.IsSet() {
			t.Logf("  %s depends on %v horizon=%s", node.ts, node.deps, node.eventHorizon)
		} else {
			t.Logf("  %s depends on %v", node.ts, node.deps)
		}
	}
}

// runApplier runs the applier pipeline with the given DAG and number of
// writers, returning the ordered log of applied transaction timestamps.
func runApplier(t *testing.T, dag []txnNode, numWriters int, rngSeed int64) []hlc.Timestamp {
	t.Helper()

	rng := rand.New(randutil.NewLockedSource(rngSeed))
	writer := &testWriter{t: t, rng: rng}
	writers := make([]txnwriter.TransactionWriter, numWriters)
	for i := range writers {
		writers[i] = writer
	}

	input := make(chan ScheduledTransaction, len(dag))
	for _, node := range dag {
		input <- ScheduledTransaction{
			Transaction:  ldrdecoder.Transaction{Timestamp: node.ts},
			Dependencies: node.deps,
			EventHorizon: node.eventHorizon,
		}
	}

	applier := NewApplier(writers)
	defer applier.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		return applier.Run(ctx, input)
	})

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
	require.True(t, err == nil || errors.Is(err, context.Canceled),
		"unexpected error: %v", err)

	checkApplierDrained(t, applier)

	writer.mu.Lock()
	applied := append([]hlc.Timestamp(nil), writer.mu.log...)
	writer.mu.Unlock()

	return applied
}

// checkApplierDrained verifies that the applier's internal buffers are empty
// after all transactions have been processed.
func checkApplierDrained(t *testing.T, applier *Applier) {
	t.Helper()
	applier.mu.Lock()
	defer applier.mu.Unlock()
	require.Empty(t, applier.mu.transactions, "transactions map should be empty")
	require.Empty(t, applier.mu.waiting, "waiting map should be empty")
	require.Equal(t, 0, applier.mu.timestamps.Len(), "timestamps buffer should be empty")
	require.Empty(t, applier.mu.horizonWaiting, "horizonWaiting should be empty")
}

// checkApplyOrder verifies that all transactions were applied after their
// dependencies and after their EventHorizon was reached.
func checkApplyOrder(t *testing.T, dag []txnNode, applied []hlc.Timestamp) {
	appliedAt := make(map[hlc.Timestamp]int)
	for i, ts := range applied {
		appliedAt[ts] = i
	}

	deps := make(map[hlc.Timestamp][]hlc.Timestamp)
	horizons := make(map[hlc.Timestamp]hlc.Timestamp)
	for _, node := range dag {
		deps[node.ts] = node.deps
		horizons[node.ts] = node.eventHorizon
	}

	for ts, order := range appliedAt {
		for _, dep := range deps[ts] {
			depOrder, ok := appliedAt[dep]
			require.True(t, ok, "dependency %s of %s was never applied", dep, ts)
			require.Less(t, depOrder, order,
				"dependency %s (position %d) applied after %s (position %d)",
				dep, depOrder, ts, order)
		}

		// Verify that the transaction was not applied before its EventHorizon
		// was reached. The EventHorizon transaction must have been applied
		// before this transaction.
		horizon := horizons[ts]
		if horizon.IsSet() {
			horizonOrder, ok := appliedAt[horizon]
			require.True(t, ok, "event horizon %s of %s was never applied", horizon, ts)
			require.Less(t, horizonOrder, order,
				"event horizon %s (position %d) applied after %s (position %d)",
				horizon, horizonOrder, ts, order)
		}
	}
}

func TestTxnApplierRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, seed := randutil.NewTestRand()
	dag := generateRandomDAG(rng, 100, rng.Intn(10))
	logDAG(t, dag)

	numWriters := max(1, rng.Intn(len(dag)))
	applied := runApplier(t, dag, numWriters, seed)
	require.Equal(t, len(dag), len(applied), "not all transactions were applied")
	checkApplyOrder(t, dag, applied)
}

func TestTxnApplierDeterministic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		dag  []txnNode
	}{
		{
			// t2 depends on t1, so t1 must be applied before t2.
			name: "dependency",
			dag:  []txnNode{txn(1), txn(2, deps(1))},
		},
		{
			// t2 has no explicit dependency on t1 but has an EventHorizon
			// equal to t1's timestamp. The applier must wait for the
			// replicated time to advance past t1 before applying t2.
			name: "event_horizon",
			dag:  []txnNode{txn(1), txn(2, horizon(1))},
		},
		{
			// t1 -> t2 -> t3: strict ordering through a chain of
			// dependencies.
			name: "chain",
			dag:  []txnNode{txn(1), txn(2, deps(1)), txn(3, deps(2))},
		},
		{
			// t3 depends on t2 and has an EventHorizon of t1, so t3 must
			// be applied after both t1 and t2.
			name: "horizon_and_dependency",
			dag:  []txnNode{txn(1), txn(2), txn(3, deps(2), horizon(1))},
		},
		{
			// t3 depends on both t1 and t2, so both must be applied
			// before t3.
			name: "multiple_dependencies",
			dag:  []txnNode{txn(1), txn(2), txn(3, deps(1, 2))},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logDAG(t, tc.dag)
			applied := runApplier(t, tc.dag, 3, 0)
			require.Equal(t, len(tc.dag), len(applied))
			checkApplyOrder(t, tc.dag, applied)
		})
	}
}

// TestTxnApplierIndependent verifies that independent transactions (no
// dependencies, no event horizon) can be applied in either order. With random
// writer delays, both orderings should be observed across iterations.
func TestTxnApplierIndependent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dag := []txnNode{txn(1), txn(2)}
	var sawT1First, sawT2First bool
	for i := 0; !(sawT1First && sawT2First); i++ {
		require.Less(t, i, 100, "expected both orderings within 100 iterations")
		applied := runApplier(t, dag, 3, int64(i))
		require.Equal(t, len(dag), len(applied))
		if applied[0].WallTime == 1 {
			sawT1First = true
		} else {
			sawT2First = true
		}
	}
}
