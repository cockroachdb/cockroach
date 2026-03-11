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
		w.t.Logf("applied txn at %s", txn.TxnID.Timestamp)
		w.mu.log = append(w.mu.log, txn.TxnID.Timestamp)
		results[i] = txnwriter.ApplyResult{AppliedRows: len(txn.WriteSet)}
	}
	return results, nil
}

func (w *testWriter) Close(ctx context.Context) {}

// txnNode represents a transaction with its dependencies and an optional
// EventHorizon that must be met before the transaction can be applied.
type txnNode struct {
	id           ldrdecoder.TxnID
	deps         []ldrdecoder.TxnID
	eventHorizon hlc.Timestamp
}

// txnOpt is an option for constructing a txnNode via the txn helper.
type txnOpt interface{ add(*txnNode) }

type depsOpt []int64

func (d depsOpt) add(n *txnNode) {
	for _, w := range d {
		n.deps = append(n.deps, ldrdecoder.TxnID{
			Timestamp: hlc.Timestamp{WallTime: w},
			ApplierID: 1,
		})
	}
}

func deps(wallTimes ...int64) depsOpt { return depsOpt(wallTimes) }

// ddepsOpt is a dependency option that specifies (applierID, wallTime) pairs,
// allowing dependencies on transactions owned by other appliers.
type ddepsOpt []ldrdecoder.TxnID

func (d ddepsOpt) add(n *txnNode) {
	n.deps = append(n.deps, d...)
}

// ddeps creates cross-applier dependencies. Arguments are (applierID, wallTime)
// pairs.
func ddeps(pairs ...int64) ddepsOpt {
	if len(pairs)%2 != 0 {
		panic("ddeps requires (applierID, wallTime) pairs")
	}
	out := make(ddepsOpt, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out[i/2] = ldrdecoder.TxnID{
			ApplierID: ldrdecoder.ApplierID(pairs[i]),
			Timestamp: hlc.Timestamp{WallTime: pairs[i+1]},
		}
	}
	return out
}

type horizonOpt int64

func (h horizonOpt) add(n *txnNode) {
	n.eventHorizon = hlc.Timestamp{WallTime: int64(h)}
}

func horizon(wallTime int64) horizonOpt { return horizonOpt(wallTime) }

func txn(wallTime int64, opts ...txnOpt) txnNode {
	n := txnNode{id: ldrdecoder.TxnID{
		Timestamp: hlc.Timestamp{WallTime: wallTime},
		ApplierID: 1,
	}}
	for _, o := range opts {
		o.add(&n)
	}
	return n
}

// dtxn creates a txnNode assigned to a specific applier.
func dtxn(applierID ldrdecoder.ApplierID, wallTime int64, opts ...txnOpt) txnNode {
	n := txnNode{id: ldrdecoder.TxnID{
		Timestamp: hlc.Timestamp{WallTime: wallTime},
		ApplierID: applierID,
	}}
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
func generateRandomDAG(rng *rand.Rand, numTxns int, maxDeps int, numAppliers int) []txnNode {
	nodes := make([]txnNode, numTxns)
	var maxHorizonWallTime int64
	for i := range nodes {
		applierID := ldrdecoder.ApplierID(1)
		if numAppliers > 1 {
			applierID = ldrdecoder.ApplierID(rng.Intn(numAppliers) + 1)
		}
		nodes[i].id = ldrdecoder.TxnID{
			Timestamp: hlc.Timestamp{WallTime: int64(i + 1)},
			ApplierID: applierID,
		}

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
				nodes[i].deps = append(nodes[i].deps, nodes[availableStart+p].id)
			}
		}
	}
	return nodes
}

func logDAG(t *testing.T, dag []txnNode) {
	t.Log("transaction dependency graph:")
	for _, node := range dag {
		if node.eventHorizon.IsSet() {
			t.Logf("  %s depends on %v horizon=%s", node.id.Timestamp, node.deps, node.eventHorizon)
		} else {
			t.Logf("  %s depends on %v", node.id.Timestamp, node.deps)
		}
	}
}

// checkApplierDrained verifies that the applier's internal buffers are empty
// after all transactions have been processed.
func checkApplierDrained(t *testing.T, applier *Applier) {
	t.Helper()
	applier.mu.Lock()
	defer applier.mu.Unlock()
	require.Empty(t, applier.mu.transactions, "transactions map should be empty")
	require.Empty(t, applier.mu.localWaiting, "waiting map should be empty")
	require.Equal(t, 0, applier.mu.txnIDs.Len(), "txnIDs buffer should be empty")
	require.Empty(t, applier.mu.horizonWaiting, "horizonWaiting should be empty")
}

// checkApplyOrder verifies that all transactions were applied after their
// dependencies and after their EventHorizon was reached.
func checkApplyOrder(t *testing.T, dag []txnNode, applied []hlc.Timestamp) {
	appliedAt := make(map[hlc.Timestamp]int)
	for i, ts := range applied {
		appliedAt[ts] = i
	}

	deps := make(map[hlc.Timestamp][]ldrdecoder.TxnID)
	horizons := make(map[hlc.Timestamp]hlc.Timestamp)
	for _, node := range dag {
		deps[node.id.Timestamp] = node.deps
		horizons[node.id.Timestamp] = node.eventHorizon
	}

	for ts, order := range appliedAt {
		for _, dep := range deps[ts] {
			depOrder, ok := appliedAt[dep.Timestamp]
			require.True(t, ok, "dependency %s of %s was never applied", dep.Timestamp, ts)
			require.Less(t, depOrder, order,
				"dependency %s (position %d) applied after %s (position %d)",
				dep.Timestamp, depOrder, ts, order)
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
	dag := generateRandomDAG(rng, 100, rng.Intn(10), 1 /* numAppliers */)
	logDAG(t, dag)

	numWriters := max(1, rng.Intn(len(dag)))
	applied := runDistributedApplier(t, dag, numWriters, seed)
	require.Equal(t, len(dag), len(applied), "not all transactions were applied")
	checkApplyOrder(t, dag, applied)
}

func TestDistributedTxnApplierRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, seed := randutil.NewTestRand()
	numAppliers := 2 + rng.Intn(4) // 2 to 5 appliers
	dag := generateRandomDAG(rng, 100, rng.Intn(10), numAppliers)
	logDAG(t, dag)
	t.Logf("numAppliers=%d", numAppliers)

	numWriters := max(1, rng.Intn(len(dag)))
	applied := runDistributedApplier(t, dag, numWriters, seed)
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
			applied := runDistributedApplier(t, tc.dag, 3, 0)
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
		applied := runDistributedApplier(t, dag, 3, int64(i))
		require.Equal(t, len(dag), len(applied))
		if applied[0].WallTime == 1 {
			sawT1First = true
		} else {
			sawT2First = true
		}
	}
}

// runDistributedApplier runs multiple Applier instances sharing a single
// DependencyTracker, routing each txnNode to the Applier matching its
// ApplierID. Returns the globally ordered log of applied transaction
// timestamps across all appliers.
func runDistributedApplier(
	t *testing.T, dag []txnNode, numWritersPerApplier int, rngSeed int64,
) []hlc.Timestamp {
	t.Helper()

	// Group txns per applier, preserving order.
	txnsByApplier := make(map[ldrdecoder.ApplierID][]txnNode)
	for _, node := range dag {
		txnsByApplier[node.id.ApplierID] = append(
			txnsByApplier[node.id.ApplierID], node)
	}

	ids := make([]ldrdecoder.ApplierID, 0, len(txnsByApplier))
	for id := range txnsByApplier {
		ids = append(ids, id)
	}

	depTracker := NewDependencyTracker(ids)

	// Shared writer so all appliers record to one log, giving us a global
	// application order.
	rng := rand.New(randutil.NewLockedSource(rngSeed))
	sharedWriter := &testWriter{t: t, rng: rng}

	// Compute maxTs across all txns for the checkpoint.
	var maxTs hlc.Timestamp
	for _, node := range dag {
		if maxTs.Less(node.id.Timestamp) {
			maxTs = node.id.Timestamp
		}
	}

	appliers := make(map[ldrdecoder.ApplierID]*Applier)
	inputs := make(map[ldrdecoder.ApplierID]chan ApplierEvent)
	for _, id := range ids {
		writers := make([]txnwriter.TransactionWriter, numWritersPerApplier)
		for i := range writers {
			writers[i] = sharedWriter
		}
		a, err := NewApplier(id, writers, depTracker, ids)
		require.NoError(t, err)

		// Build events for this applier. Send its own txns in order,
		// with gap-filling checkpoints so the applier's frontier can
		// advance through timestamp ranges where it has no txns. This
		// prevents deadlocks where another applier's event horizon
		// needs this applier's frontier to advance.
		txns := txnsByApplier[id]
		input := make(chan ApplierEvent, 2*len(txns)+1)
		for _, txn := range txns {
			gapEnd := hlc.Timestamp{WallTime: txn.id.Timestamp.WallTime - 1}
			if gapEnd.IsSet() {
				input <- Checkpoint{Timestamp: gapEnd}
			}
			input <- ScheduledTransaction{
				Transaction:  ldrdecoder.Transaction{TxnID: txn.id},
				Dependencies: txn.deps,
				EventHorizon: txn.eventHorizon,
			}
		}
		input <- Checkpoint{Timestamp: maxTs}

		appliers[id] = a
		inputs[id] = input
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	group := ctxgroup.WithContext(ctx)

	// Launch each applier.
	for id, a := range appliers {
		defer a.Close(context.Background())
		group.GoCtx(func(ctx context.Context) error {
			return a.Run(ctx, inputs[id])
		})
	}

	// Watch each applier's frontier; cancel once all have reached their
	// final timestamp.
	var frontierMu syncutil.Mutex
	done := make(map[ldrdecoder.ApplierID]bool)
	for id, a := range appliers {
		lastTs := maxTs
		_ = txnsByApplier[id] // ensure the applier has txns
		group.GoCtx(func(ctx context.Context) error {
			var prev hlc.Timestamp
			for ts := range a.Frontier() {
				if prev.IsSet() && !prev.Less(ts) {
					return errors.Newf(
						"applier %d frontier regressed: %s -> %s", id, prev, ts)
				}
				prev = ts
				t.Logf("applier %d frontier advanced to %s", id, ts)
				if ts.Equal(lastTs) {
					frontierMu.Lock()
					done[id] = true
					allDone := len(done) == len(appliers)
					frontierMu.Unlock()
					if allDone {
						cancel()
					}
					return nil
				}
			}
			return errors.Newf(
				"applier %d frontier closed before reaching final txn", id)
		})
	}

	err := group.Wait()
	require.True(t, err == nil || errors.Is(err, context.Canceled),
		"unexpected error: %v", err)

	for _, a := range appliers {
		checkApplierDrained(t, a)
	}

	sharedWriter.mu.Lock()
	applied := append([]hlc.Timestamp(nil), sharedWriter.mu.log...)
	sharedWriter.mu.Unlock()
	return applied
}

func TestDistributedApplierDeterministic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		dag  []txnNode
	}{
		{
			// Txn 2 on applier 2 depends on txn 1 on applier 1.
			// Exercises cross-applier dependency resolution via
			// DependencyTracker.
			name: "cross_applier_dependency",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(2, 2, ddeps(1, 1)),
			},
		},
		{
			// Txn 2 on applier 2 has an EventHorizon of ts=1. Applier 2
			// cannot apply txn 2 until the global replicated time advances
			// past ts=1, which requires applier 1's frontier to advance.
			name: "cross_applier_event_horizon",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(2, 2, horizon(1)),
			},
		},
		{
			// Chain across 3 appliers: applier 1 -> applier 2 -> applier 3.
			// Each applier waits for the previous one's txn to complete.
			name: "cross_applier_chain",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(2, 2, ddeps(1, 1)),
				dtxn(3, 3, ddeps(2, 2)),
			},
		},
		{
			// Txn 3 on applier 3 depends on txn 2 (applier 2) and has an
			// EventHorizon of ts=1 (must wait for applier 1 to advance).
			name: "cross_applier_horizon_and_dependency",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(2, 2),
				dtxn(3, 3, ddeps(2, 2), horizon(1)),
			},
		},
		{
			// Txn 3 on applier 3 depends on txns from both applier 1 and
			// applier 2. Must wait for both to complete.
			name: "cross_applier_multiple_dependencies",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(2, 2),
				dtxn(3, 3, ddeps(1, 1), ddeps(2, 2)),
			},
		},
		{
			// Applier 1 has two txns (ts=1 and ts=3). Applier 2 has one
			// txn (ts=2). The third txn on applier 1 depends on applier 2's
			// txn, so applier 1 must wait for applier 2 before applying it.
			name: "cross_applier_dependency_back_to_source",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(2, 2),
				dtxn(1, 3, ddeps(2, 2)),
			},
		},
		{
			// Two txns on applier 2 are both blocked by event horizons.
			// Txn 2 waits for ts=1, txn 3 waits for ts=2. Both require
			// applier 1's frontier to advance before they can be applied.
			name: "cross_applier_event_horizon_two_blocked",
			dag: []txnNode{
				dtxn(1, 1),
				dtxn(1, 2),
				dtxn(2, 3, horizon(1)),
				dtxn(2, 4, horizon(2)),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logDAG(t, tc.dag)
			applied := runDistributedApplier(t, tc.dag, 3, 0)
			require.Equal(t, len(tc.dag), len(applied))
			checkApplyOrder(t, tc.dag, applied)
		})
	}
}
