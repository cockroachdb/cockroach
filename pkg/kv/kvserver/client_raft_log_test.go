package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue/kvraftlogqueue"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
)

func TestNewTruncateDecisionMaxSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := TestStoreConfig(hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond))
	const exp = 1881
	cfg.RaftLogTruncationThreshold = exp
	ctx := context.Background()
	store := createTestStoreWithConfig(
		ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg,
	)

	repl, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	td, err := repl.NewTruncateDecision(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if td.Input.MaxLogSize != exp {
		t.Fatalf("MaxLogSize %d is unexpected, wanted %d", td.Input.MaxLogSize, exp)
	}
}

// TestNewTruncateDecision verifies that old raft log entries are correctly
// removed.
func TestNewTruncateDecision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 38584)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)
	store.SetRaftLogQueueActive(false)

	r, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	getIndexes := func() (uint64, int, uint64, error) {
		d, err := r.NewTruncateDecision(ctx)
		if err != nil {
			return 0, 0, 0, err
		}
		return d.Input.FirstIndex, d.NumTruncatableIndexes(), d.NewFirstIndex, nil
	}

	aFirst, aTruncatable, aOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if aFirst == 0 {
		t.Errorf("expected first index to be greater than 0, got %d", aFirst)
	}

	// Write a few keys to the range.
	for i := 0; i < kvqueue.RaftLogQueueStaleThreshold+1; i++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", i))
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", i)))
		if _, err := kv.SendWrapped(ctx, store.TestSender(), &args); err != nil {
			t.Fatal(err)
		}
	}

	bFirst, bTruncatable, bOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if aFirst != bFirst {
		t.Fatalf("expected firstIndex to not change, instead it changed from %d -> %d", aFirst, bFirst)
	}
	if aTruncatable >= bTruncatable {
		t.Fatalf("expected truncatableIndexes to increase, instead it changed from %d -> %d", aTruncatable, bTruncatable)
	}
	if aOldest >= bOldest {
		t.Fatalf("expected oldestIndex to increase, instead it changed from %d -> %d", aOldest, bOldest)
	}

	// Enable the raft log scanner and and force a truncation.
	store.SetRaftLogQueueActive(true)
	store.MustForceRaftLogScanAndProcess()
	store.SetRaftLogQueueActive(false)

	// There can be a delay from when the truncation command is issued and the
	// indexes updating.
	var cFirst, cOldest uint64
	var numTruncatable int
	testutils.SucceedsSoon(t, func() error {
		var err error
		cFirst, numTruncatable, cOldest, err = getIndexes()
		if err != nil {
			t.Fatal(err)
		}
		if bFirst == cFirst {
			return errors.Errorf("truncation did not occur, expected firstIndex to change, instead it remained at %d", cFirst)
		}
		return nil
	})
	if bTruncatable < numTruncatable {
		t.Errorf("expected numTruncatable to decrease, instead it changed from %d -> %d", bTruncatable, numTruncatable)
	}
	if bOldest >= cOldest {
		t.Errorf("expected oldestIndex to increase, instead it changed from %d -> %d", bOldest, cOldest)
	}

	verifyLogSizeInSync(t, r)

	// Again, enable the raft log scanner and and force a truncation. This time
	// we expect no truncation to occur.
	store.SetRaftLogQueueActive(true)
	store.MustForceRaftLogScanAndProcess()
	store.SetRaftLogQueueActive(false)

	// Unlike the last iteration, where we expect a truncation and can wait on
	// it with succeedsSoon, we can't do that here. This check is fragile in
	// that the truncation triggered here may lose the race against the call to
	// GetFirstIndex or newTruncateDecision, giving a false negative. Fixing
	// this requires additional instrumentation of the queues, which was deemed
	// to require too much work at the time of this writing.
	dFirst, dTruncatable, dOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if cFirst != dFirst {
		t.Errorf("truncation should not have occurred, but firstIndex changed from %d -> %d", cFirst, dFirst)
	}
	if numTruncatable != dTruncatable {
		t.Errorf("truncation should not have occurred, but truncatableIndexes changed from %d -> %d", numTruncatable, dTruncatable)
	}
	if cOldest != dOldest {
		t.Errorf("truncation should not have occurred, but oldestIndex changed from %d -> %d", cOldest, dOldest)
	}
}

// TestProactiveRaftLogTruncate verifies that we proactively truncate the raft
// log even when replica scanning is disabled.
func TestProactiveRaftLogTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		count     int
		valueSize int
	}{
		// Lots of small KVs.
		{kvqueue.RaftLogQueueStaleSize / 100, 5},
		// One big KV.
		{1, kvqueue.RaftLogQueueStaleSize},
	}
	for _, c := range testCases {
		testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store, _ := createTestStore(ctx, t,
				testStoreOpts{
					// This test was written before test stores could start with more than one
					// range and was not adapted.
					createSystemRanges: false,
				},
				stopper)
			st := store.ClusterSettings()
			kvraftlogqueue.LooselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)
			// Note that turning off the replica scanner does not prevent the queues
			// from processing entries (in this case specifically the raftLogQueue),
			// just that the scanner will not try to push all replicas onto the queues.
			store.SetReplicaScannerActive(false)

			r, err := store.GetReplica(1)
			if err != nil {
				t.Fatal(err)
			}

			oldFirstIndex, err := r.GetFirstIndex()
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < c.count; i++ {
				key := roachpb.Key(fmt.Sprintf("key%02d", i))
				args := putArgs(key, []byte(fmt.Sprintf("%s%02d", strings.Repeat("v", c.valueSize), i)))
				if _, err := kv.SendWrapped(ctx, store.TestSender(), &args); err != nil {
					t.Fatal(err)
				}
			}

			// Log truncation is an asynchronous process and while it will usually occur
			// fairly quickly, there is a slight race between this check and the
			// truncation, especially when under stress.
			testutils.SucceedsSoon(t, func() error {
				if looselyCoupled {
					// Flush the engine to advance durability, which triggers truncation.
					require.NoError(t, store.engine.Flush())
				}
				newFirstIndex, err := r.GetFirstIndex()
				if err != nil {
					t.Fatal(err)
				}
				if newFirstIndex <= oldFirstIndex {
					return errors.Errorf("log was not correctly truncated, old first index:%d, current first index:%d",
						oldFirstIndex, newFirstIndex)
				}
				return nil
			})
		})
	}
}

func TestSnapshotLogTruncationConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	r := &Replica{}
	var storeID roachpb.StoreID
	id1, id2 := uuid.MakeV4(), uuid.MakeV4()
	const (
		index1 = 50
		index2 = 60
	)

	// Add first constraint.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id1, index1, storeID)
	exp1 := map[uuid.UUID]snapTruncationInfo{id1: {index: index1}}

	// Make sure it registered.
	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, exp1)

	// Add another constraint with the same id. Extremely unlikely in practice
	// but we want to make sure it doesn't blow anything up. Collisions are
	// handled by ignoring the colliding update.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id1, index2, storeID)
	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, exp1)

	// Helper that grabs the min constraint index (which can trigger GC as a
	// byproduct) and asserts.
	assertMin := func(exp uint64, now time.Time) {
		t.Helper()
		const anyRecipientStore roachpb.StoreID = 0
		if maxIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now, anyRecipientStore); maxIndex != exp {
			t.Fatalf("unexpected max index %d, wanted %d", maxIndex, exp)
		}
	}

	// Queue should be told index1 is the highest pending one. Note that the
	// colliding update at index2 is not represented.
	assertMin(index1, time.Time{})

	// Add another, higher, index. We're not going to notice it's around
	// until the lower one disappears.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id2, index2, storeID)

	now := timeutil.Now()
	// The colliding snapshot comes back. Or the original, we can't tell.
	r.completeSnapshotLogTruncationConstraint(ctx, id1, now)
	// The index should show up when its deadline isn't hit.
	assertMin(index1, now)
	assertMin(index1, now.Add(kvraftlogqueue.RaftLogQueuePendingSnapshotGracePeriod))
	assertMin(index1, now.Add(kvraftlogqueue.RaftLogQueuePendingSnapshotGracePeriod))
	// Once we're over deadline, the index returned so far disappears.
	assertMin(index2, now.Add(kvraftlogqueue.RaftLogQueuePendingSnapshotGracePeriod+1))
	assertMin(index2, time.Time{})
	assertMin(index2, now.Add(10*kvraftlogqueue.RaftLogQueuePendingSnapshotGracePeriod))

	r.completeSnapshotLogTruncationConstraint(ctx, id2, now)
	assertMin(index2, now)
	assertMin(index2, now.Add(kvraftlogqueue.RaftLogQueuePendingSnapshotGracePeriod))
	assertMin(0, now.Add(2*kvraftlogqueue.RaftLogQueuePendingSnapshotGracePeriod))

	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, map[uuid.UUID]snapTruncationInfo(nil))
}

// TestTruncateLog verifies that the TruncateLog command removes a
// prefix of the raft logs (modifying FirstIndex() and making them
// inaccessible via Entries()).
func TestTruncateLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
		tc := testContext{}
		ctx := context.Background()
		cfg := TestStoreConfig(nil)
		cfg.TestingKnobs.DisableRaftLogQueue = true
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)
		st := tc.store.ClusterSettings()
		kvraftlogqueue.LooselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)

		// Populate the log with 10 entries. Save the LastIndex after each write.
		var indexes []uint64
		for i := 0; i < 10; i++ {
			args := incrementArgs([]byte("a"), int64(i))

			if _, pErr := tc.SendWrapped(args); pErr != nil {
				t.Fatal(pErr)
			}
			idx, err := tc.repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			indexes = append(indexes, idx)
		}

		rangeID := tc.repl.RangeID

		// Discard the first half of the log.
		truncateArgs := truncateLogArgs(indexes[5], rangeID)
		if _, pErr := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}

		waitForTruncationForTesting(t, tc.repl, indexes[5], looselyCoupled)

		// We can still get what remains of the log.
		tc.repl.mu.Lock()
		entries, err := tc.repl.raftEntriesLocked(indexes[5], indexes[9], math.MaxUint64)
		tc.repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != int(indexes[9]-indexes[5]) {
			t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
		}

		// But any range that includes the truncated entries returns an error.
		tc.repl.mu.Lock()
		_, err = tc.repl.raftEntriesLocked(indexes[4], indexes[9], math.MaxUint64)
		tc.repl.mu.Unlock()
		if !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}

		// The term of the last truncated entry is still available.
		tc.repl.mu.Lock()
		term, err := tc.repl.raftTermRLocked(indexes[4])
		tc.repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if term == 0 {
			t.Errorf("invalid term 0 for truncated entry")
		}

		// The terms of older entries are gone.
		tc.repl.mu.Lock()
		_, err = tc.repl.raftTermRLocked(indexes[3])
		tc.repl.mu.Unlock()
		if !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}

		// Truncating logs that have already been truncated should not return an
		// error.
		truncateArgs = truncateLogArgs(indexes[3], rangeID)
		if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Truncating logs that have the wrong rangeID included should not return
		// an error but should not truncate any logs.
		truncateArgs = truncateLogArgs(indexes[9], rangeID+1)
		if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}

		tc.repl.mu.Lock()
		// The term of the last truncated entry is still available.
		term, err = tc.repl.raftTermRLocked(indexes[4])
		tc.repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if term == 0 {
			t.Errorf("invalid term 0 for truncated entry")
		}
	})
}

// TestTruncateLogRecompute checks that if raftLogSize is not trusted, the raft
// log queue picks up the replica, recomputes the log size, and considers a
// truncation.
func TestTruncateLogRecompute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DisableRaftLogQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	key := roachpb.Key("a")
	repl := tc.store.LookupReplica(keys.MustAddr(key))

	trusted := func() bool {
		repl.mu.Lock()
		defer repl.mu.Unlock()
		return repl.mu.raftLogSizeTrusted
	}

	put := func() {
		var v roachpb.Value
		v.SetBytes(bytes.Repeat([]byte("x"), kvqueue.RaftLogQueueStaleSize*5))
		put := roachpb.NewPut(key, v)
		var ba roachpb.BatchRequest
		ba.Add(put)
		ba.RangeID = repl.RangeID

		if _, pErr := tc.store.Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	put()

	decision, err := repl.NewTruncateDecision(ctx)
	assert.NoError(t, err)
	assert.True(t, decision.ShouldTruncate())
	// Should never trust initially, until recomputed at least once.
	assert.False(t, trusted())

	repl.mu.Lock()
	repl.mu.raftLogSizeTrusted = false
	repl.mu.raftLogSize += 12          // garbage
	repl.mu.raftLogLastCheckSize += 12 // garbage
	repl.mu.Unlock()

	// Force a raft log queue run. The result should be a nonzero Raft log of
	// size below the threshold (though we won't check that since it could have
	// grown over threshold again; we compute instead that its size is correct).
	tc.store.SetRaftLogQueueActive(true)
	tc.store.MustForceRaftLogScanAndProcess()

	for i := 0; i < 2; i++ {
		verifyLogSizeInSync(t, repl)
		assert.True(t, trusted())
		put() // make sure we remain trusted and in sync
	}
}

func verifyLogSizeInSync(t *testing.T, r *Replica) {
	t.Helper()
	unlock := r.LockRaftMuForTesting()
	defer unlock()
	raftLogSize, _ := r.GetRaftLogSize()
	actualRaftLogSize, err := kvraftlogqueue.ComputeRaftLogSize(context.Background(), r.RangeID, r.Engine(), r.SideloadedRaftMuLocked())
	if err != nil {
		t.Fatal(err)
	}
	if actualRaftLogSize != raftLogSize {
		t.Fatalf("replica claims raft log size %d, but computed %d", raftLogSize, actualRaftLogSize)
	}
}

func waitForTruncationForTesting(
	t *testing.T, r *Replica, newFirstIndex uint64, looselyCoupled bool,
) {
	testutils.SucceedsSoon(t, func() error {
		if looselyCoupled {
			// Flush the engine to advance durability, which triggers truncation.
			require.NoError(t, r.Engine().Flush())
		}
		// FirstIndex should have changed.
		firstIndex, err := r.GetFirstIndex()
		require.NoError(t, err)
		if firstIndex != newFirstIndex {
			return errors.Errorf("expected firstIndex == %d, got %d", newFirstIndex, firstIndex)
		}
		// Some low-level tests also look at the raftEntryCache or sideloaded
		// storage, which are updated after, and non-atomically with the change to
		// first index (latter holds Replica.mu). Since the raftLogTruncator holds Replica.raftMu
		// for the duration of its work, we can, by acquiring and releasing raftMu here, ensure
		// that we have waited for it to finish.
		unlock := r.LockRaftMuForTesting()
		defer unlock()
		return nil
	})
}
