// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLastUpdateTimesMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	m := make(lastUpdateTimesMap)
	t1 := time.Time{}.Add(time.Second)
	t2 := t1.Add(time.Second)
	m.update(3, t1)
	m.update(1, t2)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{1: t2, 3: t1}, m)
	descs := []roachpb.ReplicaDescriptor{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}, {ReplicaID: 4}}

	t3 := t2.Add(time.Second)
	m.updateOnBecomeLeader(descs, t3)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{1: t3, 2: t3, 3: t3, 4: t3}, m)

	t4 := t3.Add(time.Second)
	descs = append(descs, []roachpb.ReplicaDescriptor{{ReplicaID: 5}, {ReplicaID: 6}}...)
	prs := map[raftpb.PeerID]tracker.Progress{
		1: {State: tracker.StateReplicate}, // should be updated
		// 2 is missing because why not
		3: {State: tracker.StateProbe},     // should be ignored
		4: {State: tracker.StateSnapshot},  // should be ignored
		5: {State: tracker.StateProbe},     // should be ignored
		6: {State: tracker.StateReplicate}, // should be added
		7: {State: tracker.StateReplicate}, // ignored, not in descs
	}
	m.updateOnUnquiesce(descs, prs, t4)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{
		1: t4,
		2: t3,
		3: t3,
		4: t3,
		6: t4,
	}, m)
}

func Test_handleRaftReadyStats_SafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	now := crtime.NowMono()
	ts := func(s int) crtime.Mono {
		return now + crtime.Mono(time.Duration(s)*time.Second)
	}

	stats := handleRaftReadyStats{
		tBegin:            ts(1),
		tEnd:              ts(6),
		tApplicationBegin: ts(1),
		tApplicationEnd:   ts(2),
		apply: applyCommittedEntriesStats{
			numBatchesProcessed: 9,
			appBatchStats: appBatchStats{
				numEntriesProcessed:      2,
				numEntriesProcessedBytes: 3,
				numEmptyEntries:          5,
				numAddSST:                3,
				numAddSSTCopies:          1,
			},
			stateAssertions:      4,
			numConfChangeEntries: 6,
		},
		append: logstore.AppendStats{
			Begin:             ts(2),
			End:               ts(3),
			RegularEntries:    7,
			RegularBytes:      1024,
			SideloadedEntries: 3,
			SideloadedBytes:   5 * (1 << 20),
			PebbleBegin:       ts(3),
			PebbleEnd:         ts(4),
			PebbleBytes:       1024 * 5,
			PebbleCommitStats: storage.BatchCommitStats{
				BatchCommitStats: pebble.BatchCommitStats{
					TotalDuration:               100 * time.Millisecond,
					SemaphoreWaitDuration:       2 * time.Millisecond,
					WALQueueWaitDuration:        5 * time.Millisecond,
					MemTableWriteStallDuration:  8 * time.Millisecond,
					L0ReadAmpWriteStallDuration: 11 * time.Millisecond,
					WALRotationDuration:         14 * time.Millisecond,
					CommitWaitDuration:          17 * time.Millisecond,
				},
			},
			Sync: true,
		},
		tSnapBegin: ts(4),
		tSnapEnd:   ts(5),
		snap: handleSnapshotStats{
			offered: true,
			applied: true,
		},
	}

	echotest.Require(t, string(redact.Sprint(stats)),
		filepath.Join(datapathutils.TestDataPath(t, "handle_raft_ready_stats.txt")))
}

// TestProposalsWithInjectedLeaseIndexAndReproposalError runs a number of
// increment operations whose proposals are randomly subjected to invalid
// LeaseAppliedIndex assignments, and for which reproposals randomly fail with
// an injected error. The test verifies that increments that "went through" are
// actually present (and not duplicated) and that operations that failed with an
// unambiguous error did not apply.
func TestProposalsWithInjectedLeaseIndexAndReproposalError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "pipelined", testProposalsWithInjectedLeaseIndexAndReproposalError)
}

func testProposalsWithInjectedLeaseIndexAndReproposalError(t *testing.T, pipelined bool) {
	stopper := stop.NewStopper()
	ctx := context.Background()

	tc := testContext{}

	isOurCommand := func(ba *kvpb.BatchRequest) (_ string, ok bool) {
		if ba == nil {
			return "", false // not local proposal
		} else if inc, found := ba.GetArg(kvpb.Increment); !found {
			return "", false
		} else if key := string(inc.(*kvpb.IncrementRequest).Key); strings.HasSuffix(key, "-testing") {
			return key, true
		}
		return "", false
	}

	rnd, seed := randutil.NewPseudoRand()
	t.Logf("seed: %d", seed)

	shouldInject := func(baseProb float64, attempt int) bool { // start at attempt one
		// Example: baseProb = 0.8
		// On attempt 1, 80% chance of catching retry.
		// On attempt 2, 40%.
		// On attempt 3, 27%.
		// And so on.
		thresh := baseProb / float64(attempt)
		return rnd.Float64() < thresh
	}

	cfg := TestStoreConfig(hlc.NewClockForTesting(nil))

	var injectedReproposalErrors atomic.Int64
	{
		var mu syncutil.Mutex
		seen := map[string]int{} // access from proposal buffer under raftMu
		cfg.TestingKnobs.InjectReproposalError = func(p *ProposalData) error {
			key, ok := isOurCommand(p.Request)
			if !ok {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			seen[key]++
			if !shouldInject(0.2, seen[key]) {
				return nil
			}
			injectedReproposalErrors.Add(1)
			t.Logf("inserting reproposal error for %s (seen %d times)", key, seen[key])
			return errors.Errorf("injected error")
		}
	}

	var insertedIllegalLeaseIndex atomic.Int64
	{
		var mu syncutil.Mutex
		seen := map[string]int{}
		cfg.TestingKnobs.LeaseIndexFilter = func(p *ProposalData) kvpb.LeaseAppliedIndex {
			key, ok := isOurCommand(p.Request)
			if !ok {
				return 0
			}
			mu.Lock()
			seen[key]++
			defer mu.Unlock()
			if !shouldInject(0.8, seen[key]) { // very frequent reproposals
				return 0
			}
			insertedIllegalLeaseIndex.Add(1)
			// LAI 1 is always going to fail because the LAI is initialized when the lease
			// comes into existence. (It's important that we pick one here that reliably
			// fails because otherwise we may accidentally regress the closed timestamp[^1].
			//
			// [^1]: https://github.com/cockroachdb/cockroach/issues/70894#issuecomment-1433244880
			return 1
		}
	}

	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	defer checkNoLeakedTraceSpans(t, tc.store)
	defer stopper.Stop(ctx)

	tc.store.cfg.Tracer().SetActiveSpansRegistryEnabled(true)
	tc.store.cfg.Tracer().PanicOnUseAfterFinish()

	kvcoord.PipelinedWritesEnabled.Override(ctx, &tc.store.ClusterSettings().SV, pipelined)

	k := func(idx int) roachpb.Key {
		var key roachpb.Key
		key = append(key, tc.repl.Desc().StartKey...)
		key = append(key, fmt.Sprintf("%05d", idx)...)
		key = append(key, "-testing"...)
		return key
	}
	var observedAsyncWriteFailures atomic.Int64
	var observedReproposalErrors atomic.Int64
	const iters = 300
	expectations := map[string]int{}
	for i := 0; i < iters; i++ {
		key := k(i)

		if err := tc.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			defer func() {
				if err != nil {
					t.Logf("%d: error: %s", i, err)
					var txnErr *kvpb.TransactionRetryWithProtoRefreshError
					// NB: the string matching here is sad but the error has been
					// flattened away at this point.
					ignored := true
					switch {
					case errors.As(err, &txnErr) && strings.Contains(txnErr.String(), `missing intent on`):
						observedAsyncWriteFailures.Add(1)
					case testutils.IsError(err, `injected error`):
						observedReproposalErrors.Add(1)
					case errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)):
						t.Logf("%d: ignoring: %s", i, err)
					default:
						ignored = false
					}
					if ignored {
						t.Logf("%d: ignoring: %s", i, err)
					} else {
						t.Errorf("%d: %s", i, err)
					}
				}
			}()
			// The handling of trace spans during reproposals is tricky,
			// so make sure everyone is tracing to have the best chance
			// at causing problems. Note that the tracer was configured
			// to always trace and also to debug use-after-finish.
			ctx, sp := tc.store.cfg.Tracer().StartSpanCtx(ctx, fmt.Sprintf("op %d", i))
			defer sp.Finish()

			// We write and delete the key first. If there were a bug in the
			// reproposal pipeline, it could happen that the increment below and this
			// rewrite cycle got reordered, and we would notice that the key didn't
			// have the right value in the end. (The Put shouldn't make a difference
			// but it removes the question that may arise in the mind of a reader of
			// whether the Delete actually causes a write). Another benefit of this
			// sequence is that if we leaked any latches, the test would stall, since
			// we are mutating the same key repeatedly.
			if err := txn.Put(ctx, key, "hello"); err != nil {
				return err
			}
			if _, err := txn.Del(ctx, key); err != nil {
				return err
			}

			resp, err := txn.Inc(ctx, key, 1)
			if err != nil {
				return err
			}
			n, err := resp.Value.GetInt()
			if err != nil {
				return err
			}
			if n != 1 {
				return errors.Errorf("%d: got %d", i, n)
			}
			return txn.Commit(ctx)
		}); err == nil {
			expectations[string(key)] = 1
		} else if !errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
			expectations[string(key)] = 0
		}
	}

	for keyString, exp := range expectations {
		keyVal, err := tc.store.DB().Get(ctx, roachpb.Key(keyString))
		require.NoError(t, err)
		if exp == 0 {
			require.Nil(t, keyVal.Value)
			continue
		}
		n, err := keyVal.Value.GetInt()
		require.NoError(t, err)
		require.EqualValues(t, exp, n)
	}

	t.Logf("observed %d async write restarts, observed %d/%d injected aborts, %d injected illegal lease applied indexes",
		observedAsyncWriteFailures.Load(), observedReproposalErrors.Load(), injectedReproposalErrors.Load(), insertedIllegalLeaseIndex.Load())
	t.Logf("commands reproposed (unchanged): %d", tc.store.metrics.RaftCommandsReproposed.Count())
	t.Logf("commands reproposed (new LAI): %d", tc.store.metrics.RaftCommandsReproposedLAI.Count())

	if pipelined {
		// If we did pipelined writes, if we needed to repropose and injected an
		// error, this should surface as an async write failure instead.
		require.Zero(t, observedReproposalErrors.Load())
		require.Equal(t, injectedReproposalErrors.Load(), observedAsyncWriteFailures.Load())
	} else {
		// If we're not pipelined, we shouldn't be able to get an async write
		// failure. This isn't testing anything about reproposals per se, rather
		// it's validation that we're truly not doing pipelined writes.
		require.Zero(t, observedAsyncWriteFailures.Load())
		// All the injected reproposal errors should manifest to the transaction.
		require.Equal(t, injectedReproposalErrors.Load(), observedReproposalErrors.Load())
	}
	// All incorrect lease indices should manifest either as a reproposal, or a
	// failed reproposal (when an error is injected).
	require.Equal(t, insertedIllegalLeaseIndex.Load(),
		tc.store.metrics.RaftCommandsReproposedLAI.Count()+injectedReproposalErrors.Load())
}

func checkNoLeakedTraceSpans(t *testing.T, store *Store) {
	testutils.SucceedsSoon(t, func() error {
		var sps []tracing.RegistrySpan
		_ = store.cfg.Tracer().VisitSpans(func(span tracing.RegistrySpan) error {
			sps = append(sps, span)
			return nil
		})
		if len(sps) == 0 {
			return nil
		}
		var buf strings.Builder
		fmt.Fprintf(&buf, "unexpectedly found %d active spans:\n", len(sps))
		var ids []uint64
		for _, sp := range sps {
			trace := sp.GetFullRecording(tracingpb.RecordingVerbose)
			for _, rs := range trace.Flatten() {
				// NB: it would be a sight easier to just include these in the output of
				// the string formatted recording, but making a change there presumably requires
				// lots of changes across various testdata in the codebase and the author is
				// trying to be nimble.
				ids = append(ids, rs.GoroutineID)
			}
			fmt.Fprintln(&buf, trace)
			fmt.Fprintln(&buf)
		}
		sl := allstacks.Get()
		return errors.Newf("%s\n\ngoroutines of interest: %v\nstacks:\n\n%s", buf.String(), ids, sl)
	})
}
