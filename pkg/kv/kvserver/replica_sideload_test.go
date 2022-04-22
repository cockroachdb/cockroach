// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue/kvraftlogqueue"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicasideload"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// This test verifies that sideloaded proposals are
// inlined correctly and can be read back.
func TestRaftSSTableSideloading(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer SetMockAddSSTable()()

	ctx := context.Background()
	tc := testContext{}

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Disable log truncation to make sure our proposal stays in the log.
	tc.store.SetRaftLogQueueActive(false)

	var ba roachpb.BatchRequest
	ba.RangeID = tc.repl.RangeID

	// Put a sideloaded proposal on the Range.
	key, val := "don't", "care"
	origSSTData, _ := MakeSSTable(ctx, key, val, hlc.Timestamp{}.Add(0, 1))
	{

		var addReq roachpb.AddSSTableRequest
		addReq.Data = origSSTData
		addReq.Key = roachpb.Key(key)
		addReq.EndKey = addReq.Key.Next()
		ba.Add(&addReq)

		_, pErr := tc.store.Send(ctx, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Check the `entries()` method which has special handling to accommodate
	// `term()`: when an empty sideload storage is passed in, `entries()` should
	// not inline, and in turn also not populate the entries cache (since its
	// contents must always be fully inlined).
	tc.repl.raftMu.Lock()
	defer tc.repl.raftMu.Unlock()
	tc.repl.mu.Lock()
	defer tc.repl.mu.Unlock()
	testutils.RunTrueAndFalse(t, "withSS", func(t *testing.T, withSS bool) {
		rsl := stateloader.Make(tc.repl.RangeID)
		lo := tc.repl.mu.state.TruncatedState.Index + 1
		hi := tc.repl.mu.lastIndex + 1

		var ss replicasideload.SideloadStorage
		if withSS {
			ss = tc.repl.raftMu.sideloaded
		}

		tc.store.raftEntryCache.Clear(tc.repl.RangeID, hi)
		ents, err := entries(
			ctx, rsl, tc.store.Engine(), tc.repl.RangeID, tc.store.raftEntryCache,
			ss, lo, hi, math.MaxUint64,
		)
		require.NoError(t, err)
		require.Len(t, ents, int(hi-lo))

		// Raft entry cache only gets populated when sideloaded storage provided.
		_, okLo := tc.store.raftEntryCache.Get(tc.repl.RangeID, lo)
		_, okHi := tc.store.raftEntryCache.Get(tc.repl.RangeID, hi-1)
		if withSS {
			require.True(t, okLo)
			require.True(t, okHi)
		} else {
			require.False(t, okLo)
			require.False(t, okHi)
		}

		// The rest of the test is the same in both cases. We find the sideloaded entry
		// and check the sideloaded storage for the payload.
		var idx int
		for idx = 0; idx < len(ents); idx++ {
			// Get the SST back from the raft log.
			if !replicasideload.SniffSideloadedRaftCommand(ents[idx].Data) {
				continue
			}
			ent, err := replicasideload.MaybeInlineSideloadedRaftCommand(ctx, tc.repl.RangeID, ents[idx], tc.repl.raftMu.sideloaded, tc.store.raftEntryCache)
			require.NoError(t, err)
			sst, err := tc.repl.raftMu.sideloaded.Get(ctx, ent.Index, ent.Term)
			require.NoError(t, err)
			require.Equal(t, origSSTData, sst)
			break
		}
		require.Less(t, idx, len(ents)) // there was an SST
	})
}

func TestRaftSSTableSideloadingTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer SetMockAddSSTable()()

	testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
		tc := testContext{}
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)
		st := tc.store.ClusterSettings()
		kvraftlogqueue.LooselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)

		const count = 10

		var indexes []uint64
		addLastIndex := func() {
			lastIndex, err := tc.repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			indexes = append(indexes, lastIndex)
		}
		for i := 0; i < count; i++ {
			addLastIndex()
			key := fmt.Sprintf("key-%d", i)
			val := fmt.Sprintf("val-%d", i)
			if err := ProposeAddSSTable(ctx, key, val, tc.Clock().Now(), tc.store); err != nil {
				t.Fatalf("%d: %+v", i, err)
			}
		}
		// Append an extra entry which, if we truncate it, should definitely also
		// remove any leftover files (ok, unless the last one is reproposed but
		// that's *very* unlikely to happen for the last one)
		addLastIndex()

		fmtSideloaded := func() []string {
			tc.repl.raftMu.Lock()
			defer tc.repl.raftMu.Unlock()
			fs, _ := tc.repl.Engine().List(tc.repl.raftMu.sideloaded.Dir())
			sort.Strings(fs)
			return fs
		}

		// Check that when we truncate, the number of on-disk files changes in ways
		// we expect. Intentionally not too strict due to the possibility of
		// reproposals, etc; it could be made stricter, but this should give enough
		// confidence already that we're calling `PurgeTo` correctly, and for the
		// remainder unit testing on each impl's PurgeTo is more useful.
		for i := range indexes {
			const rangeID = 1
			newFirstIndex := indexes[i] + 1
			truncateArgs := truncateLogArgs(newFirstIndex, rangeID)
			log.Eventf(ctx, "truncating to index < %d", newFirstIndex)
			if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{RangeID: rangeID}, &truncateArgs); pErr != nil {
				t.Fatal(pErr)
			}
			waitForTruncationForTesting(t, tc.repl, newFirstIndex, looselyCoupled)
			// Truncation done, so check sideloaded files.
			sideloadStrings := fmtSideloaded()
			if minFiles := count - i; len(sideloadStrings) < minFiles {
				t.Fatalf("after truncation at %d (i=%d), expected at least %d files left, but have:\n%v",
					indexes[i], i, minFiles, sideloadStrings)
			}
		}

		if sideloadStrings := fmtSideloaded(); len(sideloadStrings) != 0 {
			t.Fatalf("expected all files to be cleaned up, but found %v", sideloadStrings)
		}
	})
}

// TestRaftSSTableSideloadingProposal runs a straightforward application of an `AddSSTable` command.
func TestRaftSSTableSideloadingProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "InMem", func(t *testing.T, engineInMem bool) {
		var eng storage.Engine
		if engineInMem {
			eng = storage.NewDefaultInMemForTesting()
		} else {
			var cleanup func()
			ctx := context.Background()
			cleanup, eng = newOnDiskEngine(ctx, t)
			defer cleanup()
		}
		defer eng.Close()
		testRaftSSTableSideloadingProposal(t, eng)
	})
}

// XXX: Move back out.
// TestRaftSSTableSideloadingProposal runs a straightforward application of an `AddSSTable` command.
func testRaftSSTableSideloadingProposal(t *testing.T, eng storage.Engine) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer SetMockAddSSTable()()

	ctx := context.Background()
	stopper := stop.NewStopper()
	tc := testContext{}
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	tr := tc.store.cfg.AmbientCtx.Tracer
	tr.TestingRecordAsyncSpans() // we assert on async span traces in this test
	ctx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
	defer getRecAndFinish()

	const (
		key       = "foo"
		entrySize = 128
	)
	val := strings.Repeat("x", entrySize)

	ts := hlc.Timestamp{Logical: 1}

	if err := ProposeAddSSTable(ctx, key, val, ts, tc.store); err != nil {
		t.Fatal(err)
	}

	{
		var ba roachpb.BatchRequest
		get := getArgs(roachpb.Key(key))
		ba.Add(&get)
		ba.Header.RangeID = tc.repl.RangeID

		br, pErr := tc.store.Send(ctx, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
		v := br.Responses[0].GetInner().(*roachpb.GetResponse).Value
		if v == nil {
			t.Fatal("expected to read a value")
		}
		if valBytes, err := v.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(valBytes, []byte(val)) {
			t.Fatalf("expected to read '%s', but found '%s'", val, valBytes)
		}
	}

	func() {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()

		if err := testutils.MatchInOrder(
			getRecAndFinish().String(), "sideloadable proposal detected", "ingested SSTable",
		); err != nil {
			t.Fatal(err)
		}

		if n := tc.store.metrics.AddSSTableProposals.Count(); n == 0 {
			t.Fatalf("expected metric to show at least one AddSSTable proposal, but got %d", n)
		}

		if n := tc.store.metrics.AddSSTableApplications.Count(); n == 0 {
			t.Fatalf("expected metric to show at least one AddSSTable application, but got %d", n)
		}
		// We usually don't see copies because we hardlink and ingest the original SST. However, this
		// depends on luck and the file system, so don't try to assert it. We should, however, see
		// no more than one.
		expMaxCopies := int64(1)
		if n := tc.store.metrics.AddSSTableApplicationCopies.Count(); n > expMaxCopies {
			t.Fatalf("expected metric to show <= %d AddSSTable copies, but got %d", expMaxCopies, n)
		}
	}()

	// Force a log truncation followed by verification of the tracked raft log size. This exercises a
	// former bug in which the raft log size took the sideloaded payload into account when adding
	// to the log, but not when truncating.

	// Write enough keys to the range to make sure that a truncation will happen.
	for i := 0; i < kvqueue.RaftLogQueueStaleThreshold+1; i++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", i))
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", i)))
		if _, err := kv.SendWrapped(context.Background(), tc.store.TestSender(), &args); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tc.store.raftLogQueue.TestingAdd(ctx, tc.repl, 99.99 /* priority */); err != nil {
		t.Fatal(err)
	}
	tc.store.MustForceRaftLogScanAndProcess()
	// SST is definitely truncated now, so recomputing the Raft log keys should match up with
	// the tracked size.
	verifyLogSizeInSync(t, tc.repl)
}

func newOnDiskEngine(ctx context.Context, t *testing.T) (func(), storage.Engine) {
	dir, cleanup := testutils.TempDir(t)
	eng, err := storage.Open(
		ctx,
		storage.Filesystem(dir),
		storage.CacheSize(1<<20 /* 1 MiB */))
	if err != nil {
		t.Fatal(err)
	}
	return cleanup, eng
}
