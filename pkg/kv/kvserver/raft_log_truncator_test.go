// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestPendingLogTruncations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	truncs := pendingLogTruncations{}
	// No pending truncation.
	truncs.iterate(func(index int, trunc pendingTruncation) {
		require.Fail(t, "unexpected element")
	})
	require.Equal(t, 2, truncs.capacity())
	require.True(t, truncs.empty())
	require.Equal(t, int64(55), truncs.computePostTruncLogSize(55))
	require.Equal(t, uint64(5), truncs.computePostTruncFirstIndex(5))

	// One pending truncation.
	truncs.truncs[0].logDeltaBytes = -50
	truncs.truncs[0].Index = 20
	truncs.iterate(func(index int, trunc pendingTruncation) {
		require.Equal(t, 0, index)
		require.Equal(t, truncs.truncs[0], trunc)
	})
	require.False(t, truncs.empty())
	require.Equal(t, truncs.truncs[0], truncs.front())
	// Added -50.
	require.Equal(t, int64(5), truncs.computePostTruncLogSize(55))
	// Added -50 and bumped up to 0.
	require.Equal(t, int64(0), truncs.computePostTruncLogSize(45))
	// Advances to Index+1.
	require.Equal(t, uint64(21), truncs.computePostTruncFirstIndex(5))
	// Does not advance.
	require.Equal(t, uint64(30), truncs.computePostTruncFirstIndex(30))

	// Two pending truncations.
	truncs.truncs[1].logDeltaBytes = -70
	truncs.truncs[1].Index = 30
	indexes := []int(nil)
	truncs.iterate(func(index int, trunc pendingTruncation) {
		require.Greater(t, truncs.capacity(), index)
		require.Equal(t, truncs.truncs[index], trunc)
		indexes = append(indexes, index)
	})
	require.Equal(t, []int{0, 1}, indexes)
	require.False(t, truncs.empty())
	require.Equal(t, truncs.truncs[0], truncs.front())
	// Added -120.
	require.Equal(t, int64(5), truncs.computePostTruncLogSize(125))
	// Added -120 and bumped up to 0.
	require.Equal(t, int64(0), truncs.computePostTruncLogSize(115))
	// Advances to Index+1 of second entry
	require.Equal(t, uint64(31), truncs.computePostTruncFirstIndex(5))
	require.Equal(t, uint64(31), truncs.computePostTruncFirstIndex(30))
	// Does not advance.
	require.Equal(t, uint64(40), truncs.computePostTruncFirstIndex(40))

	// Pop first.
	last := truncs.truncs[1]
	truncs.pop()
	truncs.iterate(func(index int, trunc pendingTruncation) {
		require.Equal(t, 0, index)
		require.Equal(t, last, trunc)
	})
	require.False(t, truncs.empty())
	require.Equal(t, last, truncs.front())
	// Pop last.
	truncs.pop()
	require.True(t, truncs.empty())
	truncs.iterate(func(index int, trunc pendingTruncation) {
		require.Fail(t, "unexpected element")
	})
}

type replicaTruncatorTest struct {
	rangeID         roachpb.RangeID
	buf             *strings.Builder
	stateLoader     stateloader.StateLoader
	truncState      roachpb.RaftTruncatedState
	pendingTruncs   pendingLogTruncations
	sideloadedFreed int64
}

var _ replicaForTruncator = &replicaTruncatorTest{}

func makeReplicaTT(rangeID roachpb.RangeID, buf *strings.Builder) *replicaTruncatorTest {
	return &replicaTruncatorTest{
		rangeID:     rangeID,
		buf:         buf,
		stateLoader: stateloader.Make(rangeID),
	}
}

func (r *replicaTruncatorTest) GetRangeID() roachpb.RangeID {
	return r.rangeID
}

func (r *replicaTruncatorTest) lockReplicaState() {
	fmt.Fprintf(r.buf, "r%d.lockReplicaState\n", r.rangeID)
}

func (r *replicaTruncatorTest) getTruncatedStateLocked() roachpb.RaftTruncatedState {
	fmt.Fprintf(r.buf, "r%d.getTruncatedStateLocked\n", r.rangeID)
	return r.truncState
}

func (r *replicaTruncatorTest) getPendingTruncs() *pendingLogTruncations {
	fmt.Fprintf(r.buf, "r%d.getPendingTruncs\n", r.rangeID)
	return &r.pendingTruncs
}

func (r *replicaTruncatorTest) setTruncationDeltaAndTrustedLocked(
	deltaBytes int64, isDeltaTrusted bool,
) {
	fmt.Fprintf(r.buf, "r%d.setTruncationDeltaAndTrustedLocked(%d, %t)\n",
		r.rangeID, deltaBytes, isDeltaTrusted)
}

func (r *replicaTruncatorTest) unlockReplicaState() {
	fmt.Fprintf(r.buf, "r%d.unlockReplicaState\n", r.rangeID)
}

func (r *replicaTruncatorTest) sideloadedBytesIfTruncatedFromTo(
	_ context.Context, from, to uint64,
) (freed int64, _ error) {
	fmt.Fprintf(r.buf, "r%d.sideloadedBytesIfTruncatedFromTo(%d, %d)\n", r.rangeID, from, to)
	return r.sideloadedFreed, nil
}

func (r *replicaTruncatorTest) getStateLoader() stateloader.StateLoader {
	fmt.Fprintf(r.buf, "r%d.getStateLoader\n", r.rangeID)
	return r.stateLoader
}

func (r *replicaTruncatorTest) setTruncatedStateAndSideEffects(
	_ context.Context, truncState *roachpb.RaftTruncatedState, expectedFirstIndexPreTruncation uint64,
) (expectedFirstIndexWasAccurate bool) {
	expectedFirstIndexWasAccurate = r.truncState.Index+1 == expectedFirstIndexPreTruncation
	r.truncState = *truncState
	fmt.Fprintf(r.buf, "r%d.setTruncatedStateAndSideEffects(..., %d) => %t\n", r.rangeID,
		expectedFirstIndexPreTruncation, expectedFirstIndexWasAccurate)
	return expectedFirstIndexWasAccurate
}

func (r *replicaTruncatorTest) writeRaftStateToEngine(
	t *testing.T, eng storage.Engine, truncIndex uint64, lastLogEntry uint64,
) {
	require.NoError(t, r.stateLoader.SetRaftTruncatedState(context.Background(), eng,
		&roachpb.RaftTruncatedState{Index: truncIndex}))
	for i := truncIndex + 1; i < lastLogEntry; i++ {
		require.NoError(t, eng.PutUnversioned(r.stateLoader.RaftLogKey(i), []byte("something")))
	}
}

func (r *replicaTruncatorTest) writeRaftAppliedIndex(
	t *testing.T, eng storage.Engine, raftAppliedIndex uint64,
) {
	require.NoError(t, r.stateLoader.SetRangeAppliedState(context.Background(), eng,
		raftAppliedIndex, 0, 0, &enginepb.MVCCStats{}, nil))
}

func (r *replicaTruncatorTest) printEngine(t *testing.T, eng storage.Engine) {
	truncState, err := r.stateLoader.LoadRaftTruncatedState(context.Background(), eng)
	require.NoError(t, err)
	fmt.Fprintf(r.buf, "truncated index: %d\n", truncState.Index)
	prefix := r.stateLoader.RaftLogPrefix()
	iter := eng.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		UpperBound: r.stateLoader.RaftLogKey(math.MaxUint64),
	})
	defer iter.Close()
	iter.SeekGE(storage.MVCCKey{Key: r.stateLoader.RaftLogKey(0)})
	valid, err := iter.Valid()
	fmt.Fprintf(r.buf, "log entries:")
	printPrefixStr := ""
	for valid {
		key := iter.Key()
		_, index, err := encoding.DecodeUint64Ascending(key.Key[len(prefix):])
		require.NoError(t, err)
		fmt.Fprintf(r.buf, "%s %d", printPrefixStr, index)
		printPrefixStr = ","
		iter.Next()
		valid, err = iter.Valid()
	}
	require.NoError(t, err)
	fmt.Fprintf(r.buf, "\n")
	// TODO: test is pretending regular read is durable. Fix.
	as, err := r.stateLoader.LoadRangeAppliedState(context.Background(), eng)
	require.NoError(t, err)
	fmt.Fprintf(r.buf, "durable applied index: %d\n", as.RaftAppliedIndex)
}

func (r *replicaTruncatorTest) printReplicaState() {
	fmt.Fprintf(r.buf, "truncIndex: %d\npending:\n", r.truncState.Index)
	r.pendingTruncs.iterate(func(index int, trunc pendingTruncation) {
		fmt.Fprintf(r.buf, " %+v\n", trunc)
	})
}

type storeTruncatorTest struct {
	eng      storage.Engine
	buf      *strings.Builder
	replicas map[roachpb.RangeID]*replicaTruncatorTest
}

var _ storeForTruncator = &storeTruncatorTest{}

func makeStoreTT(eng storage.Engine, buf *strings.Builder) *storeTruncatorTest {
	return &storeTruncatorTest{
		eng:      eng,
		buf:      buf,
		replicas: make(map[roachpb.RangeID]*replicaTruncatorTest),
	}
}

func (s *storeTruncatorTest) Engine() storage.Engine {
	return s.eng
}

func (s *storeTruncatorTest) acquireReplicaForTruncator(
	rangeID roachpb.RangeID,
) (replicaForTruncator, error) {
	fmt.Fprintf(s.buf, "acquireReplica(%d)\n", rangeID)
	return s.replicas[rangeID], nil
}

func (s *storeTruncatorTest) releaseReplicaForTruncator(r replicaForTruncator) {
	fmt.Fprintf(s.buf, "releaseReplica(%d)\n", r.(*replicaTruncatorTest).rangeID)
}

func TestRaftLogTruncator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf strings.Builder
	flushAndReset := func() string {
		str := buf.String()
		buf.Reset()
		return str
	}
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	store := makeStoreTT(eng, &buf)
	truncator := makeRaftLogTruncator(store)

	datadriven.RunTest(t, testutils.TestDataPath(t, "raft_log_truncator"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-replica":
				var id int
				d.ScanArgs(t, "id", &id)
				rangeID := roachpb.RangeID(id)
				var truncIndex uint64
				d.ScanArgs(t, "trunc-index", &truncIndex)
				var lastLogEntry uint64
				d.ScanArgs(t, "last-log-entry", &lastLogEntry)
				r := makeReplicaTT(roachpb.RangeID(id), &buf)
				r.truncState.Index = truncIndex
				r.writeRaftStateToEngine(t, eng, truncIndex, lastLogEntry)
				store.replicas[rangeID] = r
				return flushAndReset()

			case "print-engine-state":
				var id int
				d.ScanArgs(t, "id", &id)
				store.replicas[roachpb.RangeID(id)].printEngine(t, eng)
				return flushAndReset()

			case "add-pending-truncation":
				var id int
				d.ScanArgs(t, "id", &id)
				rangeID := roachpb.RangeID(id)
				var firstIndex, truncIndex uint64
				d.ScanArgs(t, "first-index", &firstIndex)
				d.ScanArgs(t, "trunc-index", &truncIndex)
				var deltaBytes, sideloadedBytes int
				d.ScanArgs(t, "delta-bytes", &deltaBytes)
				d.ScanArgs(t, "sideloaded-bytes", &sideloadedBytes)
				r := store.replicas[rangeID]
				r.sideloadedFreed = int64(sideloadedBytes)
				truncator.addPendingTruncation(context.Background(), r,
					roachpb.RaftTruncatedState{Index: truncIndex}, firstIndex, int64(deltaBytes))
				return flushAndReset()

			case "print-replica-state":
				var id int
				d.ScanArgs(t, "id", &id)
				store.replicas[roachpb.RangeID(id)].printReplicaState()
				return flushAndReset()

			case "write-raft-applied-index":
				var id int
				d.ScanArgs(t, "id", &id)
				var raftAppliedIndex uint64
				d.ScanArgs(t, "raft-applied-index", &raftAppliedIndex)
				store.replicas[roachpb.RangeID(id)].writeRaftAppliedIndex(t, eng, raftAppliedIndex)
				return flushAndReset()

			case "durability-advanced":
				truncator.durabilityAdvanced(context.Background())
				return flushAndReset()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
