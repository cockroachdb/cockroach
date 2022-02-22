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
	"sort"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPendingLogTruncations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Single threaded test. So nothing explicitly acquires truncs.mu.
	truncs := pendingLogTruncations{}
	// No pending truncation.
	truncs.iterateLocked(func(index int, trunc pendingTruncation) {
		require.Fail(t, "unexpected element")
	})
	require.Equal(t, 2, truncs.capacity())
	require.True(t, truncs.isEmptyLocked())
	require.EqualValues(t, 55, truncs.computePostTruncLogSize(55))
	require.EqualValues(t, 5, truncs.computePostTruncFirstIndex(5))

	// One pending truncation.
	truncs.mu.truncs[0].logDeltaBytes = -50
	truncs.mu.truncs[0].Index = 20
	truncs.iterateLocked(func(index int, trunc pendingTruncation) {
		require.Equal(t, 0, index)
		require.Equal(t, truncs.mu.truncs[0], trunc)
	})
	require.False(t, truncs.isEmptyLocked())
	require.Equal(t, truncs.mu.truncs[0], truncs.frontLocked())
	// Added -50.
	require.EqualValues(t, 5, truncs.computePostTruncLogSize(55))
	// Added -50 and bumped up to 0.
	require.EqualValues(t, 0, truncs.computePostTruncLogSize(45))
	// Advances to Index+1.
	require.EqualValues(t, 21, truncs.computePostTruncFirstIndex(5))
	require.EqualValues(t, 21, truncs.computePostTruncFirstIndex(20))
	// Does not advance.
	require.EqualValues(t, 21, truncs.computePostTruncFirstIndex(21))
	require.EqualValues(t, 30, truncs.computePostTruncFirstIndex(30))

	// Two pending truncations.
	truncs.mu.truncs[1].logDeltaBytes = -70
	truncs.mu.truncs[1].Index = 30
	indexes := []int(nil)
	truncs.iterateLocked(func(index int, trunc pendingTruncation) {
		require.Greater(t, truncs.capacity(), index)
		require.Equal(t, truncs.mu.truncs[index], trunc)
		indexes = append(indexes, index)
	})
	require.Equal(t, []int{0, 1}, indexes)
	require.False(t, truncs.isEmptyLocked())
	require.Equal(t, truncs.mu.truncs[0], truncs.frontLocked())
	// Added -120.
	require.EqualValues(t, 5, truncs.computePostTruncLogSize(125))
	// Added -120 and bumped up to 0.
	require.EqualValues(t, 0, truncs.computePostTruncLogSize(115))
	// Advances to Index+1 of second entry.
	require.EqualValues(t, 31, truncs.computePostTruncFirstIndex(5))
	require.EqualValues(t, 31, truncs.computePostTruncFirstIndex(30))
	// Does not advance.
	require.EqualValues(t, 31, truncs.computePostTruncFirstIndex(31))
	require.EqualValues(t, 40, truncs.computePostTruncFirstIndex(40))

	// Pop first.
	last := truncs.mu.truncs[1]
	truncs.popLocked()
	truncs.iterateLocked(func(index int, trunc pendingTruncation) {
		require.Equal(t, 0, index)
		require.Equal(t, last, trunc)
	})
	require.False(t, truncs.isEmptyLocked())
	require.Equal(t, last, truncs.frontLocked())
	// Pop last.
	truncs.popLocked()
	require.True(t, truncs.isEmptyLocked())
	truncs.iterateLocked(func(index int, trunc pendingTruncation) {
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
	sideloadedErr   error
}

var _ replicaForTruncator = &replicaTruncatorTest{}

func makeReplicaTT(rangeID roachpb.RangeID, buf *strings.Builder) *replicaTruncatorTest {
	return &replicaTruncatorTest{
		rangeID:     rangeID,
		buf:         buf,
		stateLoader: stateloader.Make(rangeID),
	}
}

func (r *replicaTruncatorTest) getRangeID() roachpb.RangeID {
	return r.rangeID
}

func (r *replicaTruncatorTest) getTruncatedState() roachpb.RaftTruncatedState {
	fmt.Fprintf(r.buf, "r%d.getTruncatedState\n", r.rangeID)
	return r.truncState
}

func (r *replicaTruncatorTest) getPendingTruncs() *pendingLogTruncations {
	fmt.Fprintf(r.buf, "r%d.getPendingTruncs\n", r.rangeID)
	return &r.pendingTruncs
}

func (r *replicaTruncatorTest) setTruncationDeltaAndTrusted(deltaBytes int64, isDeltaTrusted bool) {
	fmt.Fprintf(r.buf, "r%d.setTruncationDeltaAndTrusted(delta:%d, trusted:%t)\n",
		r.rangeID, deltaBytes, isDeltaTrusted)
}

func (r *replicaTruncatorTest) sideloadedBytesIfTruncatedFromTo(
	_ context.Context, from, to uint64,
) (freed int64, _ error) {
	fmt.Fprintf(r.buf, "r%d.sideloadedBytesIfTruncatedFromTo(%d, %d)\n", r.rangeID, from, to)
	return r.sideloadedFreed, r.sideloadedErr
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
	fmt.Fprintf(r.buf,
		"r%d.setTruncatedStateAndSideEffects(..., expectedFirstIndex:%d) => trusted:%t\n",
		r.rangeID, expectedFirstIndexPreTruncation, expectedFirstIndexWasAccurate)
	return expectedFirstIndexWasAccurate
}

func (r *replicaTruncatorTest) writeRaftStateToEngine(
	t *testing.T, eng storage.Engine, truncIndex uint64, lastLogEntry uint64,
) {
	require.NoError(t, r.stateLoader.SetRaftTruncatedState(context.Background(), eng,
		&roachpb.RaftTruncatedState{Index: truncIndex}))
	for i := truncIndex + 1; i <= lastLogEntry; i++ {
		require.NoError(t, eng.PutUnversioned(r.stateLoader.RaftLogKey(i), []byte("something")))
	}
}

func (r *replicaTruncatorTest) writeRaftAppliedIndex(
	t *testing.T, eng storage.Engine, raftAppliedIndex uint64, flush bool,
) {
	require.NoError(t, r.stateLoader.SetRangeAppliedState(context.Background(), eng,
		raftAppliedIndex, 0, 0, &enginepb.MVCCStats{}, nil))
	// Flush to make it satisfy the contract of OnlyReadGuaranteedDurable in
	// Pebble.
	if flush {
		require.NoError(t, eng.Flush())
	}
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
	require.NoError(t, err)
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
		require.NoError(t, err)
	}
	fmt.Fprintf(r.buf, "\n")
	// It is ok to pretend that a regular read is equivalent to
	// OnlyReadGuaranteedDurable for printing in this test, since we flush in
	// the code above whenever writing RaftAppliedIndex.
	as, err := r.stateLoader.LoadRangeAppliedState(context.Background(), eng)
	require.NoError(t, err)
	fmt.Fprintf(r.buf, "durable applied index: %d\n", as.RaftAppliedIndex)
}

func (r *replicaTruncatorTest) printReplicaState() {
	r.pendingTruncs.mu.Lock()
	defer r.pendingTruncs.mu.Unlock()
	fmt.Fprintf(r.buf, "truncIndex: %d\npending:\n", r.truncState.Index)
	r.pendingTruncs.iterateLocked(func(index int, trunc pendingTruncation) {
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

func (s *storeTruncatorTest) getEngine() storage.Engine {
	return s.eng
}

func (s *storeTruncatorTest) acquireReplicaForTruncator(
	rangeID roachpb.RangeID,
) replicaForTruncator {
	fmt.Fprintf(s.buf, "acquireReplica(%d)\n", rangeID)
	rv := s.replicas[rangeID]
	if rv == nil {
		// Return nil and not an interface holding nil.
		return nil
	}
	return rv
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	truncator := makeRaftLogTruncator(
		log.MakeTestingAmbientContext(tracing.NewTracer()), store, stopper)

	datadriven.RunTest(t, testutils.TestDataPath(t, "raft_log_truncator"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "create-replica":
				rangeID := scanRangeID(t, d)
				var truncIndex uint64
				d.ScanArgs(t, "trunc-index", &truncIndex)
				var lastLogEntry uint64
				d.ScanArgs(t, "last-log-entry", &lastLogEntry)
				r := makeReplicaTT(rangeID, &buf)
				r.truncState.Index = truncIndex
				r.writeRaftStateToEngine(t, eng, truncIndex, lastLogEntry)
				store.replicas[rangeID] = r
				return flushAndReset()

			case "print-engine-state":
				store.replicas[scanRangeID(t, d)].printEngine(t, eng)
				return flushAndReset()

			case "add-pending-truncation":
				rangeID := scanRangeID(t, d)
				var firstIndex, truncIndex uint64
				d.ScanArgs(t, "first-index", &firstIndex)
				d.ScanArgs(t, "trunc-index", &truncIndex)
				var deltaBytes, sideloadedBytes int
				d.ScanArgs(t, "delta-bytes", &deltaBytes)
				d.ScanArgs(t, "sideloaded-bytes", &sideloadedBytes)
				r := store.replicas[rangeID]
				if d.HasArg("sideloaded-err") {
					var sideloadedErr bool
					d.ScanArgs(t, "sideloaded-err", &sideloadedErr)
					if sideloadedErr {
						r.sideloadedErr = errors.Errorf("side-loaded err")
					}
				}
				r.sideloadedFreed = int64(sideloadedBytes)
				truncator.addPendingTruncation(context.Background(), r,
					roachpb.RaftTruncatedState{Index: truncIndex}, firstIndex, int64(deltaBytes))
				printTruncatorState(t, &buf, truncator)
				r.sideloadedErr = nil
				return flushAndReset()

			case "print-replica-state":
				store.replicas[scanRangeID(t, d)].printReplicaState()
				return flushAndReset()

			case "write-raft-applied-index":
				rangeID := scanRangeID(t, d)
				var raftAppliedIndex uint64
				d.ScanArgs(t, "raft-applied-index", &raftAppliedIndex)
				noFlush := false
				// The initial engine memtable size is 256KB, and doubles for each new
				// memtable. Even the initial size is much larger than anything we do
				// in this test between explicit flushes. Hence we can rely on the
				// fact that no-flush will actually be respected, and we won't
				// encounter an unexpected flush.
				if d.HasArg("no-flush") {
					d.ScanArgs(t, "no-flush", &noFlush)
				}
				store.replicas[rangeID].writeRaftAppliedIndex(t, eng, raftAppliedIndex, !noFlush)
				return flushAndReset()

			case "add-replica-to-truncator":
				// In addition to replicas being added to the truncator via
				// add-pending-truncation, we can manually add them to test the
				// replica not found etc. paths.
				truncator.enqueueRange(scanRangeID(t, d))
				printTruncatorState(t, &buf, truncator)
				return flushAndReset()

			case "durability-advanced":
				truncator.durabilityAdvanced(context.Background())
				printTruncatorState(t, &buf, truncator)
				return flushAndReset()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func scanRangeID(t *testing.T, d *datadriven.TestData) roachpb.RangeID {
	var id int
	d.ScanArgs(t, "id", &id)
	return roachpb.RangeID(id)
}

func printTruncatorState(t *testing.T, buf *strings.Builder, truncator *raftLogTruncator) {
	truncator.mu.Lock()
	defer truncator.mu.Unlock()
	require.Zero(t, len(truncator.mu.drainRanges))
	ranges := make([]roachpb.RangeID, 0, len(truncator.mu.addRanges))
	for id := range truncator.mu.addRanges {
		ranges = append(ranges, id)
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i] < ranges[j] })
	fmt.Fprintf(buf, "truncator ranges:")
	prefixStr := " "
	for _, id := range ranges {
		fmt.Fprintf(buf, "%s%d", prefixStr, id)
		prefixStr = ", "
	}
}
