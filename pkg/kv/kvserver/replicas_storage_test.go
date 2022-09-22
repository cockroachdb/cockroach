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
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/time/rate"
)

const testNodeID = roachpb.NodeID(1)
const testStoredID = roachpb.StoreID(2)

// TODO(sumeer): remove this duplication of
// stateloader.raftInitialLog{Index,Term}.
const testRaftInitialLogIndex = 10
const testRaftInitialLogTerm = 5

// TODO(sumeer): additional cases to test
// - All the sideloaded cases: purging, truncation, application.
// - Skipping over provisional RangeDescriptor in ReplicasStorage.Init.
// - Failure after split creates HardState for RHS (requires adding test
//   callback in ReplicasStorage).
// - RecoveryInconsistentReplica in ReplicasStorage.Init.

func TestReplicasStorageBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, testutils.TestDataPath(t, "replicas_storage"), func(t *testing.T, path string) {
		ctx := context.Background()
		var reps *replicasStorageImpl
		fs := vfs.NewStrictMem()
		var eng storage.Engine
		defer func() {
			if eng != nil {
				eng.Close()
			}
		}()
		st := cluster.MakeTestingClusterSettings()
		limiter := rate.NewLimiter(1<<30, 1<<30)
		ssConstructor := func(rangeID roachpb.RangeID, replicaID roachpb.ReplicaID) SideloadStorage {
			ss, err := newDiskSideloadStorage(st, rangeID, replicaID, eng.GetAuxiliaryDir(), limiter, eng)
			require.NoError(t, err)
			return ss
		}
		var sstSnapshotStorage SSTSnapshotStorage
		makeEngEtc := func() {
			var err error
			eng, err = storage.Open(ctx, storage.MakeLocation(fs), storage.ForTesting, storage.MaxSize(1<<20))
			require.NoError(t, err)
			sstSnapshotStorage = NewSSTSnapshotStorage(eng, limiter)
		}
		// Construct new replicasStorageImpl.
		newReps := func(discardUnsyncedState bool) error {
			if eng != nil {
				fs.SetIgnoreSyncs(true)
				require.NoError(t, sstSnapshotStorage.Clear())
				eng.Close()
				if discardUnsyncedState {
					fs.ResetToSyncedState()
				}
				fs.SetIgnoreSyncs(false)
			}
			makeEngEtc()
			reps = MakeSingleEngineReplicasStorage(
				testNodeID, testStoredID, eng, ssConstructor, st).(*replicasStorageImpl)
			return reps.Init(ctx)
		}
		// Check some correctness of replicasStorageImpl and print contents
		// (including the underlying engine).
		checkAndPrintReps := func(discardUnsyncedState bool) string {
			beforeStr := printReps(t, reps)
			// Create a new replicasStorageImpl and check that its printed state is
			// the same.
			require.NoError(t, newReps(discardUnsyncedState))
			afterStr := printReps(t, reps)
			require.Equal(t, beforeStr, afterStr)
			return afterStr
		}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new":
				require.NoError(t, newReps(false))
				return printReps(t, reps)

			case "create-uninit":
				rangeID := scanRangeIDForTest(t, d, "")
				replicaID := scanReplicaID(t, d, "")
				r, err := reps.CreateUninitializedRange(
					ctx, FullReplicaID{RangeID: rangeID, ReplicaID: replicaID})
				if err != nil {
					return err.Error()
				}
				require.NotNil(t, r)
				return checkAndPrintReps(true)

			case "discard-replica":
				r := scanAndGetRangeStorage(t, d, "", reps)
				nextReplicaID := scanReplicaID(t, d, "next-")
				if err := reps.DiscardReplica(ctx, r, nextReplicaID); err != nil {
					return err.Error()
				}
				return checkAndPrintReps(true)

			case "ingest-snapshot":
				err := func() error {
					r := scanAndGetRangeStorage(t, d, "", reps)
					rangeID, replicaID := r.FullReplicaID().RangeID, r.FullReplicaID().ReplicaID

					raftAppliedIndex, raftAppliedIndexTerm := scanRaftIndexAndTerm(t, d)
					// The global keys to write in this snapshot. They correspond to the
					// state machine state at (raftAppliedIndex,raftAppliedIndexTerm).
					keySlice, ts := scanGlobalKeys(t, d)
					// The claimed span of the replica, in the snapshot.
					span := scanSpan(t, d, "")
					var subsumedRangeIDs []roachpb.RangeID
					if d.HasArg("subsumed-range-ids") {
						var idsStr string
						d.ScanArgs(t, "subsumed-range-ids", &idsStr)
						idSlice := strings.Split(idsStr, ",")
						for i := range idSlice {
							id, err := strconv.Atoi(idSlice[i])
							require.NoError(t, err)
							subsumedRangeIDs = append(subsumedRangeIDs, roachpb.RangeID(id))
						}
					}

					sstScratch := sstSnapshotStorage.NewScratchSpace(rangeID, uuid.UUID{234})
					defer func() {
						require.NoError(t, sstScratch.Close())
					}()
					paths, err := writeRangeSnapshot(ctx, rangeID, replicaID, span, keySlice, ts,
						raftAppliedIndex, raftAppliedIndexTerm, st, sstScratch)
					if err != nil {
						return err
					}
					if err := r.IngestRangeSnapshot(ctx, span, raftAppliedIndex, paths, subsumedRangeIDs, sstScratch); err != nil {
						return err
					}
					return nil
				}()
				if err != nil {
					return err.Error()
				}
				return checkAndPrintReps(true)

			case "mutation":
				err := func() error {
					// apply specifies whether the mutation should be applied to the state
					// machine.
					apply := true
					if d.HasArg("apply") {
						d.ScanArgs(t, "apply", &apply)
					}
					r := scanAndGetRangeStorage(t, d, "", reps)
					var raftBatch, smBatch storage.Batch
					var raftAppliedIndex, raftAppliedIndexTerm uint64
					// INVARIANT: apply => must have some global keys to write.
					if d.HasArg("keys") {
						rangeID := r.FullReplicaID().RangeID
						raftAppliedIndex, raftAppliedIndexTerm = scanRaftIndexAndTerm(t, d)
						require.Less(t, uint64(testRaftInitialLogIndex), raftAppliedIndex)
						keySlice, ts := scanGlobalKeys(t, d)
						var err error
						raftBatch, smBatch, err = writeGlobalKeysViaRaft(ctx, rangeID, keySlice, ts, raftAppliedIndex,
							raftAppliedIndexTerm, eng)
						require.NoError(t, err)
					} else {
						require.False(t, apply)
					}
					// Load the HardState since we need to modify it.
					hs, err := r.rsl.LoadHardState(ctx, eng)
					require.NoError(t, err)
					var hsPtr *raftpb.HardState
					var hi uint64
					if raftAppliedIndex > 0 {
						// There were some global keys to write. Reminder: the test is
						// writing a single raft log entry at a time, and can only apply to
						// the state machine the entry that it is writing at this momemnt.
						hi = raftAppliedIndex + 1
						if apply {
							// Need to advance the HardState.Commit.
							hs.Commit = raftAppliedIndex
							err := r.rsl.SetHardState(ctx, raftBatch, hs)
							require.NoError(t, err)
							hsPtr = &hs
						}
						// Else, not applying, so HardState.Commit is not updated.
					} else {
						// No raft log entries, so must be only updating HardState. The test
						// only updates Term and Vote, though it could be extended to update
						// Commit if it gained the ability to apply raft log entries that
						// were written previously.
						var raftTerm, raftVote uint64
						d.ScanArgs(t, "raft-term", &raftTerm)
						d.ScanArgs(t, "raft-vote", &raftVote)
						hs.Term = raftTerm
						hs.Vote = raftVote
						raftBatch = eng.NewUnindexedBatch(false)
						err := r.rsl.SetHardState(ctx, raftBatch, hs)
						require.NoError(t, err)
						hsPtr = &hs
					}
					raftMutBatch := RaftMutationBatch{
						MutationBatch: testMutationBatch{b: raftBatch},
						Lo:            raftAppliedIndex,
						Hi:            hi,
						Term:          raftAppliedIndexTerm,
						HardState:     hsPtr,
						MustSync:      true,
					}
					defer raftBatch.Close()
					if smBatch != nil {
						defer smBatch.Close()
					}
					if err := r.DoRaftMutation(ctx, raftMutBatch); err != nil {
						return err
					}
					if apply {
						if err := r.ApplyCommittedBatch(testMutationBatch{b: smBatch}); err != nil {
							return err
						}
					}
					return nil
				}()
				if err != nil {
					return err.Error()
				}
				// discardUnsyncedState=false since replicasStorageImpl.Init() cannot
				// yet roll forward the state machine by applying committed log
				// entries.
				return checkAndPrintReps(false)

			case "split-replica":
				err := func() error {
					r := scanAndGetRangeStorage(t, d, "", reps)
					rhsFullID := FullReplicaID{
						RangeID:   scanRangeIDForTest(t, d, "rhs-"),
						ReplicaID: scanReplicaID(t, d, "rhs-"),
					}
					rhsSpan := scanSpan(t, d, "rhs-")
					// Timestamp of the split. Used for RangeDescriptor updates.
					ts := scanTimestamp(t, d)
					var lhsDesc roachpb.RangeDescriptor
					found, err := storage.MVCCGetProto(ctx, eng, keys.RangeDescriptorKey(r.mu.span.Key), hlc.MaxTimestamp, &lhsDesc,
						storage.MVCCGetOptions{})
					require.True(t, found)
					require.NoError(t, err)
					lhsDesc.EndKey = rhsSpan.Key
					smBatch := eng.NewBatch()
					defer smBatch.Close()
					// RHS RangeDescriptor. This test does not bother with a distributed
					// transaction where the provisional LHS and RHS RangeDescriptors are
					// written and then resolved when the transaction commits.
					if err := writeDescriptor(ctx, rhsFullID.RangeID, rhsFullID.ReplicaID, rhsSpan, ts, smBatch); err != nil {
						return err
					}
					// LHS RangeDescriptor.
					if err := storage.MVCCBlindPutProto(ctx, smBatch, nil, keys.RangeDescriptorKey(lhsDesc.StartKey),
						hlc.Timestamp{WallTime: ts}, hlc.ClockTimestamp{}, &lhsDesc, nil); err != nil {
						return err
					}
					// RHS RangeAppliedState.
					appliedState := enginepb.RangeAppliedState{
						RaftAppliedIndex: testRaftInitialLogIndex, RaftAppliedIndexTerm: testRaftInitialLogTerm}
					if err = storage.MVCCBlindPutProto(ctx, smBatch, nil, keys.RangeAppliedStateKey(rhsFullID.RangeID),
						hlc.Timestamp{}, hlc.ClockTimestamp{}, &appliedState, nil); err != nil {
						return err
					}
					_, err = reps.SplitReplica(ctx, r, rhsFullID, rhsSpan, testMutationBatch{b: smBatch})
					if err != nil {
						return err
					}
					return nil
				}()
				if err != nil {
					return err.Error()
				}
				return checkAndPrintReps(true)

			case "merge-replicas":
				err := func() error {
					lhsR := scanAndGetRangeStorage(t, d, "lhs-", reps)
					rhsR := scanAndGetRangeStorage(t, d, "rhs-", reps)
					// Timestamp of the merge. Used for RangeDescriptor updates.
					ts := scanTimestamp(t, d)
					var lhsDesc roachpb.RangeDescriptor
					found, err := storage.MVCCGetProto(ctx, eng, keys.RangeDescriptorKey(lhsR.mu.span.Key), hlc.MaxTimestamp,
						&lhsDesc, storage.MVCCGetOptions{})
					require.True(t, found)
					require.NoError(t, err)
					lhsDesc.EndKey = rhsR.mu.span.EndKey
					smBatch := eng.NewBatch()
					defer smBatch.Close()
					// This test does not bother with a distributed transaction where the
					// provisional LHS and RHS RangeDescriptors are written and then
					// resolved when the transaction commits.

					// LHS RangeDescriptor update.
					if err := storage.MVCCBlindPutProto(ctx, smBatch, nil, keys.RangeDescriptorKey(lhsDesc.StartKey),
						hlc.Timestamp{WallTime: ts}, hlc.ClockTimestamp{}, &lhsDesc, nil); err != nil {
						return err
					}
					// RHS RangeDescriptor MVCC deletion.
					if err := storage.MVCCBlindPut(ctx, smBatch, nil, keys.RangeDescriptorKey(rhsR.mu.span.Key),
						hlc.Timestamp{WallTime: ts}, hlc.ClockTimestamp{}, roachpb.Value{}, nil); err != nil {
						return err
					}
					if err := reps.MergeReplicas(ctx, lhsR, rhsR, testMutationBatch{b: smBatch}); err != nil {
						return err
					}
					return nil
				}()
				if err != nil {
					return err.Error()
				}
				return checkAndPrintReps(true)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

type testMutationBatch struct {
	b storage.Batch
}

func (tmb testMutationBatch) Commit(sync bool) error {
	return tmb.b.Commit(sync)
}
func (tmb testMutationBatch) Batch() storage.Batch {
	return tmb.b
}

func scanRangeIDForTest(t *testing.T, d *datadriven.TestData, prefix string) roachpb.RangeID {
	var rangeID int
	d.ScanArgs(t, prefix+"range-id", &rangeID)
	return roachpb.RangeID(rangeID)
}

func scanReplicaID(t *testing.T, d *datadriven.TestData, prefix string) roachpb.ReplicaID {
	var replicaID int
	d.ScanArgs(t, prefix+"replica-id", &replicaID)
	return roachpb.ReplicaID(replicaID)
}

func scanAndGetRangeStorage(
	t *testing.T, d *datadriven.TestData, prefix string, reps *replicasStorageImpl,
) *rangeStorageImpl {
	rangeID := scanRangeIDForTest(t, d, prefix)
	replicaID := scanReplicaID(t, d, prefix)
	r, err := reps.GetHandle(FullReplicaID{RangeID: rangeID, ReplicaID: replicaID})
	require.NoError(t, err)
	require.NotNil(t, r)
	return r.(*rangeStorageImpl)
}

func scanGlobalKeys(t *testing.T, d *datadriven.TestData) (keys []string, ts int64) {
	var keysStr string
	d.ScanArgs(t, "keys", &keysStr)
	keys = strings.Split(keysStr, ",")
	ts = scanTimestamp(t, d)
	return keys, ts
}

func scanTimestamp(t *testing.T, d *datadriven.TestData) int64 {
	var tsint int
	d.ScanArgs(t, "ts", &tsint)
	return int64(tsint)
}

func scanSpan(t *testing.T, d *datadriven.TestData, prefix string) roachpb.RSpan {
	var spanStr string
	d.ScanArgs(t, prefix+"span", &spanStr)
	parts := strings.Split(spanStr, ",")
	require.Equal(t, 2, len(parts))
	return roachpb.RSpan{Key: roachpb.RKey(parts[0]), EndKey: roachpb.RKey(parts[1])}
}

func scanRaftIndexAndTerm(
	t *testing.T, d *datadriven.TestData,
) (raftIndex uint64, raftTerm uint64) {
	d.ScanArgs(t, "raft-index", &raftIndex)
	d.ScanArgs(t, "raft-term", &raftTerm)
	require.LessOrEqual(t, uint64(testRaftInitialLogIndex), raftIndex)
	require.LessOrEqual(t, uint64(testRaftInitialLogTerm), raftTerm)
	return raftIndex, raftTerm
}

func printReps(t *testing.T, reps *replicasStorageImpl) string {
	// TODO(sumeer): Print any unexpected stuff in engine, and not just the
	// state based on the ranges known to reps.
	var b strings.Builder
	for i, r := range reps.mu.replicasSpans {
		if i > 0 {
			rPrev := reps.mu.replicasSpans[i-1]
			require.True(t, rPrev.mu.span.EndKey.Compare(r.mu.span.Key) <= 0)
		}
		require.Equal(t, InitializedStateMachine, r.mu.state)
		require.LessOrEqual(t, r.mu.lastSyncedCommit, r.mu.lastCommit)
		rangeID := r.id.RangeID
		r2, ok := reps.mu.replicasMap[rangeID]
		require.True(t, ok)
		require.Equal(t, r2, r)
		printRange(t, &b, r, reps.eng)
	}
	var uninitializedRanges []*rangeStorageImpl
	for _, r := range reps.mu.replicasMap {
		require.NotEqual(t, DeletedReplica, r.mu.state)
		if r.mu.state == UninitializedStateMachine {
			uninitializedRanges = append(uninitializedRanges, r)
		}
	}
	sort.Slice(uninitializedRanges, func(i, j int) bool {
		return uninitializedRanges[i].id.RangeID < uninitializedRanges[j].id.RangeID
	})
	for _, r := range uninitializedRanges {
		printRange(t, &b, r, reps.eng)
	}
	return b.String()
}

func replicaStateStr(state ReplicaState) string {
	switch state {
	case UninitializedStateMachine:
		return "uninited"
	case InitializedStateMachine:
		return "inited"
	case DeletedReplica:
		return "deleted"
	default:
		return "unknown"
	}
}

func printRange(t *testing.T, b *strings.Builder, r *rangeStorageImpl, eng storage.Engine) {
	ctx := context.Background()
	r.mu.RLock()
	defer r.mu.RUnlock()
	fmt.Fprintf(b, "== r%s/%s %s ==\n", r.id.RangeID.String(), r.id.ReplicaID.String(), replicaStateStr(r.mu.state))
	hs, err := r.rsl.LoadHardState(ctx, eng)
	require.NoError(t, err)
	// TODO: CI fails race tests when hs is zero, by printing "hs: " instead of
	// "hs: term:0 vote:0 commit:0".
	fmt.Fprintf(b, "  hs: %s", hs.String())
	replicaID, found, err := r.rsl.LoadRaftReplicaID(ctx, eng)
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, r.id.ReplicaID, replicaID.ReplicaID)
	tombstoneKey := keys.RangeTombstoneKey(r.id.RangeID)
	var tombstone roachpb.RangeTombstone
	ok, err := storage.MVCCGetProto(ctx, eng, tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{})
	require.NoError(t, err)
	if ok {
		require.LessOrEqual(t, tombstone.NextReplicaID, r.id.ReplicaID)
		fmt.Fprintf(b, " next-replica-id: %d", tombstone.NextReplicaID)
	}
	truncState, err := r.rsl.LoadRaftTruncatedState(ctx, eng)
	require.NoError(t, err)
	fmt.Fprintf(b, " trunc-state: %s\n", truncState.String())
	func() {
		prefix := r.rsl.RaftLogPrefix()
		iter := eng.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound: prefix, UpperBound: keys.RaftLogKeyFromPrefix(prefix, math.MaxUint64)})
		defer iter.Close()
		iter.SeekGE(storage.MVCCKey{Key: prefix})
		first := true
		for {
			valid, err := iter.Valid()
			require.NoError(t, err)
			if !valid {
				break
			}
			key := iter.Key().Key
			suffix := key[len(prefix):]
			index, err := keys.DecodeRaftLogKeyFromSuffix(suffix)
			require.NoError(t, err)
			if first {
				fmt.Fprintf(b, "  raft-log: %d", index)
				first = false
			} else {
				fmt.Fprintf(b, ", %d", index)
			}
			iter.Next()
		}
		if !first {
			fmt.Fprintf(b, "\n")
		}
	}()
	if r.mu.state != InitializedStateMachine {
		return
	}
	fmt.Fprintf(b, "  span: %s", r.mu.span.String())
	var desc roachpb.RangeDescriptor
	value, _, err := storage.MVCCGet(ctx, eng, keys.RangeDescriptorKey(r.mu.span.Key), hlc.MaxTimestamp,
		storage.MVCCGetOptions{})
	require.NoError(t, err)
	require.NotNil(t, value)
	require.NoError(t, value.GetProto(&desc))
	require.True(t, r.mu.span.Equal(desc.RSpan()))
	require.Equal(t, r.id.RangeID, desc.RangeID)
	fmt.Fprintf(b, " desc-ts: %d", value.Timestamp.WallTime)

	appliedState, err := r.rsl.LoadRangeAppliedState(ctx, eng)
	require.NoError(t, err)
	fmt.Fprintf(b, " appliedState: index: %d, term: %d\n", appliedState.RaftAppliedIndex,
		appliedState.RaftAppliedIndexTerm)
	func() {
		iter := eng.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound: roachpb.Key(desc.StartKey), UpperBound: roachpb.Key(desc.EndKey)})
		defer iter.Close()
		iter.SeekGE(storage.MVCCKey{Key: roachpb.Key(desc.StartKey)})
		fmt.Fprintf(b, "  global:")
		for {
			valid, err := iter.Valid()
			require.NoError(t, err)
			if !valid {
				break
			}
			value, err := storage.DecodeMVCCValue(iter.UnsafeValue())
			require.NoError(t, err)
			val, err := value.Value.GetBytes()
			require.NoError(t, err)
			key := iter.UnsafeKey()
			fmt.Fprintf(b, " %s=%s", key.String(), string(val))
			iter.Next()
		}
		fmt.Fprintf(b, "\n")
	}()
}

func writeRangeSnapshot(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	span roachpb.RSpan,
	keySlice []string,
	ts int64,
	raftIndex uint64,
	raftTerm uint64,
	st *cluster.Settings,
	sstScratch *SSTSnapshotStorageScratch,
) (paths []string, err error) {
	rangeIDLocalSpan := rditer.MakeRangeIDLocalKeySpan(rangeID, true)
	sstFile := &storage.MemFile{}
	sstWriter := storage.MakeIngestionSSTWriter(ctx, st, sstFile)
	defer sstWriter.Close()
	if err := sstWriter.ClearRawRange(
		rangeIDLocalSpan.Key, rangeIDLocalSpan.EndKey, true, false); err != nil {
		return nil, err
	}
	appliedState := enginepb.RangeAppliedState{RaftAppliedIndex: raftIndex, RaftAppliedIndexTerm: raftTerm}
	if err = storage.MVCCBlindPutProto(ctx, &sstWriter, nil, keys.RangeAppliedStateKey(rangeID),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, &appliedState, nil); err != nil {
		return nil, err
	}
	path, err := sstWriterToFile(ctx, sstWriter, sstFile, sstScratch)
	if err != nil {
		return nil, err
	}
	paths = append(paths, path)
	rangeSpans := keyRangesForStateMachineExceptRangeIDKeys(span)
	for i := 0; i < len(rangeSpans); i++ {
		sstFile = &storage.MemFile{}
		sstWriter = storage.MakeIngestionSSTWriter(ctx, st, sstFile)
		defer sstWriter.Close()
		if err := sstWriter.ClearRawRange(
			rangeSpans[i].Key, rangeSpans[i].EndKey, true, false); err != nil {
			return nil, err
		}
		if i == 0 {
			// Range local span.
			if err := writeDescriptor(ctx, rangeID, replicaID, span, ts, &sstWriter); err != nil {
				return nil, err
			}
		}
		if i == len(rangeSpans)-1 {
			// Global span.
			if err := writeGlobalKeys(ctx, keySlice, ts, &sstWriter); err != nil {
				return nil, err
			}
		}
		path, err = sstWriterToFile(ctx, sstWriter, sstFile, sstScratch)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func writeDescriptor(
	ctx context.Context,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	span roachpb.RSpan,
	ts int64,
	w storage.Writer,
) error {
	descriptorState := roachpb.RangeDescriptor{RangeID: rangeID, StartKey: span.Key, EndKey: span.EndKey,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				// Have an additional replica unrelated to the one we are adding.
				NodeID:    testNodeID + 50,
				StoreID:   testStoredID + 50,
				ReplicaID: replicaID + 50,
			},
			{
				NodeID:    testNodeID,
				StoreID:   testStoredID,
				ReplicaID: replicaID,
			},
		}}
	if err := storage.MVCCBlindPutProto(ctx, w, nil, keys.RangeDescriptorKey(span.Key),
		hlc.Timestamp{WallTime: ts}, hlc.ClockTimestamp{}, &descriptorState, nil); err != nil {
		return err
	}
	return nil
}

// Writes the given global keys to a raftBatch and smBatch (state machine).
// This function is limited in that it puts all the key-value pairs in a
// single raft log entry.
func writeGlobalKeysViaRaft(
	ctx context.Context,
	rangeID roachpb.RangeID,
	keySlice []string,
	ts int64,
	raftIndex uint64,
	raftTerm uint64,
	eng storage.Engine,
) (raftBatch storage.Batch, smBatch storage.Batch, err error) {
	tmpBatch := eng.NewBatch()
	defer tmpBatch.Close()
	if err = writeGlobalKeys(ctx, keySlice, ts, tmpBatch); err != nil {
		return nil, nil, err
	}
	batchBytes := tmpBatch.Repr()
	raftBatch = eng.NewUnindexedBatch(false)
	// TODO(sumeer): this is not the right way to construct a raft entry in the
	// engine. It is a raftpb.Entry. Fix. This works for now since neither the
	// test code, nor replicasStorageImpl is reading the raft log entries.
	if err := storage.MVCCBlindPut(ctx, raftBatch, nil, keys.RaftLogKey(rangeID, raftIndex), hlc.Timestamp{},
		hlc.ClockTimestamp{}, roachpb.Value{RawBytes: batchBytes}, nil); err != nil {
		return nil, nil, err
	}
	smBatch = eng.NewBatch()
	if err := smBatch.ApplyBatchRepr(batchBytes, false); err != nil {
		return nil, nil, err
	}
	appliedState := enginepb.RangeAppliedState{RaftAppliedIndex: raftIndex, RaftAppliedIndexTerm: raftTerm}
	if err = storage.MVCCBlindPutProto(ctx, smBatch, nil, keys.RangeAppliedStateKey(rangeID),
		hlc.Timestamp{}, hlc.ClockTimestamp{}, &appliedState, nil); err != nil {
		return nil, nil, err
	}
	return raftBatch, smBatch, nil
}

func writeGlobalKeys(ctx context.Context, keys []string, ts int64, w storage.Writer) error {
	for i := range keys {
		var ms enginepb.MVCCStats
		var v roachpb.Value
		v.SetBytes([]byte(fmt.Sprintf("%s.%d", keys[i], ts)))
		err := storage.MVCCBlindPut(ctx, w, &ms, roachpb.Key(keys[i]),
			hlc.Timestamp{WallTime: ts}, hlc.ClockTimestamp{}, v, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
