// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func trueForEachIntField(c *OperationConfig, fn func(int) bool) bool {
	var forEachIntField func(v reflect.Value) bool
	forEachIntField = func(v reflect.Value) bool {
		switch v.Type().Kind() {
		case reflect.Ptr:
			return forEachIntField(v.Elem())
		case reflect.Int:
			ok := fn(int(v.Int()))
			if !ok {
				if log.V(1) {
					log.Infof(context.Background(), "returned false for %d: %v", v.Int(), v)
				}
			}
			return ok
		case reflect.Struct:
			for fieldIdx := 0; fieldIdx < v.NumField(); fieldIdx++ {
				if !forEachIntField(v.Field(fieldIdx)) {
					if log.V(1) {
						log.Infof(context.Background(), "returned false for %s in %s",
							v.Type().Field(fieldIdx).Name, v.Type().Name())
					}
					return false
				}
			}
			return true
		default:
			panic(errors.AssertionFailedf(`unexpected type: %s`, v.Type()))
		}

	}
	return forEachIntField(reflect.ValueOf(c))
}

// TestRandStep generates random steps until we've seen each type at least N
// times, validating each step along the way. This both verifies that the config
// returned by `newAllOperationsConfig()` in fact contains all operations as
// well as verifies that Generator actually generates all of these operation
// types.
func TestRandStep(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const minEachType = 5
	config := newAllOperationsConfig()
	config.NumNodes, config.NumReplicas = 3, 2
	rng, _ := randutil.NewTestRand()
	getReplicasFn := func(_ roachpb.Key) ([]roachpb.ReplicationTarget, []roachpb.ReplicationTarget) {
		return make([]roachpb.ReplicationTarget, rng.Intn(config.NumNodes)+1),
			make([]roachpb.ReplicationTarget, rng.Intn(config.NumNodes)+1)
	}
	g, err := MakeGenerator(config, getReplicasFn)
	require.NoError(t, err)

	keys := make(map[string]struct{})
	var updateKeys func(Operation)
	updateKeys = func(op Operation) {
		switch o := op.GetValue().(type) {
		case *PutOperation:
			keys[string(o.Key)] = struct{}{}
		case *BatchOperation:
			for _, op := range o.Ops {
				updateKeys(op)
			}
		case *ClosureTxnOperation:
			for _, op := range o.Ops {
				updateKeys(op)
			}
		}
	}

	splits := make(map[string]struct{})

	var countClientOps func(*ClientOperationConfig, *BatchOperationConfig, ...Operation)
	countClientOps = func(client *ClientOperationConfig, batch *BatchOperationConfig, ops ...Operation) {
		for _, op := range ops {
			switch o := op.GetValue().(type) {
			case *GetOperation:
				if _, ok := keys[string(o.Key)]; ok {
					if o.SkipLocked && o.ForUpdate {
						if o.GuaranteedDurability {
							client.GetExistingForUpdateSkipLockedGuaranteedDurability++
						} else {
							client.GetExistingForUpdateSkipLocked++
						}
					} else if o.SkipLocked && o.ForShare {
						if o.GuaranteedDurability {
							client.GetExistingForShareSkipLockedGuaranteedDurability++

						} else {
							client.GetExistingForShareSkipLocked++
						}
					} else if o.SkipLocked {
						client.GetExistingSkipLocked++
					} else if o.ForUpdate {
						if o.GuaranteedDurability {
							client.GetExistingForUpdateGuaranteedDurability++
						} else {
							client.GetExistingForUpdate++
						}
					} else if o.ForShare {
						if o.GuaranteedDurability {
							client.GetExistingForShareGuaranteedDurability++
						} else {
							client.GetExistingForShare++
						}
					} else {
						client.GetExisting++
					}
				} else {
					if o.SkipLocked && o.ForUpdate {
						if o.GuaranteedDurability {
							client.GetMissingForUpdateSkipLockedGuaranteedDurability++
						} else {
							client.GetMissingForUpdateSkipLocked++
						}
					} else if o.SkipLocked && o.ForShare {
						if o.GuaranteedDurability {
							client.GetMissingForShareSkipLockedGuaranteedDurability++
						} else {
							client.GetMissingForShareSkipLocked++
						}
					} else if o.SkipLocked {
						client.GetMissingSkipLocked++
					} else if o.ForUpdate {
						if o.GuaranteedDurability {
							client.GetMissingForUpdateGuaranteedDurability++
						} else {
							client.GetMissingForUpdate++
						}
					} else if o.ForShare {
						if o.GuaranteedDurability {
							client.GetMissingForShareGuaranteedDurability++
						} else {
							client.GetMissingForShare++
						}
					} else {
						client.GetMissing++
					}
				}
			case *PutOperation:
				if _, ok := keys[string(o.Key)]; ok {
					client.PutExisting++
				} else {
					client.PutMissing++
				}
			case *ScanOperation:
				if o.Reverse {
					if o.SkipLocked && o.ForUpdate {
						if o.GuaranteedDurability {
							client.ReverseScanForUpdateSkipLockedGuaranteedDurability++
						} else {
							client.ReverseScanForUpdateSkipLocked++
						}
					} else if o.SkipLocked && o.ForShare {
						if o.GuaranteedDurability {
							client.ReverseScanForShareSkipLockedGuaranteedDurability++
						} else {
							client.ReverseScanForShareSkipLocked++
						}
					} else if o.SkipLocked {
						client.ReverseScanSkipLocked++
					} else if o.ForUpdate {
						if o.GuaranteedDurability {
							client.ReverseScanForUpdateGuaranteedDurability++
						} else {
							client.ReverseScanForUpdate++
						}
					} else if o.ForShare {
						if o.GuaranteedDurability {
							client.ReverseScanForShareGuaranteedDurability++
						} else {
							client.ReverseScanForShare++
						}
					} else {
						client.ReverseScan++
					}
				} else {
					if o.SkipLocked && o.ForUpdate {
						if o.GuaranteedDurability {
							client.ScanForUpdateSkipLockedGuaranteedDurability++
						} else {
							client.ScanForUpdateSkipLocked++
						}
					} else if o.SkipLocked && o.ForShare {
						if o.GuaranteedDurability {
							client.ScanForShareSkipLockedGuaranteedDurability++
						} else {
							client.ScanForShareSkipLocked++
						}
					} else if o.SkipLocked {

						client.ScanSkipLocked++
					} else if o.ForUpdate {
						if o.GuaranteedDurability {
							client.ScanForUpdateGuaranteedDurability++
						} else {
							client.ScanForUpdate++
						}
					} else if o.ForShare {
						if o.GuaranteedDurability {
							client.ScanForShareGuaranteedDurability++
						} else {
							client.ScanForShare++
						}
					} else {
						client.Scan++
					}
				}
			case *DeleteOperation:
				if _, ok := keys[string(o.Key)]; ok {
					client.DeleteExisting++
				} else {
					client.DeleteMissing++
				}
			case *DeleteRangeOperation:
				client.DeleteRange++
			case *DeleteRangeUsingTombstoneOperation:
				client.DeleteRangeUsingTombstone++
			case *AddSSTableOperation:
				client.AddSSTable++
			case *BarrierOperation:
				client.Barrier++
			case *BatchOperation:
				batch.Batch++
				countClientOps(&batch.Ops, nil, o.Ops...)
			case *SavepointCreateOperation, *SavepointRollbackOperation, *SavepointReleaseOperation:
				// We'll count these separately.
			default:
				t.Fatalf("%T", o)
			}
		}
	}

	countSavepointOps := func(savepoint *SavepointConfig, ops ...Operation) {
		for _, op := range ops {
			switch op.GetValue().(type) {
			case *SavepointCreateOperation:
				savepoint.SavepointCreate++
			case *SavepointReleaseOperation:
				savepoint.SavepointRelease++
			case *SavepointRollbackOperation:
				savepoint.SavepointRollback++
			}
		}
	}

	counts := OperationConfig{}
	for {
		step := g.RandStep(rng)
		switch o := step.Op.GetValue().(type) {
		case *GetOperation,
			*PutOperation,
			*ScanOperation,
			*BatchOperation,
			*DeleteOperation,
			*DeleteRangeOperation,
			*DeleteRangeUsingTombstoneOperation,
			*AddSSTableOperation,
			*BarrierOperation:
			countClientOps(&counts.DB, &counts.Batch, step.Op)
		case *ClosureTxnOperation:
			countClientOps(&counts.ClosureTxn.TxnClientOps, &counts.ClosureTxn.TxnBatchOps, o.Ops...)
			countSavepointOps(&counts.ClosureTxn.SavepointOps, o.Ops...)
			if o.CommitInBatch != nil {
				switch o.IsoLevel {
				case isolation.Serializable:
					counts.ClosureTxn.CommitSerializableInBatch++
				case isolation.Snapshot:
					counts.ClosureTxn.CommitSnapshotInBatch++
				case isolation.ReadCommitted:
					counts.ClosureTxn.CommitReadCommittedInBatch++
				default:
					t.Fatalf("unexpected isolation level %s", o.IsoLevel)
				}
				countClientOps(&counts.ClosureTxn.CommitBatchOps, nil, o.CommitInBatch.Ops...)
			} else if o.Type == ClosureTxnType_Commit {
				switch o.IsoLevel {
				case isolation.Serializable:
					counts.ClosureTxn.CommitSerializable++
				case isolation.Snapshot:
					counts.ClosureTxn.CommitSnapshot++
				case isolation.ReadCommitted:
					counts.ClosureTxn.CommitReadCommitted++
				default:
					t.Fatalf("unexpected isolation level %s", o.IsoLevel)
				}
			} else if o.Type == ClosureTxnType_Rollback {
				switch o.IsoLevel {
				case isolation.Serializable:
					counts.ClosureTxn.RollbackSerializable++
				case isolation.Snapshot:
					counts.ClosureTxn.RollbackSnapshot++
				case isolation.ReadCommitted:
					counts.ClosureTxn.RollbackReadCommitted++
				default:
					t.Fatalf("unexpected isolation level %s", o.IsoLevel)
				}
			}
		case *SplitOperation:
			if _, ok := splits[string(o.Key)]; ok {
				counts.Split.SplitAgain++
			} else {
				counts.Split.SplitNew++
			}
			splits[string(o.Key)] = struct{}{}
		case *MergeOperation:
			if _, ok := splits[string(o.Key)]; ok {
				counts.Merge.MergeIsSplit++
			} else {
				counts.Merge.MergeNotSplit++
			}
		case *ChangeReplicasOperation:
			var voterAdds, voterRemoves, nonVoterAdds, nonVoterRemoves int
			for _, change := range o.Changes {
				switch change.ChangeType {
				case roachpb.ADD_VOTER:
					voterAdds++
				case roachpb.REMOVE_VOTER:
					voterRemoves++
				case roachpb.ADD_NON_VOTER:
					nonVoterAdds++
				case roachpb.REMOVE_NON_VOTER:
					nonVoterRemoves++
				}
			}
			if voterAdds == 1 && voterRemoves == 0 && nonVoterRemoves == 0 {
				counts.ChangeReplicas.AddVotingReplica++
			} else if voterAdds == 0 && voterRemoves == 1 && nonVoterAdds == 0 {
				counts.ChangeReplicas.RemoveVotingReplica++
			} else if voterAdds == 1 && voterRemoves == 1 {
				counts.ChangeReplicas.AtomicSwapVotingReplica++
			} else if nonVoterAdds == 1 && nonVoterRemoves == 0 && voterRemoves == 0 {
				counts.ChangeReplicas.AddNonVotingReplica++
			} else if nonVoterAdds == 0 && nonVoterRemoves == 1 && voterAdds == 0 {
				counts.ChangeReplicas.RemoveNonVotingReplica++
			} else if nonVoterAdds == 1 && nonVoterRemoves == 1 {
				counts.ChangeReplicas.AtomicSwapNonVotingReplica++
			} else if voterAdds == 1 && nonVoterRemoves == 1 {
				counts.ChangeReplicas.PromoteReplica++
			} else if voterRemoves == 1 && nonVoterAdds == 1 {
				counts.ChangeReplicas.DemoteReplica++
			}
		case *TransferLeaseOperation:
			counts.ChangeLease.TransferLease++
		case *ChangeSettingOperation:
			switch o.Type {
			case ChangeSettingType_SetLeaseType:
				counts.ChangeSetting.SetLeaseType++
			}
		case *ChangeZoneOperation:
			switch o.Type {
			case ChangeZoneType_ToggleGlobalReads:
				counts.ChangeZone.ToggleGlobalReads++
			}
		default:
			t.Fatalf("%T", o)
		}
		updateKeys(step.Op)

		// TODO(dan): Make sure the proportions match the requested ones to within
		// some bounds.
		if trueForEachIntField(&counts, func(count int) bool { return count >= minEachType }) {
			break
		}
	}
}

func TestRandKeyDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for i := 0; i < 10; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		k := randKey(rng)
		n := fk(k)
		require.Equal(t, k, tk(n))
	}
}

func TestRandDelRangeUsingTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var seq kvnemesisutil.Seq
	nextSeq := func() kvnemesisutil.Seq {
		seq++
		return seq
	}

	// We'll add temporary elements below. The linters prevent shadowing so we
	// need to pick a name that makes it clear which is which.
	//
	// Keep this sorted.
	goldenSplitKeys := []uint64{
		5000, 10000, 15000, 20000, 25000, 30000, math.MaxUint64 / 2,
	}

	splitPointMap := map[string]struct{}{}
	splitPointCountMap := map[string]int{}
	var splitSpans []roachpb.Span
	{
		// Temporarily put 0 and MaxUint64 into the slice to
		// help generate the leftmost and rightmost range.
		splitKeys := append([]uint64{0}, goldenSplitKeys...)
		splitKeys = append(splitKeys, math.MaxUint64)
		for i := range splitKeys {
			if i == 0 {
				continue
			}
			k := tk(splitKeys[i-1])
			ek := tk(splitKeys[i])
			splitSpans = append(splitSpans, roachpb.Span{
				Key:    roachpb.Key(k),
				EndKey: roachpb.Key(ek),
			})
			splitPointCountMap[ek] = 0
			if splitKeys[i] == math.MaxUint64 {
				// Don't put MaxUint64 into splitPointMap to make sure we're always
				// generating useful ranges. There's no room to the right of this split
				// point.
				continue
			}
			splitPointMap[ek] = struct{}{}
		}
	}

	rng := rand.New(rand.NewSource(1)) // deterministic
	const num = 1000

	// keysMap plays no role in this test but we need to pass one.
	// We could also check that we're hitting the keys in this map
	// randomly, etc, but don't currently.
	keysMap := map[string]struct{}{
		tk(5): {},
	}

	var numSingleRange, numCrossRange, numPoint int
	for i := 0; i < num; i++ {
		dr := randDelRangeUsingTombstoneImpl(splitPointMap, keysMap, nextSeq, rng).DeleteRangeUsingTombstone
		sp := roachpb.Span{Key: dr.Key, EndKey: dr.EndKey}
		nk, nek := fk(string(dr.Key)), fk(string(dr.EndKey))
		s := fmt.Sprintf("[%d,%d)", nk, nek)
		if fk(string(dr.Key))+1 == fk(string(dr.EndKey)) {
			if numPoint == 0 {
				t.Logf("first point request: %s", s)
			}
			numPoint++
			continue
		}
		var contained bool
		for _, splitSp := range splitSpans {
			if splitSp.Contains(sp) {
				// `sp` does not contain a split point, i.e. this would likely end up
				// being a single-range request.
				if numSingleRange == 0 {
					t.Logf("first single-range request: %s", s)
				}
				numSingleRange++
				contained = true
				splitPointCountMap[string(splitSp.EndKey)]++
				break
			}
		}
		if !contained {
			if numCrossRange == 0 {
				t.Logf("first cross-range request: %s", s)
			}
			numCrossRange++
		}
	}

	fracSingleRange := float64(numSingleRange) / float64(num)
	fracCrossRange := float64(numCrossRange) / float64(num)
	fracPoint := float64(numPoint) / float64(num)

	var buf strings.Builder

	fmt.Fprintf(&buf, "point:        %.3f n=%d\n", fracPoint, numPoint)
	fmt.Fprintf(&buf, "cross-range:  %.3f n=%d\n", fracCrossRange, numCrossRange)

	fmt.Fprintf(&buf, "single-range: %.3f n=%d\n", fracSingleRange, numSingleRange)

	for _, splitSp := range splitSpans {
		frac := float64(splitPointCountMap[string(splitSp.EndKey)]) / float64(numSingleRange)
		fmt.Fprintf(&buf, "              ^---- %.3f [%d,%d)\n",
			frac, fk(string(splitSp.Key)), fk(string(splitSp.EndKey)))
	}

	fmt.Fprintf(&buf, "------------------\ntotal         %.3f", fracSingleRange+fracPoint+fracCrossRange)

	echotest.Require(t, buf.String(), datapathutils.TestDataPath(t, t.Name()+".txt"))
}

func TestUpdateSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name        string
		savepoints  []int
		prevOp      Operation
		expectedSp  []int
		expectedErr string
	}{
		{
			name:       "no savepoints (nil)",
			savepoints: nil,
			prevOp:     get(k1),
			expectedSp: nil,
		},
		{
			name:       "no savepoints (empty)",
			savepoints: []int{},
			prevOp:     get(k1),
			expectedSp: []int{},
		},
		{
			name:       "prevOp is not a savepoint",
			savepoints: []int{0},
			prevOp:     get(k1),
			expectedSp: []int{0},
		},
		{
			name:       "prevOp is a savepoint create",
			savepoints: nil,
			prevOp:     createSavepoint(2),
			expectedSp: []int{2},
		},
		{
			name:       "prevOp is a savepoint release",
			savepoints: []int{1},
			prevOp:     releaseSavepoint(1),
			expectedSp: []int{},
		},
		{
			name:       "prevOp is a savepoint rollback",
			savepoints: []int{1},
			prevOp:     rollbackSavepoint(1),
			expectedSp: []int{},
		},
		{
			name:       "nested rollbacks",
			savepoints: []int{1, 2, 3, 4},
			prevOp:     rollbackSavepoint(2),
			expectedSp: []int{1},
		},
		{
			name:       "nested releases",
			savepoints: []int{1, 2, 3, 4},
			prevOp:     releaseSavepoint(2),
			expectedSp: []int{1},
		},
		{
			name:        "re-create existing savepoint",
			savepoints:  []int{1, 2, 3, 4},
			prevOp:      createSavepoint(1),
			expectedErr: "generating a savepoint create op: ID 1 already exists",
		},
		{
			name:        "release non-existent savepoint",
			savepoints:  []int{1, 2, 3, 4},
			prevOp:      releaseSavepoint(5),
			expectedErr: "generating a savepoint release op: ID 5 does not exist",
		},
		{
			name:        "roll back non-existent savepoint",
			savepoints:  []int{1, 2, 3, 4},
			prevOp:      rollbackSavepoint(5),
			expectedErr: "generating a savepoint rollback op: ID 5 does not exist",
		},
	}
	for _, test := range tests {
		if test.expectedErr != "" {
			require.PanicsWithError(t, test.expectedErr, func() { maybeUpdateSavepoints(&test.savepoints, test.prevOp) })
		} else {
			maybeUpdateSavepoints(&test.savepoints, test.prevOp)
			require.Equal(t, test.expectedSp, test.savepoints)
		}
	}
}
