// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	config.NumNodes, config.NumReplicas = 2, 1
	rng, _ := randutil.NewTestRand()
	getReplicasFn := func(_ roachpb.Key) []roachpb.ReplicationTarget {
		return make([]roachpb.ReplicationTarget, rng.Intn(2)+1)
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
					if o.ForUpdate {
						client.GetExistingForUpdate++
					} else {
						client.GetExisting++
					}
				} else {
					if o.ForUpdate {
						client.GetMissingForUpdate++
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
				if o.Reverse && o.ForUpdate {
					client.ReverseScanForUpdate++
				} else if o.Reverse {
					client.ReverseScan++
				} else if o.ForUpdate {
					client.ScanForUpdate++
				} else {
					client.Scan++
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
			case *BatchOperation:
				batch.Batch++
				countClientOps(&batch.Ops, nil, o.Ops...)
			default:
				t.Fatalf("%T", o)
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
			*DeleteRangeUsingTombstoneOperation:
			countClientOps(&counts.DB, &counts.Batch, step.Op)
		case *ClosureTxnOperation:
			countClientOps(&counts.ClosureTxn.TxnClientOps, &counts.ClosureTxn.TxnBatchOps, o.Ops...)
			if o.CommitInBatch != nil {
				counts.ClosureTxn.CommitInBatch++
				countClientOps(&counts.ClosureTxn.CommitBatchOps, nil, o.CommitInBatch.Ops...)
			} else if o.Type == ClosureTxnType_Commit {
				counts.ClosureTxn.Commit++
			} else if o.Type == ClosureTxnType_Rollback {
				counts.ClosureTxn.Rollback++
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
			var adds, removes int
			for _, change := range o.Changes {
				switch change.ChangeType {
				case roachpb.ADD_VOTER:
					adds++
				case roachpb.REMOVE_VOTER:
					removes++
				}
			}
			if adds == 1 && removes == 0 {
				counts.ChangeReplicas.AddReplica++
			} else if adds == 0 && removes == 1 {
				counts.ChangeReplicas.RemoveReplica++
			} else if adds == 1 && removes == 1 {
				counts.ChangeReplicas.AtomicSwapReplica++
			}
		case *TransferLeaseOperation:
			counts.ChangeLease.TransferLease++
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
		n := uint64FromKey(k)
		require.Equal(t, k, uint64ToKey(n))
	}
}

func TestRandDelRangeUsingTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var seq kvnemesisutil.Seq
	nextSeq := func() kvnemesisutil.Seq {
		seq++
		return seq
	}

	spAll := roachpb.Span{
		Key:    roachpb.Key(uint64ToKey(0)),
		EndKey: roachpb.Key(uint64ToKey(math.MaxUint64)),
	}
	var curRanges roachpb.SpanGroup
	curRanges.Add(spAll)
	currentSplits := map[string]struct{}{}
	for _, n := range []uint64{1, 5, 10, 15, 20, 25, 30} {
		k := uint64ToKey(n)
		sp := roachpb.Span{Key: roachpb.Key(k)}
		currentSplits[k] = struct{}{}
		curRanges.Sub(sp)
	}

	rng := rand.New(rand.NewSource(0)) // deterministic
	const num = 1000

	var numSingleRange, numCrossRange, numPoint int
	for i := 0; i < num; i++ {
		dr := randDelRangeUsingTombstoneImpl(currentSplits, nextSeq, rng).DeleteRangeUsingTombstone
		sp := roachpb.Span{Key: dr.Key, EndKey: dr.EndKey}
		nk, nek := uint64FromKey(string(dr.Key)), uint64FromKey(string(dr.EndKey))
		s := fmt.Sprintf("[%d,%d)", nk, nek)
		if uint64FromKey(string(dr.Key))+1 == uint64FromKey(string(dr.EndKey)) {
			if numPoint == 0 {
				t.Logf("first point request: %s", s)
			}
			numPoint++
			continue
		}
		if curRanges.Encloses(sp) {
			// `sp` does not contain a split point, i.e. this would likely end up
			// being a single-range request.
			if numSingleRange == 0 {
				t.Logf("first single-range request: %s", s)
			}
			numSingleRange++
		} else {
			if numCrossRange == 0 {
				t.Logf("first cross-range request: %s", s)
			}
			numCrossRange++
		}
	}
	fracSingleRange := float64(numSingleRange) / float64(num)
	fracCrossRange := float64(numCrossRange) / float64(num)
	fracPoint := float64(numPoint) / float64(num)

	echotest.Require(t,
		fmt.Sprintf(`single-range: %.3f
point:        %.3f
cross-range:  %.3f
------------------
total         %.3f
`, fracSingleRange, fracPoint, fracCrossRange,
			fracSingleRange+fracPoint+fracCrossRange),
		testutils.TestDataPath(t, t.Name()+".txt"))
}
