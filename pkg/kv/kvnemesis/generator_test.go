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
			case *AddSSTableOperation:
				client.AddSSTable++
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
			*DeleteRangeUsingTombstoneOperation,
			*AddSSTableOperation:
			countClientOps(&counts.DB, &counts.Batch, step.Op)
		case *ClosureTxnOperation:
			countClientOps(&counts.ClosureTxn.TxnClientOps, &counts.ClosureTxn.TxnBatchOps, o.Ops...)
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
