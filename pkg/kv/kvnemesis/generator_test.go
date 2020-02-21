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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TODO(dan): Test that all operations actually are in newAllOperationsConfig.

// TestRandStep generates random steps until we've seen each type at least N
// times, validating each step along the way.
func TestRandStep(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const minEachType = 5
	config := newAllOperationsConfig()
	config.NumNodes, config.NumReplicas = 2, 1
	rng, _ := randutil.NewPseudoRand()
	getReplicasFn := func(_ roachpb.Key) []roachpb.ReplicationTarget {
		return make([]roachpb.ReplicationTarget, rng.Intn(1)+1)
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

	counts := make(map[OpP]int, len(config.OpPs))
	for {
		step := g.RandStep(rng)

		switch o := step.Op.GetValue().(type) {
		case *GetOperation:
			if _, ok := keys[string(o.Key)]; ok {
				counts[OpPGetExisting]++
			} else {
				counts[OpPGetMissing]++
			}
		case *PutOperation:
			if _, ok := keys[string(o.Key)]; ok {
				counts[OpPPutExisting]++
			} else {
				counts[OpPPutMissing]++
			}
		case *ClosureTxnOperation:
			if o.CommitInBatch != nil {
				if o.Type != ClosureTxnType_Commit {
					t.Fatalf(`commit type %s is invalid with CommitInBatch`, o.Type)
				}
				counts[OpPClosureTxnCommitInBatch]++
			} else {
				counts[OpPClosureTxn]++
			}
		case *BatchOperation:
			counts[OpPBatch]++
		case *SplitOperation:
			if _, ok := splits[string(o.Key)]; ok {
				counts[OpPSplitAgain]++
			} else {
				counts[OpPSplitNew]++
			}
			splits[string(o.Key)] = struct{}{}
		case *MergeOperation:
			if _, ok := splits[string(o.Key)]; ok {
				counts[OpPMergeIsSplit]++
			} else {
				counts[OpPMergeNotSplit]++
			}
		case *ChangeReplicasOperation:
			counts[OpPChangeReplicas]++
		}
		updateKeys(step.Op)

		// TODO(dan): Make sure the proportions match the requested ones to within
		// some bounds.
		done := true
		for op := range config.OpPs {
			if counts[op] < minEachType {
				done = false
				break
			}
		}
		if done {
			break
		}
	}
}
