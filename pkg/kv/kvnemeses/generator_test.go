// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestRandStep generates random steps until we've seen each type at least N
// times, validating each step along the way.
func TestRandStep(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const minEachType = 5
	config := GeneratorConfig{
		OpPGetMissing:    1,
		OpPGetExisting:   1,
		OpPPutMissing:    1,
		OpPPutExisting:   1,
		OpPBatch:         1,
		OpPClosureTxn:    1,
		OpPSplitNew:      1,
		OpPSplitAgain:    1,
		OpPMergeNotSplit: 1,
		OpPMergeIsSplit:  1,
	}

	rng, _ := randutil.NewPseudoRand()
	g := MakeGenerator(config)

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

	counts := make(map[OpP]int, len(config))
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
			counts[OpPClosureTxn]++
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
		}
		updateKeys(step.Op)

		// TODO(dan): Make sure the proportions match the requested ones to within
		// some bounds.
		done := true
		for op := range config {
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
