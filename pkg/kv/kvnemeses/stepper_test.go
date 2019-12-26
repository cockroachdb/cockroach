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
	config := StepperConfig{
		OpPGetMissing:  1,
		OpPGetExisting: 1,
		OpPPutMissing:  1,
		OpPPutExisting: 1,
		OpPBatch:       1,
		OpPClosureTxn:  1,
		OpPSplit:       1,
		OpPMerge:       1,
	}

	rng, _ := randutil.NewPseudoRand()
	s := MakeStepper(config)

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

	counts := make(map[OpP]int, len(config))
	var step *Step
	for {
		next := s.RandStep(rng, step)
		step = &next

		switch o := next.Op.GetValue().(type) {
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
			counts[OpPSplit]++
		case *MergeOperation:
			counts[OpPMerge]++
		}
		updateKeys(next.Op)

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
