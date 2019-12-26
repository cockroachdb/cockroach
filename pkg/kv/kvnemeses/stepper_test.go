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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestValidateStep(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := ValidateStep(step(beginTxn("1"), useTxn("1")))
	require.EqualError(t, err, `txn 1 used concurrently`)
}

// TestRandStep generates random steps until we've seen each type at least N
// times, validating each step along the way.
func TestRandStep(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()
	s := MakeStepper()

	const minEachType = 5
	allOperationTypes := []reflect.Type{
		reflect.TypeOf((*GetOperation)(nil)),
		reflect.TypeOf((*PutOperation)(nil)),
		// TODO(dan): Reenable when Stepper generates BatchOperations again.
		// reflect.TypeOf((*BatchOperation)(nil)),
		reflect.TypeOf((*SplitOperation)(nil)),
		reflect.TypeOf((*MergeOperation)(nil)),
		reflect.TypeOf((*BeginTxnOperation)(nil)),
		reflect.TypeOf((*UseTxnOperation)(nil)),
		reflect.TypeOf((*CommitTxnOperation)(nil)),
		reflect.TypeOf((*RollbackTxnOperation)(nil)),
	}
	operationCounts := make(map[reflect.Type]int)

	for {
		step := s.RandStep(rng)
		if err := ValidateStep(step); err != nil {
			t.Fatal(err)
		}

		for _, op := range step.Ops {
			operationCounts[reflect.TypeOf(op.GetValue())]++
		}
		done := true
		for _, t := range allOperationTypes {
			if operationCounts[t] < minEachType {
				done = false
				break
			}
		}
		if done {
			break
		}
	}
}
