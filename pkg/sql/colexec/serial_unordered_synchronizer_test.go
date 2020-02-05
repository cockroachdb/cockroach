// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSerialUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()
	const numInputs = 3
	const numBatches = 4

	typs := []coltypes.T{coltypes.Int64}
	inputs := make([]Operator, numInputs)
	for i := range inputs {
		batch := RandomBatch(testAllocator, rng, typs, int(coldata.BatchSize()), 0 /* length */, rng.Float64())
		source := NewRepeatableBatchSource(testAllocator, batch)
		source.ResetBatchesToReturn(numBatches)
		inputs[i] = source
	}
	s := NewSerialUnorderedSynchronizer(inputs, typs)
	resultBatches := 0
	for {
		b := s.Next(ctx)
		if b.Length() == 0 {
			break
		}
		resultBatches++
	}
	require.Equal(t, numInputs*numBatches, resultBatches)
}
