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
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSerialUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()
	const numInputs = 3
	const numBatches = 4

	typs := []*types.T{types.Int}
	inputs := make([]SynchronizerInput, numInputs)
	for i := range inputs {
		batch := coldatatestutils.RandomBatch(testAllocator, rng, typs, coldata.BatchSize(), 0 /* length */, rng.Float64())
		source := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
		source.ResetBatchesToReturn(numBatches)
		inputIdx := i
		inputs[i] = SynchronizerInput{
			Op: source,
			MetadataSources: []execinfrapb.MetadataSource{
				execinfrapb.CallbackMetadataSource{
					DrainMetaCb: func(_ context.Context) []execinfrapb.ProducerMetadata {
						return []execinfrapb.ProducerMetadata{{Err: errors.Errorf("input %d test-induced metadata", inputIdx)}}
					},
				},
			},
		}
	}
	s := NewSerialUnorderedSynchronizer(inputs)
	resultBatches := 0
	for {
		b := s.Next(ctx)
		if b.Length() == 0 {
			require.Equal(t, len(inputs), len(s.DrainMeta(ctx)))
			break
		}
		resultBatches++
	}
	require.Equal(t, numInputs*numBatches, resultBatches)
}
