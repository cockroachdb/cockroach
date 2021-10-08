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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSerialUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()
	const numInputs = 3
	const numBatches = 4

	typs := []*types.T{types.Int}
	inputs := make([]colexecargs.OpWithMetaInfo, numInputs)
	for i := range inputs {
		batch := coldatatestutils.RandomBatch(testAllocator, rng, typs, coldata.BatchSize(), 0 /* length */, rng.Float64())
		source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
		source.ResetBatchesToReturn(numBatches)
		inputIdx := i
		inputs[i].Root = source
		inputs[i].MetadataSources = []colexecop.MetadataSource{
			colexectestutils.CallbackMetadataSource{
				DrainMetaCb: func() []execinfrapb.ProducerMetadata {
					return []execinfrapb.ProducerMetadata{{Err: errors.Errorf("input %d test-induced metadata", inputIdx)}}
				},
			},
		}
	}
	s := NewSerialUnorderedSynchronizer(inputs)
	s.Init(ctx)
	resultBatches := 0
	for {
		b := s.Next()
		if b.Length() == 0 {
			require.Equal(t, len(inputs), len(s.DrainMeta()))
			break
		}
		resultBatches++
	}
	require.Equal(t, numInputs*numBatches, resultBatches)
}
