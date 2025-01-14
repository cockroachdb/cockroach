// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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
	rng, _ := randutil.NewTestRand()
	const numInputs = 3
	const numBatches = 4

	typs := []*types.T{types.Int}
	inputs := make([]colexecargs.OpWithMetaInfo, numInputs)
	for i := range inputs {
		args := coldatatestutils.RandomVecArgs{Rand: rng, NullProbability: rng.Float64()}
		batch := coldatatestutils.RandomBatch(testAllocator, args, typs, coldata.BatchSize(), 0 /* length */)
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
	s := NewSerialUnorderedSynchronizer(
		&execinfra.FlowCtx{Gateway: true},
		0, /* processorID */
		testAllocator,
		typs,
		inputs,
		0,   /* serialInputIdxExclusiveUpperBound */
		nil, /* exceedsInputIdxExclusiveUpperBoundError */
	)
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
