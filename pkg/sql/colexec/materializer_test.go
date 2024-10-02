// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

func TestColumnarizeMaterialize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	nCols := 1 + rng.Intn(4)
	var typs []*types.T
	for len(typs) < nCols {
		typs = append(typs, randgen.RandType(rng))
	}
	nRows := 10000
	rows := randgen.RandEncDatumRowsOfTypes(rng, nRows, typs)
	input := execinfra.NewRepeatableRowSource(typs, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}
	c := NewBufferingColumnarizerForTests(testAllocator, flowCtx, 0, input)

	m := NewMaterializer(
		nil, /* streamingMemAcc */
		flowCtx,
		1, /* processorID */
		colexecargs.OpWithMetaInfo{Root: c},
		typs,
	)
	m.Start(ctx)

	for i := 0; i < nRows; i++ {
		row, meta := m.Next()
		if meta != nil {
			t.Fatalf("unexpected meta %+v", meta)
		}
		if row == nil {
			t.Fatal("unexpected nil row")
		}
		for j := range typs {
			if cmp, err := row[j].Datum.Compare(ctx, &evalCtx, rows[i][j].Datum); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatal("unequal rows", row, rows[i])
			}
		}
	}
	row, meta := m.Next()
	if meta != nil {
		t.Fatalf("unexpected meta %+v", meta)
	}
	if row != nil {
		t.Fatal("unexpected not nil row", row)
	}
}

func BenchmarkMaterializer(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}

	type testCase struct {
		name string
		typs []*types.T
	}
	testCases := []testCase{
		{"int", []*types.T{types.Int}},
		{"float", []*types.T{types.Float}},
		{"bytes", []*types.T{types.Bytes}},
		{"int6", []*types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Int}},
		{"string4", []*types.T{types.String, types.String, types.String, types.String}},
		{"multi80", []*types.T{
			types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int,
			types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int,
			types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int,
			types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int,
			types.String, types.String, types.String, types.String, types.String, types.String,
			types.String, types.String, types.String, types.String, types.String, types.String,
			types.String, types.String, types.String, types.String, types.String, types.String,
			types.String, types.String, types.String, types.String, types.String, types.String,
			types.Float, types.Float, types.Float, types.Float, types.Float, types.Float,
			types.Float, types.Float, types.Float, types.Float, types.Float, types.Float,
			types.Float, types.Float, types.Float, types.Float, types.Float, types.Float,
			types.Float, types.Float, types.Float, types.Float, types.Float, types.Float,
		}},
	}

	rng, _ := randutil.NewTestRand()
	nBatches := 10
	for _, tc := range testCases {
		nCols := len(tc.typs)
		for _, hasNulls := range []bool{false, true} {
			for _, useSelectionVector := range []bool{false, true} {
				for _, singleRowBatch := range []bool{false, true} {
					batchSize := coldata.BatchSize()
					batchSizeStr := ""
					if singleRowBatch {
						batchSize = 1
						batchSizeStr = "/batchSize=1"
					}
					nRows := nBatches * batchSize
					b.Run(fmt.Sprintf("%s/hasNulls=%t/useSel=%t%s", tc.name, hasNulls, useSelectionVector, batchSizeStr), func(b *testing.B) {
						nullProb := 0.0
						if hasNulls {
							nullProb = 0.1
						}
						batch := testAllocator.NewMemBatchWithMaxCapacity(tc.typs)
						for _, colVec := range batch.ColVecs() {
							coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
								Rand:             rng,
								Vec:              colVec,
								N:                coldata.BatchSize(),
								NullProbability:  nullProb,
								BytesFixedLength: 8,
							})
						}
						batch.SetLength(batchSize)
						if useSelectionVector {
							batch.SetSelection(true)
							sel := batch.Selection()
							for i := 0; i < batchSize; i++ {
								sel[i] = i
							}
						}
						input := colexectestutils.NewFiniteBatchSource(testAllocator, batch, tc.typs, nBatches)

						b.SetBytes(int64(nRows * nCols * int(memsize.Int64)))
						for i := 0; i < b.N; i++ {
							m := NewMaterializer(
								nil, /* streamingMemAcc */
								flowCtx,
								0, /* processorID */
								colexecargs.OpWithMetaInfo{Root: input},
								tc.typs,
							)
							m.Start(ctx)

							foundRows := 0
							for {
								row, meta := m.Next()
								if meta != nil {
									b.Fatalf("unexpected metadata %v", meta)
								}
								if row == nil {
									break
								}
								foundRows++
							}
							if foundRows != nRows {
								b.Fatalf("expected %d rows, found %d", nRows, foundRows)
							}
							input.Reset(nBatches)
						}
					})
				}
			}
		}
	}
}

func TestMaterializerNextErrorAfterConsumerDone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testError := errors.New("test-induced error")
	metadataSource := &colexectestutils.CallbackMetadataSource{DrainMetaCb: func() []execinfrapb.ProducerMetadata {
		colexecerror.InternalError(testError)
		// Unreachable
		return nil
	}}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}

	m := NewMaterializer(
		nil, /* streamingMemAcc */
		flowCtx,
		0, /* processorID */
		colexecargs.OpWithMetaInfo{
			Root:            &colexecop.CallbackOperator{},
			MetadataSources: colexecop.MetadataSources{metadataSource},
		},
		nil, /* typ */
	)

	m.Start(ctx)
	// Call ConsumerDone.
	m.ConsumerDone()
	// We expect Next to panic since DrainMeta panics are currently not caught by
	// the materializer and it's not clear whether they should be since
	// implementers of DrainMeta do not return errors as panics.
	testutils.IsError(
		colexecerror.CatchVectorizedRuntimeError(func() {
			m.Next()
		}),
		testError.Error(),
	)
}

func BenchmarkColumnarizeMaterialize(b *testing.B) {
	types := []*types.T{types.Int, types.Int}
	nRows := 10000
	nCols := 2
	rows := randgen.MakeIntRows(nRows, nCols)
	input := execinfra.NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}
	c := NewBufferingColumnarizerForTests(testAllocator, flowCtx, 0, input)

	b.SetBytes(int64(nRows * nCols * int(memsize.Int64)))
	for i := 0; i < b.N; i++ {
		m := NewMaterializer(
			nil, /* streamingMemAcc */
			flowCtx,
			1, /* processorID */
			colexecargs.OpWithMetaInfo{Root: c},
			types,
		)
		m.Start(ctx)

		foundRows := 0
		for {
			row, meta := m.Next()
			if meta != nil {
				b.Fatalf("unexpected metadata %v", meta)
			}
			if row == nil {
				break
			}
			foundRows++
		}
		if foundRows != nRows {
			b.Fatalf("expected %d rows, found %d", nRows, foundRows)
		}
		input.Reset()
	}
}
