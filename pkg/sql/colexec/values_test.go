// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// typeConvFn returns a conversion function if the given datum type can be
// converted to a Go type. If there is no conversion function it returns nil.
func typeConvFn(t *types.T) (fn func(tree.Datum) interface{}) {
	defer func() {
		// GetDatumToPhysicalFn panics if it cannot find a conversion function, but
		// we simply don't use the type in that case, so discard the error.
		if err := recover(); err != nil {
			fn = nil //nolint:returnerrcheck
		}
	}()
	return colconv.GetDatumToPhysicalFn(t)
}

func randTypes(rng *rand.Rand, numCols int) ([]*types.T, []func(tree.Datum) interface{}) {
	colTypes := make([]*types.T, numCols)
	convFns := make([]func(tree.Datum) interface{}, numCols)
	for i := range colTypes {
		for convFns[i] == nil {
			colTypes[i] = randgen.RandType(rng)
			convFns[i] = typeConvFn(colTypes[i])
		}
	}
	return colTypes, convFns
}

func TestValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()
	for _, numRows := range []int{0, 1, 10, 13, 15} {
		for _, numCols := range []int{1, 3} {
			colTypes, convFns := randTypes(rng, numCols)

			expected := make(colexectestutils.Tuples, numRows)
			rows := make(rowenc.EncDatumRows, numRows)
			for i := range expected {
				expected[i] = make(colexectestutils.Tuple, numCols)
				rows[i] = make(rowenc.EncDatumRow, numCols)
				for j, typ := range colTypes {
					val := randgen.RandDatum(rng, typ, true /* nullOk */)
					if val == tree.DNull {
						expected[i][j] = nil
					} else {
						expected[i][j] = convFns[j](val)
					}
					rows[i][j] = rowenc.DatumToEncDatum(typ, val)
				}
			}

			colexectestutils.RunTests(t, testAllocator, nil, expected, colexectestutils.OrderedVerifier,
				func(inputs []colexecop.Operator) (colexecop.Operator, error) {
					spec, err := execinfra.GenerateValuesSpec(colTypes, rows)
					if err != nil {
						return nil, err
					}
					return NewValuesOp(testAllocator, &spec, math.MaxInt64), nil
				})
		}
	}
}

func subBenchmarkValues(
	ctx context.Context,
	b *testing.B,
	numRows int,
	numCols int,
	name string,
	build func(*execinfrapb.ValuesCoreSpec) (colexecop.Operator, error),
) {
	b.Run(fmt.Sprintf("rows=%d,cols=%d,%s", numRows, numCols, name),
		func(b *testing.B) {
			typs := types.MakeIntCols(numCols)
			rows := randgen.MakeIntRows(numRows, numCols)
			spec, err := execinfra.GenerateValuesSpec(typs, rows)
			if err != nil {
				b.Fatal(err)
			}

			b.SetBytes(int64(numRows * numCols * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				op, err := build(&spec)
				if err != nil {
					b.Fatal(err)
				}
				op.Init(ctx)
				for batch := op.Next(); batch.Length() > 0; batch = op.Next() {
				}
			}
		})
}

func BenchmarkValues(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
	}
	post := execinfrapb.PostProcessSpec{}

	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		for _, numCols := range []int{1, 2, 4} {
			// Measure the vectorized values operator.
			subBenchmarkValues(ctx, b, numRows, numCols, "valuesOpNative",
				func(spec *execinfrapb.ValuesCoreSpec) (colexecop.Operator, error) {
					return NewValuesOp(testAllocator, spec, math.MaxInt64), nil
				})

			// For comparison, also measure the row-based values processor wrapped in
			// a columnarizer, which the vectorized values operator replaces.
			subBenchmarkValues(ctx, b, numRows, numCols, "valuesProcWrap",
				func(spec *execinfrapb.ValuesCoreSpec) (colexecop.Operator, error) {
					var core execinfrapb.ProcessorCoreUnion
					core.Values = spec
					proc, err := rowexec.NewProcessor(
						ctx, &flowCtx, 0 /* processorID */, &core, &post, nil /* inputs */, nil, /* localProcessors */
					)
					if err != nil {
						b.Fatal(err)
					}
					return NewBufferingColumnarizerForTests(
						testAllocator, &flowCtx, 0, proc.(execinfra.RowSource),
					), nil
				})
		}
	}
}
