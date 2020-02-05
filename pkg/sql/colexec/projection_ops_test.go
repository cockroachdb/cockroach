// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

func TestProjPlusInt64Int64ConstOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTests(t, []tuples{{{1}, {2}, {nil}}}, tuples{{1, 2}, {2, 3}, {nil, nil}}, orderedVerifier,
		func(input []Operator) (Operator, error) {
			return &projPlusInt64Int64ConstOp{
				projConstOpBase: projConstOpBase{
					OneInputNode: NewOneInputNode(input[0]),
					allocator:    testAllocator,
					colIdx:       0,
					outputIdx:    1,
				},
				constArg: 1,
			}, nil
		})
}

func TestProjPlusInt64Int64Op(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTests(t, []tuples{{{1, 2}, {3, 4}, {5, nil}}}, tuples{{1, 2, 3}, {3, 4, 7}, {5, nil, nil}},
		orderedVerifier,
		func(input []Operator) (Operator, error) {
			return &projPlusInt64Int64Op{
				projOpBase: projOpBase{
					OneInputNode: NewOneInputNode(input[0]),
					allocator:    testAllocator,
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				},
			}, nil
		})
}

func TestProjDivFloat64Float64Op(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runTests(t, []tuples{{{1.0, 2.0}, {3.0, 4.0}, {5.0, nil}}}, tuples{{1.0, 2.0, 0.5}, {3.0, 4.0, 0.75}, {5.0, nil, nil}},
		orderedVerifier,
		func(input []Operator) (Operator, error) {
			return &projDivFloat64Float64Op{
				projOpBase: projOpBase{
					OneInputNode: NewOneInputNode(input[0]),
					allocator:    testAllocator,
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				},
			}, nil
		})
}

func benchmarkProjPlusInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64})
	col := batch.ColVec(0).Int64()
	for i := 0; i < int(coldata.BatchSize()); i++ {
		col[i] = 1
	}
	if hasNulls {
		for i := 0; i < int(coldata.BatchSize()); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
		}
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < int(coldata.BatchSize()); i++ {
			sel[i] = uint16(i)
		}
	}
	source := NewRepeatableBatchSource(testAllocator, batch)
	source.Init()

	plusOp := &projPlusInt64Int64ConstOp{
		projConstOpBase: projConstOpBase{
			OneInputNode: NewOneInputNode(source),
			allocator:    testAllocator,
			colIdx:       0,
			outputIdx:    1,
		},
		constArg: 1,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkProjPlusInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkProjPlusInt64Int64ConstOp(b, useSel, hasNulls)
			})
		}
	}
}

func TestGetProjectionConstOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	binOp := tree.Mult
	var input Operator
	colIdx := 3
	constVal := float64(31.37)
	constArg := tree.NewDFloat(tree.DFloat(constVal))
	outputIdx := 5
	op, err := GetProjectionRConstOperator(testAllocator, types.Float, types.Float, binOp, input, colIdx, constArg, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultFloat64Float64ConstOp{
		projConstOpBase: projConstOpBase{
			OneInputNode: NewOneInputNode(input),
			allocator:    testAllocator,
			colIdx:       colIdx,
			outputIdx:    outputIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetProjectionConstMixedTypeOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	binOp := tree.GE
	var input Operator
	colIdx := 3
	constVal := int16(31)
	constArg := tree.NewDInt(tree.DInt(constVal))
	outputIdx := 5
	op, err := GetProjectionRConstOperator(testAllocator, types.Int, types.Int2, binOp, input, colIdx, constArg, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projGEInt64Int16ConstOp{
		projConstOpBase: projConstOpBase{
			OneInputNode: NewOneInputNode(input),
			allocator:    testAllocator,
			colIdx:       colIdx,
			outputIdx:    outputIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

// TestRandomComparisons runs binary comparisons against all scalar types
// (supported by the vectorized engine) with random non-null data verifying
// that the result of Datum.Compare matches the result of the exec projection.
func TestRandomComparisons(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numTuples = 2048
	rng, _ := randutil.NewPseudoRand()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	ctx := evalCtx.Ctx()
	defer evalCtx.Stop(ctx)

	expected := make([]bool, numTuples)
	var da sqlbase.DatumAlloc
	lDatums := make([]tree.Datum, numTuples)
	rDatums := make([]tree.Datum, numTuples)
	for _, ct := range types.Scalar {
		if ct.Family() == types.DateFamily {
			// TODO(jordan): #40354 tracks failure to compare infinite dates.
			continue
		}
		typ := typeconv.FromColumnType(ct)
		if typ == coltypes.Unhandled {
			continue
		}
		typs := []coltypes.T{typ, typ, coltypes.Bool}
		bytesFixedLength := 0
		if ct.Family() == types.UuidFamily {
			bytesFixedLength = 16
		}
		b := testAllocator.NewMemBatchWithSize(typs, numTuples)
		lVec := b.ColVec(0)
		rVec := b.ColVec(1)
		ret := b.ColVec(2)
		coldata.RandomVec(rng, typ, bytesFixedLength, lVec, numTuples, 0)
		coldata.RandomVec(rng, typ, bytesFixedLength, rVec, numTuples, 0)
		for i := range lDatums {
			lDatums[i] = PhysicalTypeColElemToDatum(lVec, uint16(i), da, ct)
			rDatums[i] = PhysicalTypeColElemToDatum(rVec, uint16(i), da, ct)
		}
		for _, cmpOp := range []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE} {
			for i := range lDatums {
				cmp := lDatums[i].Compare(evalCtx, rDatums[i])
				var b bool
				switch cmpOp {
				case tree.EQ:
					b = cmp == 0
				case tree.NE:
					b = cmp != 0
				case tree.LT:
					b = cmp < 0
				case tree.LE:
					b = cmp <= 0
				case tree.GT:
					b = cmp > 0
				case tree.GE:
					b = cmp >= 0
				}
				expected[i] = b
			}
			input := newChunkingBatchSource(typs, []coldata.Vec{lVec, rVec, ret}, numTuples)
			op, err := GetProjectionOperator(testAllocator, ct, ct, cmpOp, input, 0, 1, 2)
			if err != nil {
				t.Fatal(err)
			}
			op.Init()
			var idx uint16
			for batch := op.Next(ctx); batch.Length() > 0; batch = op.Next(ctx) {
				for i := uint16(0); i < batch.Length(); i++ {
					absIdx := idx + i
					assert.Equal(t, expected[absIdx], batch.ColVec(2).Bool()[i],
						"expected %s %s %s (%s[%d]) to be %t found %t", lDatums[absIdx], cmpOp, rDatums[absIdx], ct, absIdx,
						expected[absIdx], ret.Bool()[i])
				}
				idx += batch.Length()
			}
		}
	}
}

func TestGetProjectionOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ct := types.Int2
	binOp := tree.Mult
	var input Operator
	col1Idx := 5
	col2Idx := 7
	outputIdx := 9
	op, err := GetProjectionOperator(testAllocator, ct, ct, binOp, input, col1Idx, col2Idx, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultInt16Int16Op{
		projOpBase: projOpBase{
			OneInputNode: NewOneInputNode(input),
			allocator:    testAllocator,
			col1Idx:      col1Idx,
			col2Idx:      col2Idx,
			outputIdx:    outputIdx,
		},
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func benchmarkProjOp(
	b *testing.B,
	makeProjOp func(source *RepeatableBatchSource, intType coltypes.T) Operator,
	useSelectionVector bool,
	hasNulls bool,
	intType coltypes.T,
	outputType coltypes.T,
) {
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{intType, intType, outputType})
	switch intType {
	case coltypes.Int64:
		col1 := batch.ColVec(0).Int64()
		col2 := batch.ColVec(1).Int64()
		for i := 0; i < int(coldata.BatchSize()); i++ {
			col1[i] = 1
			col2[i] = 1
		}
	case coltypes.Int32:
		col1 := batch.ColVec(0).Int32()
		col2 := batch.ColVec(1).Int32()
		for i := 0; i < int(coldata.BatchSize()); i++ {
			col1[i] = 1
			col2[i] = 1
		}
	default:
		b.Fatalf("unsupported type: %s", intType)
	}
	if hasNulls {
		for i := 0; i < int(coldata.BatchSize()); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
			if rand.Float64() < nullProbability {
				batch.ColVec(1).Nulls().SetNull(uint16(i))
			}
		}
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := uint16(0); i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}
	source := NewRepeatableBatchSource(testAllocator, batch)
	source.Init()

	op := makeProjOp(source, intType)
	op.Init()

	b.SetBytes(int64(8 * coldata.BatchSize() * 2))
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}

func BenchmarkProjOp(b *testing.B) {
	projOpMap := map[string]func(*RepeatableBatchSource, coltypes.T) Operator{
		"projPlusIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			base := projOpBase{
				OneInputNode: NewOneInputNode(source),
				allocator:    testAllocator,
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
			switch intType {
			case coltypes.Int64:
				return &projPlusInt64Int64Op{
					projOpBase: base,
				}
			case coltypes.Int32:
				return &projPlusInt32Int32Op{
					projOpBase: base,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
		"projMinusIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			base := projOpBase{
				OneInputNode: NewOneInputNode(source),
				allocator:    testAllocator,
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
			switch intType {
			case coltypes.Int64:
				return &projMinusInt64Int64Op{
					projOpBase: base,
				}
			case coltypes.Int32:
				return &projMinusInt32Int32Op{
					projOpBase: base,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
		"projMultIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			base := projOpBase{
				OneInputNode: NewOneInputNode(source),
				allocator:    testAllocator,
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
			switch intType {
			case coltypes.Int64:
				return &projMultInt64Int64Op{
					projOpBase: base,
				}
			case coltypes.Int32:
				return &projMultInt32Int32Op{
					projOpBase: base,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
		"projDivIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			base := projOpBase{
				OneInputNode: NewOneInputNode(source),
				allocator:    testAllocator,
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
			switch intType {
			case coltypes.Int64:
				return &projDivInt64Int64Op{
					projOpBase: base,
				}
			case coltypes.Int32:
				return &projDivInt32Int32Op{
					projOpBase: base,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
	}

	for projOp, makeProjOp := range projOpMap {
		for _, intType := range []coltypes.T{coltypes.Int64, coltypes.Int32} {
			for _, useSel := range []bool{true, false} {
				for _, hasNulls := range []bool{true, false} {
					b.Run(fmt.Sprintf("op=%s/type=%s/useSel=%t/hasNulls=%t",
						projOp, intType, useSel, hasNulls), func(b *testing.B) {
						outputType := intType
						if projOp == "projDivIntIntOp" {
							outputType = coltypes.Decimal
						}
						benchmarkProjOp(b, makeProjOp, useSel, hasNulls, intType, outputType)
					})
				}
			}
		}
	}
}
