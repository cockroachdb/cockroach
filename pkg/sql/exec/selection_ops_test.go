// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

const (
	selectivity     = .5
	nullProbability = .1
)

func TestSelLTInt64Int64ConstOp(t *testing.T) {
	tups := tuples{{0}, {1}, {2}, {nil}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selLTInt64Int64ConstOp{
			input:    input[0],
			colIdx:   0,
			constArg: 2,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{0}, {1}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelLTInt64Int64(t *testing.T) {
	tups := tuples{
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{nil, 1},
		{-1, nil},
		{nil, nil},
	}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selLTInt64Int64Op{
			input:   input[0],
			col1Idx: 0,
			col2Idx: 1,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0, 1}, tuples{{0, 1}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestGetSelectionConstOperator(t *testing.T) {
	cmpOp := tree.LT
	var input Operator
	colIdx := 3
	constVal := int64(31)
	constArg := tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(constVal))
	op, err := GetSelectionConstOperator(semtypes.Date, cmpOp, input, colIdx, constArg)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt64Int64ConstOp{input: input, colIdx: colIdx, constArg: constVal}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionOperator(t *testing.T) {
	ct := semtypes.Int2
	cmpOp := tree.GE
	var input Operator
	col1Idx := 5
	col2Idx := 7
	op, err := GetSelectionOperator(ct, cmpOp, input, col1Idx, col2Idx)
	if err != nil {
		t.Error(err)
	}
	expected := &selGEInt16Int16Op{input: input, col1Idx: col1Idx, col2Idx: col2Idx}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func benchmarkSelLTInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64})
	col := batch.ColVec(0).Int64()
	for i := int64(0); i < coldata.BatchSize; i++ {
		if float64(i) < coldata.BatchSize*selectivity {
			col[i] = -1
		} else {
			col[i] = 1
		}
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize; i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
		}
	}
	batch.SetLength(coldata.BatchSize)
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}
	source := NewRepeatableBatchSource(batch)
	source.Init()

	plusOp := &selLTInt64Int64ConstOp{
		input:    source,
		colIdx:   0,
		constArg: 0,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelLTInt64Int64ConstOp(b, useSel, hasNulls)
			})
		}
	}
}

func benchmarkSelLTInt64Int64Op(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	for i := int64(0); i < coldata.BatchSize; i++ {
		if float64(i) < coldata.BatchSize*selectivity {
			col1[i], col2[i] = -1, 1
		} else {
			col1[i], col2[i] = 1, -1
		}
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize; i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
			if rand.Float64() < nullProbability {
				batch.ColVec(1).Nulls().SetNull(uint16(i))
			}
		}
	}
	batch.SetLength(coldata.BatchSize)
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}
	source := NewRepeatableBatchSource(batch)
	source.Init()

	plusOp := &selLTInt64Int64Op{
		input:   source,
		col1Idx: 0,
		col2Idx: 1,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64Op(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelLTInt64Int64Op(b, useSel, hasNulls)
			})
		}
	}
}
