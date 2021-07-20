// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexectestutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestOpTestInputOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	inputs := []Tuples{
		{
			{1, 2, 100},
			{1, 3, -3},
			{0, 4, 5},
			{1, 5, 0},
		},
	}
	RunTestsWithFn(t, testAllocator, inputs, nil, func(t *testing.T, sources []colexecop.Operator) {
		out := NewOpTestOutput(sources[0], inputs[0])

		if err := out.Verify(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRepeatableBatchSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	batchLen := 10
	if coldata.BatchSize() < batchLen {
		batchLen = coldata.BatchSize()
	}
	batch.SetLength(batchLen)
	input := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	input.Init(context.Background())

	b := input.Next()
	b.SetLength(0)
	b.SetSelection(true)

	b = input.Next()
	if b.Length() != batchLen {
		t.Fatalf("expected RepeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() != nil {
		t.Fatalf("expected RepeatableBatchSource to reset selection vector, found %+v", b.Selection())
	}
}

func TestRepeatableBatchSourceWithFixedSel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	rng, _ := randutil.NewPseudoRand()
	batchSize := 10
	if batchSize > coldata.BatchSize() {
		batchSize = coldata.BatchSize()
	}
	sel := coldatatestutils.RandomSel(rng, batchSize, 0 /* probOfOmitting */)
	batchLen := len(sel)
	batch.SetLength(batchLen)
	batch.SetSelection(true)
	copy(batch.Selection(), sel)
	input := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	input.Init(context.Background())
	b := input.Next()

	b.SetLength(0)
	b.SetSelection(false)
	b = input.Next()
	if b.Length() != batchLen {
		t.Fatalf("expected RepeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() == nil {
		t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
	} else {
		for i := 0; i < batchLen; i++ {
			if b.Selection()[i] != sel[i] {
				t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
			}
		}
	}

	newSel := coldatatestutils.RandomSel(rng, 10 /* batchSize */, 0.2 /* probOfOmitting */)
	newBatchLen := len(sel)
	b.SetLength(newBatchLen)
	b.SetSelection(true)
	copy(b.Selection(), newSel)
	b = input.Next()
	if b.Length() != batchLen {
		t.Fatalf("expected RepeatableBatchSource to reset batch length to %d, found %d", batchLen, b.Length())
	}
	if b.Selection() == nil {
		t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
	} else {
		for i := 0; i < batchLen; i++ {
			if b.Selection()[i] != sel[i] {
				t.Fatalf("expected RepeatableBatchSource to reset selection vector, expected %v but found %+v", sel, b.Selection())
			}
		}
	}
}
