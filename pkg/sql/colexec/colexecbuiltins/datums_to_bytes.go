// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbuiltins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecspan"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type datumsToBytesOp struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	outputIdx int

	// encoders is an ordered list of utility operators that encode each
	// argument column in the vectorized fashion.
	encoders []colexecspan.SpanEncoder
	// encCols stores the encoded vector for each argument column. It
	// corresponds one-to-one with encoders.
	encCols []*coldata.Bytes

	scratch []byte
}

var _ colexecop.ClosableOperator = (*datumsToBytesOp)(nil)

func newDatumsToBytesOp(
	allocator *colmem.Allocator,
	columnTypes []*types.T,
	argumentCols []int,
	outputIdx int,
	input colexecop.Operator,
) colexecop.Operator {
	op := &datumsToBytesOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		outputIdx:      outputIdx,
		encoders:       make([]colexecspan.SpanEncoder, len(argumentCols)),
		encCols:        make([]*coldata.Bytes, len(argumentCols)),
	}
	// Create a span encoder for each argument column. All encodings use
	// ascending direction since we're just concatenating bytes.
	for i, argumentCol := range argumentCols {
		op.encoders[i] = colexecspan.NewSpanEncoder(
			allocator, columnTypes[argumentCol], true /* asc */, argumentCol,
		)
	}
	return op
}

func (d *datumsToBytesOp) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	batch, meta := d.Input.Next()
	if meta != nil {
		return nil, meta
	}
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch, nil
	}

	for i, encoder := range d.encoders {
		d.encCols[i] = encoder.Next(batch, 0 /* startIdx */, n)
	}

	inSel := batch.Selection()
	outVec := batch.ColVec(d.outputIdx)
	outBytes := outVec.Bytes()
	d.allocator.PerformOperation([]*coldata.Vec{outVec}, func() {
		for i := 0; i < batch.Length(); i++ {
			d.scratch = d.scratch[:0]
			for _, enc := range d.encCols {
				// Note that because SpanEncoder.Next performs the deselection
				// step, we always use 'i' and ignore the selection vector - if
				// set - on the batch.
				d.scratch = append(d.scratch, enc.Get(i)...)
			}
			outIdx := i
			if inSel != nil {
				outIdx = inSel[i]
			}
			outBytes.Set(outIdx, d.scratch)
		}
	})
	return batch, nil
}

func (d *datumsToBytesOp) Close(context.Context) error {
	for _, encoder := range d.encoders {
		encoder.Close()
	}
	return nil
}
