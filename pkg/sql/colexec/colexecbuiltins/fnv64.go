// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbuiltins

import (
	"hash"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

type fnv64Op struct {
	colexecop.OneInputHelper
	argumentCols []int
	outputIdx    int

	inBytes []*coldata.Bytes
	inNulls []*coldata.Nulls
	hash    hash.Hash64
}

var _ colexecop.Operator = (*fnv64Op)(nil)

func newFNV64Op(
	argumentCols []int, outputIdx int, input colexecop.Operator, hash hash.Hash64,
) colexecop.Operator {
	return &fnv64Op{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		argumentCols:   argumentCols,
		outputIdx:      outputIdx,
		inBytes:        make([]*coldata.Bytes, len(argumentCols)),
		inNulls:        make([]*coldata.Nulls, len(argumentCols)),
		hash:           hash,
	}
}

func (f *fnv64Op) processRow(rowIdx int, outInt64 coldata.Int64s, outNulls *coldata.Nulls) {
	f.hash.Reset()
	var seenNonNull bool
	for j := 0; j < len(f.inBytes); j++ {
		if f.inNulls[j].MaybeHasNulls() && f.inNulls[j].NullAt(rowIdx) {
			// Skip all NULL keys.
			continue
		}
		seenNonNull = true
		_, _ = f.hash.Write(f.inBytes[j].Get(rowIdx)) // never returns an error, see https://pkg.go.dev/hash#Hash
	}
	if seenNonNull {
		outInt64.Set(rowIdx, int64(f.hash.Sum64()))
	} else {
		outNulls.SetNull(rowIdx)
	}
}

func (f *fnv64Op) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	batch, meta := f.Input.Next()
	if meta != nil {
		return nil, meta
	}
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch, nil
	}
	inSel := batch.Selection()
	for i, ordinal := range f.argumentCols {
		inVec := batch.ColVec(ordinal)
		f.inBytes[i] = inVec.Bytes()
		f.inNulls[i] = inVec.Nulls()
	}
	// Don't keep the references to these objects for longer than necessary.
	defer func() {
		for i := range f.inBytes {
			f.inBytes[i] = nil
			f.inNulls[i] = nil
		}
	}()

	outVec := batch.ColVec(f.outputIdx)
	outInt64 := outVec.Int64()
	outNulls := outVec.Nulls()
	// Note that this 'if' performs Sets on int64s which never change the memory
	// footprint, which is why it's not wrapped in
	// colmem.Allocator.PerformOperation (that would be redundant).
	if inSel == nil {
		for i := 0; i < batch.Length(); i++ {
			f.processRow(i, outInt64, outNulls)
		}
	} else {
		for _, idx := range inSel[:batch.Length()] {
			f.processRow(idx, outInt64, outNulls)
		}
	}
	return batch, nil
}
