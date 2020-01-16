// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
)

func newBufferedBatch(allocator *Allocator, types []coltypes.T, initialSize int) *bufferedBatch {
	b := &bufferedBatch{
		colVecs: make([]coldata.Vec, len(types)),
	}
	for i, t := range types {
		b.colVecs[i] = allocator.NewMemColumn(t, initialSize)
	}
	return b
}

// bufferedBatch is a custom implementation of coldata.Batch interface (only
// a subset of methods is implemented) which stores the length as uint64. It
// should be used whenever we're doing possibly unbounded buffering of tuples.
// WARNING: do *not* use Length() method on bufferedBatch to get the number of
// buffered tuples because it might return incorrect result. Instead, use
// 'length' field directly.
type bufferedBatch struct {
	colVecs []coldata.Vec
	length  uint64
}

var _ coldata.Batch = &bufferedBatch{}

// Length method should *only* be used to check whether the batch has a zero
// length.
func (b *bufferedBatch) Length() uint16 {
	if b.length == 0 {
		return 0
	}
	return 1
}

func (b *bufferedBatch) SetLength(uint16) {
	execerror.VectorizedInternalPanic("SetLength(uint16) should not be called on bufferedBatch;" +
		"instead, length field should be accessed directly")
}

func (b *bufferedBatch) Width() int {
	return len(b.colVecs)
}

func (b *bufferedBatch) ColVec(i int) coldata.Vec {
	return b.colVecs[i]
}

func (b *bufferedBatch) ColVecs() []coldata.Vec {
	return b.colVecs
}

// Selection is not implemented because the tuples should only be appended to
// bufferedBatch, and Append does the deselection step.
func (b *bufferedBatch) Selection() []uint16 {
	return nil
}

// SetSelection is not implemented because the tuples should only be appended
// to bufferedBatch, and Append does the deselection step.
func (b *bufferedBatch) SetSelection(bool) {
	execerror.VectorizedInternalPanic("SetSelection(bool) should not be called on bufferedBatch")
}

// AppendCol is not implemented because bufferedBatch is only initialized
// when the column schema is known.
func (b *bufferedBatch) AppendCol(coldata.Vec) {
	execerror.VectorizedInternalPanic("AppendCol(coldata.Vec) should not be called on bufferedBatch")
}

// ReplaceCol is not implemented because bufferedBatch is only initialized
// when the column schema is known.
func (b *bufferedBatch) ReplaceCol(coldata.Vec, int) {
	execerror.VectorizedInternalPanic("ReplaceCol(coldata.Vec, int) should not be called on bufferedBatch")
}

// Reset is not implemented because bufferedBatch is not reused with
// different column schemas at the moment.
func (b *bufferedBatch) Reset(types []coltypes.T, length int) {
	execerror.VectorizedInternalPanic("Reset([]coltypes.T, int) should not be called on bufferedBatch")
}

// ResetInternalBatch is not implemented because bufferedBatch is not meant
// to be used by any operator other than its "owner", and the owner should be
// using reset() instead.
func (b *bufferedBatch) ResetInternalBatch() {
	execerror.VectorizedInternalPanic("ResetInternalBatch() should not be called on bufferedBatch")
}

// reset resets the state of the buffered group so that we can reuse the
// underlying memory. Note that the underlying memory is not released, so there
// is no need to update memory account while performing this operation.
func (b *bufferedBatch) reset() {
	b.length = 0
	for _, colVec := range b.colVecs {
		// We do not need to reset the column vectors because those will be just
		// written over, but we do need to reset the nulls.
		colVec.Nulls().UnsetNulls()
		if colVec.Type() == coltypes.Bytes {
			// Bytes type is the only exception to the comment above.
			colVec.Bytes().Reset()
		}
	}
}
