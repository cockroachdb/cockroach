// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colbase

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colbase/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// CopyBatch copies the original batch and returns that copy. However, note that
// the underlying capacity might be different (a new batch is created only with
// capacity original.Length()).
func CopyBatch(allocator *Allocator, original coldata.Batch, typs []types.T) coldata.Batch {
	b := allocator.NewMemBatchWithSize(typs, original.Length())
	b.SetLength(original.Length())
	allocator.PerformOperation(b.ColVecs(), func() {
		for colIdx, col := range original.ColVecs() {
			b.ColVec(colIdx).Copy(coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:   typeconv.FromColumnType(&typs[colIdx]),
					Src:       col,
					SrcEndIdx: original.Length(),
				},
			})
		}
	})
	return b
}
