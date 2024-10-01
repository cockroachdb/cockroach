// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package coldatatestutils

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// CopyBatch copies the original batch and returns that copy. However, note that
// the underlying capacity might be different (a new batch is created only with
// capacity original.Length()).
// NOTE: memory accounting is not performed.
func CopyBatch(
	original coldata.Batch, typs []*types.T, factory coldata.ColumnFactory,
) coldata.Batch {
	b := coldata.NewMemBatchWithCapacity(typs, original.Length(), factory)
	b.SetLength(original.Length())
	for colIdx, col := range original.ColVecs() {
		b.ColVec(colIdx).Copy(coldata.SliceArgs{
			Src:       col,
			SrcEndIdx: original.Length(),
		})
	}
	return b
}
