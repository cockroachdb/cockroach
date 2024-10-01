// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecutils

import (
	"context"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Deserializer is a helper struct for deserializing columnar batches from the
// Apache Arrow format.
type Deserializer struct {
	allocator        *colmem.Allocator
	typs             []*types.T
	alwaysReallocate bool

	batch     coldata.Batch
	data      []array.Data
	converter *colserde.ArrowBatchConverter
	deser     *colserde.RecordBatchSerializer
}

// Init initializes the deserializer.
// - alwaysReallocate, if true, indicates that a new coldata.Batch must be
// allocated on each Deserialize call.
func (d *Deserializer) Init(
	allocator *colmem.Allocator, typs []*types.T, alwaysReallocate bool,
) error {
	d.allocator = allocator
	d.typs = typs
	d.alwaysReallocate = alwaysReallocate
	d.data = make([]array.Data, len(typs))
	var err error
	d.deser, err = colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		return err
	}
	d.converter, err = colserde.NewArrowBatchConverter(typs, colserde.ArrowToBatchOnly, nil /* acc */)
	return err
}

// Deserialize deserializes a single coldata.Batch from the Apache Arrow format.
// The memory footprint of the returned batch is registered with the allocator
// provided in Init. If alwaysReallocate=true was passed in Init, then this
// method does **not** invalidate previously returned batches, and all the
// batches are still tracked by the allocator.
// NOTE: this method propagates errors through the panic-catch mechanism, so it
// must be wrapped with colexecerror.CatchVectorizedRuntimeError.
func (d *Deserializer) Deserialize(serializedBatch []byte) coldata.Batch {
	d.data = d.data[:0]
	batchLength, err := d.deser.Deserialize(&d.data, serializedBatch)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if d.alwaysReallocate {
		d.batch = nil
	}
	d.batch, _ = d.allocator.ResetMaybeReallocateNoMemLimit(d.typs, d.batch, batchLength)
	d.allocator.PerformOperation(d.batch.ColVecs(), func() {
		if err = d.converter.ArrowToBatch(d.data, batchLength, d.batch); err != nil {
			colexecerror.InternalError(err)
		}
	})
	return d.batch
}

// Close closes the Deserializer. Safe to be called even if Init wasn't called.
func (d *Deserializer) Close(ctx context.Context) {
	if d.converter != nil {
		d.converter.Close(ctx)
	}
	*d = Deserializer{}
}
