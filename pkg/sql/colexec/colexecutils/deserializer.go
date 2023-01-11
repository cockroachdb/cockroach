// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// TODO: comments. Use it in the inbox and the direct scan.

type Deserializer struct {
	allocator            *colmem.Allocator
	typs                 []*types.T
	allocateFreshBatches bool

	batch     coldata.Batch
	data      []array.Data
	converter *colserde.ArrowBatchConverter
	deser     *colserde.RecordBatchSerializer
}

func (d *Deserializer) Init(
	allocator *colmem.Allocator, typs []*types.T, allocateFreshBatches bool,
) {
	d.allocator = allocator
	d.typs = typs
	d.allocateFreshBatches = allocateFreshBatches
	d.data = make([]array.Data, len(typs))
	var err error
	d.deser, err = colserde.NewRecordBatchSerializer(typs)
	if err != nil {
		colexecerror.InternalError(err)
	}
	d.converter, err = colserde.NewArrowBatchConverter(typs, colserde.ArrowToBatchOnly, nil /* acc */)
	if err != nil {
		colexecerror.InternalError(err)
	}
}

func (d *Deserializer) Deserialize(serializedBatch []byte) coldata.Batch {
	d.data = d.data[:0]
	batchLength, err := d.deser.Deserialize(&d.data, serializedBatch)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if d.allocateFreshBatches {
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
