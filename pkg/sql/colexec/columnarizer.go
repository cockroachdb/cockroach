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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Columnarizer turns an execinfra.RowSource input into an Operator output, by
// reading the input in chunks of size coldata.BatchSize() and converting each
// chunk into a coldata.Batch column by column.
type Columnarizer struct {
	execinfra.ProcessorBase
	NonExplainable

	allocator *Allocator
	input     execinfra.RowSource
	da        sqlbase.DatumAlloc

	buffered        sqlbase.EncDatumRows
	batch           coldata.Batch
	accumulatedMeta []execinfrapb.ProducerMetadata
	ctx             context.Context
	typs            []coltypes.T
}

var _ StaticMemoryOperator = &Columnarizer{}

// NewColumnarizer returns a new Columnarizer.
func NewColumnarizer(
	ctx context.Context,
	allocator *Allocator,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
) (*Columnarizer, error) {
	var err error
	c := &Columnarizer{
		allocator: allocator,
		input:     input,
		ctx:       ctx,
	}
	if err = c.ProcessorBase.Init(
		nil,
		&execinfrapb.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* output */
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{input}},
	); err != nil {
		return nil, err
	}
	c.typs, err = typeconv.FromColumnTypes(c.OutputTypes())

	return c, err
}

// EstimateStaticMemoryUsage is part of the StaticMemoryOperator
// interface.
func (c *Columnarizer) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes(c.typs, int(coldata.BatchSize()))
}

// Init is part of the Operator interface.
func (c *Columnarizer) Init() {
	c.batch = c.allocator.NewMemBatch(c.typs)
	c.buffered = make(sqlbase.EncDatumRows, coldata.BatchSize())
	for i := range c.buffered {
		c.buffered[i] = make(sqlbase.EncDatumRow, len(c.typs))
	}
	c.accumulatedMeta = make([]execinfrapb.ProducerMetadata, 0, 1)
	c.input.Start(c.ctx)
}

// Next is part of the Operator interface.
func (c *Columnarizer) Next(context.Context) coldata.Batch {
	c.batch.ResetInternalBatch()
	// Buffer up n rows.
	nRows := uint16(0)
	columnTypes := c.OutputTypes()
	for ; nRows < coldata.BatchSize(); nRows++ {
		row, meta := c.input.Next()
		if meta != nil {
			c.accumulatedMeta = append(c.accumulatedMeta, *meta)
			nRows--
			continue
		}
		if row == nil {
			break
		}
		// TODO(jordan): evaluate whether it's more efficient to skip the buffer
		// phase.
		copy(c.buffered[nRows], row)
	}

	// Write each column into the output batch.
	for idx, ct := range columnTypes {
		err := EncDatumRowsToColVec(c.buffered[:nRows], c.batch.ColVec(idx), idx, &ct, &c.da)
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}
	}
	c.batch.SetLength(nRows)
	return c.batch
}

// Run is part of the execinfra.Processor interface.
//
// Columnarizers are not expected to be Run, so we prohibit calling this method
// on them.
func (c *Columnarizer) Run(context.Context) {
	execerror.VectorizedInternalPanic("Columnarizer should not be Run")
}

var _ Operator = &Columnarizer{}
var _ execinfrapb.MetadataSource = &Columnarizer{}

// DrainMeta is part of the MetadataSource interface.
func (c *Columnarizer) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	c.MoveToDraining(nil /* err */)
	for {
		meta := c.DrainHelper()
		if meta == nil {
			break
		}
		c.accumulatedMeta = append(c.accumulatedMeta, *meta)
	}
	return c.accumulatedMeta
}

// ChildCount is part of the Operator interface.
func (c *Columnarizer) ChildCount() int {
	if _, ok := c.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the Operator interface.
func (c *Columnarizer) Child(nth int) execinfra.OpNode {
	if nth == 0 {
		if n, ok := c.input.(execinfra.OpNode); ok {
			return n
		}
		execerror.VectorizedInternalPanic("input to Columnarizer is not an execinfra.OpNode")
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
