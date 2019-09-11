// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execplan

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Columnarizer turns a RowSource input into an exec.Operator output, by
// reading the input in chunks of size coldata.BatchSize and converting each
// chunk into a coldata.Batch column by column.
type Columnarizer struct {
	distsql.ProcessorBase
	colexec.NonExplainable

	input distsql.RowSource
	da    sqlbase.DatumAlloc

	buffered        sqlbase.EncDatumRows
	batch           coldata.Batch
	accumulatedMeta []distsqlpb.ProducerMetadata
	ctx             context.Context
	typs            []coltypes.T
}

var _ colexec.StaticMemoryOperator = &Columnarizer{}

// NewColumnarizer returns a new Columnarizer.
func NewColumnarizer(
	ctx context.Context, flowCtx *distsql.FlowCtx, processorID int32, input distsql.RowSource,
) (*Columnarizer, error) {
	var err error
	c := &Columnarizer{
		input: input,
		ctx:   ctx,
	}
	if err = c.ProcessorBase.Init(
		nil,
		&distsqlpb.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* output */
		nil, /* memMonitor */
		distsql.ProcStateOpts{InputsToDrain: []distsql.RowSource{input}},
	); err != nil {
		return nil, err
	}
	c.typs, err = typeconv.FromColumnTypes(c.OutputTypes())

	return c, err
}

// EstimateStaticMemoryUsage is part of the exec.StaticMemoryOperator
// interface.
func (c *Columnarizer) EstimateStaticMemoryUsage() int {
	return colexec.EstimateBatchSizeBytes(c.typs, coldata.BatchSize)
}

// Init is part of the exec.Operator interface.
func (c *Columnarizer) Init() {
	c.batch = coldata.NewMemBatch(c.typs)
	c.buffered = make(sqlbase.EncDatumRows, coldata.BatchSize)
	for i := range c.buffered {
		c.buffered[i] = make(sqlbase.EncDatumRow, len(c.typs))
	}
	c.accumulatedMeta = make([]distsqlpb.ProducerMetadata, 0, 1)
	c.input.Start(c.ctx)
}

// Next is part of the exec.Operator interface.
func (c *Columnarizer) Next(context.Context) coldata.Batch {
	c.batch.ResetInternalBatch()
	// Buffer up n rows.
	nRows := uint16(0)
	columnTypes := c.OutputTypes()
	for ; nRows < coldata.BatchSize; nRows++ {
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
	c.batch.SetLength(nRows)

	// Write each column into the output batch.
	for idx, ct := range columnTypes {
		err := colexec.EncDatumRowsToColVec(c.buffered[:nRows], c.batch.ColVec(idx), idx, &ct, &c.da)
		if err != nil {
			panic(err)
		}
	}
	return c.batch
}

// Run is part of the Processor interface.
//
// columnarizers are not expected to be Run, so we prohibit calling this method
// on them.
func (c *Columnarizer) Run(context.Context) {
	panic("Columnarizer should not be Run")
}

var _ colexec.Operator = &Columnarizer{}
var _ distsqlpb.MetadataSource = &Columnarizer{}

// DrainMeta is part of the MetadataSource interface.
func (c *Columnarizer) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	if src, ok := c.input.(distsqlpb.MetadataSource); ok {
		c.accumulatedMeta = append(c.accumulatedMeta, src.DrainMeta(ctx)...)
	}
	return c.accumulatedMeta
}

// ChildCount is part of the exec.Operator interface.
func (c *Columnarizer) ChildCount() int {
	if _, ok := c.input.(colexec.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the exec.Operator interface.
func (c *Columnarizer) Child(nth int) colexec.OpNode {
	if nth == 0 {
		if n, ok := c.input.(colexec.OpNode); ok {
			return n
		}
		panic("input to Columnarizer is not an exec.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}
