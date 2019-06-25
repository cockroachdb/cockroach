// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// columnarizer turns a RowSource input into an exec.Operator output, by
// reading the input in chunks of size coldata.BatchSize and converting each
// chunk into a coldata.Batch column by column.
type columnarizer struct {
	ProcessorBase

	input RowSource
	da    sqlbase.DatumAlloc

	buffered        sqlbase.EncDatumRows
	batch           coldata.Batch
	accumulatedMeta []distsqlpb.ProducerMetadata
}

var _ exec.Operator = &columnarizer{}

// newColumnarizer returns a new columnarizer.
func newColumnarizer(flowCtx *FlowCtx, processorID int32, input RowSource) (*columnarizer, error) {
	c := &columnarizer{
		input: input,
	}
	if err := c.ProcessorBase.Init(
		nil,
		&distsqlpb.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* output */
		nil, /* memMonitor */
		ProcStateOpts{InputsToDrain: []RowSource{input}},
	); err != nil {
		return nil, err
	}
	c.Init()
	return c, nil
}

func (c *columnarizer) Init() {
	typs := conv.FromColumnTypes(c.OutputTypes())
	c.batch = coldata.NewMemBatch(typs)
	c.buffered = make(sqlbase.EncDatumRows, coldata.BatchSize)
	for i := range c.buffered {
		c.buffered[i] = make(sqlbase.EncDatumRow, len(typs))
	}
	c.accumulatedMeta = make([]distsqlpb.ProducerMetadata, 0, 1)
	c.input.Start(context.TODO())
}

func (c *columnarizer) Next(context.Context) coldata.Batch {
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
		err := exec.EncDatumRowsToColVec(c.buffered[:nRows], c.batch.ColVec(idx), idx, &ct, &c.da)
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
func (c *columnarizer) Run(context.Context) {
	panic("columnarizer should not be Run")
}

var _ exec.Operator = &columnarizer{}
var _ distsqlpb.MetadataSource = &columnarizer{}

// DrainMeta is part of the MetadataSource interface.
func (c *columnarizer) DrainMeta(ctx context.Context) []distsqlpb.ProducerMetadata {
	if src, ok := c.input.(distsqlpb.MetadataSource); ok {
		c.accumulatedMeta = append(c.accumulatedMeta, src.DrainMeta(ctx)...)
	}
	return c.accumulatedMeta
}
