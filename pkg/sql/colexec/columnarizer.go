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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Columnarizer turns an execinfra.RowSource input into an Operator output, by
// reading the input in chunks of size coldata.BatchSize() and converting each
// chunk into a coldata.Batch column by column.
type Columnarizer struct {
	execinfra.ProcessorBase
	NonExplainable

	allocator  *colmem.Allocator
	input      execinfra.RowSource
	da         rowenc.DatumAlloc
	initStatus OperatorInitStatus

	buffered        rowenc.EncDatumRows
	batch           coldata.Batch
	accumulatedMeta []execinfrapb.ProducerMetadata
	ctx             context.Context
	typs            []*types.T
}

var _ colexecbase.Operator = &Columnarizer{}

// NewColumnarizer returns a new Columnarizer.
func NewColumnarizer(
	ctx context.Context,
	allocator *colmem.Allocator,
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
	c.typs = c.OutputTypes()
	return c, nil
}

// Init is part of the Operator interface.
func (c *Columnarizer) Init() {
	// We don't want to call Start on the input to columnarizer and allocating
	// internal objects several times if Init method is called more than once, so
	// we have this check in place.
	if c.initStatus == OperatorNotInitialized {
		c.accumulatedMeta = make([]execinfrapb.ProducerMetadata, 0, 1)
		c.input.Start(c.ctx)
		c.initStatus = OperatorInitialized
	}
}

// Next is part of the Operator interface.
func (c *Columnarizer) Next(context.Context) coldata.Batch {
	var reallocated bool
	c.batch, reallocated = c.allocator.ResetMaybeReallocate(c.typs, c.batch, 1 /* minCapacity */)
	if reallocated {
		oldRows := c.buffered
		c.buffered = make(rowenc.EncDatumRows, c.batch.Capacity())
		for i := range c.buffered {
			if len(oldRows) > 0 {
				c.buffered[i] = oldRows[0]
				oldRows = oldRows[1:]
			} else {
				c.buffered[i] = make(rowenc.EncDatumRow, len(c.typs))
			}
		}
	}
	// Buffer up n rows.
	nRows := 0
	columnTypes := c.OutputTypes()
	for ; nRows < c.batch.Capacity(); nRows++ {
		row, meta := c.input.Next()
		if meta != nil {
			nRows--
			if meta.Err != nil {
				// If an error occurs, return it immediately.
				colexecerror.ExpectedError(meta.Err)
			}
			c.accumulatedMeta = append(c.accumulatedMeta, *meta)
			continue
		}
		if row == nil {
			break
		}
		// TODO(jordan): evaluate whether it's more efficient to skip the buffer
		// phase.
		copy(c.buffered[nRows], row)
	}

	// Check if we have buffered more rows than the current allocation size
	// and increase it if so.
	if nRows > c.da.AllocSize {
		c.da.AllocSize = nRows
	}

	// Write each column into the output batch.
	for idx, ct := range columnTypes {
		err := EncDatumRowsToColVec(c.allocator, c.buffered[:nRows], c.batch.ColVec(idx), idx, ct, &c.da)
		if err != nil {
			colexecerror.InternalError(err)
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
	colexecerror.InternalError(errors.AssertionFailedf("Columnarizer should not be Run"))
}

var (
	_ colexecbase.Operator       = &Columnarizer{}
	_ execinfrapb.MetadataSource = &Columnarizer{}
	_ colexecbase.Closer         = &Columnarizer{}
)

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

// Close is part of the Operator interface.
func (c *Columnarizer) Close(ctx context.Context) error {
	c.input.ConsumerClosed()
	return nil
}

// ChildCount is part of the Operator interface.
func (c *Columnarizer) ChildCount(verbose bool) int {
	if _, ok := c.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the Operator interface.
func (c *Columnarizer) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := c.input.(execinfra.OpNode); ok {
			return n
		}
		colexecerror.InternalError(errors.AssertionFailedf("input to Columnarizer is not an execinfra.OpNode"))
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Input returns the input of this columnarizer.
func (c *Columnarizer) Input() execinfra.RowSource {
	return c.input
}
