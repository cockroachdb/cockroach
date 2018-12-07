// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// columnarizer turns a RowSource input into an exec.Operator output, by reading
// the input in chunks of size exec.ColBatchSize and converting each chunk into
// an exec.ColBatch column by column.
type columnarizer struct {
	ProcessorBase

	input RowSource
	da    sqlbase.DatumAlloc

	buffered sqlbase.EncDatumRows
	batch    exec.ColBatch
}

// newColumnarizer returns a new columnarizer
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
		nil,
		nil, /* memMonitor */
		ProcStateOpts{InputsToDrain: []RowSource{input}},
	); err != nil {
		return nil, err
	}
	c.Init()
	return c, nil
}

func (c *columnarizer) Init() {
	typs := types.FromColumnTypes(c.OutputTypes())
	c.batch = exec.NewMemBatch(typs)
	c.buffered = make(sqlbase.EncDatumRows, exec.ColBatchSize)
	for i := range c.buffered {
		c.buffered[i] = make(sqlbase.EncDatumRow, len(typs))
	}
}

func (c *columnarizer) Next() exec.ColBatch {
	// Buffer up n rows.
	nRows := uint16(0)
	columnTypes := c.OutputTypes()
	for ; nRows < exec.ColBatchSize; nRows++ {
		row, meta := c.input.Next()
		if meta != nil {
			panic("TODO(jordan): columnarizer needs to forward metadata.")
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

var _ exec.Operator = &columnarizer{}
