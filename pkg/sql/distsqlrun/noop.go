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

	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	distsql.ProcessorBase
	input distsql.RowSource
}

var _ distsql.Processor = &noopProcessor{}
var _ distsql.RowSource = &noopProcessor{}

const noopProcName = "noop"

func newNoopProcessor(
	flowCtx *distsql.FlowCtx,
	processorID int32,
	input distsql.RowSource,
	post *distsqlpb.PostProcessSpec,
	output distsql.RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{input: input}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		distsql.ProcStateOpts{InputsToDrain: []distsql.RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *noopProcessor) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.StartInternal(ctx, noopProcName)
}

// Next is part of the RowSource interface.
func (n *noopProcessor) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	for n.State == distsql.StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		if outRow := n.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, n.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (n *noopProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}
