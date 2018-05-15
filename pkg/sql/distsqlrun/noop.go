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
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the post-processing stage. It can be useful for its
// post-processing or in the last stage of a computation, where we may only
// need the synchronizer to join streams.
type noopProcessor struct {
	processorBase
	input RowSource
}

var _ Processor = &noopProcessor{}
var _ RowSource = &noopProcessor{}

const noopProcName = "noop"

func newNoopProcessor(
	flowCtx *FlowCtx, processorID int32, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{input: input}
	if err := n.init(
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		procStateOpts{inputsToDrain: []RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *noopProcessor) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.startInternal(ctx, noopProcName)
}

// Run is part of the Processor interface.
func (n *noopProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if n.out.output == nil {
		panic("noopProcessor output not initialized for emitting rows")
	}
	ctx = n.Start(ctx)
	Run(ctx, n, n.out.output)
	if wg != nil {
		wg.Done()
	}
}

// Next is part of the RowSource interface.
func (n *noopProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for n.state == stateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.moveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.moveToDraining(nil /* err */)
			break
		}

		if outRow := n.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, n.drainHelper()
}

// ConsumerDone is part of the RowSource interface.
func (n *noopProcessor) ConsumerDone() {
	n.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (n *noopProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.internalClose()
}
