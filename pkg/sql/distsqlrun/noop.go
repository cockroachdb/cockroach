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

func newNoopProcessor(
	flowCtx *FlowCtx, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*noopProcessor, error) {
	n := &noopProcessor{input: input}
	if err := n.init(post, input.OutputTypes(), flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return n, nil
}

// Run is part of the processor interface.
func (n *noopProcessor) Run(wg *sync.WaitGroup) {
	if n.out.output == nil {
		panic("noopProcessor output not initialized for emitting rows")
	}
	Run(n.flowCtx.Ctx, n, n.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (n *noopProcessor) close() {
	if n.internalClose() {
		n.input.ConsumerClosed()
	}
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (n *noopProcessor) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !n.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(n.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		n.close()
	}
	return meta
}

// Next is part of the RowSource interface.
func (n *noopProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	n.maybeStart("noop", "" /* logTag */)

	if n.closed {
		return nil, n.producerMeta(nil /* err */)
	}

	for {
		row, meta := n.input.Next()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			return nil, n.producerMeta(nil /* err */)
		}

		outRow, status, err := n.out.ProcessRow(n.ctx, row)
		if err != nil {
			return nil, n.producerMeta(err)
		}
		switch status {
		case NeedMoreRows:
			if outRow == nil && err == nil {
				continue
			}
		case DrainRequested:
			n.input.ConsumerDone()
			continue
		}
		return outRow, nil
	}
}

// ConsumerDone is part of the RowSource interface.
func (n *noopProcessor) ConsumerDone() {
	n.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (n *noopProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.close()
}
