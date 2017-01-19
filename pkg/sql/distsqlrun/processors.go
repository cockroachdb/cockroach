// Copyright 2017 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// processor is a common interface implemented by all processors, used by the
// higher-level flow orchestration code.
type processor interface {
	// Run is the main loop of the processor.
	// If wg is non-nil, wg.Done is called before exiting.
	Run(wg *sync.WaitGroup)
}

// noopProcessor is a processor that simply passes rows through from the
// synchronizer to the router. It can be useful in the last stage of a
// computation, where we may only need the synchronizer to join streams.
type noopProcessor struct {
	flowCtx *FlowCtx
	input   RowSource
	output  RowReceiver
}

var _ processor = &noopProcessor{}

func newNoopProcessor(flowCtx *FlowCtx, input RowSource, output RowReceiver) *noopProcessor {
	return &noopProcessor{flowCtx: flowCtx, input: input, output: output}
}

// Run is part of the processor interface.
func (n *noopProcessor) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	for {
		row, err := n.input.NextRow()
		if err != nil || row == nil {
			n.output.Close(err)
			return
		}
		if log.V(3) {
			log.Infof(n.flowCtx.Context, "noop: pushing row %s", row)
		}
		if !n.output.PushRow(row) {
			return
		}
	}
}
