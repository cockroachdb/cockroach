// Copyright 2016 The Cockroach Authors.
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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// sorter sorts the input rows according to the column ordering specified by
// 'ordering'. Note that this is a no-grouping aggregator and therefore it does
// not produce a global ordering but simply guarantees an intra-stream ordering
// on the physical output stream.
type sorter struct {
	ctx      context.Context
	ordering sqlbase.ColumnOrdering
	input    RowSource
	output   RowReceiver
}

var _ processor = &sorter{}

func newSorter(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, output RowReceiver,
) *sorter {
	return &sorter{
		ctx:      log.WithLogTag(flowCtx.Context, "Sorter", nil),
		ordering: convertColumnOrdering(spec.Ordering),
		input:    input,
		output:   output,
	}
}

// Run is part of the processor interface.
func (s *sorter) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if log.V(2) {
		log.Infof(s.ctx, "starting sort")
		defer log.Infof(s.ctx, "exiting sort")
	}
}
