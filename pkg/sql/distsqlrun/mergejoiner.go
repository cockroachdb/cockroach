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
	"errors"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// mergeJoiner performs merge join, it has two input row sources with the same
// ordering on the columns that have equality constraints.
//
// It is guaranteed that the results preserve this ordering.
type mergeJoiner struct {
	joinerBase

	streamMerger streamMerger
}

var _ processor = &mergeJoiner{}

func newMergeJoiner(
	flowCtx *FlowCtx, spec *MergeJoinerSpec, inputs []RowSource, output RowReceiver,
) (*mergeJoiner, error) {
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
	}

	m := &mergeJoiner{}
	err := m.joinerBase.init(flowCtx, nil, output, spec.OutputColumns,
		spec.Type, spec.LeftTypes, spec.RightTypes, spec.Expr)
	if err != nil {
		return nil, err
	}

	m.streamMerger, err = makeStreamMerger(
		[]sqlbase.ColumnOrdering{
			convertToColumnOrdering(spec.LeftOrdering),
			convertToColumnOrdering(spec.RightOrdering),
		}, inputs)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Run is part of the processor interface.
func (m *mergeJoiner) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx, span := tracing.ChildSpan(m.ctx, "merge joiner")
	defer tracing.FinishSpan(span)

	if log.V(2) {
		log.Infof(ctx, "starting merge joiner run")
		defer log.Infof(ctx, "exiting merge joiner run")
	}

	for {
		batch, err := m.streamMerger.NextBatch()
		if err != nil || len(batch) == 0 {
			m.output.Close(err)
			return
		}
		for _, rowPair := range batch {
			row, err := m.render(rowPair[0], rowPair[1])
			if err != nil {
				m.output.Close(err)
				return
			}
			if row != nil && !m.output.PushRow(row) {
				if log.V(2) {
					log.Infof(ctx, "no more rows required")
				}
				m.output.Close(nil)
				return
			}
		}
	}
}
