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

package distsqlrun

import (
	"errors"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	flowCtx *FlowCtx
	evalCtx *tree.EvalContext

	leftSource, rightSource RowSource

	streamMerger streamMerger
}

var _ Processor = &mergeJoiner{}

func newMergeJoiner(
	flowCtx *FlowCtx,
	spec *MergeJoinerSpec,
	leftSource RowSource,
	rightSource RowSource,
	post *PostProcessSpec,
	output RowReceiver,
) (*mergeJoiner, error) {
	for i, c := range spec.LeftOrdering.Columns {
		if spec.RightOrdering.Columns[i].Direction != c.Direction {
			return nil, errors.New("Unmatched column orderings")
		}
	}

	m := &mergeJoiner{
		flowCtx:     flowCtx,
		leftSource:  leftSource,
		rightSource: rightSource,
	}
	// TODO: Adapt MergeJoiner to new joinerBase constructor.
	err := m.joinerBase.init(flowCtx, leftSource.Types(), rightSource.Types(), spec.Type, spec.OnExpr, nil, nil, 0, post, output)
	if err != nil {
		return nil, err
	}

	m.streamMerger, err = makeStreamMerger(
		leftSource,
		convertToColumnOrdering(spec.LeftOrdering),
		rightSource,
		convertToColumnOrdering(spec.RightOrdering),
		output, /*metadataSync*/
		spec.NullEquality,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Run is part of the processor interface.
func (m *mergeJoiner) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx = log.WithLogTag(ctx, "MergeJoiner", nil)
	ctx, span := processorSpan(ctx, "merge joiner")
	defer tracing.FinishSpan(span)
	log.VEventf(ctx, 2, "starting merge joiner run")

	cancelChecker := sqlbase.NewCancelChecker(ctx)
	m.evalCtx = m.flowCtx.NewEvalCtx()

	for {
		moreBatches, err := m.outputBatch(ctx, cancelChecker)
		if err != nil || !moreBatches {
			DrainAndClose(
				ctx, m.out.output, err, m.leftSource, m.rightSource)
			break
		}
	}
	log.VEventf(ctx, 2, "exiting merge joiner run")
}

// outputBatch outputs all the rows corresponding to a streamMerger batch (the
// cross-product of two groups of matching rows).
//
// Returns true if more batches are available and needed. If false is returned,
// the caller should drain the inputs (as the termination condition might have
// been dictated by the consumer saying that no more rows are needed) and close
// the output.
func (m *mergeJoiner) outputBatch(
	ctx context.Context, cancelChecker *sqlbase.CancelChecker,
) (bool, error) {
	leftRows, rightRows, err := m.streamMerger.NextBatch(m.evalCtx)
	if err != nil {
		return false, err
	}
	if leftRows == nil && rightRows == nil {
		return false, nil
	}
	var matchedRight []bool
	if m.joinType == fullOuter || m.joinType == rightOuter {
		matchedRight = make([]bool, len(rightRows))
	}

	for _, lrow := range leftRows {
		matched := false
		for rIdx, rrow := range rightRows {
			if err := cancelChecker.Check(); err != nil {
				return false, err
			}
			renderedRow, err := m.render(lrow, rrow)
			if err != nil {
				return false, err
			}
			if renderedRow != nil {
				matched = true
				if matchedRight != nil {
					matchedRight[rIdx] = true
				}
				consumerStatus, err := m.out.EmitRow(ctx, renderedRow)
				if err != nil || consumerStatus != NeedMoreRows {
					return false, err
				}
			}
		}
		if !matched {
			needMoreRows, err := m.maybeEmitUnmatchedRow(ctx, lrow, leftSide)
			if !needMoreRows || err != nil {
				return false, err
			}
		}
	}

	if matchedRight != nil {
		// Produce results for unmatched right rows (for RIGHT OUTER or FULL OUTER).
		for rIdx, rrow := range rightRows {
			if !matchedRight[rIdx] {
				needMoreRows, err := m.maybeEmitUnmatchedRow(ctx, rrow, rightSide)
				if !needMoreRows || err != nil {
					return false, err
				}
			}
		}
	}
	return true, nil
}
