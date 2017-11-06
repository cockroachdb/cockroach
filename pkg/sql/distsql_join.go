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

package sql

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func (dsp *DistSQLPlanner) createPlanForInterleaveJoin(
	planCtx *planningCtx, n *joinNode,
) (physicalPlan, error) {
	leftScan, leftOk := n.left.plan.(*scanNode)
	rightScan, rightOk := n.right.plan.(*scanNode)
	if !leftOk || !rightOk {
		panic("cannot plan interleave join with non-scan left and right nodes")
	}

	if leftScan.reverse != rightScan.reverse {
		panic("cannot plan interleave join with opposite scan directions")
	}

	if len(n.mergeJoinOrdering) <= 0 {
		panic("cannot plan interleave join with no equality join columns")
	}

	if len(n.mergeJoinOrdering) != len(n.pred.leftEqualityIndices) {
		panic("cannot plan interleave join on a subset of equality join columns")
	}

	// We iterate through each table and collate their metadata for
	// the InterleaveReaderJoinerSpec.
	tables := make([]distsqlrun.InterleaveReaderJoinerTable, 2)
	plans := make([]physicalPlan, 2)
	var totalLimitHint int64
	for i, t := range []struct {
		scan      *scanNode
		eqIndices []int
	}{
		{
			scan:      leftScan,
			eqIndices: n.pred.leftEqualityIndices,
		},
		{
			scan:      rightScan,
			eqIndices: n.pred.rightEqualityIndices,
		},
	} {
		// We don't really need to initialize a full-on plan to
		// retrieve the metadata for each table reader, but this turns
		// out to be very useful to compute ordering and remapping the
		// onCond and columns.
		var err error
		if plans[i], err = dsp.createTableReaders(planCtx, t.scan, nil); err != nil {
			return physicalPlan{}, err
		}

		eqCols := eqCols(t.eqIndices, plans[i].planToStreamColMap)
		ordering := distsqlOrdering(n.mergeJoinOrdering, eqCols)

		// Doesn't matter which processor we choose since the metadata
		// for TableReader is independent of node/processor instance.
		tr := plans[i].Processors[0].Spec.Core.TableReader

		tables[i] = distsqlrun.InterleaveReaderJoinerTable{
			Desc:     tr.Table,
			IndexIdx: tr.IndexIdx,
			Post:     plans[i].GetLastStagePost(),
			Ordering: ordering,
		}

		if tr.LimitHint >= math.MaxInt64-totalLimitHint {
			totalLimitHint = math.MaxInt64
		} else {
			totalLimitHint += tr.LimitHint
		}
	}

	// We need to combine the two set of spans from the left and right side
	// since we have one reading processor (InterleaveReaderJoiner).
	var spans []roachpb.Span
	var joinType distsqlrun.JoinType
	switch n.joinType {
	case joinTypeInner:
		// We can take the intersection of the two set of spans for
		// inner joins because we never need rows from either table
		// that do not satisfy the ON condition.
		// Since equality columns map 1-1 with the columns of the
		// interleave prefix, we need only scan interleaves together.
		// Spans for interleaved tables/indexes always refer to their
		// root ancestor and thus incorporate all interleaves of the
		// ancestors between the root and itself.
		spans = roachpb.IntersectSpans(append(leftScan.spans, rightScan.spans...))
		joinType = distsqlrun.JoinType_INNER
	default:
		// TODO(richardwu): For outer joins, we will need to take the
		// union of spans and propagate scan.origFilters down into the
		// spec. See comment above spans in InterleaveReaderJoinerSpec.
		panic("can only plan inner joins with interleave joins")
	}

	post, joinToStreamColMap := joinOutColumns(n, plans[0], plans[1])
	onExpr := remapOnExpr(planCtx.evalCtx, n, plans[0], plans[1])

	// We partition the combined spans to their data nodes (best effort).
	spanPartitions, err := dsp.partitionSpans(planCtx, spans)
	if err != nil {
		return physicalPlan{}, err
	}
	// For a given parent row, we need to ensure each
	// InterleaveReaderJoiner processor reads all children rows. If
	// children rows for a given parent row is partitioned into a different
	// node, then we will miss joining those children rows to the parent
	// row.
	// We thus fix the spans such that the end key always includes the last
	// children.
	if spanPartitions, err = fixInterleavePartitions(n, spanPartitions); err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	stageID := p.NewStageID()

	// We provision a separate InterleaveReaderJoiner per node that has
	// rows from both tables.
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for _, sp := range spanPartitions {
		irj := &distsqlrun.InterleaveReaderJoinerSpec{
			Tables: tables,
			// We previously checked that both scans are in the
			// same direction.
			Reverse:   leftScan.reverse,
			LimitHint: totalLimitHint,
			OnExpr:    onExpr,
			Type:      joinType,
		}

		irj.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for i, span := range sp.spans {
			irj.Spans[i].Span = span
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{InterleaveReaderJoiner: irj},
				Post:    post,
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		p.Processors = append(p.Processors, proc)
	}

	p.planToStreamColMap = joinToStreamColMap
	p.ResultTypes = getTypesForPlanResult(n, joinToStreamColMap)

	p.SetMergeOrdering(dsp.convertOrdering(n.props, p.planToStreamColMap))
	return p, nil
}

func joinOutColumns(
	n *joinNode, leftPlan, rightPlan physicalPlan,
) (post distsqlrun.PostProcessSpec, joinToStreamColMap []int) {
	joinToStreamColMap = makePlanToStreamColMap(len(n.columns))
	post.Projection = true

	// addOutCol appends to post.OutputColumns and returns the index
	// in the slice of the added column.
	addOutCol := func(col uint32) int {
		idx := len(post.OutputColumns)
		post.OutputColumns = append(post.OutputColumns, col)
		return idx
	}

	// The join columns are in two groups:
	//  - the columns on the left side (numLeftCols)
	//  - the columns on the right side (numRightCols)
	joinCol := 0
	for i := 0; i < n.pred.numLeftCols; i++ {
		if !n.columns[joinCol].Omitted {
			joinToStreamColMap[joinCol] = addOutCol(uint32(leftPlan.planToStreamColMap[i]))
		}
		joinCol++
	}
	for i := 0; i < n.pred.numRightCols; i++ {
		if !n.columns[joinCol].Omitted {
			joinToStreamColMap[joinCol] = addOutCol(
				uint32(rightPlan.planToStreamColMap[i] + len(leftPlan.ResultTypes)),
			)
		}
		joinCol++
	}

	return post, joinToStreamColMap
}

// remapOnExpr remaps ordinal references in the on condition (which refer to the
// join columns as described above) to values that make sense in the joiner (0
// to N-1 for the left input columns, N to N+M-1 for the right input columns).
func remapOnExpr(evalCtx *parser.EvalContext, n *joinNode, leftPlan, rightPlan physicalPlan) distsqlrun.Expression {
	if n.pred.onCond == nil {
		return distsqlrun.Expression{}
	}

	joinColMap := make([]int, len(n.columns))
	idx := 0
	for i := 0; i < n.pred.numLeftCols; i++ {
		joinColMap[idx] = leftPlan.planToStreamColMap[i]
		idx++
	}
	for i := 0; i < n.pred.numRightCols; i++ {
		joinColMap[idx] = rightPlan.planToStreamColMap[i] + len(leftPlan.ResultTypes)
		idx++
	}

	return distsqlplan.MakeExpression(n.pred.onCond, evalCtx, joinColMap)
}

// eqCols produces a slice of ordinal references for the plan columns specified
// in eqIndices using planToColMap.
// That is: eqIndices contains a slice of plan column indexes and planToColMap
// maps the plan column indexes to the ordinal references (index of the
// intermediate row produced).
func eqCols(eqIndices, planToColMap []int) []uint32 {
	eqCols := make([]uint32, len(eqIndices))
	for i, planCol := range eqIndices {
		eqCols[i] = uint32(planToColMap[planCol])
	}

	return eqCols
}

// distsqlOrdering converts the ordering specified by mergeJoinOrdering in
// terms of the index of eqCols to the ordinal references provided by eqCols.
func distsqlOrdering(
	mergeJoinOrdering sqlbase.ColumnOrdering, eqCols []uint32,
) distsqlrun.Ordering {
	var ord distsqlrun.Ordering
	ord.Columns = make([]distsqlrun.Ordering_Column, len(mergeJoinOrdering))
	for i, c := range mergeJoinOrdering {
		ord.Columns[i].ColIdx = eqCols[c.ColIdx]
		dir := distsqlrun.Ordering_Column_ASC
		if c.Direction == encoding.Descending {
			dir = distsqlrun.Ordering_Column_DESC
		}
		ord.Columns[i].Direction = dir
	}

	return ord
}

// fixInterleavePartitions ensures that every span in every spanPartition
// will be gauranteed to read every descendant row interleaved under the last
// ancestor row, where ancestor and descendant are defined based on the left
// and right children of joinNode.
//
// Given a span with the start and end keys:
// StartKey: /parent/1/2/#/child/2
// EndKey:   /parent/1/42/#/child/4
// We'd like to fix the EndKey such that the span contains all children keys
// for the given parent ID 42. The end result we want is
// StartKey: /parent/1/2/#/child/2
// EndKey:   /parent/1/43
// To do this, we must truncate up to the prefix /parent/1/42 to invoke
// encoding.PrefixEnd() to produce /parent/1/43.
// To calculate how long this prefix is, we take a look at the general encoding
// of a descendant EndKey
// /<1st-tbl>/<1st-idx>/<1st-col1>/.../<1st-colN>/#/<2nd-tbl>/<2nd-idx>/<2nd-col1>/.../<2nd-colM>/#/...
// For each ancestor (i.e. 1st, 2nd), we have
//  <X-tbl>, <X-idx>, '#' (interleave sentinel)
// or 3 values to peek at.
// We need to subtract 1 since we do not want to include the last interleave sentinel '#'.
// Furthermore, we need to count the number of columns in the shared interleave
// prefix (i.e. <1st-colX>, <2nd-colY>). We traverse the InterleaveDescriptor
// and add up SharedPrefixLen.
// Thus we need to peek (encoding.PeekLength())
//   3 * count(interleave ancestors) + sum(SharedPrefixLen) - 1
// times.
//
// Example:
// Given the following interleave hierarchy where their primary indexes
// are in parentheses
//   parent	      (pid1)
//     child	      (pid1, cid1, cid2)
//        grandchild  (pid1, cid1, cid2, gcid1)
// Let ancestor = parent and descendant = grandchild.
// A descendant span key could be
// /<parent-tbl>/<parent-idx>/<pid1>/#/<child-tbl>/<child-idx>/<cid1>/<cid2>/#/<grandchild-tbl>/...
// We'd like to take the prefix up to and including <cid2>. To do so, we must
// call encoding.PeekLength() 8 times or
//   3 * nAncestors + sum(SharedPrefixLen) - 1 = 3 * 2 + (1 + 2) - 1 = 8
func fixInterleavePartitions(n *joinNode, partitions []spanPartition) ([]spanPartition, error) {
	ancestor, descendant := n.interleaveNodes()
	if ancestor == nil || descendant == nil {
		panic("cannot fix interleave join spans since there is no interleave relation")
	}

	// The number of times we need to invoke encoding.PeekLength() requires
	// the number of ancestors up to and including our ancestor with respect
	// to descendant.
	// We also need to calculate the number of shared column fields
	// (interleave prefixes) from every ancestor.
	nAncestors := 0
	sharedPrefixLen := 0
	for _, descAncestor := range descendant.index.Interleave.Ancestors {
		nAncestors++
		sharedPrefixLen += int(descAncestor.SharedPrefixLen)
		if descAncestor.TableID == ancestor.desc.ID && descAncestor.IndexID == ancestor.index.ID {
			break
		}
	}

	// We iterate over every span in our partitions and adjust their EndKey
	// as described above.
	for i := range partitions {
		for j := 0; j < len(partitions[i].spans); j++ {
			span := &partitions[i].spans[j]
			endKey := span.EndKey
			prefixLen := 0
			earlyTermination := false
			// See the formula above for computing the number of
			// times to invoke encoding.PeekLength().
			for i := 0; i < 3*nAncestors+sharedPrefixLen-1; i++ {
				// It's possible for the span key to not
				// contain the full prefix up to the descendant
				// (e.g. when no filter is specified on any
				// interleave prefix columns, the key will be
				// in the form /ancestor/indexid instead of
				// /ancestor/indexid/pid1).
				// This is fine since we only care about EndKeys
				// which terminate in between children rows (e.g.
				// /ancestor/indexid/.../#/child/indexid/2).
				// Any keys shorter than the maximum prefix
				// length are guaranteed to include all
				// interleaved descendant rows.
				if len(endKey) == 0 {
					earlyTermination = true
					break
				}
				valLen, err := encoding.PeekLength(endKey)
				if err != nil {
					return nil, err
				}
				prefixLen += valLen
				endKey = endKey[valLen:]
			}

			// We only need to fix the key if the key is in between
			// children rows, which can only happen if the key has
			// a prefix up to the ancestor and then some.
			if !earlyTermination && len(endKey) > 0 {
				span.EndKey = span.EndKey[:prefixLen].PrefixEnd()

				// Adjust the start Key of the next span.
				nextIdx := j + 1
				// It's possible that span.EndKey now subsumes
				// subsequent spans in the partition.
				// We remove those redundant spans and from our partition.
				for nextIdx < len(partitions[i].spans) && span.EndKey.Compare(partitions[i].spans[nextIdx].EndKey) >= 0 {
					partitions[i].spans = append(partitions[i].spans[:nextIdx], partitions[i].spans[nextIdx+1:]...)
				}

				// We adjust the start key of the next span in
				// the redacted partition (if there is one).
				if nextIdx < len(partitions[i].spans) {
					partitions[i].spans[nextIdx].Key = span.EndKey
				}
			}
		}
	}

	return partitions, nil
}
