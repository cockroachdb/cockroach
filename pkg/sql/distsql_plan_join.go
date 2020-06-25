// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// joinPlanningInfo is a utility struct that contains the information needed to
// perform the physical planning of hash and merge joins.
type joinPlanningInfo struct {
	leftPlan, rightPlan *PhysicalPlan
	joinType            sqlbase.JoinType
	joinResultTypes     []*types.T
	onExpr              execinfrapb.Expression
	post                execinfrapb.PostProcessSpec
	joinToStreamColMap  []int
	// leftEqCols and rightEqCols are the indices of equality columns. These
	// are only used when planning a hash join.
	leftEqCols, rightEqCols             []uint32
	leftEqColsAreKey, rightEqColsAreKey bool
	// leftMergeOrd and rightMergeOrd are the orderings on both inputs to a
	// merge join. They must be of the same length, and if the length is 0,
	// then a hash join is planned.
	leftMergeOrd, rightMergeOrd                 execinfrapb.Ordering
	leftPlanDistribution, rightPlanDistribution physicalplan.PlanDistribution
}

// makeCoreSpec creates a processor core for hash and merge joins based on the
// join planning information. Merge ordering fields of info determine which
// kind of join is being planned.
func (info *joinPlanningInfo) makeCoreSpec() execinfrapb.ProcessorCoreUnion {
	var core execinfrapb.ProcessorCoreUnion
	if len(info.leftMergeOrd.Columns) != len(info.rightMergeOrd.Columns) {
		panic(fmt.Sprintf(
			"unexpectedly different merge join ordering lengths: left %d, right %d",
			len(info.leftMergeOrd.Columns), len(info.rightMergeOrd.Columns),
		))
	}
	if len(info.leftMergeOrd.Columns) == 0 {
		// There is no required ordering on the columns, so we plan a hash join.
		core.HashJoiner = &execinfrapb.HashJoinerSpec{
			LeftEqColumns:        info.leftEqCols,
			RightEqColumns:       info.rightEqCols,
			OnExpr:               info.onExpr,
			Type:                 info.joinType,
			LeftEqColumnsAreKey:  info.leftEqColsAreKey,
			RightEqColumnsAreKey: info.rightEqColsAreKey,
		}
	} else {
		core.MergeJoiner = &execinfrapb.MergeJoinerSpec{
			LeftOrdering:         info.leftMergeOrd,
			RightOrdering:        info.rightMergeOrd,
			OnExpr:               info.onExpr,
			Type:                 info.joinType,
			LeftEqColumnsAreKey:  info.leftEqColsAreKey,
			RightEqColumnsAreKey: info.rightEqColsAreKey,
		}
	}
	return core
}

var planInterleavedJoins = settings.RegisterBoolSetting(
	"sql.distsql.interleaved_joins.enabled",
	"if set we plan interleaved table joins instead of merge joins when possible",
	true,
)

func (dsp *DistSQLPlanner) tryCreatePlanForInterleavedJoin(
	planCtx *PlanningCtx, n *joinNode,
) (plan *PhysicalPlan, ok bool, err error) {
	plan = &PhysicalPlan{}
	if !useInterleavedJoin(n) {
		return nil, false, nil
	}

	leftScan, leftOk := n.left.plan.(*scanNode)
	rightScan, rightOk := n.right.plan.(*scanNode)

	// We know they are scan nodes from useInterleaveJoin, but we add
	// this check to prevent future panics.
	if !leftOk || !rightOk {
		return nil, false, errors.AssertionFailedf("left and right children of join node must be scan nodes to execute an interleaved join")
	}

	// We iterate through each table and collate their metadata for
	// the InterleavedReaderJoinerSpec.
	tables := make([]execinfrapb.InterleavedReaderJoinerSpec_Table, 2)
	plans := make([]*PhysicalPlan, 2)
	var totalLimitHint int64
	for i, t := range []struct {
		scan      *scanNode
		eqIndices []exec.NodeColumnOrdinal
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
		// out to be very useful for computing ordering and remapping the
		// onCond and columns.
		var err error
		if plans[i], err = dsp.createTableReaders(planCtx, t.scan); err != nil {
			return nil, false, err
		}

		eqCols := eqCols(t.eqIndices, plans[i].PlanToStreamColMap)
		ordering := distsqlOrdering(n.mergeJoinOrdering, eqCols)

		// Doesn't matter which processor we choose since the metadata
		// for TableReader is independent of node/processor instance.
		tr := plans[i].Processors[0].Spec.Core.TableReader

		tables[i] = execinfrapb.InterleavedReaderJoinerSpec_Table{
			Desc:     tr.Table,
			IndexIdx: tr.IndexIdx,
			Post:     plans[i].GetLastStagePost(),
			Ordering: ordering,
		}

		// We will set the limit hint of the final
		// InterleavedReaderJoiner as the sum of the individual tables'
		// limit hints.
		// This is because the InterleavedReaderJoiner reads rows from
		// all tables at the same time and so the hint applies to the
		// total number of rows read from all tables.
		if totalLimitHint >= math.MaxInt64-tr.LimitHint {
			totalLimitHint = math.MaxInt64
		} else {
			totalLimitHint += tr.LimitHint
		}
	}

	joinType := n.joinType

	leftMap, rightMap := plans[0].PlanToStreamColMap, plans[1].PlanToStreamColMap
	helper := &joinPlanningHelper{
		numLeftCols:             n.pred.numLeftCols,
		numRightCols:            n.pred.numRightCols,
		leftPlanToStreamColMap:  leftMap,
		rightPlanToStreamColMap: rightMap,
	}
	post, joinToStreamColMap := helper.joinOutColumns(n.joinType, n.columns)
	onExpr, err := helper.remapOnExpr(planCtx, n.pred.onCond)
	if err != nil {
		return nil, false, err
	}

	ancestor, descendant := n.interleavedNodes()

	// We partition each set of spans to their respective nodes.
	ancsPartitions, err := dsp.PartitionSpans(planCtx, ancestor.spans)
	if err != nil {
		return nil, false, err
	}
	descPartitions, err := dsp.PartitionSpans(planCtx, descendant.spans)
	if err != nil {
		return nil, false, err
	}

	// We want to ensure that all child spans with a given interleave
	// prefix value (which also happens to be our equality join columns)
	// are read on the same node as the corresponding ancestor rows.
	// We map all descendant spans in their partitions to the corresponding
	// nodes of the ascendant spans.
	//
	// Example:
	// Let PK1 and (PK1, PK2) be the primary keys of parent and child,
	// respectively. PK1 is the interleave prefix.
	// The filter WHERE PK1 = 1 AND PK2 IN (5, 7) will produce the
	// parent and child spans
	//   parent:  /1 - /2    (technically /1 - /1/#/8)
	//   child:   /1/#/5 - /1/#/6, /1/#/7 - /1/#/8
	// If the parent span is partitioned to node 1 and the child spans are
	// partitioned to node 2 and 3, then we need to move the child spans
	// to node 1 where the PK1 = 1 parent row is read.
	if descPartitions, err = alignInterleavedSpans(n, ancsPartitions, descPartitions); err != nil {
		return nil, false, err
	}

	// Figure out which nodes we need to schedule a processor on.
	seen := make(map[roachpb.NodeID]struct{})
	var nodes []roachpb.NodeID
	for _, partitions := range [][]SpanPartition{ancsPartitions, descPartitions} {
		for _, part := range partitions {
			if _, ok := seen[part.Node]; !ok {
				seen[part.Node] = struct{}{}
				nodes = append(nodes, part.Node)
			}
		}
	}

	var ancsIdx, descIdx int
	// The left table is in the 0th index, right table in the 1st index.
	if leftScan == ancestor {
		ancsIdx, descIdx = 0, 1
	} else {
		ancsIdx, descIdx = 1, 0
	}

	// We provision a separate InterleavedReaderJoiner per node that has
	// rows from either table.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(nodes))
	for i, nodeID := range nodes {
		// Find the relevant span from each table for this node.
		// Note it is possible that either set of spans can be empty
		// (but not both).
		var ancsSpans, descSpans roachpb.Spans
		for _, part := range ancsPartitions {
			if part.Node == nodeID {
				ancsSpans = part.Spans
				break
			}
		}
		for _, part := range descPartitions {
			if part.Node == nodeID {
				descSpans = part.Spans
				break
			}
		}
		if len(ancsSpans) == 0 && len(descSpans) == 0 {
			panic("cannot have empty set of spans for both tables for a given node")
		}

		// Make a copy of our spec for each table.
		processorTables := make([]execinfrapb.InterleavedReaderJoinerSpec_Table, len(tables))
		copy(processorTables, tables)
		// We set the set of spans for each table to be read by the
		// processor.
		processorTables[ancsIdx].Spans = makeTableReaderSpans(ancsSpans)
		processorTables[descIdx].Spans = makeTableReaderSpans(descSpans)

		irj := &execinfrapb.InterleavedReaderJoinerSpec{
			Tables: processorTables,
			// We previously checked that both scans are in the
			// same direction (useInterleavedJoin).
			Reverse:           ancestor.reverse,
			LimitHint:         totalLimitHint,
			LockingStrength:   ancestor.lockingStrength,
			LockingWaitPolicy: ancestor.lockingWaitPolicy,
			OnExpr:            onExpr,
			Type:              joinType,
		}

		corePlacement[i].NodeID = nodeID
		corePlacement[i].Core.InterleavedReaderJoiner = irj
	}

	resultTypes, err := getTypesForPlanResult(n, joinToStreamColMap)
	if err != nil {
		return nil, false, err
	}
	plan.GatewayNodeID, err = planCtx.ExtendedEvalCtx.ExecCfg.NodeID.OptionalNodeIDErr(50050)
	if err != nil {
		return nil, false, err
	}
	plan.AddNoInputStage(
		corePlacement, post, resultTypes, dsp.convertOrdering(n.reqOrdering, joinToStreamColMap),
	)

	plan.PlanToStreamColMap = joinToStreamColMap

	return plan, true, nil
}

// joinPlanningHelper is a utility struct that helps with the physical planning
// of joins.
type joinPlanningHelper struct {
	numLeftCols, numRightCols                       int
	leftPlanToStreamColMap, rightPlanToStreamColMap []int
}

func (h *joinPlanningHelper) joinOutColumns(
	joinType sqlbase.JoinType, columns sqlbase.ResultColumns,
) (post execinfrapb.PostProcessSpec, joinToStreamColMap []int) {
	joinToStreamColMap = makePlanToStreamColMap(len(columns))
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
	for i := 0; i < h.numLeftCols; i++ {
		joinToStreamColMap[i] = addOutCol(uint32(h.leftPlanToStreamColMap[i]))
	}

	if joinType != sqlbase.LeftSemiJoin && joinType != sqlbase.LeftAntiJoin {
		for i := 0; i < h.numRightCols; i++ {
			joinToStreamColMap[h.numLeftCols+i] = addOutCol(
				uint32(h.numLeftCols + h.rightPlanToStreamColMap[i]),
			)
		}
	}

	return post, joinToStreamColMap
}

// remapOnExpr remaps ordinal references in the on condition (which refer to the
// join columns as described above) to values that make sense in the joiner (0
// to N-1 for the left input columns, N to N+M-1 for the right input columns).
func (h *joinPlanningHelper) remapOnExpr(
	planCtx *PlanningCtx, onCond tree.TypedExpr,
) (execinfrapb.Expression, error) {
	if onCond == nil {
		return execinfrapb.Expression{}, nil
	}

	joinColMap := make([]int, h.numLeftCols+h.numRightCols)
	idx := 0
	leftCols := 0
	for i := 0; i < h.numLeftCols; i++ {
		joinColMap[idx] = h.leftPlanToStreamColMap[i]
		if h.leftPlanToStreamColMap[i] != -1 {
			leftCols++
		}
		idx++
	}
	for i := 0; i < h.numRightCols; i++ {
		joinColMap[idx] = leftCols + h.rightPlanToStreamColMap[i]
		idx++
	}

	return physicalplan.MakeExpression(onCond, planCtx, joinColMap)
}

// eqCols produces a slice of ordinal references for the plan columns specified
// in eqIndices using planToColMap.
// That is: eqIndices contains a slice of plan column indexes and planToColMap
// maps the plan column indexes to the ordinal references (index of the
// intermediate row produced).
func eqCols(eqIndices []exec.NodeColumnOrdinal, planToColMap []int) []uint32 {
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
) execinfrapb.Ordering {
	var ord execinfrapb.Ordering
	ord.Columns = make([]execinfrapb.Ordering_Column, len(mergeJoinOrdering))
	for i, c := range mergeJoinOrdering {
		ord.Columns[i].ColIdx = eqCols[c.ColIdx]
		dir := execinfrapb.Ordering_Column_ASC
		if c.Direction == encoding.Descending {
			dir = execinfrapb.Ordering_Column_DESC
		}
		ord.Columns[i].Direction = dir
	}

	return ord
}

func useInterleavedJoin(n *joinNode) bool {
	// TODO(richardwu): We currently only do an interleave join on
	// all equality columns. This can be relaxed once a hybrid
	// hash-merge join is implemented in streamMerger.
	if len(n.mergeJoinOrdering) != len(n.pred.leftEqualityIndices) {
		return false
	}

	ancestor, descendant := n.interleavedNodes()

	// There is no interleaved ancestor/descendant scan node and thus no
	// interleaved relation.
	if ancestor == nil || descendant == nil {
		return false
	}

	// We cannot do an interleaved join if the tables require scanning in
	// opposite directions.
	if ancestor.reverse != descendant.reverse {
		return false
	}

	var ancestorEqIndices []exec.NodeColumnOrdinal
	var descendantEqIndices []exec.NodeColumnOrdinal
	// We are guaranteed that both of the sources are scan nodes from
	// n.interleavedNodes().
	if ancestor == n.left.plan.(*scanNode) {
		ancestorEqIndices = n.pred.leftEqualityIndices
		descendantEqIndices = n.pred.rightEqualityIndices
	} else {
		ancestorEqIndices = n.pred.rightEqualityIndices
		descendantEqIndices = n.pred.leftEqualityIndices
	}

	// We want full 1-1 correspondence between our join columns and the
	// primary index of the ancestor.
	//  TODO(richardwu): We can relax this once we implement a hybrid
	//  hash/merge for interleaved joins after forming merge groups with the
	//  interleave prefix (or when the merge join logic is combined with
	//  the interleaved join logic).
	if len(n.mergeJoinOrdering) != len(ancestor.index.ColumnIDs) {
		return false
	}

	// We iterate through the ordering given by n.mergeJoinOrdering and check
	// if the columns have a 1-1 correspondence to the interleaved
	// ancestor's primary index columns (i.e. interleave prefix) as well as the
	// descendant's primary index columns. We naively return false if any part
	// of the ordering does not correspond.
	for i, info := range n.mergeJoinOrdering {
		colID := ancestor.index.ColumnIDs[i]
		// info.ColIdx refers to i in ancestorEqIndices[i], which refers
		// to the index of the source row. This corresponds to
		// the index in scanNode.resultColumns. To convert the colID
		// from the index descriptor, we can use the map provided by
		// colIdxMap.
		if int(ancestorEqIndices[info.ColIdx]) != ancestor.colIdxMap[colID] ||
			int(descendantEqIndices[info.ColIdx]) != descendant.colIdxMap[colID] {
			// The column in the ordering does not correspond to
			// the column in the interleave prefix.
			// We should not try to do an interleaved join.
			return false
		}
	}

	// The columns in n.mergeJoinOrdering has a 1-1 correspondence with the
	// columns in the interleaved ancestor's primary index. We can indeed
	// hint at the possibility of an interleaved join.
	return true
}

// maximalJoinPrefix takes the common ancestor scanNode that the join is
// defined on, the target scanNode that the index key belongs to and the index
// key itself, and returns the maximal prefix of the key which is also a prefix
// of all keys that need to be joined together.
//
// Let's denote a child key interleaved into a parent key in the following.
// format:
//   /table/index/<parent-pk1>/.../<parent-pkN>/#/<child-pk1>/.../<child-pkN>
//
// In the following examples, the ancestor is parent and the target is child.
//
// Let M be the longest prefix of the parent PK which is (equality) constrained
// by the join. The maximal join prefix is:
//   /table/index/<parent-pk1>/.../<parent-pkM>
//
// Examples (/table/index suppressed from keys):
//  1. Full interleave (prefix) join:
//
//    1a. Parent table PK1
//        Child table (PK1, PK2)
//        Join on PK1
//        For child key /5/#/42, the maximal join prefix is /5
//
//    1b. Parent table (PK1, PK2)
//        Child table (PK1, PK2, PK3)
//        Join on PK1, PK2
//        for child key /5/6/#/42, the maximal join prefix is /5/6
//
//  2. Prefix joins:
//        Parent table (PK1, PK2)
//        Child table (PK1, PK2, PK3)
//        Join on PK1 (this is a prefix of the parent PKs).
//        For child key /5/6/#/42, the maximal join prefix is /5
//
//  3. Subset joins:
//        Parent table (PK1, PK2, PK3)
//        Child table (PK1, PK2, PK3, PK4)
//        Join on PK1, PK3
//        For child key /5/6/7/#/32, the maximal join prefix is /5
//
// This logic can also be extended in the general case to joins between sibling
// joins with a common ancestor: the maximal join prefix will be applied to
// both tables where each sibling scan is passed as the target scanNode.
func maximalJoinPrefix(
	ancestor *scanNode, target *scanNode, key roachpb.Key,
) (roachpb.Key, bool, error) {
	// To calculate how long this prefix is, we take a look at the actual
	// encoding of an interleaved table's key
	//   /table/index/<parent-pk1>/.../<parent-pkN>/#/.../table/index/<child-pk1>/.../<child-pkN>
	// For each ancestor (including parent), we have
	//   table, index, '#' (interleaved sentinel)
	// or 3 values to peek at.
	// We truncate up to the key M which is the last column in our join.
	//   /table/index/<parent-pk1>/.../<parent-pkM>
	// For the full interleaved join case, we need to count the number of
	// columns in the shared interleave prefix (pk1 to pkM). We traverse the
	// InterleaveDescriptor and add up SharedPrefixLen.
	// We finally subtract 1 since we do not want to include the last
	// interleaved sentinel '#'.
	// Thus we need to peek (encoding.PeekLength())
	//    3 * count(interleaved ancestors) + sum(SharedPrefixLen) - 1
	// times to get the actual byte length of the prefix.
	//
	// Example:
	//
	// Given the following interleaved hierarchy (where their primary keys
	// are in parentheses)
	//   parent	      (pid1)
	//     child	      (pid1, cid1, cid2)
	//        grandchild  (pid1, cid1, cid2, gcid1)
	//
	// Let our join be defined on (pid1, cid1, cid2) and we want to join
	// the child and grandchild tables.
	//
	// A grandchild key could be (pid1=5, cid1=6, cid2=7, gcid1=8)
	//    /<parent-id>/1/5/#/<child-id>/1/6/7/#/<gchild-id>/1/8
	//
	// We'd like to take the prefix up to and including <cid2> or
	//    /<parent-id>/1/5/#/<child-id>/1/6/7
	//
	// We must call encoding.PeekLength() 8 times or
	//   3 * nAncestors + sum(SharedPrefixLen) - 1 = 3 * 2 + (1 + 2) - 1 = 8
	// where the ancestor is child.
	//
	// TODO(richardwu): this formula works only for full interleaved joins.
	// For prefix/subset joins, instead of adding the SharedPrefixLen of
	// the ancestor the join is defined on, we would add the number of
	// prefix columns in our interleave prefix that we are joining on.
	nAncestors := 0
	sharedPrefixLen := 0
	for _, targetAncs := range target.index.Interleave.Ancestors {
		nAncestors++
		sharedPrefixLen += int(targetAncs.SharedPrefixLen)
		if targetAncs.TableID == ancestor.desc.ID && targetAncs.IndexID == ancestor.index.ID {
			break
		}
	}

	initialKey := key
	prefixLen := 0
	for i := 0; i < 3*nAncestors+sharedPrefixLen-1; i++ {
		// It's possible for the span key to not contain the full join
		// prefix (a key might refer to an ancestor further up the
		// interleaved hierarchy).
		if len(key) == 0 {
			break
		}
		// Note: this key might have been edited with PrefixEnd. This can cause
		// problems for certain datatypes, like strings, which have a sentinel byte
		// sequence indicating the end of the type. In that case, PeekLength will
		// fail. If that happens, we try to UndoPrefixEnd the key and check the
		// length again.
		// TODO(jordan): this function should be aware of whether a key has been
		// PrefixEnd'd or not, and act accordingly.
		valLen, err := encoding.PeekLength(key)
		if err != nil {
			key, ok := encoding.UndoPrefixEnd(key)
			if !ok {
				return nil, false, err
			}
			valLen, err = encoding.PeekLength(key)
			if err != nil {
				return nil, false, err
			}
		}
		prefixLen += valLen
		key = key[valLen:]
	}

	if len(key) > 0 {
		// There are remaining bytes in the key: we truncate it and
		// return true.
		return initialKey[:prefixLen], true, nil
	}

	// The loop terminated early because the key is shorter than the
	// full join prefix.
	// We return false to denote that this key was not truncated to
	// form the join prefix.
	return initialKey, false, nil
}

// sortedSpanPartitions implements sort.Interface. Sorting is defined on the
// node ID of each partition.
type sortedSpanPartitions []SpanPartition

func (s sortedSpanPartitions) Len() int           { return len(s) }
func (s sortedSpanPartitions) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sortedSpanPartitions) Less(i, j int) bool { return s[i].Node < s[j].Node }

// alignInterleavedSpans takes the partitioned spans from both the parent
// (parentSpans) and (not necessarily direct) child (childSpans), "aligns" them
// and returns childSpans such that all child keys that need to be joined with
// their corresponding parent keys are mapped to the parent keys' partition.
// This ensures that we correctly join all parent-child rows within the
// node-contained InterleavedReaderJoiner.
//
// For each parentSpan, a "join span" is computed.
// The "join span" is a span that includes all child rows that need to be
// joined with parent rows in the span.
//
// With the "join span" of each parent span, we can find any child spans that
// need to be remapped to the same node as the parent span.
//
// We iterate through each child span and see which parent join span overlaps.
//
// If there is no overlap with any join span, there can't possibly be any join
// results from this child span. We still need to keep it for outer joins, but
// it doesn't need to be remapped.
//
// If there is overlap with some parent join span, there exist "some" child
// keys in the span that need to be mapped to the parent span. The sections of
// the child span that do not overlap need to be split off and potentially
// remapped to other parent join spans.
//
// The child span gets split as necessary on the join span's boundaries. The
// split that overlaps the join span is (re-)mapped to the parent span. Any
// remaining splits are considered separately with the same logic.
func alignInterleavedSpans(
	n *joinNode, parentSpans []SpanPartition, childSpans []SpanPartition,
) ([]SpanPartition, error) {
	mappedSpans := make(map[roachpb.NodeID]roachpb.Spans)

	// Map parent spans to their join span.
	joinSpans, err := joinSpans(n, parentSpans)
	if err != nil {
		return nil, err
	}

	// mapAndSplit takes a childSpan and finds the parentJoinSpan that has
	// the parent row(s) with which the child row(s) are suppose to join.
	// It does this by finding overlaps between childSpan and
	// parentJoinSpan.
	// It splits off the non-overlapping parts and appends them to
	// the passed in nonOverlaps slice for repeated application.
	mapAndSplit := func(curNodeID roachpb.NodeID, childSpan roachpb.Span, nonOverlaps roachpb.Spans) roachpb.Spans {
		// TODO(richardwu): Instead of doing a linear search for each
		// child span, we can make this O(logn) with binary search
		// after pre-sorting the parent join spans.
		for _, parentPart := range joinSpans {
			for _, parentJoinSpan := range parentPart.Spans {
				if parentJoinSpan.Overlaps(childSpan) {
					// Initialize the overlap region
					// as the entire childSpan.
					overlap := childSpan
					var nonOverlap roachpb.Span

					// Check non-overlapping region
					// before start key.
					//	    |----parentJoinSpan----...
					//  |----childSpan----...
					if bytes.Compare(parentJoinSpan.Key, childSpan.Key) > 0 {
						nonOverlap, overlap = overlap.SplitOnKey(parentJoinSpan.Key)
						nonOverlaps = append(nonOverlaps, nonOverlap)
					}

					// Check non-overlapping region
					// before end key.
					//  ...----parentJoinSpan----|
					//		  ...----childSpan----|
					if bytes.Compare(parentJoinSpan.EndKey, childSpan.EndKey) < 0 {
						overlap, nonOverlap = overlap.SplitOnKey(parentJoinSpan.EndKey)
						nonOverlaps = append(nonOverlaps, nonOverlap)
					}

					// Map the overlap region to the
					// partition/node of the
					// parentJoinSpan.
					mappedSpans[parentPart.Node] = append(mappedSpans[parentPart.Node], overlap)

					return nonOverlaps
				}
			}
		}

		// There was no corresponding parentJoinSpan for this
		// childSpan.  We simply map childSpan back to its current
		// partition/node.
		mappedSpans[curNodeID] = append(mappedSpans[curNodeID], childSpan)

		return nonOverlaps
	}

	// Buffer to store spans that still need to be mapped.
	// It is initialized with the initial childSpan and may be populated
	// with non-overlapping sub-spans as mapAndSplit is invoked.
	// Note this is unbounded since a mapAndSplit of one childSpan can
	// cause two non-overlapping spans to be generated.
	// We recurse on the non-overlapping spans until none are left before
	// moving on to the next childSpan.
	spansLeft := make(roachpb.Spans, 0, 2)
	for _, childPart := range childSpans {
		for _, childSpan := range childPart.Spans {
			spansLeft = append(spansLeft, childSpan)
			for len(spansLeft) > 0 {
				// Copy out the last span in spansLeft to
				// mapAndSplit.
				spanToMap := spansLeft[len(spansLeft)-1]
				// Discard the element from spansLeft and
				// reclaim one buffer space.
				spansLeft = spansLeft[:len(spansLeft)-1]
				// We map every child span to its
				// corresponding parent span.
				// Splitting the child span may be
				// necessary which may produce up to two
				// non-overlapping sub-spans that are
				// appended to spansLeft.
				spansLeft = mapAndSplit(childPart.Node, spanToMap, spansLeft)
			}
		}
	}

	// It's possible from the mapAndSplit logic that we end up with
	// adjacent spans on the same node. We want to clean this up by
	// merging them.
	alignedDescSpans := make(sortedSpanPartitions, 0, len(mappedSpans))
	for nodeID, spans := range mappedSpans {
		spans, _ = roachpb.MergeSpans(spans)
		alignedDescSpans = append(
			alignedDescSpans,
			SpanPartition{
				Node:  nodeID,
				Spans: spans,
			},
		)
	}

	sort.Sort(alignedDescSpans)

	return alignedDescSpans, nil
}

// The derivation of the "join span" for a parent span is as follows (see
// comment above alignInterleaveSpans for why this is needed):
//
//   1. Start key of join span (the first parent key in parentSpan)
//
//      Take the maximalJoinPrefix (MJP) of parentSpan.Key. If the MJP Is
//      the same with parentSpan.Key (no truncation occurred), then it is also
//      the join span start key (examples A, B above).
//      Otherwise, the parentSpan.Key contains more than parent keys, and
//      because child rows come after parent rows, the join span start key is
//      the PrefixEnd() of the MJP (examples C, D).
//
//   2. End key of the join span: the next parent key after the last parent key
//      in parentSpan (it needs to be the next key because child rows come after
//      the parent rows).
//
//      Take the maximalJoinPrefix (MJP) of parentSpan.EndKey. If the MJP
//      is the same with parentSpan.EndKey (no truncation occurred), then it is
//      also the join span end key (examples A, C).
//      Otherwise, parentSpan.EndKey contains more than parent keys and needs to
//      be extended to include all child rows for the last parent row; the join
//      span end key is the PrefixEnd() of the MJP (examples B, D).
//
// To illustrate, we'll use some examples of parent spans (/table/index omitted
// from keys):
//   A. /1 - /3
//      This span contains parent rows with primary keys 1, 2, and all
//      corresponding child rows. The join span is the same: /1 - /3.
//
//   B. /1 - /3/#/1
//      This span contains parent rows with primary key 1, 2, 3 and all child
//      rows corresponding to 1, 2 (note that /3/#/1 comes after all the parent
//      rows with 3 but before all corresponding child rows). The join span is:
//      /1 - /4.
//
//   C. /1/#/1 - /4
//      This span contains parent rows with primary key 2, 3 and all child rows
//      corresponding to 1, 2, 3. The join span is: /2 - /4.
//
//   D. /1/#/1 - /2/#/1
//      This span contains the parent row with primary key 2 and all child rows
//      corresponding to 1, 2. The join span is: /2 - /3.
//
// The corresponding joinSpans for a set of parentSpans is disjoint if and only
// if the parentSpans are disjoint in terms of the parent rows.
// That is, as long as only 1 node reads a given parent row for all parent
// rows, the joinSpans are guaranteed to be non-overlapping.
// End keys are only pushed forward to the next parent row if the span contains
// the previous parent row.
// Since the previous row is read on that one node, it is not possible for the
// subsequent span on a different node to contain the previous row.
// The start key will be pushed forward to at least the next row, which
// maintains the disjoint property.
func joinSpans(n *joinNode, parentSpans []SpanPartition) ([]SpanPartition, error) {
	joinSpans := make([]SpanPartition, len(parentSpans))

	parent, child := n.interleavedNodes()

	// Compute the join span for every parent span.
	for i, parentPart := range parentSpans {
		joinSpans[i].Node = parentPart.Node
		joinSpans[i].Spans = make(roachpb.Spans, len(parentPart.Spans))

		for j, parentSpan := range parentPart.Spans {
			// Step 1: start key.
			joinSpanStartKey, startTruncated, err := maximalJoinPrefix(parent, child, parentSpan.Key)
			if err != nil {
				return nil, err
			}
			if startTruncated {
				// parentSpan.Key is a child key.
				// Example C and D.
				joinSpanStartKey = joinSpanStartKey.PrefixEnd()
			}

			// Step 2: end key.
			joinSpanEndKey, endTruncated, err := maximalJoinPrefix(parent, child, parentSpan.EndKey)
			if err != nil {
				return nil, err
			}

			if endTruncated {
				// parentSpan.EndKey is a child key.
				// Example B and D.
				joinSpanEndKey = joinSpanEndKey.PrefixEnd()
			}

			// We don't need to check if joinSpanStartKey <
			// joinSpanEndKey since the invalid spans will be
			// ignored during Span.Overlaps.
			joinSpans[i].Spans[j] = roachpb.Span{
				Key:    joinSpanStartKey,
				EndKey: joinSpanEndKey,
			}
		}
	}

	return joinSpans, nil
}

func distsqlSetOpJoinType(setOpType tree.UnionType) sqlbase.JoinType {
	switch setOpType {
	case tree.ExceptOp:
		return sqlbase.ExceptAllJoin
	case tree.IntersectOp:
		return sqlbase.IntersectAllJoin
	default:
		panic(fmt.Sprintf("set op type %v unsupported by joins", setOpType))
	}
}

// getNodesOfRouters returns all nodes that routers are put on.
func getNodesOfRouters(
	routers []physicalplan.ProcessorIdx, processors []physicalplan.Processor,
) (nodes []roachpb.NodeID) {
	seen := make(map[roachpb.NodeID]struct{})
	for _, pIdx := range routers {
		n := processors[pIdx].Node
		if _, ok := seen[n]; !ok {
			seen[n] = struct{}{}
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func findJoinProcessorNodes(
	leftRouters, rightRouters []physicalplan.ProcessorIdx, processors []physicalplan.Processor,
) (nodes []roachpb.NodeID) {
	// TODO(radu): for now we run a join processor on every node that produces
	// data for either source. In the future we should be smarter here.
	return getNodesOfRouters(append(leftRouters, rightRouters...), processors)
}
