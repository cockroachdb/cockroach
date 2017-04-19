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

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package distsqlplan

import (
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

// Processor contains the information associated with a processor in a plan.
type Processor struct {
	// Node where the processor must be instantiated.
	Node roachpb.NodeID

	// Spec for the processor; note that the StreamEndpointSpecs in the input
	// synchronizers and output routers are not set until the end of the planning
	// process.
	Spec distsqlrun.ProcessorSpec
}

// ProcessorIdx identifies a processor by its index in PhysicalPlan.Processors.
type ProcessorIdx int

// Stream connects the output router of one processor to an input synchronizer
// of another processor.
type Stream struct {
	// SourceProcessor index (within the same plan).
	SourceProcessor ProcessorIdx

	// SourceRouterSlot identifies the position of this stream among the streams
	// that originate from the same router. This is important when routing by hash
	// where the order of the streams in the OutputRouterSpec matters.
	SourceRouterSlot int

	// DestProcessor index (within the same plan).
	DestProcessor ProcessorIdx

	// DestInput identifies the input of DestProcessor (some processors have
	// multiple inputs).
	DestInput int
}

// PhysicalPlan represents a network of processors and streams along with
// information about the results output by this network. The results come from
// unconnected output routers of a subset of processors; all these routers
// output the same kind of data (same schema).
type PhysicalPlan struct {
	// Processors in the plan.
	Processors []Processor

	// Streams accumulates the streams in the plan - both local (intra-node) and
	// remote (inter-node); when we have a final plan, the streams are used to
	// generate processor input and output specs (see PopulateEndpoints).
	Streams []Stream

	// ResultRouters identifies the output routers which output the results of the
	// plan. These are the routers to which we have to connect new streams in
	// order to extend the plan.
	//
	// The processors which have this routers are all part of the same "stage":
	// they have the same "schema" and PostProcessSpec.
	//
	// We assume all processors have a single output so we only need the processor
	// index.
	ResultRouters []ProcessorIdx

	// ResultTypes is the schema (column types) of the rows produced by the
	// ResultRouters.
	//
	// This is aliased with InputSyncSpec.ColumnTypes, so it must not be modified
	// in-place during planning.
	ResultTypes []sqlbase.ColumnType

	// MergeOrdering is the ordering guarantee for the result streams that must be
	// maintained when the streams eventually merge. The column indexes refer to
	// columns for the rows produced by ResultRouters.
	//
	// Empty when there is a single result router. The reason is that maintaining
	// an ordering sometimes requires to add columns to streams for the sole
	// reason of correctly merging the streams later (see AddProjection); we don't
	// want to pay this cost if we don't have multiple streams to merge.
	MergeOrdering distsqlrun.Ordering
}

// AddProcessor adds a processor to a PhysicalPlan and returns the index that
// can be used to refer to that processor.
func (p *PhysicalPlan) AddProcessor(proc Processor) ProcessorIdx {
	idx := ProcessorIdx(len(p.Processors))
	p.Processors = append(p.Processors, proc)
	return idx
}

// SetMergeOrdering sets p.MergeOrdering.
func (p *PhysicalPlan) SetMergeOrdering(o distsqlrun.Ordering) {
	if len(p.ResultRouters) > 1 {
		p.MergeOrdering = o
	} else {
		p.MergeOrdering = distsqlrun.Ordering{}
	}
}

// AddNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (p *PhysicalPlan) AddNoGroupingStage(
	core distsqlrun.ProcessorCoreUnion,
	post distsqlrun.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
	newOrdering distsqlrun.Ordering,
) {
	for i, resultProc := range p.ResultRouters {
		prevProc := &p.Processors[resultProc]

		proc := Processor{
			Node: prevProc.Node,
			Spec: distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{{
					Type:        distsqlrun.InputSyncSpec_UNORDERED,
					ColumnTypes: p.ResultTypes,
				}},
				Core: core,
				Post: post,
				Output: []distsqlrun.OutputRouterSpec{{
					Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
				}},
			},
		}

		pIdx := p.AddProcessor(proc)

		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			DestProcessor:    pIdx,
			SourceRouterSlot: 0,
			DestInput:        0,
		})

		p.ResultRouters[i] = pIdx
	}
	p.ResultTypes = outputTypes
	p.SetMergeOrdering(newOrdering)
}

// MergeResultStreams connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
func (p *PhysicalPlan) MergeResultStreams(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering distsqlrun.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
) {
	proc := &p.Processors[destProcessor]
	if len(ordering.Columns) == 0 || len(resultRouters) == 1 {
		proc.Spec.Input[destInput].Type = distsqlrun.InputSyncSpec_UNORDERED
	} else {
		proc.Spec.Input[destInput].Type = distsqlrun.InputSyncSpec_ORDERED
		proc.Spec.Input[destInput].Ordering = ordering
	}

	for _, resultProc := range resultRouters {
		p.Streams = append(p.Streams, Stream{
			SourceProcessor:  resultProc,
			SourceRouterSlot: sourceRouterSlot,
			DestProcessor:    destProcessor,
			DestInput:        destInput,
		})
	}
}

// AddSingleGroupStage adds a "single group" stage (one that cannot be
// parallelized) which consists of a single processor on the specified node. The
// previous stage (ResultRouters) are all connected to this processor.
func (p *PhysicalPlan) AddSingleGroupStage(
	nodeID roachpb.NodeID,
	core distsqlrun.ProcessorCoreUnion,
	post distsqlrun.PostProcessSpec,
	outputTypes []sqlbase.ColumnType,
) {
	proc := Processor{
		Node: nodeID,
		Spec: distsqlrun.ProcessorSpec{
			Input: []distsqlrun.InputSyncSpec{{
				// The other fields will be filled in by mergeResultStreams.
				ColumnTypes: p.ResultTypes,
			}},
			Core: core,
			Post: post,
			Output: []distsqlrun.OutputRouterSpec{{
				Type: distsqlrun.OutputRouterSpec_PASS_THROUGH,
			}},
		},
	}

	pIdx := p.AddProcessor(proc)

	// Connect the result routers to the processor.
	p.MergeResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0)

	// We now have a single result stream.
	p.ResultRouters = p.ResultRouters[:1]
	p.ResultRouters[0] = pIdx

	p.ResultTypes = outputTypes
	p.MergeOrdering = distsqlrun.Ordering{}
}

// GetLastStagePost returns the PostProcessSpec for the processors in the last
// stage (ResultRouters).
func (p *PhysicalPlan) GetLastStagePost() distsqlrun.PostProcessSpec {
	post := p.Processors[p.ResultRouters[0]].Spec.Post

	// All processors of a stage should be identical in terms of post-processing;
	// verify this assumption.
	for i := 1; i < len(p.ResultRouters); i++ {
		pi := &p.Processors[p.ResultRouters[i]].Spec.Post
		if pi.Filter != post.Filter || len(pi.OutputColumns) != len(post.OutputColumns) {
			panic(fmt.Sprintf("inconsistent post-processing: %v vs %v", post, pi))
		}
		for j, col := range pi.OutputColumns {
			if col != post.OutputColumns[j] {
				panic(fmt.Sprintf("inconsistent post-processing: %v vs %v", post, pi))
			}
		}
	}

	return post
}

// SetLastStagePost changes the PostProcess spec of the processors in the last
// stage (ResultRouters).
// The caller must update the ordering via SetOrdering.
func (p *PhysicalPlan) SetLastStagePost(
	post distsqlrun.PostProcessSpec, outputTypes []sqlbase.ColumnType,
) {
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post = post
	}
	p.ResultTypes = outputTypes
}

// AddProjection applies a projection to a plan. The new plan outputs the
// columns of the old plan as listed in the slice. The Ordering is updated;
// columns in the ordering are added to the projection as needed.
//
// Note: the columns slice is relinquished to this function, which can modify it
// or use it directly in specs.
func (p *PhysicalPlan) AddProjection(columns []uint32) {
	post := p.GetLastStagePost()

	// Update the ordering.
	if len(p.MergeOrdering.Columns) > 0 {
		newOrdering := make([]distsqlrun.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			// Look for the column in the new projection.
			found := -1
			for j, projCol := range columns {
				if projCol == c.ColIdx {
					found = j
				}
			}
			if found == -1 {
				// We have a column that is not in the projection but will be necessary
				// later when the streams are merged; add it.
				found = len(columns)
				columns = append(columns, c.ColIdx)
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	newResultTypes := make([]sqlbase.ColumnType, len(columns))
	for i, c := range columns {
		newResultTypes[i] = p.ResultTypes[c]
	}

	if post.RenderExprs != nil {
		// Apply the projection to the existing rendering; in other words, keep
		// only the renders needed by the new output columns, and reorder them
		// accordingly.
		oldRenders := post.RenderExprs
		post.RenderExprs = make([]distsqlrun.Expression, len(columns))
		for i, c := range columns {
			post.RenderExprs[i] = oldRenders[c]
		}
	} else {
		// There is no existing rendering; we can use OutputColumns to set the
		// projection.
		if post.OutputColumns != nil {
			// We already had a projection: compose it with the new one.
			for i, c := range columns {
				columns[i] = post.OutputColumns[c]
			}
		}
		post.OutputColumns = columns
	}

	p.SetLastStagePost(post, newResultTypes)
}

// exprColumn returns the column that is referenced by the expression, if the
// expression is just an IndexedVar.
//
// See MakeExpression for a description of indexVarMap.
func exprColumn(expr parser.TypedExpr, indexVarMap []int) (int, bool) {
	v, ok := expr.(*parser.IndexedVar)
	if !ok {
		return -1, false
	}
	return indexVarMap[v.Idx], true
}

// AddRendering adds a rendering (expression evaluation) to the output of a
// plan. The rendering is achieved either through an adjustment on the last
// stage post-process spec, or via a new stage.
//
// The Ordering is updated; columns in the ordering are added to the render
// expressions as necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddRendering(
	exprs []parser.TypedExpr, indexVarMap []int, outTypes []sqlbase.ColumnType,
) {
	// First check if we need an Evaluator, or we are just shuffling values. We
	// also check if the rendering is a no-op ("identity").
	needRendering := false
	identity := (len(exprs) == len(p.ResultTypes))

	for exprIdx, e := range exprs {
		varIdx, ok := exprColumn(e, indexVarMap)
		if !ok {
			needRendering = true
			break
		}
		identity = identity && (varIdx == exprIdx)
	}

	if !needRendering {
		if identity {
			// Nothing to do.
			return
		}
		// We don't need to do any rendering: the expressions effectively describe
		// just a projection.
		cols := make([]uint32, len(exprs))
		for i, e := range exprs {
			streamCol, _ := exprColumn(e, indexVarMap)
			if streamCol == -1 {
				panic(fmt.Sprintf("render %d refers to column not in source: %s", i, e))
			}
			cols[i] = uint32(streamCol)
		}
		p.AddProjection(cols)
		return
	}

	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 {
		post = distsqlrun.PostProcessSpec{}
		// The last stage contains render expressions. The new renders refer to
		// the output of these, so we need to add another "no-op" stage to which
		// to attach the new rendering.
		p.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := reverseProjection(post.OutputColumns, indexVarMap)
	post.RenderExprs = make([]distsqlrun.Expression, len(exprs))
	for i, e := range exprs {
		post.RenderExprs[i] = MakeExpression(e, compositeMap)
	}

	if len(p.MergeOrdering.Columns) > 0 {
		outTypes = outTypes[:len(outTypes):len(outTypes)]
		newOrdering := make([]distsqlrun.Ordering_Column, len(p.MergeOrdering.Columns))
		for i, c := range p.MergeOrdering.Columns {
			found := -1
			// Look for the column in the new projection.
			for exprIdx, e := range exprs {
				if varIdx, ok := exprColumn(e, indexVarMap); ok && varIdx == int(c.ColIdx) {
					found = exprIdx
					break
				}
			}
			if found == -1 {
				// We have a column that is not being rendered but will be necessary
				// later when the streams are merged; add it.

				// The new expression refers to column post.OutputColumns[c.ColIdx].
				internalColIdx := c.ColIdx
				if post.OutputColumns != nil {
					internalColIdx = post.OutputColumns[internalColIdx]
				}
				newExpr := MakeExpression(&parser.IndexedVar{Idx: int(internalColIdx)}, nil)

				found = len(post.RenderExprs)
				post.RenderExprs = append(post.RenderExprs, newExpr)
				outTypes = append(outTypes, p.ResultTypes[c.ColIdx])
			}
			newOrdering[i].ColIdx = uint32(found)
			newOrdering[i].Direction = c.Direction
		}
		p.MergeOrdering.Columns = newOrdering
	}

	post.OutputColumns = nil
	p.SetLastStagePost(post, outTypes)
}

// reverseProjection remaps expression variable indices to refer to internal
// columns (i.e. before post-processing) of a processor instead of output
// columns (i.e. after post-processing).
//
// Inputs:
//   indexVarMap is a mapping from columns that appear in an expression
//               (planNode columns) to columns in the output stream of a
//               processor.
//   outputColumns is the list of output columns in the processor's
//                 PostProcessSpec; it is effectively a mapping from the output
//                 schema to the internal schema of a processor. If nil, an
//                 identity mapping is assumed.
//
// Result: a "composite map" that maps the planNode columns to the internal
//         columns of the processor.
//
// For efficiency, the indexVarMap and the resulting map are represented as
// slices, with missing elements having values -1.
//
// Used when adding expressions (filtering, rendering) to a processor's
// PostProcessSpec. For example:
//
//  TableReader // table columns A,B,C,D
//  Internal schema (before post-processing): A, B, C, D
//  OutputColumns:  [1 3]
//  Output schema (after post-processing): B, D
//
//  Expression "B < D" might be represented as:
//    IndexedVar(4) < IndexedVar(1)
//  with associated indexVarMap:
//    [-1 1 -1 -1 0]  // 1->1, 4->0
//  This is effectively equivalent to "IndexedVar(0) < IndexedVar(1)"; 0 means
//  the first output column (B), 1 means the second output column (D).
//
//  To get an index var map that refers to the internal schema:
//    reverseProjection(
//      [1 3],           // OutputColumns
//      [-1 1 -1 -1 0],
//    ) =
//      [-1 3 -1 -1 1]   // 1->3, 4->1
//  This is effectively equivalent to "IndexedVar(1) < IndexedVar(3)"; 1
//  means the second internal column (B), 3 means the fourth internal column
//  (D).
func reverseProjection(outputColumns []uint32, indexVarMap []int) []int {
	if indexVarMap == nil {
		panic("no indexVarMap")
	}
	if len(outputColumns) == 0 {
		// No projection.
		return indexVarMap
	}
	compositeMap := make([]int, len(indexVarMap))
	for i, col := range indexVarMap {
		if col == -1 {
			compositeMap[i] = -1
		} else {
			compositeMap[i] = int(outputColumns[col])
		}
	}
	return compositeMap
}

// AddFilter adds a filter on the output of a plan. The filter is added either
// as a post-processing step to the last stage or to a new "no-op" stage, as
// necessary.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddFilter(expr parser.TypedExpr, indexVarMap []int) {
	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 || post.Offset != 0 || post.Limit != 0 {
		// The last stage contains render expressions or a limit. The filter refers
		// to the output as described by the existing spec, so we need to add
		// another "no-op" stage to which to attach the filter.
		//
		// In general, we might be able to push the filter "through" the rendering;
		// but the higher level planning code should figure this out when
		// propagating filters.
		post = distsqlrun.PostProcessSpec{}
		p.AddNoGroupingStage(
			distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
			post,
			p.ResultTypes,
			p.MergeOrdering,
		)
	}

	compositeMap := reverseProjection(post.OutputColumns, indexVarMap)
	filter := MakeExpression(expr, compositeMap)
	if post.Filter.Expr != "" {
		filter.Expr = fmt.Sprintf("(%s) AND (%s)", post.Filter.Expr, filter.Expr)
	}
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post.Filter = filter
	}
}

// emptyPlan creates a plan with a single processor that generates no rows; the
// output stream has the given types.
func emptyPlan(types []sqlbase.ColumnType, node roachpb.NodeID) PhysicalPlan {
	s := distsqlrun.ValuesCoreSpec{
		Columns: make([]distsqlrun.DatumInfo, len(types)),
	}
	for i, t := range types {
		s.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
		s.Columns[i].Type = t
	}

	return PhysicalPlan{
		Processors: []Processor{{
			Node: node,
			Spec: distsqlrun.ProcessorSpec{
				Core:   distsqlrun.ProcessorCoreUnion{Values: &s},
				Output: make([]distsqlrun.OutputRouterSpec, 1),
			},
		}},
		ResultRouters: []ProcessorIdx{0},
		ResultTypes:   types,
	}
}

// AddLimit adds a limit and/or offset to the results of the current plan. If
// there are multiple result streams, they are joined into a single processor
// that is placed on the given node.
//
// For no limit, count should be MaxInt64.
func (p *PhysicalPlan) AddLimit(count int64, offset int64, node roachpb.NodeID) error {
	if count < 0 {
		return errors.Errorf("negative limit")
	}
	if offset < 0 {
		return errors.Errorf("negative offset")
	}
	if count == 0 {
		*p = emptyPlan(p.ResultTypes, node)
		return nil
	}

	if len(p.ResultRouters) == 1 {
		// We only have one processor producing results. Just update its PostProcessSpec.
		post := p.GetLastStagePost()
		if offset != 0 {
			if post.Limit > 0 && post.Limit <= uint64(offset) {
				// The previous limit is not enough to reach the offset; we know there
				// will be no results. For example:
				//   SELECT * FROM (SELECT * FROM .. LIMIT 5) OFFSET 10
				// TODO(radu): perform this optimization while propagating filters
				// instead of having to detect it here.
				*p = emptyPlan(p.ResultTypes, node)
				return nil
			}
			post.Limit -= uint64(offset)
			post.Offset += uint64(offset)
		}
		if count != math.MaxInt64 && (post.Limit == 0 || post.Limit > uint64(count)) {
			post.Limit = uint64(count)
		}
		p.SetLastStagePost(post, p.ResultTypes)
		return nil
	}

	// We have multiple processors producing results. We will add a single
	// processor stage that limits. As an optimization, we also set a
	// "local" limit on each processor producing results.
	if count != math.MaxInt64 {
		post := p.GetLastStagePost()
		// If we have OFFSET 10 LIMIT 5, we may need as much as 15 rows from any
		// processor.
		localLimit := uint64(count + offset)
		if post.Limit == 0 || post.Limit > localLimit {
			post.Limit = localLimit
			p.SetLastStagePost(post, p.ResultTypes)
		}
	}

	post := distsqlrun.PostProcessSpec{
		Offset: uint64(offset),
	}
	if count != math.MaxInt64 {
		post.Limit = uint64(count)
	}
	p.AddSingleGroupStage(
		node,
		distsqlrun.ProcessorCoreUnion{Noop: &distsqlrun.NoopCoreSpec{}},
		post,
		p.ResultTypes,
	)
	return nil
}

// PopulateEndpoints processes p.Streams and adds the corresponding
// StreamEndpointSpecs to the processors' input and output specs. This should be
// used when the plan is completed and ready to be executed.
//
// The nodeAddresses map contains the address of all the nodes referenced in the
// plan.
func (p *PhysicalPlan) PopulateEndpoints(nodeAddresses map[roachpb.NodeID]string) {
	// Note: instead of using p.Streams, we could fill in the input/output specs
	// directly throughout the planning code, but this makes the rest of the code
	// a bit simpler.
	for sIdx, s := range p.Streams {
		p1 := &p.Processors[s.SourceProcessor]
		p2 := &p.Processors[s.DestProcessor]
		endpoint := distsqlrun.StreamEndpointSpec{StreamID: distsqlrun.StreamID(sIdx)}
		if p1.Node == p2.Node {
			endpoint.Type = distsqlrun.StreamEndpointSpec_LOCAL
		} else {
			endpoint.Type = distsqlrun.StreamEndpointSpec_REMOTE
		}
		p2.Spec.Input[s.DestInput].Streams = append(p2.Spec.Input[s.DestInput].Streams, endpoint)
		if endpoint.Type == distsqlrun.StreamEndpointSpec_REMOTE {
			var ok bool
			endpoint.TargetAddr, ok = nodeAddresses[p2.Node]
			if !ok {
				panic(fmt.Sprintf("node %d node in nodeAddresses map", p2.Node))
			}
		}

		router := &p1.Spec.Output[0]
		// We are about to put this stream on the len(router.Streams) position in
		// the router; verify this matches the sourceRouterSlot. We expect it to
		// because the streams should be in order; if that assumption changes we can
		// reorder them here according to sourceRouterSlot.
		if len(router.Streams) != s.SourceRouterSlot {
			panic(fmt.Sprintf(
				"sourceRouterSlot mismatch: %d, expected %d", len(router.Streams), s.SourceRouterSlot,
			))
		}
		router.Streams = append(router.Streams, endpoint)
	}
}

// GenerateFlowSpecs takes a plan (with populated endpoints) and generates the
// set of FlowSpecs (one per node involved in the plan).
func (p *PhysicalPlan) GenerateFlowSpecs() map[roachpb.NodeID]distsqlrun.FlowSpec {
	flowID := distsqlrun.FlowID{UUID: uuid.MakeV4()}
	flows := make(map[roachpb.NodeID]distsqlrun.FlowSpec)

	for _, proc := range p.Processors {
		flowSpec, ok := flows[proc.Node]
		if !ok {
			flowSpec = distsqlrun.FlowSpec{FlowID: flowID}
		}
		flowSpec.Processors = append(flowSpec.Processors, proc.Spec)
		flows[proc.Node] = flowSpec
	}
	return flows
}

// MergePlans merges the processors and streams of two plan into a new plan.
// The result routers for each side are also returned (they point at processors
// in the merged plan).
func MergePlans(
	left, right *PhysicalPlan,
) (mergedPlan PhysicalPlan, leftRouters []ProcessorIdx, rightRouters []ProcessorIdx) {
	mergedPlan.Processors = append(left.Processors, right.Processors...)
	rightProcStart := ProcessorIdx(len(left.Processors))

	mergedPlan.Streams = append(left.Streams, right.Streams...)

	// Update the processor indices in the right streams.
	for i := len(left.Streams); i < len(mergedPlan.Streams); i++ {
		mergedPlan.Streams[i].SourceProcessor += rightProcStart
		mergedPlan.Streams[i].DestProcessor += rightProcStart
	}

	leftRouters = left.ResultRouters
	rightRouters = append([]ProcessorIdx(nil), right.ResultRouters...)
	// Update the processor indices in the right routers.
	for i := range rightRouters {
		rightRouters[i] += rightProcStart
	}

	return mergedPlan, leftRouters, rightRouters
}
