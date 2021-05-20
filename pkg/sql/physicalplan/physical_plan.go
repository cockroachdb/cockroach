// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package physicalplan

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Processor contains the information associated with a processor in a plan.
type Processor struct {
	// Node where the processor must be instantiated.
	Node roachpb.NodeID

	// Spec for the processor; note that the StreamEndpointSpecs in the input
	// synchronizers and output routers are not set until the end of the planning
	// process.
	Spec execinfrapb.ProcessorSpec
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

// PhysicalInfrastructure stores information about processors and streams.
// Multiple PhysicalPlans can be associated with the same PhysicalInfrastructure
// instance; this allows merging plans.
type PhysicalInfrastructure struct {
	// -- The following fields are immutable --

	FlowID        uuid.UUID
	GatewayNodeID roachpb.NodeID

	// -- The following fields are mutable --

	// Processors in the plan.
	Processors []Processor

	// LocalProcessors contains all of the planNodeToRowSourceWrappers that were
	// installed in this physical plan to wrap any planNodes that couldn't be
	// properly translated into DistSQL processors. This will be empty if no
	// wrapping had to happen.
	LocalProcessors []execinfra.LocalProcessor

	// Streams accumulates the streams in the plan - both local (intra-node) and
	// remote (inter-node); when we have a final plan, the streams are used to
	// generate processor input and output specs (see PopulateEndpoints).
	Streams []Stream

	// Used internally for numbering stages.
	stageCounter int32
}

// AddProcessor adds a processor and returns the index that can be used to refer
// to that processor.
func (p *PhysicalInfrastructure) AddProcessor(proc Processor) ProcessorIdx {
	idx := ProcessorIdx(len(p.Processors))
	p.Processors = append(p.Processors, proc)
	return idx
}

// AddLocalProcessor adds a local processor and returns the index that can be
// used to refer to that processor.
func (p *PhysicalInfrastructure) AddLocalProcessor(proc execinfra.LocalProcessor) int {
	idx := len(p.LocalProcessors)
	p.LocalProcessors = append(p.LocalProcessors, proc)
	return idx
}

// PhysicalPlan represents a network of processors and streams along with
// information about the results output by this network. The results come from
// unconnected output routers of a subset of processors; all these routers
// output the same kind of data (same schema).
type PhysicalPlan struct {
	*PhysicalInfrastructure

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

	// ResultColumns is the schema (result columns) of the rows produced by the
	// ResultRouters.
	ResultColumns colinfo.ResultColumns

	// MergeOrdering is the ordering guarantee for the result streams that must be
	// maintained when the streams eventually merge. The column indexes refer to
	// columns for the rows produced by ResultRouters.
	//
	// Empty when there is a single result router. The reason is that maintaining
	// an ordering sometimes requires to add columns to streams for the sole
	// reason of correctly merging the streams later (see AddProjection); we don't
	// want to pay this cost if we don't have multiple streams to merge.
	MergeOrdering execinfrapb.Ordering

	// TotalEstimatedScannedRows is the sum of the row count estimate of all the
	// table readers in the plan.
	// TODO(radu): move this field to PlanInfrastructure.
	TotalEstimatedScannedRows uint64

	// Distribution is the indicator of the distribution of the physical plan.
	// TODO(radu): move this field to PlanInfrastructure?
	Distribution PlanDistribution
}

// MakePhysicalInfrastructure initializes a PhysicalInfrastructure that can then
// be used with MakePhysicalPlan.
func MakePhysicalInfrastructure(
	flowID uuid.UUID, gatewayNodeID roachpb.NodeID,
) PhysicalInfrastructure {
	return PhysicalInfrastructure{
		FlowID:        flowID,
		GatewayNodeID: gatewayNodeID,
	}
}

// MakePhysicalPlan initializes a PhysicalPlan.
func MakePhysicalPlan(infra *PhysicalInfrastructure) PhysicalPlan {
	return PhysicalPlan{
		PhysicalInfrastructure: infra,
	}
}

// GetResultTypes returns the schema (column types) of the rows produced by the
// ResultRouters which *must* contain at least one index into Processors slice.
// This is aliased with ColumnTypes of the processor spec, so it must not be
// modified in-place during planning.
func (p *PhysicalPlan) GetResultTypes() []*types.T {
	if len(p.ResultRouters) == 0 {
		panic(errors.AssertionFailedf("unexpectedly no result routers in %v", *p))
	}
	return p.Processors[p.ResultRouters[0]].Spec.ResultTypes
}

// NewStage updates the distribution of the plan given the fact whether the new
// stage contains at least one processor planned on a remote node and returns a
// stage identifier of the new stage that can be used in processor specs.
func (p *PhysicalPlan) NewStage(containsRemoteProcessor bool) int32 {
	newStageDistribution := LocalPlan
	if containsRemoteProcessor {
		newStageDistribution = FullyDistributedPlan
	}
	if p.stageCounter == 0 {
		// This is the first stage of the plan, so we simply use the
		// distribution as is.
		p.Distribution = newStageDistribution
	} else {
		p.Distribution = p.Distribution.compose(newStageDistribution)
	}
	p.stageCounter++
	return p.stageCounter
}

// NewStageOnNodes is the same as NewStage but takes in the information about
// the nodes participating in the new stage and the gateway.
func (p *PhysicalPlan) NewStageOnNodes(nodes []roachpb.NodeID) int32 {
	return p.NewStageWithFirstNode(len(nodes), nodes[0])
}

// NewStageWithFirstNode is the same as NewStage but takes in the number of
// total nodes participating in the new stage and the first node's id.
func (p *PhysicalPlan) NewStageWithFirstNode(nNodes int, firstNode roachpb.NodeID) int32 {
	// We have a remote processor either when we have multiple nodes
	// participating in the stage or the single processor is scheduled not on
	// the gateway.
	return p.NewStage(nNodes > 1 || firstNode != p.GatewayNodeID /* containsRemoteProcessor */)
}

// SetMergeOrdering sets p.MergeOrdering.
func (p *PhysicalPlan) SetMergeOrdering(o execinfrapb.Ordering) {
	if len(p.ResultRouters) > 1 {
		p.MergeOrdering = o
	} else {
		p.MergeOrdering = execinfrapb.Ordering{}
	}
}

// ProcessorCorePlacement indicates on which node a particular processor core
// needs to be planned.
type ProcessorCorePlacement struct {
	NodeID roachpb.NodeID
	Core   execinfrapb.ProcessorCoreUnion
	// EstimatedRowCount, if set to non-zero, is the optimizer's guess of how
	// many rows will be emitted from this processor.
	EstimatedRowCount uint64
}

// AddNoInputStage creates a stage of processors that don't have any input from
// the other stages (if such exist). nodes and cores must be a one-to-one
// mapping so that a particular processor core is planned on the appropriate
// node.
func (p *PhysicalPlan) AddNoInputStage(
	corePlacements []ProcessorCorePlacement,
	post execinfrapb.PostProcessSpec,
	outputTypes []*types.T,
	newOrdering execinfrapb.Ordering,
) {
	stageID := p.NewStageWithFirstNode(len(corePlacements), corePlacements[0].NodeID)
	p.ResultRouters = make([]ProcessorIdx, len(corePlacements))
	for i := range p.ResultRouters {
		proc := Processor{
			Node: corePlacements[i].NodeID,
			Spec: execinfrapb.ProcessorSpec{
				Core: corePlacements[i].Core,
				Post: post,
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID:           stageID,
				ResultTypes:       outputTypes,
				EstimatedRowCount: corePlacements[i].EstimatedRowCount,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	p.SetMergeOrdering(newOrdering)
}

// AddNoGroupingStage adds a processor for each result router, on the same node
// with the source of the stream; all processors have the same core. This is for
// stages that correspond to logical blocks that don't require any grouping
// (e.g. evaluator, sorting, etc).
func (p *PhysicalPlan) AddNoGroupingStage(
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []*types.T,
	newOrdering execinfrapb.Ordering,
) {
	p.AddNoGroupingStageWithCoreFunc(
		func(_ int, _ *Processor) execinfrapb.ProcessorCoreUnion { return core },
		post,
		outputTypes,
		newOrdering,
	)
}

// AddNoGroupingStageWithCoreFunc is like AddNoGroupingStage, but creates a core
// spec based on the input processor's spec.
func (p *PhysicalPlan) AddNoGroupingStageWithCoreFunc(
	coreFunc func(int, *Processor) execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []*types.T,
	newOrdering execinfrapb.Ordering,
) {
	// New stage has the same distribution as the previous one, so we need to
	// figure out whether the last stage contains a remote processor.
	stageID := p.NewStage(p.IsLastStageDistributed())
	prevStageResultTypes := p.GetResultTypes()
	for i, resultProc := range p.ResultRouters {
		prevProc := &p.Processors[resultProc]

		proc := Processor{
			Node: prevProc.Node,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					Type:        execinfrapb.InputSyncSpec_PARALLEL_UNORDERED,
					ColumnTypes: prevStageResultTypes,
				}},
				Core: coreFunc(int(resultProc), prevProc),
				Post: post,
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID:     stageID,
				ResultTypes: outputTypes,
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
	p.SetMergeOrdering(newOrdering)
}

// MergeResultStreams connects a set of resultRouters to a synchronizer. The
// synchronizer is configured with the provided ordering.
// forceSerialization determines whether the streams are forced to be serialized
// (i.e. whether we don't want any parallelism).
func (p *PhysicalPlan) MergeResultStreams(
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering execinfrapb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
	forceSerialization bool,
) {
	proc := &p.Processors[destProcessor]
	if len(ordering.Columns) > 0 && len(resultRouters) > 1 {
		proc.Spec.Input[destInput].Type = execinfrapb.InputSyncSpec_ORDERED
		proc.Spec.Input[destInput].Ordering = ordering
	} else {
		if forceSerialization && len(resultRouters) > 1 {
			// If we're forced to serialize the streams and we have multiple
			// result routers, we have to use a slower serial unordered sync.
			proc.Spec.Input[destInput].Type = execinfrapb.InputSyncSpec_SERIAL_UNORDERED
		} else {
			proc.Spec.Input[destInput].Type = execinfrapb.InputSyncSpec_PARALLEL_UNORDERED
		}
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
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []*types.T,
) {
	proc := Processor{
		Node: nodeID,
		Spec: execinfrapb.ProcessorSpec{
			Input: []execinfrapb.InputSyncSpec{{
				// The other fields will be filled in by mergeResultStreams.
				ColumnTypes: p.GetResultTypes(),
			}},
			Core: core,
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
			}},
			// We're planning a single processor on the node nodeID, so we'll
			// have a remote processor only when the node is different from the
			// gateway.
			StageID:     p.NewStage(nodeID != p.GatewayNodeID),
			ResultTypes: outputTypes,
		},
	}

	pIdx := p.AddProcessor(proc)

	// Connect the result routers to the processor.
	p.MergeResultStreams(p.ResultRouters, 0, p.MergeOrdering, pIdx, 0, false /* forceSerialization */)

	// We now have a single result stream.
	p.ResultRouters = p.ResultRouters[:1]
	p.ResultRouters[0] = pIdx

	p.MergeOrdering = execinfrapb.Ordering{}
}

// EnsureSingleStreamOnGateway ensures that there is only one stream on the
// gateway node in the plan (meaning it possibly merges multiple streams or
// brings a single stream from a remote node to the gateway).
func (p *PhysicalPlan) EnsureSingleStreamOnGateway() {
	// If we don't already have a single result router on the gateway, add a
	// single grouping stage.
	if len(p.ResultRouters) != 1 ||
		p.Processors[p.ResultRouters[0]].Node != p.GatewayNodeID {
		p.AddSingleGroupStage(
			p.GatewayNodeID,
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			execinfrapb.PostProcessSpec{},
			p.GetResultTypes(),
		)
		if len(p.ResultRouters) != 1 || p.Processors[p.ResultRouters[0]].Node != p.GatewayNodeID {
			panic("ensuring a single stream on the gateway failed")
		}
	}
	// We now must have a single stream in the whole physical plan, so there is no
	// ordering to be maintained for the merge of multiple streams. This also
	// adheres to the comment on p.MergeOrdering.
	p.MergeOrdering = execinfrapb.Ordering{}
}

// CheckLastStagePost checks that the processors of the last stage of the
// PhysicalPlan have identical post-processing, returning an error if not.
func (p *PhysicalPlan) CheckLastStagePost() error {
	post := p.Processors[p.ResultRouters[0]].Spec.Post

	// All processors of a stage should be identical in terms of post-processing;
	// verify this assumption.
	for i := 1; i < len(p.ResultRouters); i++ {
		pi := &p.Processors[p.ResultRouters[i]].Spec.Post
		if pi.Projection != post.Projection ||
			len(pi.OutputColumns) != len(post.OutputColumns) ||
			len(pi.RenderExprs) != len(post.RenderExprs) {
			return errors.Errorf("inconsistent post-processing: %v vs %v", post, pi)
		}
		for j, col := range pi.OutputColumns {
			if col != post.OutputColumns[j] {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post, pi)
			}
		}
		for j, col := range pi.RenderExprs {
			if col != post.RenderExprs[j] {
				return errors.Errorf("inconsistent post-processing: %v vs %v", post, pi)
			}
		}
	}

	return nil
}

// GetLastStagePost returns the PostProcessSpec for the processors in the last
// stage (ResultRouters).
func (p *PhysicalPlan) GetLastStagePost() execinfrapb.PostProcessSpec {
	if err := p.CheckLastStagePost(); err != nil {
		panic(err)
	}
	return p.Processors[p.ResultRouters[0]].Spec.Post
}

// SetLastStagePost changes the PostProcess spec of the processors in the last
// stage (ResultRouters).
// The caller must update the ordering via SetOrdering.
func (p *PhysicalPlan) SetLastStagePost(post execinfrapb.PostProcessSpec, outputTypes []*types.T) {
	for _, pIdx := range p.ResultRouters {
		p.Processors[pIdx].Spec.Post = post
		p.Processors[pIdx].Spec.ResultTypes = outputTypes
	}
}

func isIdentityProjection(columns []uint32, numExistingCols int) bool {
	if len(columns) != numExistingCols {
		return false
	}
	for i, c := range columns {
		if c != uint32(i) {
			return false
		}
	}
	return true
}

// AddProjection applies a projection to a plan. The new plan outputs the
// columns of the old plan as listed in the slice. newMergeOrdering must refer
// to the columns **after** the projection is applied.
//
// The PostProcessSpec may not be updated if the resulting projection keeps all
// the columns in their original order.
//
// Note: the columns slice is relinquished to this function, which can modify it
// or use it directly in specs.
func (p *PhysicalPlan) AddProjection(columns []uint32, newMergeOrdering execinfrapb.Ordering) {
	// Set the merge ordering early since we might short-circuit.
	p.SetMergeOrdering(newMergeOrdering)

	// If the projection we are trying to apply projects every column, don't
	// update the spec.
	if isIdentityProjection(columns, len(p.GetResultTypes())) {
		return
	}

	newResultTypes := make([]*types.T, len(columns))
	for i, c := range columns {
		newResultTypes[i] = p.GetResultTypes()[c]
	}

	post := p.GetLastStagePost()

	if post.RenderExprs != nil {
		// Apply the projection to the existing rendering; in other words, keep
		// only the renders needed by the new output columns, and reorder them
		// accordingly.
		oldRenders := post.RenderExprs
		post.RenderExprs = make([]execinfrapb.Expression, len(columns))
		for i, c := range columns {
			post.RenderExprs[i] = oldRenders[c]
		}
	} else {
		// There is no existing rendering; we can use OutputColumns to set the
		// projection.
		if post.Projection {
			// We already had a projection: compose it with the new one.
			for i, c := range columns {
				columns[i] = post.OutputColumns[c]
			}
		}
		post.OutputColumns = columns
		post.Projection = true
	}

	p.SetLastStagePost(post, newResultTypes)
}

// exprColumn returns the column that is referenced by the expression, if the
// expression is just an IndexedVar.
//
// See MakeExpression for a description of indexVarMap.
func exprColumn(expr tree.TypedExpr, indexVarMap []int) (int, bool) {
	v, ok := expr.(*tree.IndexedVar)
	if !ok {
		return -1, false
	}
	return indexVarMap[v.Idx], true
}

// AddRendering adds a rendering (expression evaluation) to the output of a
// plan. The rendering is achieved either through an adjustment on the last
// stage post-process spec, or via a new stage.
//
// newMergeOrdering must refer to the columns **after** the rendering is
// applied.
//
// See MakeExpression for a description of indexVarMap.
func (p *PhysicalPlan) AddRendering(
	exprs []tree.TypedExpr,
	exprCtx ExprContext,
	indexVarMap []int,
	outTypes []*types.T,
	newMergeOrdering execinfrapb.Ordering,
) error {
	// First check if we need an Evaluator, or we are just shuffling values. We
	// also check if the rendering is a no-op ("identity").
	needRendering := false
	identity := len(exprs) == len(p.GetResultTypes())

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
			return nil
		}
		// We don't need to do any rendering: the expressions effectively describe
		// just a projection.
		cols := make([]uint32, len(exprs))
		for i, e := range exprs {
			streamCol, _ := exprColumn(e, indexVarMap)
			if streamCol == -1 {
				panic(errors.AssertionFailedf("render %d refers to column not in source: %s", i, e))
			}
			cols[i] = uint32(streamCol)
		}
		p.AddProjection(cols, newMergeOrdering)
		return nil
	}

	post := p.GetLastStagePost()
	if len(post.RenderExprs) > 0 {
		post = execinfrapb.PostProcessSpec{}
		// The last stage contains render expressions. The new renders refer to
		// the output of these, so we need to add another "no-op" stage to which
		// to attach the new rendering.
		p.AddNoGroupingStage(
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			post,
			p.GetResultTypes(),
			p.MergeOrdering,
		)
	}

	compositeMap := indexVarMap
	if post.Projection {
		compositeMap = reverseProjection(post.OutputColumns, indexVarMap)
	}
	post.RenderExprs = make([]execinfrapb.Expression, len(exprs))
	for i, e := range exprs {
		var err error
		post.RenderExprs[i], err = MakeExpression(e, exprCtx, compositeMap)
		if err != nil {
			return err
		}
	}
	p.SetMergeOrdering(newMergeOrdering)
	post.Projection = false
	post.OutputColumns = nil
	p.SetLastStagePost(post, outTypes)
	return nil
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
//                 schema to the internal schema of a processor.
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
func (p *PhysicalPlan) AddFilter(
	expr tree.TypedExpr, exprCtx ExprContext, indexVarMap []int,
) error {
	if expr == nil {
		return errors.Errorf("nil filter")
	}
	filter, err := MakeExpression(expr, exprCtx, indexVarMap)
	if err != nil {
		return err
	}
	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Filterer: &execinfrapb.FiltererSpec{
			Filter: filter,
		}},
		execinfrapb.PostProcessSpec{},
		p.GetResultTypes(),
		p.MergeOrdering,
	)
	return nil
}

// AddLimit adds a limit and/or offset to the results of the current plan. If
// there are multiple result streams, they are joined into a single processor
// that is placed on the given node.
//
// For no limit, count should be MaxInt64.
func (p *PhysicalPlan) AddLimit(count int64, offset int64, exprCtx ExprContext) error {
	if count < 0 {
		return errors.Errorf("negative limit")
	}
	if offset < 0 {
		return errors.Errorf("negative offset")
	}
	// limitZero is set to true if the limit is a legitimate LIMIT 0 requested by
	// the user. This needs to be tracked as a separate condition because DistSQL
	// uses count=0 to mean no limit, not a limit of 0. Normally, DistSQL will
	// short circuit 0-limit plans, but wrapped local planNodes sometimes need to
	// be fully-executed despite having 0 limit, so if we do in fact have a
	// limit-0 case when there's local planNodes around, we add an empty plan
	// instead of completely eliding the 0-limit plan.
	limitZero := false
	if count == 0 {
		count = 1
		limitZero = true
	}

	if len(p.ResultRouters) == 1 {
		// We only have one processor producing results. Just update its PostProcessSpec.
		// SELECT FROM (SELECT OFFSET 10 LIMIT 1000) OFFSET 5 LIMIT 20 becomes
		// SELECT OFFSET 10+5 LIMIT min(1000, 20).
		post := p.GetLastStagePost()
		if offset != 0 {
			switch {
			case post.Limit > 0 && post.Limit <= uint64(offset):
				// The previous limit is not enough to reach the offset; we know there
				// will be no results. For example:
				//   SELECT * FROM (SELECT * FROM .. LIMIT 5) OFFSET 10
				count = 1
				limitZero = true

			case post.Offset > math.MaxUint64-uint64(offset):
				// The sum of the offsets would overflow. There is no way we'll ever
				// generate enough rows.
				count = 1
				limitZero = true

			default:
				// If we're collapsing an offset into a stage that already has a limit,
				// we have to be careful, since offsets always are applied first, before
				// limits. So, if the last stage already has a limit, we subtract the
				// offset from that limit to preserve correctness.
				//
				// As an example, consider the requirement of applying an offset of 3 on
				// top of a limit of 10. In this case, we need to emit 7 result rows. But
				// just propagating the offset blindly would produce 10 result rows, an
				// incorrect result.
				post.Offset += uint64(offset)
				if post.Limit > 0 {
					// Note that this can't fall below 1 - we would have already caught this
					// case above.
					post.Limit -= uint64(offset)
				}
			}
		}
		if count != math.MaxInt64 && (post.Limit == 0 || post.Limit > uint64(count)) {
			post.Limit = uint64(count)
		}
		p.SetLastStagePost(post, p.GetResultTypes())
		if limitZero {
			if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil); err != nil {
				return err
			}
		}
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
			p.SetLastStagePost(post, p.GetResultTypes())
		}
	}

	post := execinfrapb.PostProcessSpec{
		Offset: uint64(offset),
	}
	if count != math.MaxInt64 {
		post.Limit = uint64(count)
	}
	p.AddSingleGroupStage(
		p.GatewayNodeID,
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		post,
		p.GetResultTypes(),
	)
	if limitZero {
		if err := p.AddFilter(tree.DBoolFalse, exprCtx, nil); err != nil {
			return err
		}
	}
	return nil
}

// PopulateEndpoints processes p.Streams and adds the corresponding
// StreamEndpointSpecs to the processors' input and output specs. This should be
// used when the plan is completed and ready to be executed.
func (p *PhysicalPlan) PopulateEndpoints() {
	// Note: instead of using p.Streams, we could fill in the input/output specs
	// directly throughout the planning code, but this makes the rest of the code
	// a bit simpler.
	for sIdx, s := range p.Streams {
		p1 := &p.Processors[s.SourceProcessor]
		p2 := &p.Processors[s.DestProcessor]
		endpoint := execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(sIdx)}
		if p1.Node == p2.Node {
			endpoint.Type = execinfrapb.StreamEndpointSpec_LOCAL
		} else {
			endpoint.Type = execinfrapb.StreamEndpointSpec_REMOTE
		}
		if endpoint.Type == execinfrapb.StreamEndpointSpec_REMOTE {
			endpoint.OriginNodeID = p1.Node
			endpoint.TargetNodeID = p2.Node
		}
		p2.Spec.Input[s.DestInput].Streams = append(p2.Spec.Input[s.DestInput].Streams, endpoint)

		router := &p1.Spec.Output[0]
		// We are about to put this stream on the len(router.Streams) position in
		// the router; verify this matches the sourceRouterSlot. We expect it to
		// because the streams should be in order; if that assumption changes we can
		// reorder them here according to sourceRouterSlot.
		if len(router.Streams) != s.SourceRouterSlot {
			panic(errors.AssertionFailedf(
				"sourceRouterSlot mismatch: %d, expected %d", len(router.Streams), s.SourceRouterSlot,
			))
		}
		router.Streams = append(router.Streams, endpoint)
	}
}

// GenerateFlowSpecs takes a plan (with populated endpoints) and generates the
// set of FlowSpecs (one per node involved in the plan).
//
// gateway is the current node's NodeID.
func (p *PhysicalPlan) GenerateFlowSpecs() map[roachpb.NodeID]*execinfrapb.FlowSpec {
	flowID := execinfrapb.FlowID{
		UUID: p.FlowID,
	}
	flows := make(map[roachpb.NodeID]*execinfrapb.FlowSpec, 1)

	for _, proc := range p.Processors {
		flowSpec, ok := flows[proc.Node]
		if !ok {
			flowSpec = NewFlowSpec(flowID, p.GatewayNodeID)
			flows[proc.Node] = flowSpec
		}
		flowSpec.Processors = append(flowSpec.Processors, proc.Spec)
	}
	return flows
}

// SetRowEstimates updates p according to the row estimates of left and right
// plans.
func (p *PhysicalPlan) SetRowEstimates(left, right *PhysicalPlan) {
	p.TotalEstimatedScannedRows = left.TotalEstimatedScannedRows + right.TotalEstimatedScannedRows
}

// MergePlans is used when merging two plans into a new plan. All plans must
// share the same PlanInfrastructure.
func MergePlans(
	mergedPlan *PhysicalPlan,
	left, right *PhysicalPlan,
	leftPlanDistribution, rightPlanDistribution PlanDistribution,
) {
	if mergedPlan.PhysicalInfrastructure != left.PhysicalInfrastructure ||
		mergedPlan.PhysicalInfrastructure != right.PhysicalInfrastructure {
		panic(errors.AssertionFailedf("can only merge plans that share infrastructure"))
	}
	mergedPlan.SetRowEstimates(left, right)
	mergedPlan.Distribution = leftPlanDistribution.compose(rightPlanDistribution)
}

// AddJoinStage adds join processors at each of the specified nodes, and wires
// the left and right-side outputs to these processors.
func (p *PhysicalPlan) AddJoinStage(
	nodes []roachpb.NodeID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	leftEqCols, rightEqCols []uint32,
	leftTypes, rightTypes []*types.T,
	leftMergeOrd, rightMergeOrd execinfrapb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
	resultTypes []*types.T,
) {
	pIdxStart := ProcessorIdx(len(p.Processors))
	stageID := p.NewStageOnNodes(nodes)

	for _, n := range nodes {
		inputs := make([]execinfrapb.InputSyncSpec, 0, 2)
		inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: leftTypes})
		inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: rightTypes})

		proc := Processor{
			Node: n,
			Spec: execinfrapb.ProcessorSpec{
				Input:       inputs,
				Core:        core,
				Post:        post,
				Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID:     stageID,
				ResultTypes: resultTypes,
			},
		}
		p.Processors = append(p.Processors, proc)
	}

	if len(nodes) > 1 {
		// Parallel hash or merge join: we distribute rows (by hash of
		// equality columns) to len(nodes) join processors.

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: leftEqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: rightEqCols,
			}
		}
	}
	p.ResultRouters = p.ResultRouters[:0]

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0, false /* forceSerialization */)
		// Connect right routers to the processor's second input if it has one.
		p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1, false /* forceSerialization */)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

// AddStageOnNodes adds a stage of processors that take in a single input
// logical stream on the specified nodes and connects them to the previous
// stage via a hash router.
func (p *PhysicalPlan) AddStageOnNodes(
	nodes []roachpb.NodeID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	hashCols []uint32,
	inputTypes, resultTypes []*types.T,
	mergeOrd execinfrapb.Ordering,
	routers []ProcessorIdx,
) {
	pIdxStart := len(p.Processors)
	newStageID := p.NewStageOnNodes(nodes)

	for _, n := range nodes {
		proc := Processor{
			Node: n,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{
					{ColumnTypes: inputTypes},
				},
				Core:        core,
				Post:        post,
				Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID:     newStageID,
				ResultTypes: resultTypes,
			},
		}
		p.AddProcessor(proc)
	}

	if len(nodes) > 1 {
		// Set up the routers.
		for _, resultProc := range routers {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: hashCols,
			}
		}
	}

	// Connect the result streams to the processors.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := ProcessorIdx(pIdxStart + bucket)
		p.MergeResultStreams(routers, bucket, mergeOrd, pIdx, 0, false /* forceSerialization */)
	}

	// Set the new result routers.
	p.ResultRouters = p.ResultRouters[:0]
	for i := 0; i < len(nodes); i++ {
		p.ResultRouters = append(p.ResultRouters, ProcessorIdx(pIdxStart+i))
	}
}

// AddDistinctSetOpStage creates a distinct stage and a join stage to implement
// INTERSECT and EXCEPT plans.
//
// TODO(yuzefovich): If there's a strong key on the left or right side, we
// can elide the distinct stage on that side.
func (p *PhysicalPlan) AddDistinctSetOpStage(
	nodes []roachpb.NodeID,
	joinCore execinfrapb.ProcessorCoreUnion,
	distinctCores []execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	eqCols []uint32,
	leftTypes, rightTypes []*types.T,
	leftMergeOrd, rightMergeOrd execinfrapb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
	resultTypes []*types.T,
) {
	// Create distinct stages for the left and right sides, where left and right
	// sources are sent by hash to the node which will contain the join processor.
	// The distinct stage must be before the join stage for EXCEPT queries to
	// produce correct results (e.g., (VALUES (1),(1),(2)) EXCEPT (VALUES (1))
	// would return (1),(2) instead of (2) if there was no distinct processor
	// before the EXCEPT ALL join).
	distinctProcs := make(map[roachpb.NodeID][]ProcessorIdx)
	p.AddStageOnNodes(
		nodes, distinctCores[0], execinfrapb.PostProcessSpec{}, eqCols,
		leftTypes, leftTypes, leftMergeOrd, leftRouters,
	)
	for _, leftDistinctProcIdx := range p.ResultRouters {
		node := p.Processors[leftDistinctProcIdx].Node
		distinctProcs[node] = append(distinctProcs[node], leftDistinctProcIdx)
	}
	p.AddStageOnNodes(
		nodes, distinctCores[1], execinfrapb.PostProcessSpec{}, eqCols,
		rightTypes, rightTypes, rightMergeOrd, rightRouters,
	)
	for _, rightDistinctProcIdx := range p.ResultRouters {
		node := p.Processors[rightDistinctProcIdx].Node
		distinctProcs[node] = append(distinctProcs[node], rightDistinctProcIdx)
	}

	// Create a join stage, where the distinct processors on the same node are
	// connected to a join processor.
	joinStageID := p.NewStageOnNodes(nodes)
	p.ResultRouters = p.ResultRouters[:0]

	for _, n := range nodes {
		proc := Processor{
			Node: n,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:        joinCore,
				Post:        post,
				Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID:     joinStageID,
				ResultTypes: resultTypes,
			},
		}
		pIdx := p.AddProcessor(proc)

		for side, distinctProc := range distinctProcs[n] {
			p.Streams = append(p.Streams, Stream{
				SourceProcessor:  distinctProc,
				SourceRouterSlot: 0,
				DestProcessor:    pIdx,
				DestInput:        side,
			})
		}

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

// EnsureSingleStreamPerNode goes over the ResultRouters and merges any group of
// routers that are on the same node, using a no-op processor.
// forceSerialization determines whether the streams are forced to be serialized
// (i.e. whether we don't want any parallelism).
//
// TODO(radu): a no-op processor is not ideal if the next processor is on the
// same node. A fix for that is much more complicated, requiring remembering
// extra state in the PhysicalPlan.
func (p *PhysicalPlan) EnsureSingleStreamPerNode(
	forceSerialization bool, post execinfrapb.PostProcessSpec,
) {
	// Fast path - check if we need to do anything.
	var nodes util.FastIntSet
	var foundDuplicates bool
	for _, pIdx := range p.ResultRouters {
		proc := &p.Processors[pIdx]
		if nodes.Contains(int(proc.Node)) {
			foundDuplicates = true
			break
		}
		nodes.Add(int(proc.Node))
	}
	if !foundDuplicates {
		return
	}
	streams := make([]ProcessorIdx, 0, 2)

	for i := 0; i < len(p.ResultRouters); i++ {
		pIdx := p.ResultRouters[i]
		node := p.Processors[p.ResultRouters[i]].Node
		streams = append(streams[:0], pIdx)
		// Find all streams on the same node.
		for j := i + 1; j < len(p.ResultRouters); {
			if p.Processors[p.ResultRouters[j]].Node == node {
				streams = append(streams, p.ResultRouters[j])
				// Remove the stream.
				copy(p.ResultRouters[j:], p.ResultRouters[j+1:])
				p.ResultRouters = p.ResultRouters[:len(p.ResultRouters)-1]
			} else {
				j++
			}
		}
		if len(streams) == 1 {
			// Nothing to do for this node.
			continue
		}

		// Merge the streams into a no-op processor.
		proc := Processor{
			Node: node,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					// The other fields will be filled in by MergeResultStreams.
					ColumnTypes: p.GetResultTypes(),
				}},
				Post:        post,
				Core:        execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				ResultTypes: p.GetResultTypes(),
			},
		}
		mergedProcIdx := p.AddProcessor(proc)
		p.MergeResultStreams(streams, 0 /* sourceRouterSlot */, p.MergeOrdering, mergedProcIdx, 0 /* destInput */, forceSerialization)
		p.ResultRouters[i] = mergedProcIdx
	}
}

// GetLastStageDistribution returns the distribution *only* of the last stage.
// Note that if the last stage consists of a single processor planned on a
// remote node, such stage is considered distributed.
func (p *PhysicalPlan) GetLastStageDistribution() PlanDistribution {
	for i := range p.ResultRouters {
		if p.Processors[p.ResultRouters[i]].Node != p.GatewayNodeID {
			return FullyDistributedPlan
		}
	}
	return LocalPlan
}

// IsLastStageDistributed returns whether the last stage of processors is
// distributed (meaning that it contains at least one remote processor).
func (p *PhysicalPlan) IsLastStageDistributed() bool {
	return p.GetLastStageDistribution() != LocalPlan
}

// PlanDistribution describes the distribution of the physical plan.
type PlanDistribution int

const (
	// LocalPlan indicates that the whole plan is executed on the gateway node.
	LocalPlan PlanDistribution = iota

	// PartiallyDistributedPlan indicates that some parts of the plan are
	// distributed while other parts are not (due to limitations of DistSQL).
	// Note that such plans can only be created by distSQLSpecExecFactory.
	//
	// An example of such plan is the plan with distributed scans that have a
	// filter which operates with an OID type (DistSQL currently doesn't
	// support distributed operations with such type). As a result, we end
	// up planning a noop processor on the gateway node that receives all
	// scanned rows from the remote nodes while performing the filtering
	// locally.
	PartiallyDistributedPlan

	// FullyDistributedPlan indicates that the whole plan is distributed.
	FullyDistributedPlan
)

// WillDistribute is a small helper that returns whether at least a part of the
// plan is distributed.
func (a PlanDistribution) WillDistribute() bool {
	return a != LocalPlan
}

func (a PlanDistribution) String() string {
	switch a {
	case LocalPlan:
		return "local"
	case PartiallyDistributedPlan:
		return "partial"
	case FullyDistributedPlan:
		return "full"
	default:
		panic(errors.AssertionFailedf("unsupported PlanDistribution %d", a))
	}
}

// compose returns the distribution indicator of a plan given indicators for
// two parts of it.
func (a PlanDistribution) compose(b PlanDistribution) PlanDistribution {
	if a != b {
		return PartiallyDistributedPlan
	}
	return a
}
