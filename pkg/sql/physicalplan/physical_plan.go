// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file defines structures and basic functionality that is useful when
// building distsql plans. It does not contain the actual physical planning
// code.

package physicalplan

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Processor contains the information associated with a processor in a plan.
type Processor struct {
	// Node where the processor must be instantiated.
	SQLInstanceID base.SQLInstanceID

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

	FlowID               uuid.UUID
	GatewaySQLInstanceID base.SQLInstanceID

	// -- The following fields are mutable --

	// Processors in the plan.
	Processors []Processor

	// LocalProcessors contains all of the planNodeToRowSourceWrappers that were
	// installed in this physical plan to wrap any planNodes that couldn't be
	// properly translated into DistSQL processors. This will be empty if no
	// wrapping had to happen.
	LocalProcessors []execinfra.LocalProcessor

	// LocalVectorSources contains canned coldata.Batch's to be used as vector
	// engine input sources. This is currently used for COPY, eventually
	// should probably be replaced by a proper copy processor that
	// materializes coldata batches from pgwire stream in a distsql
	// processor itself but that might have to wait until we deprecate
	// non-atomic COPY support (maybe? a COPY distsql processor could just
	// just finish when running non atomic and N rows were inserted if we
	// could pull rows from the pgwire buffer across distsql executions).
	// In that case we wouldn't be plumbing coldata.Batch's here we'd be
	// plumbing "vector" sources which would be an interface with
	// implementations for COPY and other batch streams (ie prepared
	// batches). Use any to avoid creating unwanted package dependencies.
	LocalVectorSources map[int32]any

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

var infraPool = sync.Pool{
	New: func() interface{} {
		return &PhysicalInfrastructure{}
	},
}

// SerialStreamErrorSpec specifies when to error out serial unordered stream
// execution, and the error to use.
type SerialStreamErrorSpec struct {
	// SerialInputIdxExclusiveUpperBound indicates the InputIdx to error out on
	// should execution fail to halt prior to this input. This is only valid if
	// non-zero.
	SerialInputIdxExclusiveUpperBound uint32

	// ExceedsInputIdxExclusiveUpperBoundError is the error to return when
	// `SerialInputIdxExclusiveUpperBound` - 1 streams have been executed to
	// completion without satisfying the query.
	ExceedsInputIdxExclusiveUpperBoundError error
}

// NewPhysicalInfrastructure initializes a PhysicalInfrastructure that can then
// be used with MakePhysicalPlan.
func NewPhysicalInfrastructure(
	flowID uuid.UUID, gatewaySQLInstanceID base.SQLInstanceID,
) *PhysicalInfrastructure {
	infra := infraPool.Get().(*PhysicalInfrastructure)
	infra.FlowID = flowID
	infra.GatewaySQLInstanceID = gatewaySQLInstanceID
	return infra
}

// Release resets the object and puts it back into the pool for reuse.
func (p *PhysicalInfrastructure) Release() {
	// We only need to nil out the local processors since these are the only
	// pointer types.
	for i := range p.LocalProcessors {
		p.LocalProcessors[i] = nil
	}
	*p = PhysicalInfrastructure{
		Processors:      p.Processors[:0],
		LocalProcessors: p.LocalProcessors[:0],
		Streams:         p.Streams[:0],
	}
	infraPool.Put(p)
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
func (p *PhysicalPlan) NewStage(containsRemoteProcessor bool, allowPartialDistribution bool) int32 {
	newStageDistribution := LocalPlan
	if containsRemoteProcessor {
		newStageDistribution = FullyDistributedPlan
	}
	if p.stageCounter == 0 {
		// This is the first stage of the plan, so we simply use the
		// distribution as is.
		p.Distribution = newStageDistribution
	} else {
		p.Distribution = p.Distribution.compose(newStageDistribution, allowPartialDistribution)
	}
	p.stageCounter++
	return p.stageCounter
}

// NewStageOnNodes is the same as NewStage but takes in the information about
// the nodes participating in the new stage and the gateway.
func (p *PhysicalPlan) NewStageOnNodes(sqlInstanceIDs []base.SQLInstanceID) int32 {
	// We have a remote processor either when we have multiple nodes
	// participating in the stage or the single processor is scheduled not on
	// the gateway.
	return p.NewStage(
		len(sqlInstanceIDs) > 1 || sqlInstanceIDs[0] != p.GatewaySQLInstanceID, /* containsRemoteProcessor */
		false, /* allowPartialDistribution */
	)
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
	SQLInstanceID base.SQLInstanceID
	Core          execinfrapb.ProcessorCoreUnion
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
	// Note that in order to find out whether we have a remote processor it is
	// not sufficient to have len(corePlacements) be greater than one - we might
	// plan multiple table readers on the gateway if the plan is local.
	containsRemoteProcessor := false
	for i := range corePlacements {
		if corePlacements[i].SQLInstanceID != p.GatewaySQLInstanceID {
			containsRemoteProcessor = true
			break
		}
	}
	stageID := p.NewStage(containsRemoteProcessor, false /* allowPartialDistribution */)
	p.ResultRouters = make([]ProcessorIdx, len(corePlacements))
	for i := range p.ResultRouters {
		proc := Processor{
			SQLInstanceID: corePlacements[i].SQLInstanceID,
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
	// New stage has the same distribution as the previous one, so we need to
	// figure out whether the last stage contains a remote processor.
	stageID := p.NewStage(p.IsLastStageDistributed(), false /* allowPartialDistribution */)
	prevStageResultTypes := p.GetResultTypes()
	for i, resultProc := range p.ResultRouters {
		prevProc := &p.Processors[resultProc]

		proc := Processor{
			SQLInstanceID: prevProc.SQLInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					Type:        execinfrapb.InputSyncSpec_PARALLEL_UNORDERED,
					ColumnTypes: prevStageResultTypes,
				}},
				Core: core,
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
	ctx context.Context,
	resultRouters []ProcessorIdx,
	sourceRouterSlot int,
	ordering execinfrapb.Ordering,
	destProcessor ProcessorIdx,
	destInput int,
	forceSerialization bool,
	serialStreamErrorSpec SerialStreamErrorSpec,
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

			// If the serial unordered stream is limited to executing the first branch
			// of a locality-optimized search due to the `enforce_home_region` session
			// flag being set, set up the erroring info in the destination input spec.
			if serialStreamErrorSpec.SerialInputIdxExclusiveUpperBound > 0 {
				proc.Spec.Input[destInput].EnforceHomeRegionStreamExclusiveUpperBound =
					serialStreamErrorSpec.SerialInputIdxExclusiveUpperBound
				proc.Spec.Input[destInput].EnforceHomeRegionError =
					execinfrapb.NewError(ctx, serialStreamErrorSpec.ExceedsInputIdxExclusiveUpperBoundError)
			}
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
	ctx context.Context,
	sqlInstanceID base.SQLInstanceID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []*types.T,
) {
	proc := Processor{
		SQLInstanceID: sqlInstanceID,
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
			// We're planning a single processor on the instance sqlInstanceID, so
			// we'll have a remote processor only when the node is different from the
			// gateway.
			StageID:     p.NewStage(sqlInstanceID != p.GatewaySQLInstanceID, false /* allowPartialDistribution */),
			ResultTypes: outputTypes,
		},
	}

	pIdx := p.AddProcessor(proc)

	// Connect the result routers to the processor.
	p.MergeResultStreams(ctx, p.ResultRouters, 0, p.MergeOrdering, pIdx, 0, false, /* forceSerialization */
		SerialStreamErrorSpec{},
	)

	// We now have a single result stream.
	p.ResultRouters = p.ResultRouters[:1]
	p.ResultRouters[0] = pIdx

	p.MergeOrdering = execinfrapb.Ordering{}
}

// ReplaceLastStage replaces the processors of the last stage with the provided
// arguments while keeping all other attributes unchanged.
func (p *PhysicalPlan) ReplaceLastStage(
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	outputTypes []*types.T,
	newOrdering execinfrapb.Ordering,
) {
	for _, prevProcIdx := range p.ResultRouters {
		oldProcSpec := &p.Processors[prevProcIdx].Spec
		oldProcSpec.Core = core
		oldProcSpec.Post = post
		oldProcSpec.ResultTypes = outputTypes
	}
	p.SetMergeOrdering(newOrdering)
}

// EnsureSingleStreamOnGateway ensures that there is only one stream on the
// gateway node in the plan (meaning it possibly merges multiple streams or
// brings a single stream from a remote node to the gateway).
func (p *PhysicalPlan) EnsureSingleStreamOnGateway(ctx context.Context) {
	// If we don't already have a single result router on the gateway, add a
	// single grouping stage.
	if len(p.ResultRouters) != 1 ||
		p.Processors[p.ResultRouters[0]].SQLInstanceID != p.GatewaySQLInstanceID {
		p.AddSingleGroupStage(
			ctx,
			p.GatewaySQLInstanceID,
			execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			execinfrapb.PostProcessSpec{},
			p.GetResultTypes(),
		)
		if len(p.ResultRouters) != 1 || p.Processors[p.ResultRouters[0]].SQLInstanceID != p.GatewaySQLInstanceID {
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
	// If the projection we are trying to apply projects every column, don't
	// update the spec.
	if isIdentityProjection(columns, len(p.GetResultTypes())) {
		p.SetMergeOrdering(newMergeOrdering)
		return
	}

	newResultTypes := make([]*types.T, len(columns))
	for i, c := range columns {
		newResultTypes[i] = p.GetResultTypes()[c]
	}

	post := p.GetLastStagePost()

	if post.RenderExprs != nil {
		// Check whether each render expression is projected exactly once. If that's
		// not the case, then we must add another processor in order for each render
		// expression to be evaluated once (this is needed for edge cases like the
		// render expressions resulting in errors or other side effects).
		var addNewProcessor bool
		if len(columns) < len(post.RenderExprs) {
			// We're definitely not projecting some render expressions, so we'll
			// need a new processor.
			addNewProcessor = true
		} else {
			var seenCols intsets.Fast
			renderUsed := make([]bool, len(post.RenderExprs))
			for _, c := range columns {
				if seenCols.Contains(int(c)) {
					addNewProcessor = true
				}
				seenCols.Add(int(c))
				renderUsed[c] = true
			}
			for _, used := range renderUsed {
				// Need to add a new processor if at least one render is not
				// projected.
				addNewProcessor = addNewProcessor || !used
			}
		}
		if addNewProcessor {
			p.AddNoGroupingStage(
				execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				execinfrapb.PostProcessSpec{
					Projection:    true,
					OutputColumns: columns,
				},
				newResultTypes,
				newMergeOrdering,
			)
			return
		}
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
	p.SetMergeOrdering(newMergeOrdering)
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
	ctx context.Context,
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
	if len(post.RenderExprs) > 0 || len(post.OutputColumns) > 0 {
		post = execinfrapb.PostProcessSpec{}
		// The last stage contains render expressions, or is projecting input columns.
		// The new renders refer to the output of these in a particular order, so we
		// need to add another "no-op" stage to which to attach the new rendering.
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
	var ef ExprFactory
	ef.Init(ctx, exprCtx, compositeMap)
	// The number of expressions to render is a rough estimate for the number of
	// indexed vars that will be created in all the calls to ef.Make below.
	ef.IndexedVarsHint(len(exprs))
	for i, e := range exprs {
		var err error
		post.RenderExprs[i], err = ef.Make(e)
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
//
//	indexVarMap is a mapping from columns that appear in an expression
//	            (planNode columns) to columns in the output stream of a
//	            processor.
//	outputColumns is the list of output columns in the processor's
//	              PostProcessSpec; it is effectively a mapping from the output
//	              schema to the internal schema of a processor.
//
// Result: a "composite map" that maps the planNode columns to the internal
//
//	columns of the processor.
//
// For efficiency, the indexVarMap and the resulting map are represented as
// slices, with missing elements having values -1.
//
// Used when adding expressions (filtering, rendering) to a processor's
// PostProcessSpec. For example:
//
//	TableReader // table columns A,B,C,D
//	Internal schema (before post-processing): A, B, C, D
//	OutputColumns:  [1 3]
//	Output schema (after post-processing): B, D
//
//	Expression "B < D" might be represented as:
//	  IndexedVar(4) < IndexedVar(1)
//	with associated indexVarMap:
//	  [-1 1 -1 -1 0]  // 1->1, 4->0
//	This is effectively equivalent to "IndexedVar(0) < IndexedVar(1)"; 0 means
//	the first output column (B), 1 means the second output column (D).
//
//	To get an index var map that refers to the internal schema:
//	  reverseProjection(
//	    [1 3],           // OutputColumns
//	    [-1 1 -1 -1 0],
//	  ) =
//	    [-1 3 -1 -1 1]   // 1->3, 4->1
//	This is effectively equivalent to "IndexedVar(1) < IndexedVar(3)"; 1
//	means the second internal column (B), 3 means the fourth internal column
//	(D).
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
	ctx context.Context, expr tree.TypedExpr, exprCtx ExprContext, indexVarMap []int,
) error {
	if expr == nil {
		return errors.Errorf("nil filter")
	}
	filter, err := MakeExpression(ctx, expr, exprCtx, indexVarMap)
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
func (p *PhysicalPlan) AddLimit(
	ctx context.Context, count int64, offset int64, exprCtx ExprContext,
) error {
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
			if err := p.AddFilter(ctx, tree.DBoolFalse, exprCtx, nil); err != nil {
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
		ctx,
		p.GatewaySQLInstanceID,
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		post,
		p.GetResultTypes(),
	)
	if limitZero {
		if err := p.AddFilter(ctx, tree.DBoolFalse, exprCtx, nil); err != nil {
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
		if p1.SQLInstanceID == p2.SQLInstanceID {
			endpoint.Type = execinfrapb.StreamEndpointSpec_LOCAL
		} else {
			endpoint.Type = execinfrapb.StreamEndpointSpec_REMOTE
		}
		if endpoint.Type == execinfrapb.StreamEndpointSpec_REMOTE {
			endpoint.OriginNodeID = p1.SQLInstanceID
			endpoint.TargetNodeID = p2.SQLInstanceID
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
// The returned function should be called whenever the caller is done with all
// FlowSpecs. The caller is free to ignore it if the specs will be released
// separately.
func (p *PhysicalPlan) GenerateFlowSpecs() (
	_ map[base.SQLInstanceID]*execinfrapb.FlowSpec,
	cleanup func(map[base.SQLInstanceID]*execinfrapb.FlowSpec),
) {
	flowID := execinfrapb.FlowID{
		UUID: p.FlowID,
	}
	flows := make(map[base.SQLInstanceID]*execinfrapb.FlowSpec, 1)

	for _, proc := range p.Processors {
		flowSpec, ok := flows[proc.SQLInstanceID]
		if !ok {
			flowSpec = newFlowSpec(flowID, p.GatewaySQLInstanceID)
			flows[proc.SQLInstanceID] = flowSpec
		}
		flowSpec.Processors = append(flowSpec.Processors, proc.Spec)
	}
	// Note that we don't return an anonymous function with no arguments to not
	// incur an allocation.
	return flows, releaseAll
}

func releaseAll(flows map[base.SQLInstanceID]*execinfrapb.FlowSpec) {
	for _, flowSpec := range flows {
		ReleaseFlowSpec(flowSpec)
	}
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
	allowPartialDistribution bool,
) {
	if mergedPlan.PhysicalInfrastructure != left.PhysicalInfrastructure ||
		mergedPlan.PhysicalInfrastructure != right.PhysicalInfrastructure {
		panic(errors.AssertionFailedf("can only merge plans that share infrastructure"))
	}
	mergedPlan.SetRowEstimates(left, right)
	mergedPlan.Distribution = leftPlanDistribution.compose(rightPlanDistribution, allowPartialDistribution)
}

// AddJoinStage adds join processors at each of the specified nodes, and wires
// the left and right-side outputs to these processors.
func (p *PhysicalPlan) AddJoinStage(
	ctx context.Context,
	sqlInstanceIDs []base.SQLInstanceID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	leftEqCols, rightEqCols []uint32,
	leftTypes, rightTypes []*types.T,
	leftMergeOrd, rightMergeOrd execinfrapb.Ordering,
	leftRouters, rightRouters []ProcessorIdx,
	resultTypes []*types.T,
) {
	pIdxStart := ProcessorIdx(len(p.Processors))
	stageID := p.NewStageOnNodes(sqlInstanceIDs)

	for _, sqlInstanceID := range sqlInstanceIDs {
		inputs := make([]execinfrapb.InputSyncSpec, 0, 2)
		inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: leftTypes})
		inputs = append(inputs, execinfrapb.InputSyncSpec{ColumnTypes: rightTypes})

		proc := Processor{
			SQLInstanceID: sqlInstanceID,
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

	if len(sqlInstanceIDs) > 1 {
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
	for bucket := 0; bucket < len(sqlInstanceIDs); bucket++ {
		pIdx := pIdxStart + ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(ctx, leftRouters, bucket, leftMergeOrd, pIdx, 0, false, /* forceSerialization */
			SerialStreamErrorSpec{},
		)
		// Connect right routers to the processor's second input if it has one.
		p.MergeResultStreams(ctx, rightRouters, bucket, rightMergeOrd, pIdx, 1, false, /* forceSerialization */
			SerialStreamErrorSpec{},
		)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
}

// AddStageOnNodes adds a stage of processors that take in a single input
// logical stream on the specified nodes and connects them to the previous
// stage via a hash router.
func (p *PhysicalPlan) AddStageOnNodes(
	ctx context.Context,
	sqlInstanceIDs []base.SQLInstanceID,
	core execinfrapb.ProcessorCoreUnion,
	post execinfrapb.PostProcessSpec,
	hashCols []uint32,
	inputTypes, resultTypes []*types.T,
	mergeOrd execinfrapb.Ordering,
	routers []ProcessorIdx,
) {
	pIdxStart := len(p.Processors)
	newStageID := p.NewStageOnNodes(sqlInstanceIDs)

	for _, sqlInstanceID := range sqlInstanceIDs {
		proc := Processor{
			SQLInstanceID: sqlInstanceID,
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

	if len(sqlInstanceIDs) > 1 {
		// Set up the routers.
		for _, resultProc := range routers {
			p.Processors[resultProc].Spec.Output[0] = execinfrapb.OutputRouterSpec{
				Type:        execinfrapb.OutputRouterSpec_BY_HASH,
				HashColumns: hashCols,
			}
		}
	}

	// Connect the result streams to the processors.
	for bucket := 0; bucket < len(sqlInstanceIDs); bucket++ {
		pIdx := ProcessorIdx(pIdxStart + bucket)
		p.MergeResultStreams(ctx, routers, bucket, mergeOrd, pIdx, 0, false, /* forceSerialization */
			SerialStreamErrorSpec{},
		)
	}

	// Set the new result routers.
	p.ResultRouters = p.ResultRouters[:0]
	for i := 0; i < len(sqlInstanceIDs); i++ {
		p.ResultRouters = append(p.ResultRouters, ProcessorIdx(pIdxStart+i))
	}
}

// AddDistinctSetOpStage creates a distinct stage and a join stage to implement
// INTERSECT and EXCEPT plans.
//
// TODO(yuzefovich): If there's a strong key on the left or right side, we
// can elide the distinct stage on that side.
func (p *PhysicalPlan) AddDistinctSetOpStage(
	ctx context.Context,
	sqlInstanceIDs []base.SQLInstanceID,
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
	distinctProcs := make(map[base.SQLInstanceID][]ProcessorIdx)
	p.AddStageOnNodes(
		ctx, sqlInstanceIDs, distinctCores[0], execinfrapb.PostProcessSpec{}, eqCols,
		leftTypes, leftTypes, leftMergeOrd, leftRouters,
	)
	for _, leftDistinctProcIdx := range p.ResultRouters {
		node := p.Processors[leftDistinctProcIdx].SQLInstanceID
		distinctProcs[node] = append(distinctProcs[node], leftDistinctProcIdx)
	}
	p.AddStageOnNodes(
		ctx, sqlInstanceIDs, distinctCores[1], execinfrapb.PostProcessSpec{}, eqCols,
		rightTypes, rightTypes, rightMergeOrd, rightRouters,
	)
	for _, rightDistinctProcIdx := range p.ResultRouters {
		node := p.Processors[rightDistinctProcIdx].SQLInstanceID
		distinctProcs[node] = append(distinctProcs[node], rightDistinctProcIdx)
	}

	// Create a join stage, where the distinct processors on the same node are
	// connected to a join processor.
	joinStageID := p.NewStageOnNodes(sqlInstanceIDs)
	p.ResultRouters = p.ResultRouters[:0]

	for _, n := range sqlInstanceIDs {
		proc := Processor{
			SQLInstanceID: n,
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
	ctx context.Context,
	forceSerialization bool,
	post execinfrapb.PostProcessSpec,
	serialStreamErrorSpec SerialStreamErrorSpec,
) {
	// Fast path - check if we need to do anything.
	var nodes intsets.Fast
	var foundDuplicates bool
	for _, pIdx := range p.ResultRouters {
		proc := &p.Processors[pIdx]
		if nodes.Contains(int(proc.SQLInstanceID)) {
			foundDuplicates = true
			break
		}
		nodes.Add(int(proc.SQLInstanceID))
	}
	if !foundDuplicates {
		return
	}
	streams := make([]ProcessorIdx, 0, 2)

	for i := 0; i < len(p.ResultRouters); i++ {
		pIdx := p.ResultRouters[i]
		node := p.Processors[p.ResultRouters[i]].SQLInstanceID
		streams = append(streams[:0], pIdx)
		// Find all streams on the same node.
		for j := i + 1; j < len(p.ResultRouters); {
			if p.Processors[p.ResultRouters[j]].SQLInstanceID == node {
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
			SQLInstanceID: node,
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
		p.MergeResultStreams(ctx, streams, 0 /* sourceRouterSlot */, p.MergeOrdering, mergedProcIdx,
			0 /* destInput */, forceSerialization, serialStreamErrorSpec,
		)
		p.ResultRouters[i] = mergedProcIdx
	}
}

// GetLastStageDistribution returns the distribution *only* of the last stage.
// Note that if the last stage consists of a single processor planned on a
// remote node, such stage is considered distributed.
func (p *PhysicalPlan) GetLastStageDistribution() PlanDistribution {
	for i := range p.ResultRouters {
		if p.Processors[p.ResultRouters[i]].SQLInstanceID != p.GatewaySQLInstanceID {
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
func (a PlanDistribution) compose(
	b PlanDistribution, allowPartialDistribution bool,
) PlanDistribution {
	if allowPartialDistribution && a != b {
		return PartiallyDistributedPlan
	}
	// TODO(yuzefovich): this is not quite correct - using
	// PartiallyDistributedPlan would make more sense when a != b, but that
	// would lead to confusing to users "distribution: partial" in the EXPLAIN
	// output, so for now we have this hack.
	if a != LocalPlan || b != LocalPlan {
		return FullyDistributedPlan
	}
	return LocalPlan
}
