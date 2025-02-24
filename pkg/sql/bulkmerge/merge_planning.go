// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func newBulkMergePlan(
	ctx context.Context, execCtx sql.JobExecContext, taskCount int,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	// NOTE: This implementation is inspired by the physical plan created by
	// restore in `pkg/backup/restore_processor_planning.go`
	planCtx, sqlInstanceIDs, err := execCtx.DistSQLPlanner().SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
	if err != nil {
		return nil, nil, err
	}

	plan := planCtx.NewPhysicalPlan()
	// TODO(jeffswenson): how should we determine the coordinator? Maybe we
	// should use the gateway node by default.
	coordinatorID := sqlInstanceIDs[0:1]

	router, err := makeInstanceRouter(sqlInstanceIDs)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to make instance router")
	}

	loopbackID := plan.AddProcessor(physicalplan.Processor{
		SQLInstanceID: coordinatorID[0],
		Spec: execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{
				MergeLoopback: &execinfrapb.MergeLoopbackSpec{},
			},
			Post: execinfrapb.PostProcessSpec{},
			Output: []execinfrapb.OutputRouterSpec{{
				Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: router,
			}},
			StageID:     plan.NewStageOnNodes(coordinatorID),
			ResultTypes: mergeLoopbackOutputTypes,
		},
	})

	mergeStage := plan.NewStageOnNodes(sqlInstanceIDs)
	for streamID, sqlInstanceID := range sqlInstanceIDs {
		pIdx := plan.AddProcessor(physicalplan.Processor{
			SQLInstanceID: sqlInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					ColumnTypes: mergeLoopbackOutputTypes,
				}},
				Core: execinfrapb.ProcessorCoreUnion{
					BulkMerge: &execinfrapb.BulkMergeSpec{
						// TODO(jeffswenson): fill in the rest of the spec
					},
				},
				Post: execinfrapb.PostProcessSpec{},
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID:     mergeStage,
				ResultTypes: bulkMergeProcessorOutputTypes,
			},
		})
		plan.Streams = append(plan.Streams, physicalplan.Stream{
			SourceProcessor:  loopbackID,
			SourceRouterSlot: streamID,
			DestProcessor:    pIdx,
			DestInput:        0,
		})
		plan.ResultRouters = append(plan.ResultRouters, pIdx)
	}

	plan.AddSingleGroupStage(ctx, coordinatorID[0], execinfrapb.ProcessorCoreUnion{
		MergeCoordinator: &execinfrapb.MergeCoordinatorSpec{
			TaskCount:            int64(taskCount),
			WorkerSqlInstanceIds: getRoutingKeys(router),
		},
	}, execinfrapb.PostProcessSpec{}, mergeCoordinatorOutputTypes)

	plan.PlanToStreamColMap = []int{0} // Needed for FinalizePlan to populate ResultTypes
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}

// TODO(jeffswenson): dedupe this with the instance in pkg/backup
func routingDatumsForSQLInstance(
	sqlInstanceID base.SQLInstanceID,
) (rowenc.EncDatum, rowenc.EncDatum) {
	routingBytes := roachpb.Key(fmt.Sprintf("node%d", sqlInstanceID))
	startDatum := rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes)))
	endDatum := rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes.Next())))
	return startDatum, endDatum
}

// routingSpanForSQLInstance provides the mapping to be used during distsql planning
// when setting up the output router.
// TODO(jeffswenson): dedupe this with the instance in pkg/backup
func routingSpanForSQLInstance(sqlInstanceID base.SQLInstanceID) ([]byte, []byte, error) {
	var alloc tree.DatumAlloc
	startDatum, endDatum := routingDatumsForSQLInstance(sqlInstanceID)

	startBytes, endBytes := make([]byte, 0), make([]byte, 0)
	startBytes, err := startDatum.Encode(types.Bytes, &alloc, catenumpb.DatumEncoding_ASCENDING_KEY, startBytes)
	if err != nil {
		return nil, nil, err
	}
	endBytes, err = endDatum.Encode(types.Bytes, &alloc, catenumpb.DatumEncoding_ASCENDING_KEY, endBytes)
	if err != nil {
		return nil, nil, err
	}
	return startBytes, endBytes, nil
}

// TODO(jeffswenson): refactor backup/crosscluster to use this function.
func makeInstanceRouter(
	ids []base.SQLInstanceID,
) (execinfrapb.OutputRouterSpec_RangeRouterSpec, error) {
	var zero execinfrapb.OutputRouterSpec_RangeRouterSpec
	// TODO(jeffswenson): can I add an assertion if something is routed to the
	// default stream?
	defaultStream := int32(0)
	rangeRouterSpec := execinfrapb.OutputRouterSpec_RangeRouterSpec{
		Spans:       nil,
		DefaultDest: &defaultStream,
		Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: catenumpb.DatumEncoding_ASCENDING_KEY,
			},
		},
	}
	for stream, sqlInstanceID := range ids {
		startBytes, endBytes, err := routingSpanForSQLInstance(sqlInstanceID)
		if err != nil {
			return zero, err
		}

		span := execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  startBytes,
			End:    endBytes,
			Stream: int32(stream),
		}
		rangeRouterSpec.Spans = append(rangeRouterSpec.Spans, span)
	}
	slices.SortFunc(rangeRouterSpec.Spans, func(a, b execinfrapb.OutputRouterSpec_RangeRouterSpec_Span) int {
		return bytes.Compare(a.Start, b.Start)
	})
	return rangeRouterSpec, nil
}

func getRoutingKeys(router execinfrapb.OutputRouterSpec_RangeRouterSpec) []string {
	keys := make([]string, 0, len(router.Spans))
	for _, span := range router.Spans {
		keys = append(keys, string(span.Start), string(span.End))
	}
	return keys
}
