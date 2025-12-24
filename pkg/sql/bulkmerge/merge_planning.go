// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func newBulkMergePlan(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	spans []roachpb.Span,
	genOutputURI func(sqlInstance base.SQLInstanceID) string,
	iteration int,
	maxIterations int,
	writeTS *hlc.Timestamp,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	if len(spans) == 0 {
		return nil, nil, errors.Newf("no spans specified")
	}
	// NOTE: This implementation is inspired by the physical plan created by
	// restore in `pkg/backup/restore_processor_planning.go`
	// TODO(mw5h): We need to be careful about mixed version clusters, so consider
	// where we'll want to add a version gate.
	planCtx, sqlInstanceIDs, err := execCtx.DistSQLPlanner().SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
	if err != nil {
		return nil, nil, err
	}

	plan := planCtx.NewPhysicalPlan()
	// Use the gateway node as the coordinator, which is where the job was initiated.
	coordinatorID := plan.GatewaySQLInstanceID

	keys := make([][]byte, 0, len(sqlInstanceIDs))
	for _, id := range sqlInstanceIDs {
		keys = append(keys, physicalplan.RoutingKeyForSQLInstance(id))
	}

	router, err := physicalplan.MakeInstanceRouter(sqlInstanceIDs)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to make instance router")
	}

	loopbackID := plan.AddProcessor(physicalplan.Processor{
		SQLInstanceID: coordinatorID,
		Spec: execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{
				MergeLoopback: &execinfrapb.MergeLoopbackSpec{},
			},
			Post: execinfrapb.PostProcessSpec{},
			Output: []execinfrapb.OutputRouterSpec{{
				Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: router,
			}},
			StageID:     plan.NewStageOnNodes([]base.SQLInstanceID{coordinatorID}),
			ResultTypes: mergeLoopbackOutputTypes,
		},
	})

	mergeStage := plan.NewStageOnNodes(sqlInstanceIDs)
	var writeTimestamp hlc.Timestamp
	if writeTS != nil {
		writeTimestamp = *writeTS
	}
	for streamID, sqlInstanceID := range sqlInstanceIDs {
		var outputStorageConf cloudpb.ExternalStorage
		if iteration < maxIterations {
			outputStorage, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
				ctx,
				genOutputURI(sqlInstanceID),
				execCtx.User(),
			)
			if err != nil {
				return nil, nil, err
			}
			outputStorageConf = outputStorage.Conf()
			outputStorage.Close()
		}
		pIdx := plan.AddProcessor(physicalplan.Processor{
			SQLInstanceID: sqlInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					ColumnTypes: mergeLoopbackOutputTypes,
				}},
				Core: execinfrapb.ProcessorCoreUnion{
					BulkMerge: &execinfrapb.BulkMergeSpec{
						SSTs:           ssts,
						Spans:          spans,
						OutputStorage:  outputStorageConf,
						Iteration:      int32(iteration),
						MaxIterations:  int32(maxIterations),
						WriteTimestamp: writeTimestamp,
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

	plan.AddSingleGroupStage(ctx, coordinatorID, execinfrapb.ProcessorCoreUnion{
		MergeCoordinator: &execinfrapb.MergeCoordinatorSpec{
			TaskCount:            int64(len(spans)),
			WorkerSqlInstanceIds: keys,
		},
	}, execinfrapb.PostProcessSpec{}, mergeCoordinatorOutputTypes, nil /* finalizeLastStageCb */)

	plan.PlanToStreamColMap = []int{0} // Needed for FinalizePlan to populate ResultTypes
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}
