// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/errors"
)

func newBulkMergePlan(
	ctx context.Context, execCtx sql.JobExecContext,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
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

	plan.AddSingleGroupStage(ctx, coordinatorID, execinfrapb.ProcessorCoreUnion{
		MergeCoordinator: &execinfrapb.MergeCoordinatorSpec{
			// TODO fill in the rest of the spec
		},
	}, execinfrapb.PostProcessSpec{}, mergeCoordinatorOutputTypes, nil /* finalizeLastStageCb */)

	plan.PlanToStreamColMap = []int{0} // Needed for FinalizePlan to populate ResultTypes
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}
