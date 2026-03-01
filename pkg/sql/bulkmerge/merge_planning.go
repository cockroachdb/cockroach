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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func newBulkMergePlan(
	ctx context.Context,
	execCtx sql.JobExecContext,
	ssts []execinfrapb.BulkMergeSpec_SST,
	spans []roachpb.Span,
	genOutputURIAndRecordPrefix func(sqlInstance base.SQLInstanceID) (string, error),
	opts MergeOptions,
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

	// For non-final iterations, use the full span with one task per node.
	// Each node merges its local SSTs across the entire key range, producing
	// one merged output per node. The final iteration then merges these N
	// node-level outputs using split spans for balanced work distribution.
	var mergeSpans []roachpb.Span
	var taskCount int
	if opts.Iteration < opts.MaxIterations {
		// Compute full span from the input spans (they're contiguous and sorted).
		fullSpan := roachpb.Span{
			Key:    spans[0].Key,
			EndKey: spans[len(spans)-1].EndKey,
		}
		// One task per node, each covering the full span.
		mergeSpans = make([]roachpb.Span, len(sqlInstanceIDs))
		for i := range mergeSpans {
			mergeSpans[i] = fullSpan
		}
		taskCount = len(sqlInstanceIDs)
	} else {
		// Final iteration uses split spans for balanced work distribution.
		mergeSpans = spans
		taskCount = len(spans)
	}

	log.Dev.Infof(ctx, "bulk merge plan: taskCount=%d", taskCount)

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
	if opts.WriteTimestamp != nil {
		writeTimestamp = *opts.WriteTimestamp
	}
	for streamID, sqlInstanceID := range sqlInstanceIDs {
		var outputStorageConf cloudpb.ExternalStorage
		if opts.Iteration < opts.MaxIterations {
			outputURI, err := genOutputURIAndRecordPrefix(sqlInstanceID)
			if err != nil {
				return nil, nil, err
			}
			outputStorage, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
				ctx,
				outputURI,
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
						SSTs:              ssts,
						Spans:             mergeSpans,
						OutputStorage:     outputStorageConf,
						Iteration:         int32(opts.Iteration),
						MaxIterations:     int32(opts.MaxIterations),
						WriteTimestamp:    writeTimestamp,
						EnforceUniqueness: opts.EnforceUniqueness,
						MemoryMonitor:     opts.MemoryMonitor,
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
			TaskCount:            int64(taskCount),
			WorkerSqlInstanceIds: keys,
		},
	}, execinfrapb.PostProcessSpec{}, mergeCoordinatorOutputTypes, nil /* finalizeLastStageCb */)

	plan.PlanToStreamColMap = []int{0} // Needed for FinalizePlan to populate ResultTypes
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}
