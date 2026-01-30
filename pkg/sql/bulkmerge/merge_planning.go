// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
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

	// Read the setting to determine how many processors to create per node.
	processorsPerNodeCount := int(processorsPerNode.Get(&execCtx.ExecCfg().Settings.SV))

	// Build routing keys for all processors across all SQL instances.
	// Each SQL instance gets processorsPerNodeCount processors.
	type processorInfo struct {
		sqlInstanceID base.SQLInstanceID
		processorID   int
	}
	var processorInfos []processorInfo
	keys := make([][]byte, 0, len(sqlInstanceIDs)*processorsPerNodeCount)
	for _, id := range sqlInstanceIDs {
		for procID := 0; procID < processorsPerNodeCount; procID++ {
			processorInfos = append(processorInfos, processorInfo{
				sqlInstanceID: id,
				processorID:   procID,
			})
			keys = append(keys, physicalplan.RoutingKeyForProcessor(id, procID))
		}
	}

	// Create router that maps routing keys to stream IDs.
	rangeRouterSpec := execinfrapb.OutputRouterSpec_RangeRouterSpec{
		Spans:       nil,
		DefaultDest: nil,
		Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: catenumpb.DatumEncoding_ASCENDING_KEY,
			},
		},
	}
	for stream, info := range processorInfos {
		startBytes, endBytes, err := physicalplan.RoutingSpanForProcessor(info.sqlInstanceID, info.processorID)
		if err != nil {
			return nil, nil, err
		}
		span := execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  startBytes,
			End:    endBytes,
			Stream: int32(stream),
		}
		rangeRouterSpec.Spans = append(rangeRouterSpec.Spans, span)
	}
	// The router expects the spans to be sorted.
	slices.SortFunc(rangeRouterSpec.Spans, func(a, b execinfrapb.OutputRouterSpec_RangeRouterSpec_Span) int {
		return bytes.Compare(a.Start, b.Start)
	})

	loopbackID := plan.AddProcessor(physicalplan.Processor{
		SQLInstanceID: coordinatorID,
		Spec: execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{
				MergeLoopback: &execinfrapb.MergeLoopbackSpec{},
			},
			Post: execinfrapb.PostProcessSpec{},
			Output: []execinfrapb.OutputRouterSpec{{
				Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: rangeRouterSpec,
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
	// Create processors for all SQL instances and processor IDs.
	for streamID, info := range processorInfos {
		var outputStorageConf cloudpb.ExternalStorage
		if opts.Iteration < opts.MaxIterations {
			outputURI, err := genOutputURIAndRecordPrefix(info.sqlInstanceID)
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
			SQLInstanceID: info.sqlInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					ColumnTypes: mergeLoopbackOutputTypes,
				}},
				Core: execinfrapb.ProcessorCoreUnion{
					BulkMerge: &execinfrapb.BulkMergeSpec{
						SSTs:              ssts,
						Spans:             spans,
						OutputStorage:     outputStorageConf,
						Iteration:         int32(opts.Iteration),
						MaxIterations:     int32(opts.MaxIterations),
						WriteTimestamp:    writeTimestamp,
						EnforceUniqueness: opts.EnforceUniqueness,
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
			TaskCount:           int64(len(spans)),
			WorkerProcessorKeys: keys,
		},
	}, execinfrapb.PostProcessSpec{}, mergeCoordinatorOutputTypes, nil /* finalizeLastStageCb */)

	plan.PlanToStreamColMap = []int{0} // Needed for FinalizePlan to populate ResultTypes
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}
