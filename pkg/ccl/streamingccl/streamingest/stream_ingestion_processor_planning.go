// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/logtags"
)

func distStreamIngestionPlanSpecs(
	streamAddress streamingccl.StreamAddress,
	topology streamclient.Topology,
	sqlInstanceIDs []base.SQLInstanceID,
	initialScanTimestamp hlc.Timestamp,
	previousHighWater hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	jobID jobspb.JobID,
	streamID streampb.StreamID,
	sourceTenantID roachpb.TenantID,
	destinationTenantID roachpb.TenantID,
) ([]*execinfrapb.StreamIngestionDataSpec, *execinfrapb.StreamIngestionFrontierSpec, error) {
	// For each stream partition in the topology, assign it to a node.
	streamIngestionSpecs := make([]*execinfrapb.StreamIngestionDataSpec, 0, len(sqlInstanceIDs))

	trackedSpans := make([]roachpb.Span, 0)
	subscribingSQLInstances := make(map[string]uint32)
	for i, partition := range topology.Partitions {
		// Round robin assign the stream partitions to nodes. Partitions 0 through
		// len(nodes) - 1 creates the spec. Future partitions just add themselves to
		// the partition addresses.
		if i < len(sqlInstanceIDs) {
			spec := &execinfrapb.StreamIngestionDataSpec{
				StreamID:                   uint64(streamID),
				JobID:                      int64(jobID),
				PreviousHighWaterTimestamp: previousHighWater,
				InitialScanTimestamp:       initialScanTimestamp,
				Checkpoint:                 checkpoint, // TODO: Only forward relevant checkpoint info
				StreamAddress:              string(streamAddress),
				PartitionSpecs:             make(map[string]execinfrapb.StreamIngestionPartitionSpec),
				TenantRekey: execinfrapb.TenantRekey{
					OldID: sourceTenantID,
					NewID: destinationTenantID,
				},
			}
			streamIngestionSpecs = append(streamIngestionSpecs, spec)
		}
		n := i % len(sqlInstanceIDs)

		subscribingSQLInstances[partition.ID] = uint32(sqlInstanceIDs[n])
		streamIngestionSpecs[n].PartitionSpecs[partition.ID] = execinfrapb.StreamIngestionPartitionSpec{
			PartitionID:       partition.ID,
			SubscriptionToken: string(partition.SubscriptionToken),
			Address:           string(partition.SrcAddr),
			Spans:             partition.Spans,
		}

		trackedSpans = append(trackedSpans, partition.Spans...)
	}

	// Create a spec for the StreamIngestionFrontier processor on the coordinator
	// node.
	streamIngestionFrontierSpec := &execinfrapb.StreamIngestionFrontierSpec{
		HighWaterAtStart:        previousHighWater,
		TrackedSpans:            trackedSpans,
		JobID:                   int64(jobID),
		StreamID:                uint64(streamID),
		StreamAddresses:         topology.StreamAddresses(),
		SubscribingSQLInstances: subscribingSQLInstances,
		Checkpoint:              checkpoint,
	}

	return streamIngestionSpecs, streamIngestionFrontierSpec, nil
}

func distStreamIngest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	sqlInstanceIDs []base.SQLInstanceID,
	planCtx *sql.PlanningCtx,
	dsp *sql.DistSQLPlanner,
	streamIngestionSpecs []*execinfrapb.StreamIngestionDataSpec,
	streamIngestionFrontierSpec *execinfrapb.StreamIngestionFrontierSpec,
) error {
	ctx = logtags.AddTag(ctx, "stream-ingest-distsql", nil)
	evalCtx := execCtx.ExtendedEvalContext()
	var noTxn *kv.Txn

	if len(streamIngestionSpecs) == 0 {
		return nil
	}

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(streamIngestionSpecs))
	for i := range streamIngestionSpecs {
		corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
		corePlacement[i].Core.StreamIngestionData = streamIngestionSpecs[i]
	}

	p := planCtx.NewPhysicalPlan()
	p.AddNoInputStage(
		corePlacement,
		execinfrapb.PostProcessSpec{},
		streamIngestionResultTypes,
		execinfrapb.Ordering{},
	)

	execCfg := execCtx.ExecCfg()
	gatewayNodeID, err := execCfg.NodeInfo.NodeID.OptionalNodeIDErr(48274)
	if err != nil {
		return err
	}

	// The ResultRouters from the previous stage will feed in to the
	// StreamIngestionFrontier processor.
	p.AddSingleGroupStage(base.SQLInstanceID(gatewayNodeID),
		execinfrapb.ProcessorCoreUnion{StreamIngestionFrontier: streamIngestionFrontierSpec},
		execinfrapb.PostProcessSpec{}, streamIngestionResultTypes)

	p.PlanToStreamColMap = []int{0}
	dsp.FinalizePlan(planCtx, p)

	rw := sql.NewRowResultWriter(nil /* rowContainer */)

	recv := sql.MakeDistSQLReceiver(
		ctx,
		rw,
		tree.Rows,
		nil, /* rangeCache */
		noTxn,
		nil, /* clockUpdater */
		evalCtx.Tracing,
	)
	defer recv.Release()

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(ctx, planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	return rw.Err()
}
