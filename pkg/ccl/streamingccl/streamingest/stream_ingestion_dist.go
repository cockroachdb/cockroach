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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

func startDistIngestion(
	ctx context.Context, execCtx sql.JobExecContext, ingestionJob *jobs.Job,
) error {

	details := ingestionJob.Details().(jobspb.StreamIngestionDetails)
	streamProgress := ingestionJob.Progress().Details.(*jobspb.Progress_StreamIngest).StreamIngest

	streamID := streampb.StreamID(details.StreamID)
	initialScanTimestamp := details.ReplicationStartTime
	replicatedTime := streamProgress.ReplicatedTime

	if replicatedTime.IsEmpty() && initialScanTimestamp.IsEmpty() {
		return jobs.MarkAsPermanentJobError(errors.AssertionFailedf("initial timestamp and replicated timestamp are both empty"))
	}

	// Start from the last checkpoint if it exists.
	var heartbeatTimestamp hlc.Timestamp
	if !replicatedTime.IsEmpty() {
		heartbeatTimestamp = replicatedTime
	} else {
		heartbeatTimestamp = initialScanTimestamp
	}

	msg := fmt.Sprintf("resuming stream (producer job %d) from %s",
		streamID, heartbeatTimestamp)
	updateRunningStatus(ctx, ingestionJob, jobspb.InitializingReplication, msg)

	client, err := connectToActiveClient(ctx, ingestionJob, execCtx.ExecCfg().InternalDB)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Close(ctx); err != nil {
			log.Warningf(ctx, "stream ingestion client did not shut down properly: %s", err.Error())
		}
	}()
	if err := waitUntilProducerActive(ctx, client, streamID, heartbeatTimestamp, ingestionJob.ID()); err != nil {
		return err
	}

	log.Infof(ctx, "producer job %d is active, planning DistSQL flow", streamID)
	dsp := execCtx.DistSQLPlanner()

	p, planCtx, err := makePlan(
		execCtx,
		ingestionJob,
		details,
		client,
		replicatedTime,
		streamProgress.Checkpoint,
		initialScanTimestamp,
		dsp.GatewayID(),
	)(ctx, dsp)
	if err != nil {
		return err
	}

	execPlan := func(ctx context.Context) error {
		ctx = logtags.AddTag(ctx, "stream-ingest-distsql", nil)

		rw := sql.NewRowResultWriter(nil /* rowContainer */)

		var noTxn *kv.Txn
		recv := sql.MakeDistSQLReceiver(
			ctx,
			rw,
			tree.Rows,
			nil, /* rangeCache */
			noTxn,
			nil, /* clockUpdater */
			execCtx.ExtendedEvalContext().Tracing,
		)
		defer recv.Release()

		jobsprofiler.StorePlanDiagram(ctx, execCtx.ExecCfg().DistSQLSrv.Stopper, p, execCtx.ExecCfg().InternalDB,
			ingestionJob.ID())

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *execCtx.ExtendedEvalContext()
		dsp.Run(ctx, planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return rw.Err()
	}

	updateRunningStatus(ctx, ingestionJob, jobspb.Replicating, "running replicating stream")
	// TODO(msbutler): Implement automatic replanning in the spirit of changefeed replanning.
	return execPlan(ctx)
}

// TODO (msbutler): this function signature was written to use in automatic job replanning via
// sql.PhysicalPlanChangeChecker(). Actually implement c2c replanning.
func makePlan(
	execCtx sql.JobExecContext,
	ingestionJob *jobs.Job,
	details jobspb.StreamIngestionDetails,
	client streamclient.Client,
	previousReplicatedTime hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	initialScanTimestamp hlc.Timestamp,
	gatewayID base.SQLInstanceID,
) func(context.Context, *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	return func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
		jobID := ingestionJob.ID()
		log.Infof(ctx, "Re Planning DistSQL flow for stream ingestion job %d", jobID)

		streamID := streampb.StreamID(details.StreamID)
		topology, err := client.Plan(ctx, streamID)
		if err != nil {
			return nil, nil, err
		}
		err = ingestionJob.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			md.Progress.GetStreamIngest().StreamAddresses = topology.StreamAddresses()
			ju.UpdateProgress(md.Progress)
			return nil
		})
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to update job progress")
		}

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
		if err != nil {
			return nil, nil, err
		}

		streamIngestionSpecs, streamIngestionFrontierSpec, err := constructStreamIngestionPlanSpecs(
			streamingccl.StreamAddress(details.StreamAddress),
			topology,
			sqlInstanceIDs,
			initialScanTimestamp,
			previousReplicatedTime,
			checkpoint,
			jobID,
			streamID,
			topology.SourceTenantID,
			details.DestinationTenantID)
		if err != nil {
			return nil, nil, err
		}
		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.AfterReplicationFlowPlan != nil {
			knobs.AfterReplicationFlowPlan(streamIngestionSpecs, streamIngestionFrontierSpec)
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

		// The ResultRouters from the previous stage will feed in to the
		// StreamIngestionFrontier processor.
		p.AddSingleGroupStage(ctx, gatewayID,
			execinfrapb.ProcessorCoreUnion{StreamIngestionFrontier: streamIngestionFrontierSpec},
			execinfrapb.PostProcessSpec{}, streamIngestionResultTypes)

		for src, dst := range streamIngestionFrontierSpec.SubscribingSQLInstances {
			log.Infof(ctx, "physical replication src-dst pair: %s:%d",
				src, dst)
		}

		p.PlanToStreamColMap = []int{0}
		sql.FinalizePlan(ctx, planCtx, p)
		return p, planCtx, nil
	}
}

func constructStreamIngestionPlanSpecs(
	streamAddress streamingccl.StreamAddress,
	topology streamclient.Topology,
	sqlInstanceIDs []base.SQLInstanceID,
	initialScanTimestamp hlc.Timestamp,
	previousReplicatedTimestamp hlc.Timestamp,
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
				StreamID:                    uint64(streamID),
				JobID:                       int64(jobID),
				PreviousReplicatedTimestamp: previousReplicatedTimestamp,
				InitialScanTimestamp:        initialScanTimestamp,
				Checkpoint:                  checkpoint, // TODO: Only forward relevant checkpoint info
				StreamAddress:               string(streamAddress),
				PartitionSpecs:              make(map[string]execinfrapb.StreamIngestionPartitionSpec),
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
		ReplicatedTimeAtStart:   previousReplicatedTimestamp,
		TrackedSpans:            trackedSpans,
		JobID:                   int64(jobID),
		StreamID:                uint64(streamID),
		StreamAddresses:         topology.StreamAddresses(),
		SubscribingSQLInstances: subscribingSQLInstances,
		Checkpoint:              checkpoint,
	}

	return streamIngestionSpecs, streamIngestionFrontierSpec, nil
}
