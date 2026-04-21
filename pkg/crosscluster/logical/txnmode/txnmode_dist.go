// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// PlanTxnReplication constructs a 3-stage DistSQL physical plan for
// transactional LDR:
//
//	Stage 1: LDR Coordinator (pinned to gateway) → BY_RANGE → N appliers
//	Stage 2: N LDR Appliers (one per leaseholder instance) → BY_RANGE → N dep resolvers
//	Stage 3: N LDR Dep Resolvers (co-located with appliers) → PASS_THROUGH → noop (gateway)
func PlanTxnReplication(
	ctx context.Context,
	job *jobs.Job,
	execCtx sql.JobExecContext,
	sourcePlan streamclient.LogicalReplicationPlan,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	dsp := execCtx.DistSQLPlanner()

	planCtx := dsp.NewPlanningCtx(
		ctx, execCtx.ExtendedEvalContext(),
		nil /* planner */, nil /* txn */, sql.FullDistribution,
	)

	// Compute replicating spans for partition assignment.
	replicatingSpans := make(roachpb.Spans, len(sourcePlan.SourceSpans))
	copy(replicatingSpans, sourcePlan.SourceSpans)

	// Determine which instances are leaseholders for the replicating spans.
	partitions, err := dsp.PartitionSpans(
		ctx, planCtx, replicatingSpans, sql.PartitionSpansBoundDefault,
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "partitioning spans")
	}

	// Deduplicate instance IDs from partitions. These are the instances that
	// will run applier and dep resolver processors.
	instanceSet := make(map[base.SQLInstanceID]struct{}, len(partitions))
	for _, p := range partitions {
		instanceSet[p.SQLInstanceID] = struct{}{}
	}

	applierInstanceIDs := make([]base.SQLInstanceID, 0, len(instanceSet))
	for id := range instanceSet {
		applierInstanceIDs = append(applierInstanceIDs, id)
	}

	// ApplierID is an identity mapping to SQL instance ID.
	applierIDs := make([]int32, len(applierInstanceIDs))
	for i, instanceID := range applierInstanceIDs {
		applierIDs[i] = int32(instanceID)
	}

	spec := buildCoordinatorSpec(job, sourcePlan, applierIDs)

	plan := planCtx.NewPhysicalPlan()
	if err := buildTxnReplicationPlan(
		ctx, plan, applierInstanceIDs, spec,
	); err != nil {
		return nil, nil, err
	}
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}

// buildTxnReplicationPlan constructs the 3-stage processor topology on
// the given physical plan. It is separated from PlanTxnReplication so
// that the topology can be unit-tested with synthetic partition layouts
// without requiring a multi-node cluster.
func buildTxnReplicationPlan(
	ctx context.Context,
	plan *sql.PhysicalPlan,
	applierInstanceIDs []base.SQLInstanceID,
	spec execinfrapb.TxnLDRCoordinatorSpec,
) error {
	applierIDs := make([]int32, len(applierInstanceIDs))
	for i, instanceID := range applierInstanceIDs {
		applierIDs[i] = int32(instanceID)
	}

	gatewayID := plan.GatewaySQLInstanceID

	// --- Stage 1: Coordinator processor (pinned to gateway) ---

	coordinatorToApplierRouter, err := physicalplan.MakeInstanceRouter(
		applierInstanceIDs,
	)
	if err != nil {
		return errors.Wrap(err, "creating coordinator-to-applier router")
	}

	coordinatorStage := plan.NewStageOnNodes([]base.SQLInstanceID{gatewayID})
	coordinatorProcIdx := plan.AddProcessor(physicalplan.Processor{
		SQLInstanceID: gatewayID,
		Spec: execinfrapb.ProcessorSpec{
			Core: execinfrapb.ProcessorCoreUnion{
				TxnLdrCoordinator: &spec,
			},
			Post: execinfrapb.PostProcessSpec{},
			Output: []execinfrapb.OutputRouterSpec{{
				Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: coordinatorToApplierRouter,
			}},
			StageID:     coordinatorStage,
			ResultTypes: coordinatorOutputTypes,
		},
	})

	// --- Stage 2: Applier processors (one per leaseholder instance) ---

	applierToDepResolverRouter, err := physicalplan.MakeInstanceRouter(
		applierInstanceIDs,
	)
	if err != nil {
		return errors.Wrap(err, "creating applier-to-dep-resolver router")
	}

	applierStage := plan.NewStageOnNodes(applierInstanceIDs)
	applierProcIdxs := make(
		map[base.SQLInstanceID]physicalplan.ProcessorIdx,
	)

	for slot, instanceID := range applierInstanceIDs {
		applierSpec := execinfrapb.TxnLDRApplierSpec{
			ApplierID:             int32(instanceID),
			AllApplierIds:         applierIDs,
			JobID:                 spec.JobID,
			TableMetadataByDestID: spec.TableMetadataByDestID,
			TypeDescriptors:       spec.TypeDescriptors,
		}

		pIdx := plan.AddProcessor(physicalplan.Processor{
			SQLInstanceID: instanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					ColumnTypes: coordinatorOutputTypes,
				}},
				Core: execinfrapb.ProcessorCoreUnion{
					TxnLdrApplier: &applierSpec,
				},
				Post: execinfrapb.PostProcessSpec{},
				Output: []execinfrapb.OutputRouterSpec{{
					Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
					RangeRouterSpec: applierToDepResolverRouter,
				}},
				StageID:     applierStage,
				ResultTypes: applierOutputTypes,
			},
		})
		applierProcIdxs[instanceID] = pIdx

		// Wire coordinator → applier stream.
		plan.Streams = append(plan.Streams, physicalplan.Stream{
			SourceProcessor:  coordinatorProcIdx,
			SourceRouterSlot: slot,
			DestProcessor:    pIdx,
			DestInput:        0,
		})
	}

	// --- Stage 3: Dep resolver processors (co-located with appliers) ---

	depResolverStage := plan.NewStageOnNodes(applierInstanceIDs)
	depResolverProcIdxs := make([]physicalplan.ProcessorIdx, 0, len(applierInstanceIDs))

	for slot, instanceID := range applierInstanceIDs {
		depResolverSpec := execinfrapb.TxnLDRDepResolverSpec{
			ApplierID:     int32(instanceID),
			AllApplierIds: applierIDs,
		}

		pIdx := plan.AddProcessor(physicalplan.Processor{
			SQLInstanceID: instanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{{
					ColumnTypes: applierOutputTypes,
				}},
				Core: execinfrapb.ProcessorCoreUnion{
					TxnLdrDepResolver: &depResolverSpec,
				},
				Post: execinfrapb.PostProcessSpec{},
				Output: []execinfrapb.OutputRouterSpec{{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
				}},
				StageID:     depResolverStage,
				ResultTypes: applierOutputTypes,
			},
		})
		depResolverProcIdxs = append(depResolverProcIdxs, pIdx)

		// Wire all applier processors → this dep resolver.
		// Each applier's BY_RANGE router will route to the correct dep
		// resolver based on the target applier ID's instance.
		for _, applierInstanceID := range applierInstanceIDs {
			plan.Streams = append(plan.Streams, physicalplan.Stream{
				SourceProcessor:  applierProcIdxs[applierInstanceID],
				SourceRouterSlot: slot, // slot in the router for this dep resolver's instance
				DestProcessor:    pIdx,
				DestInput:        0,
			})
		}
	}

	// Metadata produced by applier processors flows through dep resolvers
	// (via their PASS_THROUGH output) to the gateway noop.
	plan.ResultRouters = depResolverProcIdxs

	// Add a final noop stage on the gateway to collect metadata forwarded
	// through dep resolver processors.
	plan.AddSingleGroupStage(
		ctx,
		gatewayID,
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		execinfrapb.PostProcessSpec{},
		applierOutputTypes,
		nil, /* finalizeLastStageCb */
	)

	plan.PlanToStreamColMap = []int{0, 1}
	return nil
}

// buildCoordinatorSpec constructs the full TxnLDRCoordinatorSpec from job
// metadata and the source replication plan.
func buildCoordinatorSpec(
	job *jobs.Job, sourcePlan streamclient.LogicalReplicationPlan, applierIDs []int32,
) execinfrapb.TxnLDRCoordinatorSpec {
	payload := job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails
	progress := job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication

	asOf := progress.ReplicatedTime
	if asOf.IsEmpty() {
		asOf = payload.ReplicationStartTime
	}

	partitionSpecs := make(
		[]execinfrapb.StreamIngestionPartitionSpec,
		len(sourcePlan.Topology.Partitions),
	)
	for i, partition := range sourcePlan.Topology.Partitions {
		partitionSpecs[i] = execinfrapb.StreamIngestionPartitionSpec{
			PartitionID:       partition.ID,
			SubscriptionToken: string(partition.SubscriptionToken),
			PartitionConnUri:  partition.ConnUri.Serialize(),
			Spans:             partition.Spans,
			SrcInstanceID:     base.SQLInstanceID(partition.SrcInstanceID),
		}
	}

	tableMetadataByDestID := make(map[int32]descpb.TableDescriptor)
	for _, pair := range payload.ReplicationPairs {
		srcTableDesc := sourcePlan.DescriptorMap[pair.SrcDescriptorID]
		tableMetadataByDestID[pair.DstDescriptorID] = srcTableDesc
	}

	return execinfrapb.TxnLDRCoordinatorSpec{
		JobID:                 job.ID(),
		StreamID:              payload.StreamID,
		ReplicatedTime:        asOf,
		ApplierIds:            applierIDs,
		PartitionSpecs:        partitionSpecs,
		TableMetadataByDestID: tableMetadataByDestID,
		TypeDescriptors:       sourcePlan.SourceTypes,
	}
}

// RunDistSQLFlow executes the DistSQL physical plan and processes results.
// Frontier updates from applier processors are persisted to the job and
// forwarded to frontierUpdates for the heartbeat sender.
func RunDistSQLFlow(
	ctx context.Context,
	execCtx sql.JobExecContext,
	plan *sql.PhysicalPlan,
	planCtx *sql.PlanningCtx,
	job *jobs.Job,
	frontierUpdates chan<- hlc.Timestamp,
) error {
	ch := &checkpointHandler{
		job:              job,
		sv:               &execCtx.ExecCfg().Settings.SV,
		frontierUpdates:  frontierUpdates,
		applierFrontiers: make(map[base.SQLInstanceID]hlc.Timestamp),
	}

	execCfg := execCtx.ExecCfg()
	rowResultWriter := sql.NewRowResultWriter(nil)
	distSQLReceiver := sql.MakeDistSQLReceiver(
		ctx,
		sql.NewMetadataCallbackWriter(rowResultWriter, ch.handleMeta),
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil, // txn
		nil, // clockUpdater
		execCtx.ExtendedEvalContext().Tracing,
	)
	defer distSQLReceiver.Release()

	evalCtxCopy := execCtx.ExtendedEvalContext().Context.Copy()

	execCtx.DistSQLPlanner().Run(
		ctx,
		planCtx,
		nil, // txn
		plan,
		distSQLReceiver,
		evalCtxCopy,
		nil, // finishedSetupFn
	)

	return rowResultWriter.Err()
}

// checkpointHandler receives frontier metadata from applier processors,
// tracks the minimum frontier across all appliers, and periodically
// persists it as the job's replicated time.
//
// TODO(msbutler): add time based checkpointing based on existing cluster
// setting and finer grain frontier updates.
type checkpointHandler struct {
	job             *jobs.Job
	sv              *settings.Values
	frontierUpdates chan<- hlc.Timestamp

	applierFrontiers map[base.SQLInstanceID]hlc.Timestamp
}

func (ch *checkpointHandler) handleMeta(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	if meta.BulkProcessorProgress == nil {
		return nil
	}

	var frontier hlc.Timestamp
	if err := pbtypes.UnmarshalAny(
		&meta.BulkProcessorProgress.ProgressDetails, &frontier,
	); err != nil {
		return errors.Wrap(err, "unmarshaling applier frontier")
	}

	applierID := meta.BulkProcessorProgress.NodeID
	ch.applierFrontiers[applierID] = frontier

	replicatedTime := ch.minFrontier()
	if !replicatedTime.IsSet() {
		return nil
	}

	log.Dev.VInfof(ctx, 2, "persisting replicated time of %s",
		replicatedTime.GoTime())

	if err := ch.job.NoTxn().Update(ctx,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if err := md.CheckRunningOrReverting(); err != nil {
				return err
			}
			progress := md.Progress
			prog := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
			prog.ReplicatedTime = replicatedTime
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &replicatedTime,
			}
			ju.UpdateProgress(progress)
			return nil
		}); err != nil {
		return err
	}

	select {
	case ch.frontierUpdates <- replicatedTime:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (ch *checkpointHandler) minFrontier() hlc.Timestamp {
	var min hlc.Timestamp
	for _, ts := range ch.applierFrontiers {
		if !ts.IsSet() {
			return hlc.Timestamp{}
		}
		if !min.IsSet() || ts.Less(min) {
			min = ts
		}
	}
	return min
}
