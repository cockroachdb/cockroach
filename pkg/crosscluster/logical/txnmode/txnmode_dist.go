// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrsettings"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnpb"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// PlanTxnReplication constructs a 3-stage DistSQL physical plan for
// transactional LDR:
//
//	Stage 1: LDR Coordinator (pinned to gateway) → BY_RANGE → N appliers
//	Stage 2: N LDR Appliers (one per SQL instance) → BY_RANGE → N dep resolvers
//	Stage 3: N LDR Dep Resolvers (co-located with appliers) → PASS_THROUGH → noop (gateway)
func PlanTxnReplication(
	ctx context.Context,
	job *jobs.Job,
	execCtx sql.JobExecContext,
	sourcePlan streamclient.LogicalReplicationPlan,
	replicatedTime hlc.Timestamp,
	endTime hlc.Timestamp,
) (_ *sql.PhysicalPlan, _ *sql.PlanningCtx, applierInstanceIDs []base.SQLInstanceID, _ error) {
	dsp := execCtx.DistSQLPlanner()

	// Set up planning across all available SQL instances.
	//
	// TODO(msbutler): restrict to instances that are leaseholders for the
	// destination key spans. Doing this accurately requires addressing #169815.
	planCtx, applierInstanceIDs, err := dsp.SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg(),
	)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "setting up all-nodes planning")
	}

	// ApplierID is an identity mapping to SQL instance ID.
	applierIDs := make([]int32, len(applierInstanceIDs))
	for i, instanceID := range applierInstanceIDs {
		applierIDs[i] = int32(instanceID)
	}

	spec := buildCoordinatorSpec(job, sourcePlan, applierIDs, replicatedTime, endTime)

	plan := planCtx.NewPhysicalPlan()
	if err := buildTxnReplicationPlan(
		ctx, plan, applierInstanceIDs, spec,
	); err != nil {
		return nil, nil, nil, err
	}
	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, applierInstanceIDs, nil
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

	// --- Stage 2: Applier processors (one per SQL instance) ---

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
			ApplierID:     int32(instanceID),
			AllApplierIds: applierIDs,
			JobID:         spec.JobID,
			Schema:        spec.Schema,
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
	job *jobs.Job,
	sourcePlan streamclient.LogicalReplicationPlan,
	applierIDs []int32,
	replicatedTime hlc.Timestamp,
	endTime hlc.Timestamp,
) execinfrapb.TxnLDRCoordinatorSpec {
	payload := job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails

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
		JobID:          job.ID(),
		StreamID:       payload.StreamID,
		ReplicatedTime: replicatedTime,
		ApplierIds:     applierIDs,
		PartitionSpecs: partitionSpecs,
		Schema: execinfrapb.LDRSchema{
			TableMetadataByDestID: tableMetadataByDestID,
			TypeDescriptors:       sourcePlan.SourceTypes,
		},
		EndTime: endTime,
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
	applierInstanceIDs []base.SQLInstanceID,
	replicatedTime hlc.Timestamp,
	endTime hlc.Timestamp,
) error {
	ctx, cancelFlow := context.WithCancel(ctx)
	defer cancelFlow()

	ch := newCheckpointHandler(
		job,
		execCtx.ExecCfg().InternalDB,
		&execCtx.ExecCfg().Settings.SV,
		frontierUpdates,
		applierInstanceIDs,
		replicatedTime,
		endTime,
		cancelFlow,
	)

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
// persists it as the job's replicated time. Persistence is throttled
// by ldrsettings.JobCheckpointFrequency so that intermediate frontier
// advances do not each incur KV writes.
//
// TODO(msbutler): understand why returnin an error in handlemeta doesn't shut
// the flow down.
type checkpointHandler struct {
	job             *jobs.Job
	db              isql.DB
	sv              *settings.Values
	frontierUpdates chan<- hlc.Timestamp
	endTime         hlc.Timestamp
	cancelFlow      context.CancelFunc
	replicatedTime  hlc.Timestamp

	lastPersistenceTime time.Time
	applierFrontiers    map[base.SQLInstanceID]hlc.Timestamp
}

// newCheckpointHandler creates a checkpointHandler pre-initialized with
// entries for every applier instance. On fresh start (replicatedTime is
// empty), each entry is unset so minFrontier waits for all appliers. On
// resume, each entry is seeded with replicatedTime so minFrontier can
// immediately return the checkpoint while waiting for fresher updates.
func newCheckpointHandler(
	job *jobs.Job,
	db isql.DB,
	sv *settings.Values,
	frontierUpdates chan<- hlc.Timestamp,
	applierInstanceIDs []base.SQLInstanceID,
	replicatedTime hlc.Timestamp,
	endTime hlc.Timestamp,
	cancelFlow context.CancelFunc,
) *checkpointHandler {
	applierFrontiers := make(
		map[base.SQLInstanceID]hlc.Timestamp, len(applierInstanceIDs),
	)
	for _, id := range applierInstanceIDs {
		applierFrontiers[id] = replicatedTime
	}
	return &checkpointHandler{
		job:              job,
		db:               db,
		sv:               sv,
		frontierUpdates:  frontierUpdates,
		endTime:          endTime,
		cancelFlow:       cancelFlow,
		replicatedTime:   replicatedTime,
		applierFrontiers: applierFrontiers,
	}
}

// handleMeta is a MetadataCallbackWriter callback. It only extracts frontier
// progress; meta.Err is handled separately by DistSQLReceiver.pushMeta, which
// calls r.SetError(meta.Err) after this callback returns.
func (ch *checkpointHandler) handleMeta(
	ctx context.Context, meta *execinfrapb.ProducerMetadata,
) error {
	if meta.Err != nil {
		ch.cancelFlow()
		return nil
	}

	if meta.BulkProcessorProgress == nil ||
		len(meta.BulkProcessorProgress.ProgressMessage) == 0 {
		return nil
	}

	var progress txnpb.TxnLDRProcProgress
	if err := protoutil.Unmarshal(
		meta.BulkProcessorProgress.ProgressMessage, &progress,
	); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "unmarshaling applier frontier progress")
	}

	applierID := base.SQLInstanceID(progress.ApplierID)
	if _, ok := ch.applierFrontiers[applierID]; !ok {
		return errors.AssertionFailedf(
			"received frontier progress from unknown applier %d", applierID)
	}
	ch.applierFrontiers[applierID] = progress.Checkpoint

	replicatedTime := ch.minFrontier()
	if !replicatedTime.IsSet() {
		return nil
	}

	if replicatedTime.LessEq(ch.replicatedTime) {
		return nil
	}

	reachedEndTime := ch.endTime.LessEq(replicatedTime)
	updateFreq := ldrsettings.JobCheckpointFrequency.Get(ch.sv)
	if !reachedEndTime && (updateFreq == 0 || timeutil.Since(ch.lastPersistenceTime) < updateFreq) {
		return nil
	}

	if !replicatedTime.LessEq(ch.replicatedTime) {
		if err := ch.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return ch.job.ProgressStorage().SetResolved(ctx, txn, replicatedTime)
		}); err != nil {
			return err
		}
		ch.replicatedTime = replicatedTime
		ch.lastPersistenceTime = timeutil.Now()
	}

	select {
	case ch.frontierUpdates <- replicatedTime:
	case <-ctx.Done():
		return ctx.Err()
	}
	if reachedEndTime {
		ch.cancelFlow()
		log.Dev.Infof(ctx, "reached end time: %s", ch.endTime.GoTime())
		return ErrEndTimeReached
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
