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
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var replanThreshold = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"stream_replication.replan_flow_threshold",
	"fraction of nodes in the producer or consumer job that would need to change to refresh the"+
		" physical execution plan. If set to 0, the physical plan will not automatically refresh.",
	0,
	settings.NonNegativeFloatWithMaximum(1),
)

var replanFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"stream_replication.replan_flow_frequency",
	"frequency at which the consumer job checks to refresh its physical execution plan",
	10*time.Minute,
	settings.PositiveDuration,
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

	planner := replicationFlowPlanner{}

	initialPlan, planCtx, err := planner.makePlan(
		execCtx,
		ingestionJob.ID(),
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

	err = ingestionJob.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		// Persist the initial Stream Addresses to the jobs table before execution begins.
		if !planner.containsInitialStreamAddresses() {
			return jobs.MarkAsPermanentJobError(errors.AssertionFailedf(
				"attempted to persist an empty list of stream addresses"))
		}
		md.Progress.GetStreamIngest().StreamAddresses = planner.initialStreamAddresses
		ju.UpdateProgress(md.Progress)
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to update job progress")
	}
	jobsprofiler.StorePlanDiagram(ctx, execCtx.ExecCfg().DistSQLSrv.Stopper, initialPlan, execCtx.ExecCfg().InternalDB,
		ingestionJob.ID())

	replanOracle := sql.ReplanOnCustomFunc(
		measurePlanChange,
		func() float64 {
			return replanThreshold.Get(execCtx.ExecCfg().SV())
		},
	)

	replanner, stopReplanner := sql.PhysicalPlanChangeChecker(ctx,
		initialPlan,
		planner.makePlan(
			execCtx,
			ingestionJob.ID(),
			details,
			client,
			replicatedTime,
			streamProgress.Checkpoint,
			initialScanTimestamp,
			dsp.GatewayID(),
		),
		execCtx,
		replanOracle,
		func() time.Duration { return replanFrequency.Get(execCtx.ExecCfg().SV()) },
	)

	execInitialPlan := func(ctx context.Context) error {
		log.Infof(ctx, "starting to run DistSQL flow for stream ingestion job %d",
			ingestionJob.ID())
		defer stopReplanner()
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

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *execCtx.ExtendedEvalContext()
		dsp.Run(ctx, planCtx, noTxn, initialPlan, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return rw.Err()
	}

	updateRunningStatus(ctx, ingestionJob, jobspb.Replicating,
		"running the SQL flow for the stream ingestion job")
	err = ctxgroup.GoAndWait(ctx, execInitialPlan, replanner)
	if errors.Is(err, sql.ErrPlanChanged) {
		execCtx.ExecCfg().JobRegistry.MetricsStruct().StreamIngest.(*Metrics).ReplanCount.Inc(1)
	}
	return err
}

type replicationFlowPlanner struct {
	// initial contains the stream addresses found during the replicationFlowPlanner's first makePlan call.
	initialStreamAddresses []string
}

func (p *replicationFlowPlanner) containsInitialStreamAddresses() bool {
	return len(p.initialStreamAddresses) > 0
}

func (p *replicationFlowPlanner) makePlan(
	execCtx sql.JobExecContext,
	ingestionJobID jobspb.JobID,
	details jobspb.StreamIngestionDetails,
	client streamclient.Client,
	previousReplicatedTime hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	initialScanTimestamp hlc.Timestamp,
	gatewayID base.SQLInstanceID,
) func(context.Context, *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	return func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
		log.Infof(ctx, "Generating DistSQL plan candidate for stream ingestion job %d",
			ingestionJobID)

		streamID := streampb.StreamID(details.StreamID)
		topology, err := client.Plan(ctx, streamID)
		if err != nil {
			return nil, nil, err
		}
		if !p.containsInitialStreamAddresses() {
			p.initialStreamAddresses = topology.StreamAddresses()
		}

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
		if err != nil {
			return nil, nil, err
		}
		destNodeLocalities, err := getDestNodeLocalities(ctx, dsp, sqlInstanceIDs)
		if err != nil {
			return nil, nil, err
		}

		streamIngestionSpecs, streamIngestionFrontierSpec, err := constructStreamIngestionPlanSpecs(
			ctx,
			streamingccl.StreamAddress(details.StreamAddress),
			topology,
			destNodeLocalities,
			initialScanTimestamp,
			previousReplicatedTime,
			checkpoint,
			ingestionJobID,
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

		p.PlanToStreamColMap = []int{0}
		sql.FinalizePlan(ctx, planCtx, p)
		return p, planCtx, nil
	}
}

// measurePlanChange computes the number of node changes (addition or removal)
// in the source and destination clusters as a fraction of the total number of
// nodes in both clusters in the previous plan.
func measurePlanChange(before, after *sql.PhysicalPlan) float64 {

	getNodes := func(plan *sql.PhysicalPlan) (src, dst map[string]struct{}, nodeCount int) {
		dst = make(map[string]struct{})
		src = make(map[string]struct{})
		count := 0
		for _, proc := range plan.Processors {
			if proc.Spec.Core.StreamIngestionData == nil {
				// Skip other processors in the plan (like the Frontier processor).
				continue
			}
			dst[proc.SQLInstanceID.String()] = struct{}{}
			count += 1
			for id := range proc.Spec.Core.StreamIngestionData.PartitionSpecs {
				src[id] = struct{}{}
				count += 1
			}
		}
		return src, dst, count
	}

	countMissingElements := func(set1, set2 map[string]struct{}) int {
		diff := 0
		for id := range set1 {
			if _, ok := set2[id]; !ok {
				diff++
			}
		}
		return diff
	}

	oldSrc, oldDst, oldCount := getNodes(before)
	newSrc, newDst, _ := getNodes(after)
	diff := 0
	// To check for both introduced nodes and removed nodes, swap input order.
	diff += countMissingElements(oldSrc, newSrc)
	diff += countMissingElements(newSrc, oldSrc)
	diff += countMissingElements(oldDst, newDst)
	diff += countMissingElements(newDst, oldDst)
	return float64(diff) / float64(oldCount)
}

type partitionWithCandidates struct {
	partition          streamclient.PartitionInfo
	closestDestIDs     []base.SQLInstanceID
	sharedPrefixLength int
}

type candidatesByPriority []partitionWithCandidates

func (a candidatesByPriority) Len() int      { return len(a) }
func (a candidatesByPriority) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a candidatesByPriority) Less(i, j int) bool {
	return a[i].sharedPrefixLength > a[j].sharedPrefixLength
}

// nodeMatcher matches each source cluster node to a destination cluster node,
// given a list of available nodes in each cluster. The matcher has a primary goal
// to match src-dst nodes that are "close" to each other, i.e. have common
// locality tags, and a secondary goal to distribute source node assignments
// evenly across destination nodes. Here's the algorithm:
//
// - For each src node, find their closest dst nodes and the number of
// localities that match, the LocalityMatchCount, via the sql.ClosestInstances()
// function. Example: Consider Src-A [US,East] which has match candidates Dst-A
// [US,West], Dst-B [US, Central]. In the example, the LocalityMatchCount is 1,
// as only US matches with the src node's locality.
//
// - Prioritize matching src nodes with a higher locality match count, via the
// findSourceNodePriority() function.
//
// - While we have src nodes left to match, match the highest priority src node
// to the dst node candidate that has the fewest matches already, via the
// findMatch() function.

type nodeMatcher struct {
	destMatchCount     map[base.SQLInstanceID]int
	destNodesInfo      []sql.InstanceLocality
	destNodeToLocality map[base.SQLInstanceID]roachpb.Locality
}

func makeNodeMatcher(destNodesInfo []sql.InstanceLocality) *nodeMatcher {
	nodeToLocality := make(map[base.SQLInstanceID]roachpb.Locality, len(destNodesInfo))
	for _, node := range destNodesInfo {
		nodeToLocality[node.GetInstanceID()] = node.GetLocality()
	}
	return &nodeMatcher{
		destMatchCount:     make(map[base.SQLInstanceID]int, len(destNodesInfo)),
		destNodesInfo:      destNodesInfo,
		destNodeToLocality: nodeToLocality,
	}
}

func (nm *nodeMatcher) destNodeIDs() []base.SQLInstanceID {
	allDestNodeIDs := make([]base.SQLInstanceID, 0, len(nm.destNodesInfo))
	for _, info := range nm.destNodesInfo {
		allDestNodeIDs = append(allDestNodeIDs, info.GetInstanceID())
	}
	return allDestNodeIDs
}

// findSourceNodePriority finds the closest dest nodes for each source node and
// returns a list of (source node, dest node match candidates) pairs ordered by
// matching priority. A source node is earlier (higher priority) in the list if
// it shares more locality tiers with their destination node match candidates.
func (nm *nodeMatcher) findSourceNodePriority(topology streamclient.Topology) candidatesByPriority {

	allDestNodeIDs := nm.destNodeIDs()
	candidates := make(candidatesByPriority, 0, len(topology.Partitions))
	for _, partition := range topology.Partitions {
		closestDestIDs, sharedPrefixLength := sql.ClosestInstances(nm.destNodesInfo,
			partition.SrcLocality)
		if sharedPrefixLength == 0 {
			closestDestIDs = allDestNodeIDs
		}
		candidate := partitionWithCandidates{
			partition:          partition,
			closestDestIDs:     closestDestIDs,
			sharedPrefixLength: sharedPrefixLength,
		}
		candidates = append(candidates, candidate)
	}
	sort.Sort(candidates)

	return candidates
}

// findMatch returns the destination node id with the fewest src node matches from the input list.
func (nm *nodeMatcher) findMatch(destIDCandidates []base.SQLInstanceID) base.SQLInstanceID {
	minCount := math.MaxInt
	currentMatch := base.SQLInstanceID(0)

	for _, destID := range destIDCandidates {
		currentDestCount := nm.destMatchCount[destID]
		if currentDestCount < minCount {
			currentMatch = destID
			minCount = currentDestCount
		}
	}
	nm.destMatchCount[currentMatch]++
	return currentMatch
}

func getDestNodeLocalities(
	ctx context.Context, dsp *sql.DistSQLPlanner, instanceIDs []base.SQLInstanceID,
) ([]sql.InstanceLocality, error) {

	instanceInfos := make([]sql.InstanceLocality, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		nodeDesc, err := dsp.GetSQLInstanceInfo(id)
		if err != nil {
			log.Eventf(ctx, "unable to get node descriptor for sql node %s", id)
			return nil, err
		}
		instanceInfos = append(instanceInfos, sql.MakeInstanceLocality(id, nodeDesc.Locality))
	}
	return instanceInfos, nil
}

func constructStreamIngestionPlanSpecs(
	ctx context.Context,
	streamAddress streamingccl.StreamAddress,
	topology streamclient.Topology,
	destSQLInstances []sql.InstanceLocality,
	initialScanTimestamp hlc.Timestamp,
	previousReplicatedTimestamp hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	jobID jobspb.JobID,
	streamID streampb.StreamID,
	sourceTenantID roachpb.TenantID,
	destinationTenantID roachpb.TenantID,
) ([]*execinfrapb.StreamIngestionDataSpec, *execinfrapb.StreamIngestionFrontierSpec, error) {

	streamIngestionSpecs := make([]*execinfrapb.StreamIngestionDataSpec, 0, len(destSQLInstances))
	destSQLInstancesToIdx := make(map[base.SQLInstanceID]int, len(destSQLInstances))
	for i, id := range destSQLInstances {
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
		destSQLInstancesToIdx[id.GetInstanceID()] = i
	}

	trackedSpans := make([]roachpb.Span, 0)
	subscribingSQLInstances := make(map[string]uint32)

	// Update stream ingestion specs with their matched source node.
	matcher := makeNodeMatcher(destSQLInstances)
	for _, candidate := range matcher.findSourceNodePriority(topology) {
		destID := matcher.findMatch(candidate.closestDestIDs)
		log.Infof(ctx, "physical replication src-dst pair candidate: %s (locality %s) - %d ("+
			"locality %s)",
			candidate.partition.ID,
			candidate.partition.SrcLocality,
			destID,
			matcher.destNodeToLocality[destID])
		partition := candidate.partition
		subscribingSQLInstances[partition.ID] = uint32(destID)

		specIdx, ok := destSQLInstancesToIdx[destID]
		if !ok {
			return nil, nil, errors.AssertionFailedf(
				"matched destination node id does not contain a stream ingestion spec")
		}
		streamIngestionSpecs[specIdx].PartitionSpecs[partition.ID] = execinfrapb.
			StreamIngestionPartitionSpec{
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
