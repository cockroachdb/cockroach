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
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
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

	msg := redact.Sprintf("resuming stream (producer job %d) from %s", streamID, heartbeatTimestamp)
	updateRunningStatus(ctx, ingestionJob, jobspb.InitializingReplication, msg)

	client, err := connectToActiveClient(ctx, ingestionJob, execCtx.ExecCfg().InternalDB,
		streamclient.WithStreamID(streamID))
	if err != nil {
		return err
	}
	defer closeAndLog(ctx, client)
	if err := waitUntilProducerActive(ctx, client, streamID, heartbeatTimestamp, ingestionJob.ID()); err != nil {
		return err
	}

	log.Infof(ctx, "producer job %d is active, planning DistSQL flow", streamID)
	dsp := execCtx.DistSQLPlanner()

	planner, err := makeReplicationFlowPlanner(
		ctx,
		dsp,
		execCtx,
		ingestionJob.ID(),
		details,
		client,
		replicatedTime,
		streamProgress.Checkpoint,
		initialScanTimestamp,
		dsp.GatewayID())
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
	jobsprofiler.StorePlanDiagram(ctx, execCtx.ExecCfg().DistSQLSrv.Stopper, planner.initialPlan, execCtx.ExecCfg().InternalDB,
		ingestionJob.ID())

	replanOracle := sql.ReplanOnCustomFunc(
		measurePlanChange,
		func() float64 {
			return replanThreshold.Get(execCtx.ExecCfg().SV())
		},
	)

	replanner, stopReplanner := sql.PhysicalPlanChangeChecker(ctx,
		planner.initialPlan,
		planner.generatePlan,
		execCtx,
		replanOracle,
		func() time.Duration { return replanFrequency.Get(execCtx.ExecCfg().SV()) },
	)

	tracingAggCh := make(chan *execinfrapb.TracingAggregatorEvents)
	tracingAggLoop := func(ctx context.Context) error {
		if err := bulk.AggregateTracingStats(ctx, ingestionJob.ID(),
			execCtx.ExecCfg().Settings, execCtx.ExecCfg().InternalDB, tracingAggCh); err != nil {
			log.Warningf(ctx, "failed to aggregate tracing stats: %v", err)
			// Drain the channel if the loop to aggregate tracing stats has returned
			// an error.
			for range tracingAggCh {
			}
		}
		return nil
	}

	spanConfigIngestStopper := make(chan struct{})
	streamSpanConfigs := func(ctx context.Context) error {
		if knobs := execCtx.ExecCfg().StreamingTestingKnobs; knobs != nil && knobs.SkipSpanConfigReplication {
			return nil
		}
		sourceTenantID, err := planner.getSrcTenantID()
		if err != nil {
			return err
		}
		ingestor, err := makeSpanConfigIngestor(ctx, execCtx.ExecCfg(), ingestionJob, sourceTenantID, spanConfigIngestStopper)
		if err != nil {
			return err
		}
		return ingestor.ingestSpanConfigs(ctx, details.SourceTenantName)
	}

	execInitialPlan := func(ctx context.Context) error {
		defer func() {
			stopReplanner()
			close(tracingAggCh)
			close(spanConfigIngestStopper)
		}()
		ctx = logtags.AddTag(ctx, "stream-ingest-distsql", nil)

		metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
			if meta.AggregatorEvents != nil {
				tracingAggCh <- meta.AggregatorEvents
			}
			return nil
		}

		rw := sql.NewRowResultWriter(nil /* rowContainer */)

		var noTxn *kv.Txn
		recv := sql.MakeDistSQLReceiver(
			ctx,
			sql.NewMetadataCallbackWriter(rw, metaFn),
			tree.Rows,
			nil, /* rangeCache */
			noTxn,
			nil, /* clockUpdater */
			execCtx.ExtendedEvalContext().Tracing,
		)
		defer recv.Release()

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *execCtx.ExtendedEvalContext()
		dsp.Run(ctx, planner.initialPlanCtx, noTxn, planner.initialPlan, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return rw.Err()
	}

	updateRunningStatus(ctx, ingestionJob, jobspb.Replicating, "physical replication running")
	err = ctxgroup.GoAndWait(ctx, execInitialPlan, replanner, tracingAggLoop, streamSpanConfigs)
	if errors.Is(err, sql.ErrPlanChanged) {
		execCtx.ExecCfg().JobRegistry.MetricsStruct().StreamIngest.(*Metrics).ReplanCount.Inc(1)
	}
	return err
}

// makeReplicationFlowPlanner creates a replicationFlowPlanner and the initial physical plan.
func makeReplicationFlowPlanner(
	ctx context.Context,
	dsp *sql.DistSQLPlanner,
	execCtx sql.JobExecContext,
	ingestionJobID jobspb.JobID,
	details jobspb.StreamIngestionDetails,
	client streamclient.Client,
	previousReplicatedTime hlc.Timestamp,
	checkpoint jobspb.StreamIngestionCheckpoint,
	initialScanTimestamp hlc.Timestamp,
	gatewayID base.SQLInstanceID,
) (replicationFlowPlanner, error) {

	planner := replicationFlowPlanner{}
	planner.generatePlan = planner.constructPlanGenerator(execCtx, ingestionJobID, details, client, previousReplicatedTime, checkpoint, initialScanTimestamp, gatewayID)

	var err error
	planner.initialPlan, planner.initialPlanCtx, err = planner.generatePlan(ctx, dsp)
	return planner, err

}

// replicationFlowPlanner can generate c2c physical plans. To populate the
// replicationFlowPlanner's state correctly, it must be constructed via
// makeReplicationFlowPlanner.
type replicationFlowPlanner struct {
	// generatePlan generates a c2c physical plan.
	generatePlan func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error)

	initialPlan *sql.PhysicalPlan

	initialPlanCtx *sql.PlanningCtx

	initialStreamAddresses []string

	srcTenantID roachpb.TenantID
}

func (p *replicationFlowPlanner) containsInitialStreamAddresses() bool {
	return len(p.initialStreamAddresses) > 0
}

func (p *replicationFlowPlanner) getSrcTenantID() (roachpb.TenantID, error) {
	if p.srcTenantID.InternalValue == 0 {
		return p.srcTenantID, errors.AssertionFailedf("makeReplicationFlowPlanner must be called before p.getSrcID")
	}
	return p.srcTenantID, nil
}

func (p *replicationFlowPlanner) constructPlanGenerator(
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
		log.Infof(ctx, "generating DistSQL plan candidate")
		streamID := streampb.StreamID(details.StreamID)
		topology, err := client.Plan(ctx, streamID)
		if err != nil {
			return nil, nil, err
		}
		if !p.containsInitialStreamAddresses() {
			p.initialStreamAddresses = topology.StreamAddresses()
		}

		p.srcTenantID = topology.SourceTenantID

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
		corePlacement := make([]physicalplan.ProcessorCorePlacement, 0, len(streamIngestionSpecs))
		for instanceID := range streamIngestionSpecs {
			for _, partSpec := range streamIngestionSpecs[instanceID] {
				corePlacement = append(corePlacement, physicalplan.ProcessorCorePlacement{
					SQLInstanceID: instanceID,
					Core: execinfrapb.ProcessorCoreUnion{
						StreamIngestionData: partSpec,
					},
				})
			}
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

// maxProcessorsPerSQLInstance controls how many ingestion processors
// we allow to be assigned to a given SQL instance.
const maxProcessorsPerSQLInstance = 1

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
) (
	map[base.SQLInstanceID][]*execinfrapb.StreamIngestionDataSpec,
	*execinfrapb.StreamIngestionFrontierSpec,
	error,
) {
	streamIngestionSpecs := make(map[base.SQLInstanceID][]*execinfrapb.StreamIngestionDataSpec, len(destSQLInstances))
	trackedSpans := make([]roachpb.Span, 0)
	subscribingSQLInstances := make(map[string]uint32)

	ingestionDataSpec := func() *execinfrapb.StreamIngestionDataSpec {
		return &execinfrapb.StreamIngestionDataSpec{
			StreamID:                    uint64(streamID),
			JobID:                       int64(jobID),
			PreviousReplicatedTimestamp: previousReplicatedTimestamp,
			InitialScanTimestamp:        initialScanTimestamp,
			PartitionSpecs:              make(map[string]execinfrapb.StreamIngestionPartitionSpec),
			Checkpoint:                  checkpoint, // TODO: Only forward relevant checkpoint info
			StreamAddress:               string(streamAddress),
			TenantRekey: execinfrapb.TenantRekey{
				OldID: sourceTenantID,
				NewID: destinationTenantID,
			},
		}
	}

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

		partSpec := execinfrapb.StreamIngestionPartitionSpec{
			PartitionID:       partition.ID,
			SubscriptionToken: string(partition.SubscriptionToken),
			Address:           string(partition.SrcAddr),
			Spans:             partition.Spans,
		}

		if len(streamIngestionSpecs[destID]) < maxProcessorsPerSQLInstance {
			spec := ingestionDataSpec()
			spec.PartitionSpecs[partition.ID] = partSpec
			streamIngestionSpecs[destID] = append(streamIngestionSpecs[destID], spec)
		} else {
			// Add this to an existing processor that has
			// the fewest partitions already assigned to
			// it.
			fewestPartitions := func(specs []*execinfrapb.StreamIngestionDataSpec) int {
				lowestIdx := 0
				lowestCount := 0
				for i := range specs {
					partSpecCount := len(specs[i].PartitionSpecs)
					if partSpecCount < lowestCount {
						lowestCount = partSpecCount
						lowestIdx = i
					}
				}
				return lowestIdx
			}
			procIdx := fewestPartitions(streamIngestionSpecs[destID])
			streamIngestionSpecs[destID][procIdx].PartitionSpecs[partition.ID] = partSpec
		}
		trackedSpans = append(trackedSpans, partition.Spans...)
	}

	err := divideSpecsIfCapacity(streamIngestionSpecs, ingestionDataSpec, maxProcessorsPerSQLInstance)
	if err != nil {
		return nil, nil, err
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

// divideSpecsIfCapacity iterates the given spec map and if processors
// are available for a given SQL instance it finds the partition with
// the most spans and splits them amongst the remaining processor
// capacity.
//
// We don't look to divide specs across nodes since we've already done
// locality matching by the time we call this code.
func divideSpecsIfCapacity(
	specMap map[base.SQLInstanceID][]*execinfrapb.StreamIngestionDataSpec,
	defaultSpecBuilder func() *execinfrapb.StreamIngestionDataSpec,
	maxWorkerCount int,
) error {

	splitSpans := func(spans []roachpb.Span, maxBatches int) [][]roachpb.Span {
		batchSize := len(spans) / maxBatches
		if batchSize == 0 {
			batchSize = 1
		}
		batches := make([][]roachpb.Span, 0, maxBatches)
		for batchSize < len(spans) && len(batches) < (maxBatches-1) {
			spans, batches = spans[batchSize:], append(batches, spans[0:batchSize:batchSize])
		}
		batches = append(batches, spans)
		return batches
	}

	partitionSpecWithMostSpans := func(processors []*execinfrapb.StreamIngestionDataSpec) (int, string) {
		processorIdx := 0
		partitionSpecID := ""
		maxSpansSeen := 0
		for i := range processors {
			for partID, partSpec := range processors[i].PartitionSpecs {
				if len(partSpec.Spans) > maxSpansSeen {
					processorIdx = i
					partitionSpecID = partID
					maxSpansSeen = len(partSpec.Spans)
				}
			}
		}
		return processorIdx, partitionSpecID
	}

	tokenForPartitionSpecWithSpans := func(spec streampb.StreamPartitionSpec, spans []roachpb.Span) (string, error) {
		spec.Spans = spans
		marshaledToken, err := protoutil.Marshal(&spec)
		if err != nil {
			return "", err
		}
		return string(marshaledToken), nil
	}

	for destID, specs := range specMap {
		if len(specs) < maxWorkerCount {
			remainingProcs := maxWorkerCount - len(specs)
			processorID, partID := partitionSpecWithMostSpans(specs)
			if partID == "" {
				continue
			}

			origPartSpec := specs[processorID].PartitionSpecs[partID]
			if len(origPartSpec.Spans) == 1 {
				continue
			}

			// Parse the original subscription token. We
			// need to rewrite this to represent the new
			// set of spans.
			origSubscriptionSpec := streampb.StreamPartitionSpec{}
			err := protoutil.Unmarshal(
				streamclient.SubscriptionToken(origPartSpec.SubscriptionToken),
				&origSubscriptionSpec)
			if err != nil {
				return err
			}

			spanBatches := splitSpans(origPartSpec.Spans, remainingProcs+1)

			// Modify the existing processor that contained this
			// partition.
			spec := origPartSpec
			spec.Spans = spanBatches[0]
			spec.SubscriptionToken, err = tokenForPartitionSpecWithSpans(origSubscriptionSpec, spec.Spans)
			if err != nil {
				return err
			}
			specMap[destID][processorID].PartitionSpecs[partID] = spec

			// Add additional processors for the remaining batches.
			for i := 1; i < len(spanBatches); i++ {
				spec := defaultSpecBuilder()
				partSpec := origPartSpec
				partSpec.Spans = spanBatches[i]
				partSpec.SubscriptionToken, err = tokenForPartitionSpecWithSpans(origSubscriptionSpec, partSpec.Spans)
				if err != nil {
					return err
				}
				spec.PartitionSpecs[partID] = partSpec
				specMap[destID] = append(specMap[destID], spec)
			}

		}
	}
	return nil
}

// waitUntilProducerActive pings the producer job and waits until it
// is active/running. It returns nil when the job is active.
func waitUntilProducerActive(
	ctx context.Context,
	client streamclient.Client,
	streamID streampb.StreamID,
	heartbeatTimestamp hlc.Timestamp,
	ingestionJobID jobspb.JobID,
) error {
	ro := retry.Options{
		InitialBackoff: 1 * time.Second,
		Multiplier:     2,
		MaxBackoff:     5 * time.Second,
		MaxRetries:     4,
	}
	// Make sure the producer job is active before start the stream replication.
	var status streampb.StreamReplicationStatus
	var err error
	for r := retry.Start(ro); r.Next(); {
		status, err = client.Heartbeat(ctx, streamID, heartbeatTimestamp)
		if err != nil {
			return errors.Wrapf(err, "failed to resume ingestion job %d due to producer job %d error",
				ingestionJobID, streamID)
		}
		if status.StreamStatus != streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY {
			break
		}
		log.Warningf(ctx, "producer job %d has status %s, retrying", streamID, status.StreamStatus)
	}
	if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE {
		return jobs.MarkAsPermanentJobError(errors.Errorf("failed to resume ingestion job %d "+
			"as the producer job %d is not active and in status %s", ingestionJobID,
			streamID, status.StreamStatus))
	}
	return nil
}
