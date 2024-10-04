// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	streamIngestionFrontierProcName = `ingestfntr`

	// frontierEntriesFilename is the name of the file at which the stream ingestion
	// frontier periodically dumps its state.
	frontierEntriesFilename = "~replication-frontier-entries.binpb"
)

type streamIngestionFrontier struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.StreamIngestionFrontierSpec
	alloc   tree.DatumAlloc

	// input returns rows from one or more streamIngestion processors.
	input execinfra.RowSource

	// frontier contains the current resolved timestamp high-water for the tracked
	// span set.
	frontier *span.Frontier

	// metrics are monitoring all running ingestion jobs.
	metrics *Metrics

	// heartbeatSender sends heartbeats to the source cluster to keep the replication
	// stream alive.
	heartbeatSender *heartbeatSender

	// replicatedTimeAtStart is the job's replicated time when
	// this processor started. It's used in an assertion that we
	// never regress the job's replicated time.
	replicatedTimeAtStart hlc.Timestamp
	// persistedReplicatedTime stores the highwater mark of
	// progress that is persisted in the job record.
	persistedReplicatedTime hlc.Timestamp

	lastPartitionUpdate time.Time
	lastFrontierDump    time.Time
	partitionProgress   map[string]jobspb.StreamIngestionProgress_PartitionProgress

	lastNodeLagCheck time.Time

	// replicatedTimeAtLastPositiveLagNodeCheck records the replicated time the
	// last time the lagging node checker detected a lagging node.
	replicatedTimeAtLastPositiveLagNodeCheck hlc.Timestamp
}

var _ execinfra.Processor = &streamIngestionFrontier{}
var _ execinfra.RowSource = &streamIngestionFrontier{}

func init() {
	rowexec.NewStreamIngestionFrontierProcessor = newStreamIngestionFrontierProcessor
}

func newStreamIngestionFrontierProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.StreamIngestionFrontierSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	frontier, err := span.MakeFrontierAt(spec.ReplicatedTimeAtStart, spec.TrackedSpans...)
	if err != nil {
		return nil, err
	}
	for _, resolvedSpan := range spec.Checkpoint.ResolvedSpans {
		if _, err := frontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp); err != nil {
			return nil, err
		}
	}

	heartbeatSender, err := newHeartbeatSender(ctx, flowCtx, spec)
	if err != nil {
		return nil, err
	}
	partitionProgress := make(map[string]jobspb.StreamIngestionProgress_PartitionProgress)
	for srcPartitionID, destSQLInstanceID := range spec.SubscribingSQLInstances {
		partitionProgress[srcPartitionID] = jobspb.StreamIngestionProgress_PartitionProgress{
			DestSQLInstanceID: base.SQLInstanceID(destSQLInstanceID),
		}
	}
	sf := &streamIngestionFrontier{
		flowCtx:                 flowCtx,
		spec:                    spec,
		input:                   input,
		replicatedTimeAtStart:   spec.ReplicatedTimeAtStart,
		frontier:                frontier,
		partitionProgress:       partitionProgress,
		metrics:                 flowCtx.Cfg.JobRegistry.MetricsStruct().StreamIngest.(*Metrics),
		heartbeatSender:         heartbeatSender,
		persistedReplicatedTime: spec.ReplicatedTimeAtStart,
	}
	if err := sf.Init(
		ctx,
		sf,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{sf.input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				sf.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return sf, nil
}

// MustBeStreaming implements the execinfra.Processor interface.
func (sf *streamIngestionFrontier) MustBeStreaming() bool {
	return true
}

type heartbeatSender struct {
	lastSent        time.Time
	client          streamclient.Client
	streamID        streampb.StreamID
	frontierUpdates chan hlc.Timestamp
	frontier        hlc.Timestamp
	sv              *settings.Values
	// cg runs the heartbeatSender thread.
	cg ctxgroup.Group
	// cancel stops heartbeat sender.
	cancel func()
	// heartbeatSender closes this channel when it stops.
	stoppedChan chan struct{}
}

func newHeartbeatSender(
	ctx context.Context, flowCtx *execinfra.FlowCtx, spec execinfrapb.StreamIngestionFrontierSpec,
) (*heartbeatSender, error) {

	streamID := streampb.StreamID(spec.StreamID)
	streamClient, err := streamclient.GetFirstActiveClient(ctx, spec.StreamAddresses, flowCtx.Cfg.DB, streamclient.WithStreamID(streamID))
	if err != nil {
		return nil, err
	}
	return &heartbeatSender{
		client:          streamClient,
		streamID:        streamID,
		sv:              &flowCtx.EvalCtx.Settings.SV,
		frontierUpdates: make(chan hlc.Timestamp),
		cancel:          func() {},
		stoppedChan:     make(chan struct{}),
	}, nil
}

func (h *heartbeatSender) maybeHeartbeat(
	ctx context.Context,
	ts timeutil.TimeSource,
	frontier hlc.Timestamp,
	heartbeatFrequency time.Duration,
) (bool, streampb.StreamReplicationStatus, error) {
	if h.lastSent.Add(heartbeatFrequency).After(ts.Now()) {
		return false, streampb.StreamReplicationStatus{}, nil
	}
	h.lastSent = ts.Now()
	s, err := h.client.Heartbeat(ctx, h.streamID, frontier)
	return true, s, err
}

func (h *heartbeatSender) startHeartbeatLoop(ctx context.Context, ts timeutil.TimeSource) {
	ctx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.cg = ctxgroup.WithContext(ctx)
	h.cg.GoCtx(func(ctx context.Context) error {
		sendHeartbeats := func() error {
			// The heartbeat thread send heartbeats when there is a frontier update,
			// and it has been a while since last time we sent it, or when we need
			// to heartbeat to keep the stream alive even if the frontier has no update.
			timer := ts.NewTimer()
			timer.Reset(streamingccl.StreamReplicationConsumerHeartbeatFrequency.Get(h.sv))
			defer timer.Stop()
			unknownStreamStatusRetryErr := log.Every(1 * time.Minute)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.Ch():
					timer.MarkRead()
					timer.Reset(streamingccl.StreamReplicationConsumerHeartbeatFrequency.Get(h.sv))
				case frontier := <-h.frontierUpdates:
					h.frontier.Forward(frontier)
				}
				heartbeatFrequency := streamingccl.StreamReplicationConsumerHeartbeatFrequency.Get(h.sv)
				sent, streamStatus, err := h.maybeHeartbeat(ctx, ts, h.frontier, heartbeatFrequency)
				if err != nil {
					log.Errorf(ctx, "replication stream %d received an error from the producer job: %v", h.streamID, err)
					continue
				}

				if !sent || streamStatus.StreamStatus == streampb.StreamReplicationStatus_STREAM_ACTIVE {
					continue
				}

				if streamStatus.StreamStatus == streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY {
					if unknownStreamStatusRetryErr.ShouldLog() {
						log.Warningf(ctx, "replication stream %d has unknown stream status error and will retry later", h.streamID)
					}
					continue
				}
				// The replication stream is either paused or inactive.
				return streamingccl.NewStreamStatusErr(h.streamID, streamStatus.StreamStatus)
			}
		}
		err := errors.CombineErrors(sendHeartbeats(), h.client.Close(ctx))
		close(h.stoppedChan)
		return err
	})
}

// Stop the heartbeat loop and returns any error at time of heartbeatSender's exit.
// Can be called multiple times.
func (h *heartbeatSender) stop() error {
	h.cancel()
	return h.wait()
}

// Wait for heartbeatSender to be stopped and returns any error.
func (h *heartbeatSender) wait() error {
	err := h.cg.Wait()
	// We expect to see context cancelled when shutting down.
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Start is part of the RowSource interface.
func (sf *streamIngestionFrontier) Start(ctx context.Context) {
	ctx = sf.StartInternal(ctx, streamIngestionFrontierProcName)
	sf.metrics.RunningCount.Inc(1)
	sf.input.Start(ctx)
	sf.heartbeatSender.startHeartbeatLoop(ctx, timeutil.DefaultTimeSource{})
}

// Next is part of the RowSource interface.
func (sf *streamIngestionFrontier) Next() (
	row rowenc.EncDatumRow,
	meta *execinfrapb.ProducerMetadata,
) {
	for sf.State == execinfra.StateRunning {
		row, meta := sf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				sf.MoveToDrainingAndLogError(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			sf.MoveToDrainingAndLogError(nil /* err */)
			break
		}

		if err := sf.noteResolvedTimestamps(row[0]); err != nil {
			sf.MoveToDrainingAndLogError(err)
			break
		}

		if err := sf.maybeUpdateProgress(); err != nil {
			log.Errorf(sf.Ctx(), "failed to update progress: %+v", err)
			sf.MoveToDrainingAndLogError(err)
			break
		}

		if err := sf.maybePersistFrontierEntries(); err != nil {
			log.Errorf(sf.Ctx(), "failed to persist frontier entries: %+v", err)
		}

		if err := sf.maybeCheckForLaggingNodes(); err != nil {
			sf.MoveToDrainingAndLogError(err)
			break
		}

		// Send back a row to the job so that it can update the progress.
		select {
		case <-sf.Ctx().Done():
			sf.MoveToDrainingAndLogError(sf.Ctx().Err())
			return nil, sf.DrainHelper()
			// Send the latest persisted replicated time in the heartbeat to the source cluster
			// as even with retries we will never request an earlier row than it, and
			// the source cluster is free to clean up earlier data.
		case sf.heartbeatSender.frontierUpdates <- sf.persistedReplicatedTime:
			// If heartbeatSender has error, it means remote has error, we want to
			// stop the processor.
		case <-sf.heartbeatSender.stoppedChan:
			err := sf.heartbeatSender.wait()
			if err != nil {
				log.Errorf(sf.Ctx(), "heartbeat sender exited with error: %s", err)
			}
			sf.MoveToDrainingAndLogError(err)
			return nil, sf.DrainHelper()
		}
	}
	return nil, sf.DrainHelper()
}

func (sf *streamIngestionFrontier) MoveToDrainingAndLogError(err error) {
	log.Infof(sf.Ctx(), "gracefully draining with error %s", err)
	sf.MoveToDraining(err)
}

func (sf *streamIngestionFrontier) close() {
	if err := sf.heartbeatSender.stop(); err != nil {
		log.Errorf(sf.Ctx(), "heartbeat sender exited with error: %s", err)
	}
	if sf.InternalClose() {
		sf.metrics.RunningCount.Dec(1)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (sf *streamIngestionFrontier) ConsumerClosed() {
	sf.close()
}

// decodeResolvedSpans decodes an encoded datum of jobspb.ResolvedSpans into a
// jobspb.ResolvedSpans object.
func decodeResolvedSpans(
	alloc *tree.DatumAlloc, resolvedSpanDatums rowenc.EncDatum,
) (*jobspb.ResolvedSpans, error) {
	if err := resolvedSpanDatums.EnsureDecoded(streamIngestionResultTypes[0], alloc); err != nil {
		return nil, err
	}
	raw, ok := resolvedSpanDatums.Datum.(*tree.DBytes)
	if !ok {
		return nil, errors.AssertionFailedf(`unexpected datum type %T: %s`,
			resolvedSpanDatums.Datum, resolvedSpanDatums.Datum)
	}
	var resolvedSpans jobspb.ResolvedSpans
	if err := protoutil.Unmarshal([]byte(*raw), &resolvedSpans); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved timestamp: %x`, raw)
	}
	return &resolvedSpans, nil
}

// noteResolvedTimestamps processes a batch of resolved timestamp events.
func (sf *streamIngestionFrontier) noteResolvedTimestamps(
	resolvedSpanDatums rowenc.EncDatum,
) error {
	resolvedSpans, err := decodeResolvedSpans(&sf.alloc, resolvedSpanDatums)
	if err != nil {
		return err
	}
	for _, resolved := range resolvedSpans.ResolvedSpans {
		// Inserting a timestamp less than the one the ingestion flow started at could
		// potentially regress the job progress. This is not expected and thus we
		// assert to catch such unexpected behavior.
		if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(sf.replicatedTimeAtStart) {
			return errors.AssertionFailedf(
				`got a resolved timestamp %s that is less than the frontier processor start time %s`,
				redact.Safe(resolved.Timestamp), redact.Safe(sf.replicatedTimeAtStart))
		}

		if _, err := sf.frontier.Forward(resolved.Span, resolved.Timestamp); err != nil {
			return err
		}
	}
	return nil
}

// maybeUpdateProgress updates the job progress with the
// latest replicated time and partition-specific information to track
// the status of each partition.
func (sf *streamIngestionFrontier) maybeUpdateProgress() error {
	ctx := sf.Ctx()
	updateFreq := streamingccl.JobCheckpointFrequency.Get(&sf.flowCtx.Cfg.Settings.SV)
	if updateFreq == 0 || timeutil.Since(sf.lastPartitionUpdate) < updateFreq {
		return nil
	}
	f := sf.frontier
	registry := sf.flowCtx.Cfg.JobRegistry
	jobID := jobspb.JobID(sf.spec.JobID)

	frontierResolvedSpans := make([]jobspb.ResolvedSpan, 0)
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		frontierResolvedSpans = append(frontierResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})

	replicatedTime := f.Frontier()
	partitionProgress := sf.partitionProgress

	sf.lastPartitionUpdate = timeutil.Now()
	log.VInfof(ctx, 2, "persisting replicated time of %s", replicatedTime)
	if err := registry.UpdateJobWithTxn(ctx, jobID, nil, false, func(
		txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}

		progress := md.Progress
		streamProgress := progress.Details.(*jobspb.Progress_StreamIngest).StreamIngest
		streamProgress.PartitionProgress = partitionProgress
		streamProgress.Checkpoint.ResolvedSpans = frontierResolvedSpans

		// Keep the recorded replicatedTime empty until some advancement has been made
		if sf.replicatedTimeAtStart.Less(replicatedTime) {
			streamProgress.ReplicatedTime = replicatedTime
			// The HighWater is for informational purposes
			// only.
			progress.Progress = &jobspb.Progress_HighWater{
				HighWater: &replicatedTime,
			}
		}

		ju.UpdateProgress(progress)

		// Reset RunStats.NumRuns to 1 since the stream ingestion has returned to
		// a steady state. By resetting NumRuns,we avoid future job system level
		// retries from having a large backoff because of past failures.
		if md.RunStats != nil && md.RunStats.NumRuns > 1 {
			ju.UpdateRunStats(1, md.RunStats.LastRun)
		}

		// Update the protected timestamp record protecting the destination tenant's
		// keyspan if the replicatedTime has moved forward since the last time we
		// recorded progress. This makes older revisions of replicated values with a
		// timestamp less than replicatedTime - ReplicationTTLSeconds eligible for
		// garbage collection.
		replicationDetails := md.Payload.GetStreamIngestion()
		if replicationDetails.ProtectedTimestampRecordID == nil {
			return errors.AssertionFailedf("expected replication job to have a protected timestamp " +
				"record over the destination tenant's keyspan")
		}
		ptp := sf.flowCtx.Cfg.ProtectedTimestampProvider.WithTxn(txn)
		record, err := ptp.GetRecord(ctx, *replicationDetails.ProtectedTimestampRecordID)
		if err != nil {
			return err
		}
		newProtectAbove := replicatedTime.Add(
			-int64(replicationDetails.ReplicationTTLSeconds)*time.Second.Nanoseconds(), 0)

		// If we have a CutoverTime set, keep the protected
		// timestamp at or below the cutover time.
		if !streamProgress.CutoverTime.IsEmpty() && streamProgress.CutoverTime.Less(newProtectAbove) {
			newProtectAbove = streamProgress.CutoverTime
		}

		if record.Timestamp.Less(newProtectAbove) {
			return ptp.UpdateTimestamp(ctx, *replicationDetails.ProtectedTimestampRecordID, newProtectAbove)
		}

		return nil
	}); err != nil {
		return err
	}
	sf.metrics.JobProgressUpdates.Inc(1)
	sf.persistedReplicatedTime = f.Frontier()
	sf.metrics.ReplicatedTimeSeconds.Update(sf.persistedReplicatedTime.GoTime().Unix())
	return nil
}

// maybePersistFrontierEntries periodically persists the current state of the
// frontier to the `system.job_info` table. This information is used to hydrate
// the execution details that can be requested for the C2C ingestion job. Note,
// we always persist the entries to the same info key and so we never have more
// than one row describing the state of the frontier at a given point in time.
func (sf *streamIngestionFrontier) maybePersistFrontierEntries() error {
	dumpFreq := streamingccl.DumpFrontierEntries.Get(&sf.FlowCtx.Cfg.Settings.SV)
	if dumpFreq == 0 || timeutil.Since(sf.lastFrontierDump) < dumpFreq {
		return nil
	}
	ctx := sf.Ctx()
	f := sf.frontier
	jobID := jobspb.JobID(sf.spec.JobID)

	frontierEntries := &execinfrapb.FrontierEntries{ResolvedSpans: make([]jobspb.ResolvedSpan, 0)}
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		frontierEntries.ResolvedSpans = append(frontierEntries.ResolvedSpans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})

	frontierBytes, err := protoutil.Marshal(frontierEntries)
	if err != nil {
		return err
	}

	if err = sf.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return jobs.WriteChunkedFileToJobInfo(ctx, frontierEntriesFilename, frontierBytes, txn, jobID)
	}); err != nil {
		return err
	}

	sf.lastFrontierDump = timeutil.Now()
	return nil
}

func (sf *streamIngestionFrontier) maybeCheckForLaggingNodes() error {
	ctx := sf.Ctx()

	// We halve the frequency relative to the ReplanFrequency setting (i.e.
	// check twice as often), because the node lag checker will only restart the
	// distSQL plan if a node is lagging for 2 checks in a row.
	checkFreq := streamingccl.ReplanFrequency.Get(&sf.FlowCtx.Cfg.Settings.SV) / 2
	maxLag := streamingccl.InterNodeLag.Get(&sf.FlowCtx.Cfg.Settings.SV)
	if checkFreq == 0 || maxLag == 0 || timeutil.Since(sf.lastNodeLagCheck) < checkFreq {
		log.VEventf(ctx, 2, "skipping lag replanning check: maxLag %d; checkFreq %.2f; last node check %s; time since last check %.2f",
			maxLag, checkFreq.Minutes(), sf.lastNodeLagCheck, timeutil.Since(sf.lastNodeLagCheck).Minutes())
		return nil
	}
	// Don't check for lagging nodes if the hwm has yet to advance.
	if sf.replicatedTimeAtStart.Equal(sf.persistedReplicatedTime) {
		log.VEventf(ctx, 2, "skipping lag replanning check: hwm has yet to advance past %s", sf.replicatedTimeAtStart)
		return nil
	}
	defer func() {
		sf.lastNodeLagCheck = timeutil.Now()
	}()
	executionDetails := constructSpanFrontierExecutionDetailsWithFrontier(sf.spec.PartitionSpecs, sf.frontier)
	log.VEvent(ctx, 2, "checking for lagging nodes")
	err := checkLaggingNodes(
		ctx,
		executionDetails,
		maxLag,
	)
	return sf.handleLaggingNodeError(ctx, err)
}

func (sf *streamIngestionFrontier) handleLaggingNodeError(ctx context.Context, err error) error {
	switch {
	case err == nil:
		sf.replicatedTimeAtLastPositiveLagNodeCheck = hlc.Timestamp{}
		log.VEvent(ctx, 2, "no lagging nodes after check")
		return nil
	case !errors.Is(err, ErrNodeLagging):
		return err
	case sf.replicatedTimeAtLastPositiveLagNodeCheck.Less(sf.persistedReplicatedTime):
		log.Infof(ctx, "detected a lagging node: %s. Don't forward error because replicated time at last check %s is less than current replicated time %s", err, sf.replicatedTimeAtLastPositiveLagNodeCheck, sf.persistedReplicatedTime)
		sf.replicatedTimeAtLastPositiveLagNodeCheck = sf.persistedReplicatedTime
		return nil
	case sf.replicatedTimeAtLastPositiveLagNodeCheck.Equal(sf.persistedReplicatedTime):
		return errors.Wrapf(err, "hwm has not advanced from %s", sf.persistedReplicatedTime)
	default:
		return errors.Wrapf(err, "unable to handle replanning error with replicated time %s and last node lag check replicated time %s", sf.persistedReplicatedTime, sf.replicatedTimeAtLastPositiveLagNodeCheck)
	}
}
