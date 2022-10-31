// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CompleteStreamIngestion implements streamingmanagers.StreamIngestManageNew interface.
func (p *planner) CompleteStreamIngestion(
	ctx context.Context, ingestionJobID jobspb.JobID, cutoverTimestamp hlc.Timestamp,
) error {
	jobRegistry := p.ExecutorConfig().(*ExecutorConfig).JobRegistry
	return jobRegistry.UpdateJobWithTxn(ctx, ingestionJobID, p.Txn(), false, /* useReadLock */
		func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			// TODO(adityamaru): This should change in the future, a user should be
			// allowed to correct their cutover time if the process of reverting the job
			// has not started.
			if jobCutoverTime := md.Progress.GetStreamIngest().CutoverTime; !jobCutoverTime.IsEmpty() {
				return errors.Newf("cutover timestamp already set to %s, "+
					"job %d is in the process of cutting over", jobCutoverTime.String(), ingestionJobID)
			}

			// Update the sentinel being polled by the stream ingestion job to
			// check if a complete has been signaled.
			md.Progress.GetStreamIngest().CutoverTime = cutoverTimestamp
			ju.UpdateProgress(md.Progress)
			return nil
		})
}

// GetStreamIngestionStats implements streamingmanagers.StreamIngestManageNew interface.
func (p *planner) GetStreamIngestionStats(
	ctx context.Context, ingestionJobID jobspb.JobID,
) (*streampb.StreamIngestionStats, error) {
	registry := p.ExecutorConfig().(*ExecutorConfig).JobRegistry
	j, err := registry.LoadJobWithTxn(ctx, ingestionJobID, p.Txn())
	if err != nil {
		return nil, err
	}
	details, ok := j.Details().(jobspb.StreamIngestionDetails)
	if !ok {
		return nil, errors.Errorf("job with id %d is not a stream ingestion job", ingestionJobID)
	}

	progress := j.Progress()
	stats := &streampb.StreamIngestionStats{
		IngestionDetails:  &details,
		IngestionProgress: progress.GetStreamIngest(),
	}
	if highwater := progress.GetHighWater(); highwater != nil && !highwater.IsEmpty() {
		lagInfo := &streampb.StreamIngestionStats_ReplicationLagInfo{
			MinIngestedTimestamp: *highwater,
		}
		lagInfo.EarliestCheckpointedTimestamp = hlc.MaxTimestamp
		lagInfo.LatestCheckpointedTimestamp = hlc.MinTimestamp
		// TODO(casper): track spans that the slowest partition is associated
		for _, resolvedSpan := range progress.GetStreamIngest().Checkpoint.ResolvedSpans {
			if resolvedSpan.Timestamp.Less(lagInfo.EarliestCheckpointedTimestamp) {
				lagInfo.EarliestCheckpointedTimestamp = resolvedSpan.Timestamp
			}

			if lagInfo.LatestCheckpointedTimestamp.Less(resolvedSpan.Timestamp) {
				lagInfo.LatestCheckpointedTimestamp = resolvedSpan.Timestamp
			}
		}
		lagInfo.SlowestFastestIngestionLag = lagInfo.LatestCheckpointedTimestamp.GoTime().
			Sub(lagInfo.EarliestCheckpointedTimestamp.GoTime())
		lagInfo.ReplicationLag = timeutil.Since(highwater.GoTime())
		stats.ReplicationLagInfo = lagInfo
	}

	client, err := streamclient.GetFirstActiveClient(ctx, progress.GetStreamIngest().StreamAddresses)
	if err != nil {
		return nil, err
	}
	streamStatus, err := client.Heartbeat(ctx, streaming.StreamID(details.StreamID), hlc.MaxTimestamp)
	if err != nil {
		stats.ProducerError = err.Error()
	} else {
		stats.ProducerStatus = &streamStatus
	}
	return stats, client.Close(ctx)
}

// StartReplicationStream implements streamingmanagers.ReplicationStreamManagerNew interface.
func (p *planner) StartReplicationStream(
	ctx context.Context, tenantID uint64,
) (streaming.StreamID, error) {
	execConfig := p.ExecutorConfig().(*ExecutorConfig)
	hasAdminRole, err := p.HasAdminRole(ctx)

	if err != nil {
		return streaming.InvalidStreamID, err
	}

	if !hasAdminRole {
		return streaming.InvalidStreamID, errors.New("admin role required to start stream replication jobs")
	}

	registry := execConfig.JobRegistry
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&execConfig.Settings.SV)
	ptsID := uuid.MakeV4()
	jr := makeProducerJobRecord(registry, tenantID, timeout, p.SessionData().User(), ptsID)
	if _, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, p.Txn()); err != nil {
		return streaming.InvalidStreamID, err
	}

	ptp := execConfig.ProtectedTimestampProvider
	statementTime := hlc.Timestamp{
		WallTime: p.EvalContext().GetStmtTimestamp().UnixNano(),
	}

	deprecatedSpansToProtect := roachpb.Spans{*makeTenantSpan(tenantID)}
	targetToProtect := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MakeTenantID(tenantID)})

	pts := jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), statementTime,
		deprecatedSpansToProtect, jobsprotectedts.Jobs, targetToProtect)

	if err := ptp.Protect(ctx, p.Txn(), pts); err != nil {
		return streaming.InvalidStreamID, err
	}
	return streaming.StreamID(jr.JobID), nil
}

// HeartbeatReplicationStream implements streamingmanagers.ReplicationStreamManagerNew interface.
func (p *planner) HeartbeatReplicationStream(
	ctx context.Context, streamID streaming.StreamID, frontier hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	execConfig := p.ExecutorConfig().(*ExecutorConfig)
	timeout := streamingccl.StreamReplicationJobLivenessTimeout.Get(&execConfig.Settings.SV)
	expirationTime := timeutil.Now().Add(timeout)
	// MaxTimestamp indicates not a real heartbeat, skip updating the producer
	// job progress.
	if frontier == hlc.MaxTimestamp {
		var status streampb.StreamReplicationStatus
		pj, err := execConfig.JobRegistry.LoadJob(ctx, jobspb.JobID(streamID))
		if jobs.HasJobNotFoundError(err) || testutils.IsError(err, "not found in system.jobs table") {
			status.StreamStatus = streampb.StreamReplicationStatus_STREAM_INACTIVE
			return status, nil
		}
		if err != nil {
			return streampb.StreamReplicationStatus{}, err
		}
		status.StreamStatus = convertProducerJobStatusToStreamStatus(pj.Status())
		payload := pj.Payload()
		ptsRecord, err := execConfig.ProtectedTimestampProvider.GetRecord(ctx, p.Txn(),
			payload.GetStreamReplication().ProtectedTimestampRecordID)
		// Nil protected timestamp indicates it was not created or has been released.
		if errors.Is(err, protectedts.ErrNotExists) {
			return status, nil
		}
		if err != nil {
			return streampb.StreamReplicationStatus{}, err
		}
		status.ProtectedTimestamp = &ptsRecord.Timestamp
		return status, nil
	}

	return updateReplicationStreamProgress(ctx,
		expirationTime, execConfig.ProtectedTimestampProvider, execConfig.JobRegistry,
		streamID, frontier, p.Txn())
}

// StreamPartition implements streamingmanagers.ReplicationStreamManagerNew interface.
func (p *planner) StreamPartition(
	ctx context.Context, streamID streaming.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	execConfig := p.ExecutorConfig().(*ExecutorConfig)
	if !p.SessionData().AvoidBuffering {
		return nil, errors.New("partition streaming requires 'SET avoid_buffering = true' option")
	}

	var spec streampb.StreamPartitionSpec
	if err := protoutil.Unmarshal(opaqueSpec, &spec); err != nil {
		return nil, errors.Wrapf(err, "invalid partition spec for stream %d", streamID)
	}

	if len(spec.Spans) == 0 {
		return nil, errors.AssertionFailedf("expected at least one span, got none")
	}

	setConfigDefaults(&spec.Config)

	var subscribedSpans roachpb.SpanGroup
	for _, sp := range spec.Spans {
		subscribedSpans.Add(sp)
	}
	return &eventStream{
		streamID:        streamID,
		spec:            spec,
		subscribedSpans: subscribedSpans,
		execCfg:         execConfig,
		mon:             p.Mon(),
	}, nil
}

// GetReplicationStreamSpec implements streamingmanagers.ReplicationStreamManagerNew interface.
func (p *planner) GetReplicationStreamSpec(
	ctx context.Context, streamID streaming.StreamID,
) (*streampb.ReplicationStreamSpec, error) {
	// Returns error if the replication stream is not active
	j, err := p.ExecCfg().JobRegistry.LoadJob(ctx, jobspb.JobID(streamID))
	if err != nil {
		return nil, errors.Wrapf(err, "replication stream %d has error", streamID)
	}
	if j.Status() != jobs.StatusRunning {
		return nil, errors.Errorf("replication stream %d is not running", streamID)
	}

	// Partition the spans with SQLPlanner
	var noTxn *kv.Txn
	dsp := p.DistSQLPlanner()
	planCtx := dsp.NewPlanningCtx(ctx, p.ExtendedEvalContext(),
		nil /* planner */, noTxn, DistributionTypeSystemTenantOnly)

	details, ok := j.Details().(jobspb.StreamReplicationDetails)
	if !ok {
		return nil, errors.Errorf("job with id %d is not a replication stream job", streamID)
	}
	replicatedSpans := details.Spans
	spans := make([]roachpb.Span, 0, len(replicatedSpans))
	for _, span := range replicatedSpans {
		spans = append(spans, *span)
	}
	spanPartitions, err := dsp.PartitionSpans(ctx, planCtx, spans)
	if err != nil {
		return nil, err
	}

	res := &streampb.ReplicationStreamSpec{
		Partitions: make([]streampb.ReplicationStreamSpec_Partition, 0, len(spanPartitions)),
	}
	for _, sp := range spanPartitions {
		nodeInfo, err := dsp.GetSQLInstanceInfo(sp.SQLInstanceID)
		if err != nil {
			return nil, err
		}
		res.Partitions = append(res.Partitions, streampb.ReplicationStreamSpec_Partition{
			NodeID:     roachpb.NodeID(sp.SQLInstanceID),
			SQLAddress: nodeInfo.SQLAddress,
			Locality:   nodeInfo.Locality,
			PartitionSpec: &streampb.StreamPartitionSpec{
				Spans: sp.Spans,
				Config: streampb.StreamPartitionSpec_ExecutionConfig{
					MinCheckpointFrequency: streamingccl.StreamReplicationMinCheckpointFrequency.Get(&p.ExecutorConfig().(*ExecutorConfig).Settings.SV),
				},
			},
		})
	}
	return res, nil
}

// CompleteReplicationStream implements streamingmanagers.ReplicationStreamManagerNew interface.
func (p *planner) CompleteReplicationStream(
	ctx context.Context, streamID streaming.StreamID, successfulIngestion bool,
) error {
	registry := p.ExecutorConfig().(*ExecutorConfig).JobRegistry
	const useReadLock = false
	return registry.UpdateJobWithTxn(ctx, jobspb.JobID(streamID), p.Txn(), useReadLock,
		func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			// Updates the stream ingestion status, make the job resumer exit running
			// when picking up the new status.
			if (md.Status == jobs.StatusRunning || md.Status == jobs.StatusPending) &&
				md.Progress.GetStreamReplication().StreamIngestionStatus ==
					jobspb.StreamReplicationProgress_NOT_FINISHED {
				if successfulIngestion {
					md.Progress.GetStreamReplication().StreamIngestionStatus =
						jobspb.StreamReplicationProgress_FINISHED_SUCCESSFULLY
					md.Progress.RunningStatus = "succeeding this producer job as the corresponding " +
						"stream ingestion finished successfully"
				} else {
					md.Progress.GetStreamReplication().StreamIngestionStatus =
						jobspb.StreamReplicationProgress_FINISHED_UNSUCCESSFULLY
					md.Progress.RunningStatus = "canceling this producer job as the corresponding " +
						"stream ingestion did not finish successfully"
				}
				ju.UpdateProgress(md.Progress)
			}
			return nil
		})
}

func makeProducerJobRecord(
	registry *jobs.Registry,
	tenantID uint64,
	timeout time.Duration,
	user username.SQLUsername,
	ptsID uuid.UUID,
) jobs.Record {
	return jobs.Record{
		JobID:       registry.MakeJobID(),
		Description: fmt.Sprintf("stream replication for tenant %d", tenantID),
		Username:    user,
		Details: jobspb.StreamReplicationDetails{
			ProtectedTimestampRecordID: ptsID,
			Spans:                      []*roachpb.Span{makeTenantSpan(tenantID)},
		},
		Progress: jobspb.StreamReplicationProgress{
			Expiration: timeutil.Now().Add(timeout),
		},
	}
}

func makeTenantSpan(tenantID uint64) *roachpb.Span {
	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID))
	return &roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// Convert the producer job's status into corresponding replication
// stream status.
func convertProducerJobStatusToStreamStatus(
	jobStatus jobs.Status,
) streampb.StreamReplicationStatus_StreamStatus {
	switch {
	case jobStatus == jobs.StatusRunning:
		return streampb.StreamReplicationStatus_STREAM_ACTIVE
	case jobStatus == jobs.StatusPaused:
		return streampb.StreamReplicationStatus_STREAM_PAUSED
	case jobStatus.Terminal():
		return streampb.StreamReplicationStatus_STREAM_INACTIVE
	default:
		// This means the producer job is in transient state, the call site
		// has to retry until other states are reached.
		return streampb.StreamReplicationStatus_UNKNOWN_STREAM_STATUS_RETRY
	}
}

// updateReplicationStreamProgress updates the job progress for an active replication
// stream specified by 'streamID'.
func updateReplicationStreamProgress(
	ctx context.Context,
	expiration time.Time,
	ptsProvider protectedts.Provider,
	registry *jobs.Registry,
	streamID streaming.StreamID,
	consumedTime hlc.Timestamp,
	txn *kv.Txn,
) (status streampb.StreamReplicationStatus, err error) {
	const useReadLock = false
	err = registry.UpdateJobWithTxn(ctx, jobspb.JobID(streamID), txn, useReadLock,
		func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			status.StreamStatus = convertProducerJobStatusToStreamStatus(md.Status)
			// Skip checking PTS record in cases that it might already be released
			if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE &&
				status.StreamStatus != streampb.StreamReplicationStatus_STREAM_PAUSED {
				return nil
			}

			ptsID := md.Payload.GetStreamReplication().ProtectedTimestampRecordID
			ptsRecord, err := ptsProvider.GetRecord(ctx, txn, ptsID)
			if err != nil {
				return err
			}
			status.ProtectedTimestamp = &ptsRecord.Timestamp
			if status.StreamStatus != streampb.StreamReplicationStatus_STREAM_ACTIVE {
				return nil
			}

			// TODO(casper): Error out when the protected timestamp moves backward as the ingestion
			// processors may consume kv changes that are not protected. We are fine for now
			// for the sake of long GC window.
			// Now this can happen because the frontier processor moves forward the protected timestamp
			// in the source cluster through heartbeats before it reports the new frontier to the
			// ingestion job resumer which later updates the job high watermark. When we retry another
			// ingestion using the previous ingestion high watermark, it can fall behind the
			// source cluster protected timestamp.
			if shouldUpdatePTS := ptsRecord.Timestamp.Less(consumedTime); shouldUpdatePTS {
				if err = ptsProvider.UpdateTimestamp(ctx, txn, ptsID, consumedTime); err != nil {
					return err
				}
				status.ProtectedTimestamp = &consumedTime
			}
			// Allow expiration time to go backwards as user may set a smaller timeout.
			md.Progress.GetStreamReplication().Expiration = expiration
			ju.UpdateProgress(md.Progress)
			return nil
		})

	if jobs.HasJobNotFoundError(err) || testutils.IsError(err, "not found in system.jobs table") {
		status.StreamStatus = streampb.StreamReplicationStatus_STREAM_INACTIVE
		err = nil
	}

	return status, err
}

func setConfigDefaults(cfg *streampb.StreamPartitionSpec_ExecutionConfig) {
	const defaultInitialScanParallelism = 16
	const defaultMinCheckpointFrequency = 10 * time.Second
	const defaultBatchSize = 1 << 20

	if cfg.InitialScanParallelism <= 0 {
		cfg.InitialScanParallelism = defaultInitialScanParallelism
	}

	if cfg.MinCheckpointFrequency <= 0 {
		cfg.MinCheckpointFrequency = defaultMinCheckpointFrequency
	}

	if cfg.BatchByteSize <= 0 {
		cfg.BatchByteSize = defaultBatchSize
	}
}

type eventStream struct {
	streamID        streaming.StreamID
	execCfg         *ExecutorConfig
	spec            streampb.StreamPartitionSpec
	subscribedSpans roachpb.SpanGroup
	mon             *mon.BytesMonitor
	acc             mon.BoundAccount

	data tree.Datums // Data to send to the consumer

	// Fields below initialized when Start called.
	rf          *rangefeed.RangeFeed          // Currently running rangefeed.
	streamGroup ctxgroup.Group                // Context group controlling stream execution.
	doneChan    chan struct{}                 // Channel signaled to close the stream loop.
	eventsCh    chan kvcoord.RangeFeedMessage // Channel receiving rangefeed events.
	errCh       chan error                    // Signaled when error occurs in rangefeed.
	streamCh    chan tree.Datums              // Channel signaled to forward datums to consumer.
	sp          *tracing.Span                 // Span representing the lifetime of the eventStream.
}

var _ eval.ValueGenerator = (*eventStream)(nil)

var eventStreamReturnType = types.MakeLabeledTuple(
	[]*types.T{types.Bytes},
	[]string{"stream_event"},
)

// ResolvedType implements tree.ValueGenerator interface.
func (s *eventStream) ResolvedType() *types.T {
	return eventStreamReturnType
}

// Start implements tree.ValueGenerator interface.
func (s *eventStream) Start(ctx context.Context, txn *kv.Txn) error {
	// ValueGenerator API indicates that Start maybe called again if Next returned
	// false.  However, this generator never terminates without an error,
	// so this method should be called once.  Be defensive and return an error
	// if this method is called again.
	if s.errCh != nil {
		return errors.AssertionFailedf("expected to be started once")
	}

	s.acc = s.mon.MakeBoundAccount()

	// errCh consumed by ValueGenerator and is signaled when go routines encounter error.
	s.errCh = make(chan error)

	// Events channel gets RangeFeedEvents and is consumed by ValueGenerator.
	s.eventsCh = make(chan kvcoord.RangeFeedMessage)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	s.doneChan = make(chan struct{})

	// Common rangefeed options.
	opts := []rangefeed.Option{
		rangefeed.WithOnCheckpoint(s.onCheckpoint),

		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			s.maybeSetError(err)
		}),

		rangefeed.WithMemoryMonitor(s.mon),

		rangefeed.WithOnSSTable(s.onSSTable),

		rangefeed.WithOnDeleteRange(s.onDeleteRange),
	}

	frontier, err := span.MakeFrontier(s.spec.Spans...)
	if err != nil {
		return err
	}

	if s.spec.StartFrom.IsEmpty() {
		// Arrange to perform initial scan.
		s.spec.StartFrom = s.execCfg.Clock.Now()

		opts = append(opts,
			rangefeed.WithInitialScan(func(ctx context.Context) {}),
			rangefeed.WithScanRetryBehavior(rangefeed.ScanRetryRemaining),
			rangefeed.WithRowTimestampInInitialScan(true),
			rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
				// TODO(yevgeniy): Update metrics
				return false
			}),

			rangefeed.WithInitialScanParallelismFn(func() int {
				return int(s.spec.Config.InitialScanParallelism)
			}),

			rangefeed.WithOnScanCompleted(s.onSpanCompleted),
		)
	} else {
		// When resuming from cursor, advance frontier to the cursor position.
		for _, sp := range s.spec.Spans {
			if _, err := frontier.Forward(sp, s.spec.StartFrom); err != nil {
				return err
			}
		}
	}

	// Start rangefeed, which spins up a separate go routine to perform it's job.
	s.rf = s.execCfg.RangeFeedFactory.New(
		fmt.Sprintf("streamID=%d", s.streamID), s.spec.StartFrom, s.onValue, opts...)
	if err := s.rf.Start(ctx, s.spec.Spans); err != nil {
		return err
	}

	// Reserve batch kvsSize bytes from monitor.  We might have to do something more fancy
	// in the future, but for now, grabbing chunk of memory from the monitor would do the trick.
	if err := s.acc.Grow(ctx, s.spec.Config.BatchByteSize); err != nil {
		return errors.Wrapf(err, "failed to allocated %d bytes from monitor", s.spec.Config.BatchByteSize)
	}

	// NB: statements below should not return errors (otherwise, we may fail to release
	// bound account resources).
	s.startStreamProcessor(ctx, frontier)
	return nil
}

func (s *eventStream) maybeSetError(err error) {
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *eventStream) startStreamProcessor(ctx context.Context, frontier *span.Frontier) {
	type ctxGroupFn = func(ctx context.Context) error

	// withErrCapture wraps fn to capture and report error to the error channel.
	withErrCapture := func(fn ctxGroupFn) ctxGroupFn {
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err != nil {
				// Signal ValueGenerator that this stream is terminating due to an error
				// TODO(yevgeniy): Metrics
				log.Errorf(ctx, "event stream %d terminating with error %v", s.streamID, err)
				s.maybeSetError(err)
			}
			return err
		}
	}

	// Context group responsible for coordinating rangefeed event production with
	// ValueGenerator implementation that consumes rangefeed events and forwards them to the
	// destination cluster consumer.
	streamCtx, sp := tracing.ChildSpan(ctx, "event stream")
	s.sp = sp
	s.streamGroup = ctxgroup.WithContext(streamCtx)
	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		return s.streamLoop(ctx, frontier)
	}))

	// TODO(yevgeniy): Add go routine to monitor stream job liveness.
	// TODO(yevgeniy): Add validation that partition spans are a subset of stream spans.
}

// Next implements tree.ValueGenerator interface.
func (s *eventStream) Next(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-s.errCh:
		return false, err
	case s.data = <-s.streamCh:
		return true, nil
	}
}

// Values implements tree.ValueGenerator interface.
func (s *eventStream) Values() (tree.Datums, error) {
	return s.data, nil
}

// Close implements tree.ValueGenerator interface.
func (s *eventStream) Close(ctx context.Context) {
	s.rf.Close()
	s.acc.Close(ctx)

	close(s.doneChan)
	if err := s.streamGroup.Wait(); err != nil {
		// Note: error in close is normal; we expect to be terminated with context canceled.
		log.Errorf(ctx, "partition stream %d terminated with error %v", s.streamID, err)
	}

	s.sp.Finish()
}

func (s *eventStream) onValue(ctx context.Context, value *roachpb.RangeFeedValue) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{RangeFeedEvent: &roachpb.RangeFeedEvent{Val: value}}:
		log.VInfof(ctx, 1, "onValue: %s@%s", value.Key, value.Value.Timestamp)
	}
}

func (s *eventStream) onCheckpoint(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{RangeFeedEvent: &roachpb.RangeFeedEvent{Checkpoint: checkpoint}}:
		log.VInfof(ctx, 1, "onCheckpoint: %s@%s", checkpoint.Span, checkpoint.ResolvedTS)
	}
}

func (s *eventStream) onSpanCompleted(ctx context.Context, sp roachpb.Span) error {
	checkpoint := roachpb.RangeFeedCheckpoint{
		Span:       sp,
		ResolvedTS: s.spec.StartFrom,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.eventsCh <- kvcoord.RangeFeedMessage{
		RangeFeedEvent: &roachpb.RangeFeedEvent{Checkpoint: &checkpoint},
	}:
		log.VInfof(ctx, 1, "onSpanCompleted: %s@%s", checkpoint.Span, checkpoint.ResolvedTS)
		return nil
	}
}

func (s *eventStream) onSSTable(
	ctx context.Context, sst *roachpb.RangeFeedSSTable, registeredSpan roachpb.Span,
) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{
		RangeFeedEvent: &roachpb.RangeFeedEvent{SST: sst},
		RegisteredSpan: registeredSpan,
	}:
		log.VInfof(ctx, 1, "onSSTable: %s@%s with registered span %s",
			sst.Span, sst.WriteTS, registeredSpan)
	}
}

func (s *eventStream) onDeleteRange(ctx context.Context, delRange *roachpb.RangeFeedDeleteRange) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{RangeFeedEvent: &roachpb.RangeFeedEvent{DeleteRange: delRange}}:
		log.VInfof(ctx, 1, "onDeleteRange: %s@%s", delRange.Span, delRange.Timestamp)
	}
}

// makeCheckpoint generates checkpoint based on the frontier.
func makeCheckpoint(f *span.Frontier) (checkpoint streampb.StreamEvent_StreamCheckpoint) {
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		checkpoint.ResolvedSpans = append(checkpoint.ResolvedSpans, jobspb.ResolvedSpan{
			Span:      sp,
			Timestamp: ts,
		})
		return span.ContinueMatch
	})
	return
}

func (s *eventStream) flushEvent(ctx context.Context, event *streampb.StreamEvent) error {
	data, err := protoutil.Marshal(event)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.streamCh <- tree.Datums{tree.NewDBytes(tree.DBytes(data))}:
		return nil
	}
}

// streamLoop is the main processing loop responsible for reading rangefeed events,
// accumulating them in a batch, and sending those events to the ValueGenerator.
func (s *eventStream) streamLoop(ctx context.Context, frontier *span.Frontier) error {
	pacer := makeCheckpointPacer(s.spec.Config.MinCheckpointFrequency)

	var batch streampb.StreamEvent_Batch
	batchSize := 0
	addValue := func(v *roachpb.RangeFeedValue) {
		keyValue := roachpb.KeyValue{
			Key:   v.Key,
			Value: v.Value,
		}
		batch.KeyValues = append(batch.KeyValues, keyValue)
		batchSize += keyValue.Size()
	}

	addDelRange := func(delRange *roachpb.RangeFeedDeleteRange) error {
		// DelRange's span is already trimmed to enclosed within
		// the subscribed span, just emit it.
		batch.DelRanges = append(batch.DelRanges, *delRange)
		batchSize += delRange.Size()
		return nil
	}

	maybeFlushBatch := func(force bool) error {
		if (force && batchSize > 0) || batchSize > int(s.spec.Config.BatchByteSize) {
			defer func() {
				batchSize = 0
				batch.KeyValues = batch.KeyValues[:0]
				batch.Ssts = batch.Ssts[:0]
				batch.DelRanges = batch.DelRanges[:0]
			}()
			return s.flushEvent(ctx, &streampb.StreamEvent{Batch: &batch})
		}
		return nil
	}

	const forceFlush = true
	const flushIfNeeded = false

	// Note: we rely on the closed timestamp system to publish events periodically.
	// Thus, we don't need to worry about flushing batched data on a timer -- we simply
	// piggy-back on the fact that eventually, frontier must advance, and we must emit
	// previously batched KVs prior to emitting checkpoint record.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.doneChan:
			return nil
		case ev := <-s.eventsCh:
			switch {
			case ev.Val != nil:
				addValue(ev.Val)
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			case ev.Checkpoint != nil:
				advanced, err := frontier.Forward(ev.Checkpoint.Span, ev.Checkpoint.ResolvedTS)
				if err != nil {
					return err
				}

				if pacer.shouldCheckpoint(frontier.Frontier(), advanced) {
					if err := maybeFlushBatch(forceFlush); err != nil {
						return err
					}
					checkpoint := makeCheckpoint(frontier)
					if err := s.flushEvent(ctx, &streampb.StreamEvent{Checkpoint: &checkpoint}); err != nil {
						return err
					}
				}
			case ev.SST != nil:
				size, err := s.addSST(ev.SST, ev.RegisteredSpan, &batch)
				if err != nil {
					return err
				}
				batchSize += size
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			case ev.DeleteRange != nil:
				if err := addDelRange(ev.DeleteRange); err != nil {
					return err
				}
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf("unexpected event")
			}
		}
	}
}

type checkpointPacer struct {
	pace    time.Duration
	next    time.Time
	skipped bool
}

func makeCheckpointPacer(frequency time.Duration) checkpointPacer {
	return checkpointPacer{
		pace:    frequency,
		next:    timeutil.Now().Add(frequency),
		skipped: false,
	}
}

func (p *checkpointPacer) shouldCheckpoint(
	currentFrontier hlc.Timestamp, frontierAdvanced bool,
) bool {
	now := timeutil.Now()
	enoughTimeElapsed := p.next.Before(now)

	// Handle previously skipped updates.
	// Normally, we want to emit checkpoint records when frontier advances.
	// However, checkpoints could be skipped if the frontier advanced too rapidly
	// (i.e. more rapid than MinCheckpointFrequency).  In those cases, we skip emitting
	// the checkpoint, but we will emit it at a later time.
	if p.skipped {
		if enoughTimeElapsed {
			p.skipped = false
			p.next = now.Add(p.pace)
			return true
		}
		return false
	}

	isInitialScanCheckpoint := currentFrontier.IsEmpty()

	// Handle updates when frontier advances.
	if frontierAdvanced || isInitialScanCheckpoint {
		if enoughTimeElapsed {
			p.next = now.Add(p.pace)
			return true
		}
		p.skipped = true
		return false
	}
	return false
}

// Add a RangeFeedSSTable into current batch and return number of bytes added.
func (s *eventStream) addSST(
	sst *roachpb.RangeFeedSSTable, registeredSpan roachpb.Span, batch *streampb.StreamEvent_Batch,
) (int, error) {
	// We send over the whole SSTable if the sst span is within
	// the registered span boundaries.
	if registeredSpan.Contains(sst.Span) {
		batch.Ssts = append(batch.Ssts, *sst)
		return sst.Size(), nil
	}
	// If the sst span exceeds boundaries of the watched spans,
	// we trim the sst data to avoid sending unnecessary data.
	// TODO(casper): add metrics to track number of SSTs, and number of ssts
	// that are not inside the boundaries (and possible count+size of kvs in such ssts).
	size := 0
	// Extract the received SST to only contain data within the boundaries of
	// matching registered span. Execute the specified operations on each MVCC
	// key value and each MVCCRangeKey value in the trimmed SSTable.
	if err := streamingccl.ScanSST(sst, registeredSpan,
		func(mvccKV storage.MVCCKeyValue) error {
			batch.KeyValues = append(batch.KeyValues, roachpb.KeyValue{
				Key: mvccKV.Key.Key,
				Value: roachpb.Value{
					RawBytes:  mvccKV.Value,
					Timestamp: mvccKV.Key.Timestamp,
				},
			})
			size += batch.KeyValues[len(batch.KeyValues)-1].Size()
			return nil
		}, func(rangeKeyVal storage.MVCCRangeKeyValue) error {
			batch.DelRanges = append(batch.DelRanges, roachpb.RangeFeedDeleteRange{
				Span: roachpb.Span{
					Key:    rangeKeyVal.RangeKey.StartKey,
					EndKey: rangeKeyVal.RangeKey.EndKey,
				},
				Timestamp: rangeKeyVal.RangeKey.Timestamp,
			})
			size += batch.DelRanges[len(batch.DelRanges)-1].Size()
			return nil
		}); err != nil {
		return 0, err
	}
	return size, nil
}
