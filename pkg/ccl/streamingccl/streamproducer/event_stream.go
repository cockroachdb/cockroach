// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamproducer

import (
	"context"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type eventStream struct {
	streamID        streampb.StreamID
	execCfg         *sql.ExecutorConfig
	spec            streampb.StreamPartitionSpec
	subscribedSpans roachpb.SpanGroup
	mon             *mon.BytesMonitor

	data tree.Datums // Data to send to the consumer

	// Fields below initialized when Start called.
	rf          *rangefeed.RangeFeed          // Currently running rangefeed.
	streamGroup ctxgroup.Group                // Context group controlling stream execution.
	doneChan    chan struct{}                 // Channel signaled to close the stream loop.
	eventsCh    chan kvcoord.RangeFeedMessage // Channel receiving rangefeed events.
	errCh       chan error                    // Signaled when error occurs in rangefeed.
	streamCh    chan tree.Datums              // Channel signaled to forward datums to consumer.
	sp          *tracing.Span                 // Span representing the lifetime of the eventStream.
	acc         mon.BoundAccount
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

	sourceTenantID, err := s.validateProducerJobAndSpec(ctx)
	if err != nil {
		return err
	}

	log.Infof(ctx, "starting physical replication event stream: tenant=%s initial_scan_timestamp=%s previous_replicated_time=%s",
		sourceTenantID, s.spec.InitialScanTimestamp, s.spec.PreviousReplicatedTimestamp)

	s.acc = s.mon.MakeBoundAccount()

	// errCh is buffered to ensure the sender can send an error to
	// the buffer, without waiting, when the channel receiver is not waiting on
	// the channel.
	s.errCh = make(chan error, 1)

	// Events channel gets RangeFeedEvents and is consumed by ValueGenerator.
	s.eventsCh = make(chan kvcoord.RangeFeedMessage)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	s.doneChan = make(chan struct{})

	useMux := streamingccl.StreamProducerMuxRangefeeds.Get(&s.execCfg.Settings.SV)

	// Common rangefeed options.
	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("job", fmt.Sprintf("id=%d", s.streamID)),
		rangefeed.WithOnCheckpoint(s.onCheckpoint),

		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			s.maybeSetError(err)
		}),

		rangefeed.WithMemoryMonitor(s.mon),

		rangefeed.WithOnSSTable(s.onSSTable),
		rangefeed.WithMuxRangefeed(useMux),
		rangefeed.WithOnDeleteRange(s.onDeleteRange),
	}

	frontier, err := span.MakeFrontier(s.spec.Spans...)
	if err != nil {
		return err
	}

	initialTimestamp := s.spec.InitialScanTimestamp
	if s.spec.PreviousReplicatedTimestamp.IsEmpty() {
		log.Infof(ctx, "starting event stream with initial scan at %s", initialTimestamp)
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

			rangefeed.WithOnScanCompleted(s.onInitialScanSpanCompleted),
		)
	} else {
		initialTimestamp = s.spec.PreviousReplicatedTimestamp
		// When resuming from cursor, advance frontier to the cursor position.
		log.Infof(ctx, "resuming event stream (no initial scan) from %s", initialTimestamp)
		for _, sp := range s.spec.Spans {
			if _, err := frontier.Forward(sp, s.spec.PreviousReplicatedTimestamp); err != nil {
				return err
			}
		}
	}

	// Start rangefeed, which spins up a separate go routine to perform it's job.
	s.rf = s.execCfg.RangeFeedFactory.New(
		fmt.Sprintf("streamID=%d", s.streamID), initialTimestamp, s.onValue, opts...)
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
	// Only send the error if the channel is empty, else it's ok to swallow the
	// error because the first error in the channel will shut down the event
	// stream.
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
			// Attach the streamID as a job ID so that the job-specific
			// CPU profile on the Job's advanced debug page includes
			// stacks from these streams.
			defer pprof.SetGoroutineLabels(ctx)
			ctx = logtags.AddTag(ctx, "job", s.streamID)
			ctx = pprof.WithLabels(ctx, pprof.Labels("job", fmt.Sprintf("id=%d", s.streamID)))
			pprof.SetGoroutineLabels(ctx)

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
	if s.rf != nil {
		s.rf.Close()
	}
	s.acc.Close(ctx)
	if s.doneChan != nil {
		close(s.doneChan)
	}
	if err := s.streamGroup.Wait(); err != nil {
		// Note: error in close is normal; we expect to be terminated with context canceled.
		log.Errorf(ctx, "partition stream %d terminated with error %v", s.streamID, err)
	}
	if s.sp != nil {
		s.sp.Finish()
	}
}

func (s *eventStream) onValue(ctx context.Context, value *kvpb.RangeFeedValue) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{RangeFeedEvent: &kvpb.RangeFeedEvent{Val: value}}:
		log.VInfof(ctx, 1, "onValue: %s@%s", value.Key, value.Value.Timestamp)
	}
}

func (s *eventStream) onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{RangeFeedEvent: &kvpb.RangeFeedEvent{Checkpoint: checkpoint}}:
		log.VInfof(ctx, 1, "onCheckpoint: %s@%s", checkpoint.Span, checkpoint.ResolvedTS)
	}
}

func (s *eventStream) onInitialScanSpanCompleted(ctx context.Context, sp roachpb.Span) error {
	checkpoint := kvpb.RangeFeedCheckpoint{
		Span:       sp,
		ResolvedTS: s.spec.InitialScanTimestamp,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.eventsCh <- kvcoord.RangeFeedMessage{
		RangeFeedEvent: &kvpb.RangeFeedEvent{Checkpoint: &checkpoint},
	}:
		log.VInfof(ctx, 1, "onSpanCompleted: %s@%s", checkpoint.Span, checkpoint.ResolvedTS)
		return nil
	}
}

func (s *eventStream) onSSTable(
	ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span,
) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{
		RangeFeedEvent: &kvpb.RangeFeedEvent{SST: sst},
		RegisteredSpan: registeredSpan,
	}:
		log.VInfof(ctx, 1, "onSSTable: %s@%s with registered span %s",
			sst.Span, sst.WriteTS, registeredSpan)
	}
}

func (s *eventStream) onDeleteRange(ctx context.Context, delRange *kvpb.RangeFeedDeleteRange) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- kvcoord.RangeFeedMessage{RangeFeedEvent: &kvpb.RangeFeedEvent{DeleteRange: delRange}}:
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
	case <-s.doneChan:
		return nil
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

// Add a RangeFeedSSTable into current batch.
func (s *eventStream) addSST(
	sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span, seb *streamEventBatcher,
) error {
	// We send over the whole SSTable if the sst span is within
	// the registered span boundaries.
	if registeredSpan.Contains(sst.Span) {
		seb.addSST(sst)
		return nil
	}
	// If the sst span exceeds boundaries of the watched spans,
	// we trim the sst data to avoid sending unnecessary data.
	// TODO(casper): add metrics to track number of SSTs, and number of ssts
	// that are not inside the boundaries (and possible count+size of kvs in such ssts).
	//
	// Extract the received SST to only contain data within the boundaries of
	// matching registered span. Execute the specified operations on each MVCC
	// key value and each MVCCRangeKey value in the trimmed SSTable.
	return replicationutils.ScanSST(sst, registeredSpan,
		func(mvccKV storage.MVCCKeyValue) error {
			seb.addKV(&roachpb.KeyValue{
				Key: mvccKV.Key.Key,
				Value: roachpb.Value{
					RawBytes:  mvccKV.Value,
					Timestamp: mvccKV.Key.Timestamp}})
			return nil
		}, func(rangeKeyVal storage.MVCCRangeKeyValue) error {
			seb.addDelRange(&kvpb.RangeFeedDeleteRange{
				Span: roachpb.Span{
					Key:    rangeKeyVal.RangeKey.StartKey,
					EndKey: rangeKeyVal.RangeKey.EndKey,
				},
				Timestamp: rangeKeyVal.RangeKey.Timestamp,
			})
			return nil
		})
}

// streamLoop is the main processing loop responsible for reading rangefeed events,
// accumulating them in a batch, and sending those events to the ValueGenerator.
func (s *eventStream) streamLoop(ctx context.Context, frontier *span.Frontier) error {
	pacer := makeCheckpointPacer(s.spec.Config.MinCheckpointFrequency)
	seb := makeStreamEventBatcher()

	maybeFlushBatch := func(force bool) error {
		if (force && seb.getSize() > 0) || seb.getSize() > int(s.spec.Config.BatchByteSize) {
			defer func() {
				seb.reset()
			}()
			return s.flushEvent(ctx, &streampb.StreamEvent{Batch: &seb.batch})
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
				seb.addKV(&roachpb.KeyValue{
					Key:   ev.Val.Key,
					Value: ev.Val.Value,
				})
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
				err := s.addSST(ev.SST, ev.RegisteredSpan, seb)
				if err != nil {
					return err
				}
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			case ev.DeleteRange != nil:
				seb.addDelRange(ev.DeleteRange)
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			default:
				return errors.AssertionFailedf("unexpected event")
			}
		}
	}
}

func (s *eventStream) validateProducerJobAndSpec(ctx context.Context) (roachpb.TenantID, error) {
	producerJobID := jobspb.JobID(s.streamID)
	job, err := s.execCfg.JobRegistry.LoadJob(ctx, producerJobID)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	payload := job.Payload()
	sp, ok := payload.GetDetails().(*jobspb.Payload_StreamReplication)
	if !ok {
		return roachpb.TenantID{}, notAReplicationJobError(producerJobID)
	}
	if sp.StreamReplication == nil {
		return roachpb.TenantID{}, errors.AssertionFailedf("unexpected nil StreamReplication in producer job %d payload", producerJobID)
	}
	if job.Status() != jobs.StatusRunning {
		return roachpb.TenantID{}, jobIsNotRunningError(producerJobID, job.Status(), "stream events")
	}

	// Validate that the requested spans are a subset of the
	// source tenant's keyspace.
	sourceTenantID := sp.StreamReplication.TenantID
	sourceTenantSpans := keys.MakeTenantSpan(sourceTenantID)
	for _, sp := range s.spec.Spans {
		if !sourceTenantSpans.Contains(sp) {
			err := pgerror.Newf(pgcode.InvalidParameterValue, "requested span %s is not contained within the keyspace of source tenant %d",
				sp,
				sourceTenantID)
			return roachpb.TenantID{}, err
		}
	}
	return sourceTenantID, nil
}

const defaultBatchSize = 1 << 20

func setConfigDefaults(cfg *streampb.StreamPartitionSpec_ExecutionConfig) {
	const defaultInitialScanParallelism = 16
	const defaultMinCheckpointFrequency = 10 * time.Second

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

func streamPartition(
	evalCtx *eval.Context, streamID streampb.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	var spec streampb.StreamPartitionSpec
	if err := protoutil.Unmarshal(opaqueSpec, &spec); err != nil {
		return nil, errors.Wrapf(err, "invalid partition spec for stream %d", streamID)
	}
	if !evalCtx.SessionData().AvoidBuffering {
		return nil, errors.New("partition streaming requires 'SET avoid_buffering = true' option")
	}
	if len(spec.Spans) == 0 {
		return nil, errors.AssertionFailedf("expected at least one span, got none")
	}
	setConfigDefaults(&spec.Config)

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	var subscribedSpans roachpb.SpanGroup
	for _, sp := range spec.Spans {
		subscribedSpans.Add(sp)
	}
	return &eventStream{
		streamID:        streamID,
		spec:            spec,
		subscribedSpans: subscribedSpans,
		execCfg:         execCfg,
		mon:             evalCtx.Planner.Mon(),
	}, nil
}
