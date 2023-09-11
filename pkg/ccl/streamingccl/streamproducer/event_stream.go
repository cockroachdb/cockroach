// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bulk"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/codahale/hdrhistogram"
)

type eventStream struct {
	streamID        streampb.StreamID
	execCfg         *sql.ExecutorConfig
	spec            streampb.StreamPartitionSpec
	subscribedSpans roachpb.SpanGroup
	mon             *mon.BytesMonitor
	acc             mon.BoundAccount

	data tree.Datums // Data to send to the consumer

	// Fields below initialized when Start called.
	rf           *rangefeed.RangeFeed                      // Currently running rangefeed.
	streamGroup  ctxgroup.Group                            // Context group controlling stream execution.
	doneChan     chan struct{}                             // Channel signaled to close the stream loop.
	eventsCh     chan kvcoord.RangeFeedMessage             // Channel receiving rangefeed events.
	errCh        chan error                                // Signaled when error occurs in rangefeed.
	streamCh     chan tree.Datums                          // Channel signaled to forward datums to consumer.
	tracingAggCh chan *execinfrapb.TracingAggregatorEvents // Channel signaled to forward tracing aggregator events.
	sp           *tracing.Span                             // Span representing the lifetime of the eventStream.

	mu struct {
		syncutil.Mutex

		rangefeedEventReceiveWaitHist *hdrhistogram.Histogram

		lastBatchFlushTime    time.Time
		betweenBatchFlushWait *hdrhistogram.Histogram

		lastCheckpointFlushTime    time.Time
		betweenCheckpointFlushWait *hdrhistogram.Histogram

		flushWaitHist       *hdrhistogram.Histogram
		sendEventWaitHist   *hdrhistogram.Histogram
		betweenNextWaitHist *hdrhistogram.Histogram
		eventStreamStats    *EventStreamPerformanceStats
	}

	lastNextFinishedAt time.Time

	// Aggregator that aggregates StructuredEvents emitted in the eventStreams'
	// trace recording.
	agg      *bulk.TracingAggregator
	aggTimer *timeutil.Timer
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

	// errCh is buffered to ensure the sender can send an error to
	// the buffer, without waiting, when the channel receiver is not waiting on
	// the channel.
	s.errCh = make(chan error, 1)

	// Events channel gets RangeFeedEvents and is consumed by ValueGenerator.
	s.eventsCh = make(chan kvcoord.RangeFeedMessage)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	s.doneChan = make(chan struct{})

	// Common rangefeed options.
	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("job", fmt.Sprintf("id=%d", s.streamID)),
		rangefeed.WithOnCheckpoint(s.onCheckpoint),

		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			s.maybeSetError(err)
		}),

		rangefeed.WithMemoryMonitor(s.mon),

		rangefeed.WithOnSSTable(s.onSSTable),
		rangefeed.WithMuxRangefeed(true),
		rangefeed.WithOnDeleteRange(s.onDeleteRange),
	}

	frontier, err := span.MakeFrontier(s.spec.Spans...)
	if err != nil {
		return err
	}

	initialTimestamp := s.spec.InitialScanTimestamp
	if s.spec.PreviousReplicatedTimestamp.IsEmpty() {
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
	log.Infof(ctx, "starting stream processors on %s", s.execCfg.NodeInfo.NodeID.SQLInstanceID().String())
	// Set up a tracing aggregator that aggregates StructuredEvents emitted in the
	// eventStreams' trace recording.
	s.aggTimer = timeutil.NewTimer()
	s.agg = bulk.TracingAggregatorForContext(ctx)
	if s.agg != nil {
		s.aggTimer.Reset(15 * time.Second)
	}

	// Context group responsible for coordinating rangefeed event production with
	// ValueGenerator implementation that consumes rangefeed events and forwards
	// them to the destination cluster consumer.
	sp := tracing.SpanFromContext(ctx)
	ctx, aggSp := sp.Tracer().StartSpanCtx(ctx, "eventStream.startStreamProcessor",
		tracing.WithRecording(tracingpb.RecordingStructured), tracing.WithEventListeners(s.agg))
	s.sp = aggSp

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

	s.mu.eventStreamStats = &EventStreamPerformanceStats{}
	s.streamGroup = ctxgroup.WithContext(ctx)
	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		return s.streamLoop(ctx, frontier)
	}))

	// None of the goroutines involved in aggregating or persisting stats should
	// cause the eventStream to error out.
	s.tracingAggCh = make(chan *execinfrapb.TracingAggregatorEvents)
	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		if err := s.constructTracingAggregatorStats(ctx); err != nil {
			log.Warningf(ctx, "error while constructing tracing aggregator stats: %v", err)
		}
		return nil
	}))
	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		if err := bulk.AggregateTracingStats(ctx, jobspb.JobID(s.streamID), s.execCfg.Settings, s.execCfg.InternalDB, s.tracingAggCh); err != nil {
			log.Warningf(ctx, "error while aggregating tracing stats: %v", err)
		}
		return nil
	}))

	// TODO(yevgeniy): Add go routine to monitor stream job liveness.
	// TODO(yevgeniy): Add validation that partition spans are a subset of stream spans.
}

// Next implements tree.ValueGenerator interface.
func (s *eventStream) Next(ctx context.Context) (bool, error) {
	defer func() {
		s.lastNextFinishedAt = timeutil.Now()
	}()

	timeSincePrevNext := timeutil.Since(s.lastNextFinishedAt)
	beforeEventSend := timeutil.Now()
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case err := <-s.errCh:
			return false, err
		case <-time.After(5 * time.Second):
			profileRequest := serverpb.ProfileRequest{
				NodeId:      "all",
				Type:        serverpb.ProfileRequest_GOROUTINE,
				Labels:      true,
				LabelFilter: fmt.Sprintf("%d", s.streamID),
			}
			resp, err := s.execCfg.SQLStatusServer.Profile(ctx, &profileRequest)
			if err != nil {
				log.Errorf(ctx, "failed to collect goroutines for job %d: %v", s.streamID, err.Error())
			}
			log.Infof(ctx, "goroutines for stream %d: %s", s.streamID, resp.Data)
		case s.data = <-s.streamCh:
			s.mu.Lock()
			log.Infof(ctx, "send wait: %s; aggregate: %s", timeutil.Since(beforeEventSend).String(),
				s.mu.eventStreamStats.EventSendWait.String())
			eventSendWait := timeutil.Since(beforeEventSend)
			if err := s.mu.sendEventWaitHist.RecordValue(eventSendWait.Nanoseconds()); err != nil {
				log.Warningf(ctx, "error recording send wait to histogram: %v", err)
			}
			s.mu.eventStreamStats.EventSendWait += eventSendWait
			s.mu.eventStreamStats.LastSendTime = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			if err := s.mu.betweenNextWaitHist.RecordValue(timeSincePrevNext.Nanoseconds()); err != nil {
				log.Warningf(ctx, "error recording between next to histogram: %v", err)
			}
			s.mu.Unlock()
			return true, nil
		}
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
	s.aggTimer.Stop()
	close(s.tracingAggCh)
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
		s.mu.Lock()
		s.mu.eventStreamStats.LastSpanCompleteTime = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		s.mu.Unlock()
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
	defer func() {
		s.mu.Lock()
		if event.Batch != nil {
			s.mu.lastBatchFlushTime = time.Now()
		} else {
			s.mu.lastCheckpointFlushTime = time.Now()
		}
		s.mu.Unlock()
	}()

	s.mu.Lock()
	if event.Batch != nil && !s.mu.lastBatchFlushTime.IsZero() {
		err := s.mu.betweenBatchFlushWait.RecordValue(timeutil.Since(s.mu.lastBatchFlushTime).Nanoseconds())
		if err != nil {
			log.Warningf(ctx, "error recording between batch flush wait to histogram: %v", err)
		}
	} else if event.Checkpoint != nil && !s.mu.lastCheckpointFlushTime.IsZero() {
		err := s.mu.betweenCheckpointFlushWait.RecordValue(timeutil.Since(s.mu.lastCheckpointFlushTime).Nanoseconds())
		if err != nil {
			log.Warningf(ctx, "error recording between checkpoint flush wait to histogram: %v", err)
		}
	}
	s.mu.Unlock()

	data, err := protoutil.Marshal(event)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if event.Batch != nil {
		s.mu.eventStreamStats.NumFlushedBatches++
	} else {
		s.mu.eventStreamStats.NumFlushedCheckpoints++
	}
	s.mu.Unlock()

	beforeFlush := timeutil.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.streamCh <- tree.Datums{tree.NewDBytes(tree.DBytes(data))}:
		s.mu.Lock()
		log.Infof(ctx, "flush wait: %s; aggregate: %s", timeutil.Since(beforeFlush).String(),
			s.mu.eventStreamStats.FlushWait.String())
		flushWait := timeutil.Since(beforeFlush)
		if err := s.mu.flushWaitHist.RecordValue(flushWait.Nanoseconds()); err != nil {
			log.Warningf(ctx, "error recording flush wait to histogram: %v", err)
		}
		s.mu.eventStreamStats.FlushWait += flushWait
		s.mu.eventStreamStats.LastFlushTime = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		s.mu.Unlock()
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
	ctx context.Context,
	sst *kvpb.RangeFeedSSTable,
	registeredSpan roachpb.Span,
	seb *streamEventBatcher,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "eventStream.addSST")
	_ = ctx // ctx is currently unused, but this new ctx should be used below in the future.
	defer sp.Finish()
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
	s.mu.Lock()
	s.mu.eventStreamStats.NumTrimmedSsts++
	s.mu.Unlock()
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

	const (
		sigFigs    = 1
		minLatency = time.Microsecond
		maxLatency = 100 * time.Second
	)
	s.mu.Lock()
	s.mu.rangefeedEventReceiveWaitHist = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	s.mu.flushWaitHist = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	s.mu.sendEventWaitHist = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	s.mu.betweenNextWaitHist = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	s.mu.betweenBatchFlushWait = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	s.mu.betweenCheckpointFlushWait = hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	s.mu.Unlock()

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
		beforeEventReceived := timeutil.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.doneChan:
			return nil
		case ev := <-s.eventsCh:
			s.mu.Lock()
			waitTime := timeutil.Since(beforeEventReceived)
			if err := s.mu.rangefeedEventReceiveWaitHist.RecordValue(waitTime.Nanoseconds()); err != nil {
				log.Warningf(ctx, "failed to record event wait time to histogram: %v", err)
			}
			s.mu.eventStreamStats.EventReceiveWait += waitTime
			s.mu.eventStreamStats.LastRecvTime = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			switch {
			case ev.Val != nil:
				s.mu.eventStreamStats.KvEvents++
				s.mu.Unlock()
				seb.addKV(&roachpb.KeyValue{
					Key:   ev.Val.Key,
					Value: ev.Val.Value,
				})
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			case ev.Checkpoint != nil:
				s.mu.eventStreamStats.CheckpointEvents++
				s.mu.Unlock()
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
				s.mu.eventStreamStats.SstEvents++
				s.mu.Unlock()
				err := s.addSST(ctx, ev.SST, ev.RegisteredSpan, seb)
				if err != nil {
					return err
				}
				if err := maybeFlushBatch(flushIfNeeded); err != nil {
					return err
				}
			case ev.DeleteRange != nil:
				s.mu.eventStreamStats.DeleteRangeEvents++
				s.mu.Unlock()
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

func (s *eventStream) constructTracingAggregatorStats(ctx context.Context) error {
	sp := tracing.SpanFromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.doneChan:
			return nil
		case <-s.aggTimer.C:
			s.aggTimer.Read = true
			s.aggTimer.Reset(15 * time.Second)
			if s.agg == nil {
				return nil
			}
			if sp != nil {
				s.mu.Lock()
				s.mu.eventStreamStats.Name = fmt.Sprintf("%d", sp.SpanID())
				s.mu.eventStreamStats.RangefeedEventWait = &HistogramData{
					Min:   s.mu.rangefeedEventReceiveWaitHist.Min(),
					P5:    s.mu.rangefeedEventReceiveWaitHist.ValueAtQuantile(5),
					P50:   s.mu.rangefeedEventReceiveWaitHist.ValueAtQuantile(50),
					P90:   s.mu.rangefeedEventReceiveWaitHist.ValueAtQuantile(90),
					P99:   s.mu.rangefeedEventReceiveWaitHist.ValueAtQuantile(99),
					P99_9: s.mu.rangefeedEventReceiveWaitHist.ValueAtQuantile(99.9),
					Max:   s.mu.rangefeedEventReceiveWaitHist.Max(),
					Mean:  float32(s.mu.rangefeedEventReceiveWaitHist.Mean()),
					Count: s.mu.rangefeedEventReceiveWaitHist.TotalCount(),
				}
				s.mu.eventStreamStats.FlushEventWait = &HistogramData{
					Min:   s.mu.flushWaitHist.Min(),
					P5:    s.mu.flushWaitHist.ValueAtQuantile(5),
					P50:   s.mu.flushWaitHist.ValueAtQuantile(50),
					P90:   s.mu.flushWaitHist.ValueAtQuantile(90),
					P99:   s.mu.flushWaitHist.ValueAtQuantile(99),
					P99_9: s.mu.flushWaitHist.ValueAtQuantile(99.9),
					Max:   s.mu.flushWaitHist.Max(),
					Mean:  float32(s.mu.flushWaitHist.Mean()),
					Count: s.mu.flushWaitHist.TotalCount(),
				}
				s.mu.eventStreamStats.SendEventWait = &HistogramData{
					Min:   s.mu.sendEventWaitHist.Min(),
					P5:    s.mu.sendEventWaitHist.ValueAtQuantile(5),
					P50:   s.mu.sendEventWaitHist.ValueAtQuantile(50),
					P90:   s.mu.sendEventWaitHist.ValueAtQuantile(90),
					P99:   s.mu.sendEventWaitHist.ValueAtQuantile(99),
					P99_9: s.mu.sendEventWaitHist.ValueAtQuantile(99.9),
					Max:   s.mu.sendEventWaitHist.Max(),
					Mean:  float32(s.mu.sendEventWaitHist.Mean()),
					Count: s.mu.sendEventWaitHist.TotalCount(),
				}
				s.mu.eventStreamStats.SinceNextWait = &HistogramData{
					Min:   s.mu.betweenNextWaitHist.Min(),
					P5:    s.mu.betweenNextWaitHist.ValueAtQuantile(5),
					P50:   s.mu.betweenNextWaitHist.ValueAtQuantile(50),
					P90:   s.mu.betweenNextWaitHist.ValueAtQuantile(90),
					P99:   s.mu.betweenNextWaitHist.ValueAtQuantile(99),
					P99_9: s.mu.betweenNextWaitHist.ValueAtQuantile(99.9),
					Max:   s.mu.betweenNextWaitHist.Max(),
					Mean:  float32(s.mu.betweenNextWaitHist.Mean()),
					Count: s.mu.betweenNextWaitHist.TotalCount(),
				}
				s.mu.eventStreamStats.SinceLastBatchFlush = &HistogramData{
					Min:   s.mu.betweenBatchFlushWait.Min(),
					P5:    s.mu.betweenBatchFlushWait.ValueAtQuantile(5),
					P50:   s.mu.betweenBatchFlushWait.ValueAtQuantile(50),
					P90:   s.mu.betweenBatchFlushWait.ValueAtQuantile(90),
					P99:   s.mu.betweenBatchFlushWait.ValueAtQuantile(99),
					P99_9: s.mu.betweenBatchFlushWait.ValueAtQuantile(99.9),
					Max:   s.mu.betweenBatchFlushWait.Max(),
					Mean:  float32(s.mu.betweenBatchFlushWait.Mean()),
					Count: s.mu.betweenBatchFlushWait.TotalCount(),
				}
				s.mu.eventStreamStats.SinceLastCheckpointFlush = &HistogramData{
					Min:   s.mu.betweenCheckpointFlushWait.Min(),
					P5:    s.mu.betweenCheckpointFlushWait.ValueAtQuantile(5),
					P50:   s.mu.betweenCheckpointFlushWait.ValueAtQuantile(50),
					P90:   s.mu.betweenCheckpointFlushWait.ValueAtQuantile(90),
					P99:   s.mu.betweenCheckpointFlushWait.ValueAtQuantile(99),
					P99_9: s.mu.betweenCheckpointFlushWait.ValueAtQuantile(99.9),
					Max:   s.mu.betweenCheckpointFlushWait.Max(),
					Mean:  float32(s.mu.betweenCheckpointFlushWait.Mean()),
					Count: s.mu.betweenCheckpointFlushWait.TotalCount(),
				}
				for _, b := range s.mu.rangefeedEventReceiveWaitHist.Distribution() {
					s.mu.eventStreamStats.RangefeedEventBars = append(s.mu.eventStreamStats.RangefeedEventBars, &Bar{
						From:  b.From,
						To:    b.To,
						Count: b.Count,
					})
				}
				for _, b := range s.mu.flushWaitHist.Distribution() {
					s.mu.eventStreamStats.FlushEventBars = append(s.mu.eventStreamStats.FlushEventBars, &Bar{
						From:  b.From,
						To:    b.To,
						Count: b.Count,
					})
				}
				for _, b := range s.mu.sendEventWaitHist.Distribution() {
					s.mu.eventStreamStats.SendEventBars = append(s.mu.eventStreamStats.SendEventBars, &Bar{
						From:  b.From,
						To:    b.To,
						Count: b.Count,
					})
				}
				sp.RecordStructured(s.mu.eventStreamStats)
				s.mu.eventStreamStats = &EventStreamPerformanceStats{}
				s.mu.Unlock()
			}

			log.Infof(ctx, "agg events being flushed by %s",
				s.execCfg.NodeInfo.NodeID.SQLInstanceID().String())
			aggEvents := &execinfrapb.TracingAggregatorEvents{
				SQLInstanceID: s.execCfg.NodeInfo.NodeID.SQLInstanceID(),
				FlowID:        execinfrapb.FlowID{},
				Events:        make(map[string][]byte),
			}
			s.agg.ForEachAggregatedEvent(func(name string, event bulk.TracingAggregatorEvent) {
				var data []byte
				var err error
				if data, err = bulk.TracingAggregatorEventToBytes(ctx, event); err != nil {
					// This should never happen but if it does skip the aggregated event.
					log.Warningf(ctx, "failed to unmarshal aggregated event: %v", err.Error())
					return
				}
				aggEvents.Events[name] = data
			})

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.doneChan:
				return nil
			case s.tracingAggCh <- aggEvents:
			}
		}
	}
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
