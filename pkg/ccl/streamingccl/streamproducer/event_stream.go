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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type eventStream struct {
	streamID streaming.StreamID
	execCfg  *sql.ExecutorConfig
	spec     streampb.StreamPartitionSpec
	mon      *mon.BytesMonitor
	acc      mon.BoundAccount

	data tree.Datums // Data to send to the consumer

	// Fields below initialized when Start called.
	rf          *rangefeed.RangeFeed        // Currently running rangefeed.
	streamGroup ctxgroup.Group              // Context group controlling stream execution.
	eventsCh    chan roachpb.RangeFeedEvent // Channel receiving rangefeed events.
	errCh       chan error                  // Signaled when error occurs in rangefeed.
	streamCh    chan tree.Datums            // Channel signaled to forward datums to consumer.
	sp          *tracing.Span               // Span representing the lifetime of the eventStream.
}

var _ tree.ValueGenerator = (*eventStream)(nil)

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
	s.eventsCh = make(chan roachpb.RangeFeedEvent)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	// Common rangefeed options.
	opts := []rangefeed.Option{
		rangefeed.WithOnCheckpoint(s.onCheckpoint),

		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			s.maybeSetError(err)
		}),

		rangefeed.WithMemoryMonitor(s.mon),
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
		fmt.Sprintf("streamID=%d", s.streamID), s.spec.StartFrom, s.onEvent, opts...)
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

	if err := s.streamGroup.Wait(); err != nil {
		// Note: error in close is normal; we expect to be terminated with context canceled.
		log.Errorf(ctx, "partition stream %d terminated with error %v", s.streamID, err)
	}

	s.sp.Finish()
}

func (s *eventStream) onEvent(ctx context.Context, value *roachpb.RangeFeedValue) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- roachpb.RangeFeedEvent{Val: value}:
		if log.V(1) {
			log.Infof(ctx, "onEvent: %s@%s", value.Key, value.Value.Timestamp)
		}
	}
}

func (s *eventStream) onCheckpoint(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
	select {
	case <-ctx.Done():
	case s.eventsCh <- roachpb.RangeFeedEvent{Checkpoint: checkpoint}:
		if log.V(1) {
			log.Infof(ctx, "onCheckpoint: %s@%s", checkpoint.Span, checkpoint.ResolvedTS)
		}
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
	case s.eventsCh <- roachpb.RangeFeedEvent{Checkpoint: &checkpoint}:
		if log.V(1) {
			log.Infof(ctx, "onSpanCompleted: %s@%s", checkpoint.Span, checkpoint.ResolvedTS)
		}
		return nil
	}
}

// makeCheckpoint generates checkpoint based on the frontier.
func makeCheckpoint(f *span.Frontier) (checkpoint streampb.StreamEvent_StreamCheckpoint) {
	f.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		checkpoint.Spans = append(checkpoint.Spans, streampb.StreamEvent_SpanCheckpoint{
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

	maybeFlushBatch := func(force bool) error {
		if (force && batchSize > 0) || batchSize > int(s.spec.Config.BatchByteSize) {
			defer func() {
				batchSize = 0
				batch.KeyValues = batch.KeyValues[:0]
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
			default:
				// TODO(yevgeniy): Handle SSTs.
				return errors.AssertionFailedf("unexpected event")
			}
		}
	}
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

func streamPartition(
	evalCtx *tree.EvalContext, streamID streaming.StreamID, opaqueSpec []byte,
) (tree.ValueGenerator, error) {
	if !evalCtx.SessionData().AvoidBuffering {
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

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	return &eventStream{
		streamID: streamID,
		spec:     spec,
		execCfg:  execCfg,
		mon:      evalCtx.Mon,
	}, nil
}
