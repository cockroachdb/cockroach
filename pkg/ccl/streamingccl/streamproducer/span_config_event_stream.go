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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type spanConfigEventStream struct {
	execCfg *sql.ExecutorConfig
	spec    streampb.StreamPartitionSpec
	mon     *mon.BytesMonitor
	acc     mon.BoundAccount

	data tree.Datums // Data to send to the consumer

	// Fields below initialized when Start called.
	rfc         *rangefeedcache.Watcher
	streamGroup ctxgroup.Group // Context group controlling stream execution.
	doneChan    chan struct{}  // Channel signaled to close the stream loop.
	updateCh    chan rangefeedcache.Update
	errCh       chan error       // Signaled when error occurs in rangefeed.
	streamCh    chan tree.Datums // Channel signaled to forward datums to consumer.
	sp          *tracing.Span    // Span representing the lifetime of the eventStream.
}

var _ eval.ValueGenerator = (*spanConfigEventStream)(nil)

// ResolvedType implements tree.ValueGenerator interface.
func (s *spanConfigEventStream) ResolvedType() *types.T {
	return eventStreamReturnType
}

// Start implements tree.ValueGenerator interface.
func (s *spanConfigEventStream) Start(ctx context.Context, txn *kv.Txn) error {
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

	// updateCh gets buffered RangeFeedEvents and is consumed by the ValueGenerator.
	s.updateCh = make(chan rangefeedcache.Update)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	s.doneChan = make(chan struct{})

	// Reserve batch kvsSize bytes from monitor.
	if err := s.acc.Grow(ctx, s.spec.Config.BatchByteSize); err != nil {
		return errors.Wrapf(err, "failed to allocated %d bytes from monitor", s.spec.Config.BatchByteSize)
	}

	s.rfc = rangefeedcache.NewWatcher(
		"spanconfig-subscriber",
		s.execCfg.Clock, s.execCfg.RangeFeedFactory,
		int(s.spec.Config.BatchByteSize),
		s.spec.Spans,
		true, // withPrevValue
		spanconfigkvsubscriber.NewSpanConfigDecoder().TranslateEvent,
		s.handleUpdate,
		nil,
	)

	// NB: statements below should not return errors (otherwise, we may fail to release
	// bound account resources).
	s.startStreamProcessor(ctx)
	return nil
}

func (s *spanConfigEventStream) maybeSetError(err error) {
	// Only send the error if the channel is empty, else it's ok to swallow the
	// error because the first error in the channel will shut down the event
	// stream.
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *spanConfigEventStream) startStreamProcessor(ctx context.Context) {
	type ctxGroupFn = func(ctx context.Context) error

	// withErrCapture wraps fn to capture and report error to the error channel.
	withErrCapture := func(fn ctxGroupFn) ctxGroupFn {
		return func(ctx context.Context) error {
			err := fn(ctx)
			if err != nil {
				log.Errorf(ctx, "span config event stream terminating with error %v", err)
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
		return s.streamLoop(ctx)
	}))

	// TODO(msbutler): consider using rangefeedcache.Run(), though it seems overly complicated.
	s.streamGroup.GoCtx(withErrCapture(s.rfc.Run))
}

// Next implements tree.ValueGenerator interface.
func (s *spanConfigEventStream) Next(ctx context.Context) (bool, error) {
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
func (s *spanConfigEventStream) Values() (tree.Datums, error) {
	return s.data, nil
}

// Close implements tree.ValueGenerator interface.
func (s *spanConfigEventStream) Close(ctx context.Context) {
	s.acc.Close(ctx)

	close(s.doneChan)
	if err := s.streamGroup.Wait(); err != nil {
		// Note: error in close is normal; we expect to be terminated with context canceled.
		log.Errorf(ctx, "span config stream terminated with error %v", err)
	}
	s.sp.Finish()
}

func (s *spanConfigEventStream) handleUpdate(ctx context.Context, update rangefeedcache.Update) {
	select {
	case <-ctx.Done():
		s.errCh <- ctx.Err()
	case s.updateCh <- update:
		if update.Type == rangefeedcache.CompleteUpdate {
			log.VInfof(ctx, 1, "completed initial scan")
		}
	}
}

// TODO(msbutler): this code is duplicate to event_stream. dedupe.
func (s *spanConfigEventStream) flushEvent(ctx context.Context, event *streampb.StreamEvent) error {
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

// streamLoop is the main processing loop responsible for reading buffered rangefeed events,
// accumulating them in a batch, and sending those events to the ValueGenerator.
func (s *spanConfigEventStream) streamLoop(ctx context.Context) error {

	// TODO(msbutler): We may not need a pacer, given how little traffic will come from this
	// stream. That being said, we'd still want to buffer updates to ensure we
	// don't clog up the rangefeed. Consider using async flushing.
	pacer := makeCheckpointPacer(s.spec.Config.MinCheckpointFrequency)
	bufferedEvents := make([]streampb.StreamedSpanConfigEntry, 0)
	batcher := makeStreamEventBatcher()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.doneChan:
			return nil
		case update := <-s.updateCh:
			for _, ev := range update.Events {
				spcfgEvent := ev.(*spanconfigkvsubscriber.BufferEvent)
				target := spcfgEvent.Update.GetTarget().ToProto()
				_, tenantID, err := keys.DecodeTenantPrefix(target.GetSpan().Key)
				if err != nil {
					return err
				}
				if !tenantID.Equal(s.spec.Config.SpanConfigForTenant) {
					continue
				}

				streamedSpanCfgEntry := streampb.StreamedSpanConfigEntry{
					SpanConfig: roachpb.SpanConfigEntry{
						Target: target,
						Config: spcfgEvent.Update.GetConfig(),
					},
					Timestamp: spcfgEvent.Timestamp(),
				}
				bufferedEvents = append(bufferedEvents, streamedSpanCfgEntry)
			}
			batcher.addSpanConfigs(bufferedEvents)
			bufferedEvents = bufferedEvents[:0]
			if pacer.shouldCheckpoint(update.Timestamp, true) {
				if err := s.flushEvent(ctx, &streampb.StreamEvent{Batch: &batcher.batch}); err != nil {
					return err
				}
				batcher.reset()
			}
		}
	}
}

func streamSpanConfigPartition(
	evalCtx *eval.Context, spec streampb.StreamPartitionSpec,
) (eval.ValueGenerator, error) {
	if err := validateSpecs(evalCtx, spec); err != nil {
		return nil, err
	}
	setConfigDefaults(&spec.Config)

	return &spanConfigEventStream{
		spec:    spec,
		execCfg: evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig),
		mon:     evalCtx.Planner.Mon(),
	}, nil
}
