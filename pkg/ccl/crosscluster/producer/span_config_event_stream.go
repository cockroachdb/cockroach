// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type spanConfigEventStream struct {
	execCfg *sql.ExecutorConfig
	spec    streampb.SpanConfigEventStreamSpec
	mon     *mon.BytesMonitor
	acc     mon.BoundAccount

	data tree.Datums // Data to send to the consumer

	// Fields below initialized when Start called.
	rfc         *rangefeedcache.Watcher[*spanconfigkvsubscriber.BufferEvent]
	streamGroup ctxgroup.Group // Context group controlling stream execution.
	doneChan    chan struct{}  // Channel signaled to close the stream loop.
	updateCh    chan rangefeedcache.Update[*spanconfigkvsubscriber.BufferEvent]
	errCh       chan error       // Signaled when error occurs in rangefeed.
	streamCh    chan tree.Datums // Channel signaled to forward datums to consumer.
	sp          *tracing.Span    // Span representing the lifetime of the eventStream.
}

var _ eval.ValueGenerator = (*spanConfigEventStream)(nil)

// ResolvedType implements eval.ValueGenerator interface.
func (s *spanConfigEventStream) ResolvedType() *types.T {
	return eventStreamReturnType
}

// Start implements eval.ValueGenerator interface.
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
	s.updateCh = make(chan rangefeedcache.Update[*spanconfigkvsubscriber.BufferEvent])

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	s.doneChan = make(chan struct{})

	// Reserve batch kvsSize bytes from monitor.
	if err := s.acc.Grow(ctx, defaultBatchSize); err != nil {
		return errors.Wrapf(err, "failed to allocated %d bytes from monitor", defaultBatchSize)
	}

	var rangefeedCacheKnobs *rangefeedcache.TestingKnobs
	if s.execCfg.StreamingTestingKnobs != nil && s.execCfg.StreamingTestingKnobs.SpanConfigRangefeedCacheKnobs != nil {
		rangefeedCacheKnobs = s.execCfg.StreamingTestingKnobs.SpanConfigRangefeedCacheKnobs
	}

	s.rfc = rangefeedcache.NewWatcher(
		"spanconfig-subscriber",
		s.execCfg.Clock, s.execCfg.RangeFeedFactory,
		defaultBatchSize,
		roachpb.Spans{s.spec.Span},
		true, // withPrevValue
		true, // withRowTSInInitialScan
		spanconfigkvsubscriber.NewSpanConfigDecoder().TranslateEvent,
		s.handleUpdate,
		rangefeedCacheKnobs,
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
	streamCtx, sp := tracing.ChildSpan(ctx, "span config event stream")
	s.sp = sp
	s.streamGroup = ctxgroup.WithContext(streamCtx)
	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		return s.streamLoop(ctx)
	}))

	s.streamGroup.GoCtx(withErrCapture(func(ctx context.Context) error {
		return rangefeedcache.Start(ctx, s.execCfg.DistSQLSrv.Stopper, s.rfc, nil)
	}))
}

// Next implements eval.ValueGenerator interface.
func (s *spanConfigEventStream) Next(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-s.errCh:
		return false, err
	case s.data = <-s.streamCh:
		select {
		case err := <-s.errCh:
			return false, err
		default:
			return true, nil
		}
	}
}

// Values implements eval.ValueGenerator interface.
func (s *spanConfigEventStream) Values() (tree.Datums, error) {
	return s.data, nil
}

// Close implements eval.ValueGenerator interface.
func (s *spanConfigEventStream) Close(ctx context.Context) {
	s.acc.Close(ctx)

	close(s.doneChan)
	if err := s.streamGroup.Wait(); err != nil {
		// Note: error in close is normal; we expect to be terminated with context canceled.
		log.Errorf(ctx, "span config stream terminated with error %v", err)
	}
	s.sp.Finish()
}

func (s *spanConfigEventStream) handleUpdate(
	ctx context.Context, update rangefeedcache.Update[*spanconfigkvsubscriber.BufferEvent],
) {
	select {
	case <-ctx.Done():
		log.Warningf(ctx, "rangefeedcache context cancelled with error %s", ctx.Err())
	case s.updateCh <- update:
		if update.Type == rangefeedcache.CompleteUpdate {
			log.VInfof(ctx, 1, "observed complete scan")
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

type checkpointPacer struct {
	pace time.Duration
	next time.Time
}

func makeCheckpointPacer(frequency time.Duration) checkpointPacer {
	return checkpointPacer{
		pace: frequency,
		next: timeutil.Now().Add(frequency),
	}
}

func (p *checkpointPacer) shouldCheckpoint() bool {
	now := timeutil.Now()
	if p.next.Before(now) {
		p.next = now.Add(p.pace)
		return true
	}
	return false
}

// streamLoop is the main processing loop responsible for reading buffered rangefeed events,
// accumulating them in a batch, and sending those events to the ValueGenerator.
func (s *spanConfigEventStream) streamLoop(ctx context.Context) error {

	// TODO(msbutler): We may not need a pacer, given how little traffic will come from this
	// stream. That being said, we'd still want to buffer updates to ensure we
	// don't clog up the rangefeed. Consider using async flushing.
	pacer := makeCheckpointPacer(s.spec.MinCheckpointFrequency)
	bufferedEvents := make([]streampb.StreamedSpanConfigEntry, 0)
	batcher := makeStreamEventBatcher(s.spec.WrappedEvents)
	frontier := makeSpanConfigFrontier(s.spec.Span)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.doneChan:
			return nil
		case update := <-s.updateCh:
			fromFullScan := update.Type == rangefeedcache.CompleteUpdate
			if fromFullScan {
				// If we're about to send over a full scan, reset the
				// spanConfigFrontier, since the full scan will likely contain span
				// config updates that have timestamps that are older than the frontier.
				batcher.spanConfigFrontier = hlc.MinTimestamp
			}
			for _, ev := range update.Events {
				target := ev.Update.GetTarget()
				if target.IsSystemTarget() {
					// We skip replicating SystemTarget Span configs as they are not
					// necessary to replicate. System target span configurations are
					// created to manage protected timestamps during system and app tenant
					// cluster backups, and backups of tenants. There's no need to
					// replicate protected timestamps for app tenant cluster backups, as
					// replicated backup jobs will fail after cutover anyway.
					//
					// If a new application that uses SystemTargets requires replication,
					// we'll need to special case the handling of these records. For now,
					// don't worry about it.
					continue
				}
				_, tenantID, err := keys.DecodeTenantPrefix(target.GetSpan().Key)
				if err != nil {
					return err
				}
				if !tenantID.Equal(s.spec.TenantID) {
					continue
				}

				streamedSpanCfgEntry := streampb.StreamedSpanConfigEntry{
					SpanConfig: roachpb.SpanConfigEntry{
						Target: target.ToProto(),
						Config: ev.Update.GetConfig(),
					},
					Timestamp:    ev.Timestamp(),
					FromFullScan: fromFullScan,
				}
				bufferedEvents = append(bufferedEvents, streamedSpanCfgEntry)
			}
			batcher.addSpanConfigs(bufferedEvents, update.Timestamp)
			bufferedEvents = bufferedEvents[:0]
			if pacer.shouldCheckpoint() || fromFullScan {
				log.VEventf(ctx, 2, "checkpointing span config stream at %s", update.Timestamp.GoTime())
				if batcher.getSize() > 0 {
					log.VEventf(ctx, 2, "sending %d span config events", len(batcher.batch.SpanConfigs))
					if err := s.flushEvent(ctx, &streampb.StreamEvent{Batch: &batcher.batch}); err != nil {
						return err
					}
				}
				frontier.update(update.Timestamp)
				if err := s.flushEvent(ctx, &streampb.StreamEvent{Checkpoint: &frontier.checkpoint}); err != nil {
					return err
				}
				batcher.reset()
			}
		}
	}
}

func makeSpanConfigFrontier(span roachpb.Span) *spanConfigFrontier {

	checkpoint := streampb.StreamEvent_StreamCheckpoint{
		ResolvedSpans: []jobspb.ResolvedSpan{{
			Span: span,
		}},
	}
	return &spanConfigFrontier{
		checkpoint: checkpoint,
	}
}

type spanConfigFrontier struct {
	checkpoint streampb.StreamEvent_StreamCheckpoint
}

func (spf *spanConfigFrontier) update(frontier hlc.Timestamp) {
	spf.checkpoint.ResolvedSpans[0].Timestamp = frontier
}

func streamSpanConfigs(
	evalCtx *eval.Context, spec streampb.SpanConfigEventStreamSpec,
) (eval.ValueGenerator, error) {

	if !evalCtx.SessionData().AvoidBuffering {
		return nil, errors.New("partition streaming requires 'SET avoid_buffering = true' option")
	}

	return &spanConfigEventStream{
		spec:    spec,
		execCfg: evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig),
		mon:     evalCtx.Planner.Mon(),
	}, nil
}
