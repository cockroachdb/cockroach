// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// noLingerBufferLimit caps the number of events the pendingBuffer holds
// before addRow blocks. Matches the existing batchingSink eventCh depth.
const noLingerBufferLimit = 256

// makeBatchingOrNoLingerSink dispatches between makeBatchingSink and
// makeNoLingerSink based on the changefeed.no_linger_sink.enabled
// cluster setting. Both helpers share an identical signature so the
// v2 sink construction sites only have to call this in place of
// makeBatchingSink.
func makeBatchingOrNoLingerSink(
	ctx context.Context,
	concreteType sinkType,
	client SinkClient,
	minFlushFrequency time.Duration,
	retryOpts retry.Options,
	numWorkers int,
	topicNamer *TopicNamer,
	pacerFactory func() *admission.Pacer,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
	settings *cluster.Settings,
) Sink {
	if changefeedbase.NoLingerSinkEnabled.Get(&settings.SV) {
		return makeNoLingerSink(ctx, concreteType, client, minFlushFrequency,
			retryOpts, numWorkers, topicNamer, pacerFactory, timeSource,
			metrics, settings)
	}
	return makeBatchingSink(ctx, concreteType, client, minFlushFrequency,
		retryOpts, numWorkers, topicNamer, pacerFactory, timeSource,
		metrics, settings)
}

// noLingerSink is the pull-based replacement for batchingSink. EmitRow
// pushes into a pendingBuffer; a fixed pool of workers pull batches and
// flush them through the SinkClient. Resolved timestamps go straight to
// the SinkClient (the row buffer is bypassed; future commits will
// drain it via Flush).
//
// Still missing relative to batchingSink (TODOs land in subsequent M3
// commits): retry semantics on Flush, terminal-error propagation, real
// Flush drain, pacer integration. Gated off by default behind
// changefeed.no_linger_sink.enabled.
type noLingerSink struct {
	client       SinkClient
	topicNamer   *TopicNamer
	concreteType sinkType
	retryOpts    retry.Options
	metrics      metricsRecorder
	buffer       *pendingBuffer
	wg           ctxgroup.Group

	// TODO(M4): the noLingerSink prototype only supports single-topic
	// changefeeds. Once the two-level (topicHeap + per-topic keyHeaps)
	// design lands, multi-topic changefeeds should work and this guard
	// goes away. See docs/tech-notes/changefeed-no-linger-batching.md.
	topicGuard struct {
		syncutil.Mutex
		seen string // first observed topic; "" until first EmitRow
	}
}

var _ Sink = (*noLingerSink)(nil)
var _ SinkWithTopics = (*noLingerSink)(nil)

// makeNoLingerSink has the same signature as makeBatchingSink so it can
// be swapped in at any of the v2 sink construction sites without
// touching the callers.
func makeNoLingerSink(
	ctx context.Context,
	concreteType sinkType,
	client SinkClient,
	_ time.Duration, // minFlushFrequency, unused (no linger timer)
	retryOpts retry.Options,
	numWorkers int,
	topicNamer *TopicNamer,
	_ func() *admission.Pacer, // pacerFactory, TODO M3 commit 5
	_ timeutil.TimeSource, // timeSource, unused
	metrics metricsRecorder,
	_ *cluster.Settings, // settings, unused
) Sink {
	if numWorkers < 1 {
		numWorkers = 1
	}
	s := &noLingerSink{
		client:       client,
		topicNamer:   topicNamer,
		concreteType: concreteType,
		retryOpts:    retryOpts,
		metrics:      metrics,
		buffer: newPendingBuffer(pendingBufferConfig{
			maxMessages: 100,
			maxBytes:    1 << 20,
			bufferLimit: noLingerBufferLimit,
		}),
		wg: ctxgroup.WithContext(ctx),
	}
	for i := 0; i < numWorkers; i++ {
		s.wg.GoCtx(func(ctx context.Context) error {
			s.runWorker(ctx)
			return nil
		})
	}
	return s
}

// runWorker pulls batches from the pendingBuffer and flushes them
// through the SinkClient until the buffer is closed.
func (s *noLingerSink) runWorker(ctx context.Context) {
	for {
		batch, err := s.buffer.getBatch(ctx)
		if err != nil {
			// errPendingBufferClosed is the expected exit. Any other
			// error type is unexpected today (no buffer path returns
			// one), so log it loudly so a future buffer change
			// doesn't add a silent failure mode here.
			// TODO(M3 commit 3): plumb termErr instead.
			if !errors.Is(err, errPendingBufferClosed) {
				log.Changefeed.Warningf(ctx,
					"noLingerSink worker exiting on unexpected getBatch error: %v", err)
			}
			return
		}
		if flushErr := s.flushBatch(ctx, batch); flushErr != nil {
			// WARNING: until M3 commits 2 (retries) and 3 (termErr)
			// land, a Flush failure here is a silent data loss: the
			// completeBatch below releases each event's
			// kvevent.Alloc, which lets the resolved-timestamp
			// frontier advance past undelivered MVCC timestamps with
			// no replay. This is acceptable only because
			// changefeed.no_linger_sink.enabled is off by default;
			// users who flip it on get the no-retry behavior.
			log.Changefeed.Errorf(ctx, "noLingerSink flush failed: %v", flushErr)
		}
		s.buffer.completeBatch(ctx, batch)
	}
}

// flushBatch translates a pendingBatch into a SinkClient.Flush call by
// constructing a per-batch BatchBuffer, appending each event, and
// closing it to produce a sink-specific payload. Single-topic batches
// only; the topic name is derived from the first event's
// TopicDescriptor (the single-topic guard ensures all events agree).
func (s *noLingerSink) flushBatch(ctx context.Context, batch *pendingBatch) error {
	// Defensive: pendingBuffer.buildBatchLocked never returns a non-nil
	// empty batch today, but the cost of guarding is one branch.
	if len(batch.events) == 0 {
		return nil
	}
	topicStr := ""
	if s.topicNamer != nil {
		var err error
		topicStr, err = s.topicNamer.Name(batch.events[0].topicDescriptor)
		if err != nil {
			return err
		}
	}
	bb := s.client.MakeBatchBuffer(topicStr)
	for _, ev := range batch.events {
		bb.Append(ctx, ev.key, ev.val, attributes{
			tableName:       ev.topicDescriptor.GetTableName(),
			headers:         ev.headers,
			mvcc:            ev.mvcc,
			csvColumnHeader: ev.csvColumnHeader,
		})
	}
	payload, err := bb.Close()
	if err != nil {
		return err
	}
	return s.client.Flush(ctx, payload)
}

// EmitRow implements the Sink interface.
func (s *noLingerSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	csvColumnHeader []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
	headers rowHeaders,
) error {
	if err := s.checkSingleTopic(topic); err != nil {
		return err
	}

	s.metrics.recordMessageSize(int64(len(key) + len(value) + headersLen(headers)))

	ev := newRowEvent()
	ev.key = key
	ev.val = value
	ev.topicDescriptor = topic
	ev.headers = headers
	ev.csvColumnHeader = csvColumnHeader
	ev.mvcc = mvcc
	ev.alloc = alloc

	if err := s.buffer.addRow(ctx, ev); err != nil {
		return err
	}
	log.Changefeed.Infof(ctx, "noLingerSink addRow key=%x val_size=%d", key, len(value))
	return nil
}

// checkSingleTopic enforces the prototype's single-topic restriction.
// The first EmitRow records its topic; any subsequent row with a
// different topic returns a clear error so the changefeed job fails
// fast instead of silently producing wrong output. See the topicGuard
// TODO on the struct.
func (s *noLingerSink) checkSingleTopic(topic TopicDescriptor) error {
	name := topic.GetTableName()
	s.topicGuard.Lock()
	defer s.topicGuard.Unlock()
	if s.topicGuard.seen == "" {
		s.topicGuard.seen = name
		return nil
	}
	if s.topicGuard.seen == name {
		return nil
	}
	return errors.Newf(
		"changefeed.no_linger_sink.enabled does not yet support multi-topic "+
			"changefeeds (saw both %q and %q); disable the setting or restrict "+
			"the changefeed to a single table",
		s.topicGuard.seen, name)
}

// Flush implements the Sink interface. M2 demo: no-op. Resolved
// timestamps still propagate through the SinkClient.
func (s *noLingerSink) Flush(_ context.Context) error {
	return nil
}

// EmitResolvedTimestamp implements the Sink interface. Bypasses the
// pendingBuffer and writes the resolved payload directly through the
// SinkClient (matches batchingSink behavior modulo the Flush call,
// which is a no-op here).
func (s *noLingerSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	return s.client.FlushResolvedPayload(ctx, data, s.topicNamer.Each, s.retryOpts)
}

// Close implements the Sink interface. Closes the buffer (which wakes
// blocked getBatch waiters; previously-pulled events still drain
// through workers before getBatch returns the closed sentinel),
// waits for the worker pool, then closes the SinkClient.
//
// Workers stuck in client.Flush at Close time are not interrupted —
// shutdown waits on the parent ctx (the one passed to makeNoLingerSink)
// being cancelled to unblock them. This matches batchingSink's
// behavior and preserves drain semantics: if we cancelled here, an
// in-flight Flush would error and the subsequent completeBatch would
// release the alloc, advancing the resolved frontier past undelivered
// events. A future commit can layer a deadline-then-cancel pattern
// for graceful-shutdown-with-bounded-wait.
func (s *noLingerSink) Close() error {
	s.buffer.close()
	_ = s.wg.Wait()
	return s.client.Close()
}

// Dial implements the Sink interface.
func (s *noLingerSink) Dial() error {
	return s.client.CheckConnection(context.TODO())
}

// Topics implements SinkWithTopics.
func (s *noLingerSink) Topics() []string {
	if s.topicNamer == nil {
		return nil
	}
	return s.topicNamer.DisplayNamesSlice()
}

// getConcreteType implements the Sink interface.
func (s *noLingerSink) getConcreteType() sinkType {
	return s.concreteType
}
