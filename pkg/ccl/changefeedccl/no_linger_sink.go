// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

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

// noLingerSink is the M2-prototype Sink that pushes EmitRow payloads
// into a pendingBuffer with no consumer attached. It exists to demo the
// addRow path against a real changefeed; events accumulate, a real
// downstream sink is never written to, and resolved timestamps are
// passed straight to the SinkClient without draining row events.
//
// M3 will replace the no-consumer stub with a real worker pool that
// pulls batches and flushes them through SinkClient.Flush.
type noLingerSink struct {
	client       SinkClient
	topicNamer   *TopicNamer
	concreteType sinkType
	retryOpts    retry.Options
	metrics      metricsRecorder
	buffer       *pendingBuffer

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
	_ time.Duration, // minFlushFrequency, unused
	retryOpts retry.Options,
	_ int, // numWorkers, unused (no consumer in M2)
	topicNamer *TopicNamer,
	_ func() *admission.Pacer, // pacerFactory, unused
	_ timeutil.TimeSource, // timeSource, unused
	metrics metricsRecorder,
	_ *cluster.Settings, // settings, unused
) Sink {
	return &noLingerSink{
		client:       client,
		topicNamer:   topicNamer,
		concreteType: concreteType,
		retryOpts:    retryOpts,
		metrics:      metrics,
		buffer: newPendingBuffer(pendingBufferConfig{
			maxMessages: 100,
			maxBytes:    1 << 20,
			bufferLimit: math.MaxInt32, // unbounded for demo; OOMs eventually
		}),
	}
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

// Close implements the Sink interface.
func (s *noLingerSink) Close() error {
	s.buffer.close()
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
