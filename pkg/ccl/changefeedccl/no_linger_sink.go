// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"container/heap"
	"context"
	"hash"
	"hash/crc32"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// pendingEvent holds a single changefeed event waiting to be batched.
type pendingEvent struct {
	topic TopicDescriptor
	key   []byte
	val   []byte
	attrs attributes
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

// pendingBatch is returned by getBatch: a slice of events and the set
// of key hashes included.
type pendingBatch struct {
	events []pendingEvent
	keys   intsets.Fast
}

// keyHeapEntry pairs a key hash with the mvcc timestamp of the oldest
// pending event for that key, used for age-based heap ordering.
type keyHeapEntry struct {
	keyHash int
	mvcc    hlc.Timestamp
}

// keyHeap is a min-heap of keyHeapEntry ordered by mvcc timestamp
// (oldest first).
type keyHeap []keyHeapEntry

func (h keyHeap) Len() int            { return len(h) }
func (h keyHeap) Less(i, j int) bool  { return h[i].mvcc.Less(h[j].mvcc) }
func (h keyHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *keyHeap) Push(x interface{}) { *h = append(*h, x.(keyHeapEntry)) }
func (h *keyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// pendingBuffer is a thread-safe buffer of pending events organized by
// key hash. Workers pull non-conflicting batches via getBatch; completed
// batches are returned via completeBatch to unlock those keys.
type pendingBuffer struct {
	mu          sync.Mutex
	cond        *sync.Cond
	keyHeap     keyHeap
	keyMessages map[int][]pendingEvent
	inflight    intsets.Fast
	totalEvents int
	// totalInflight counts events currently being processed by workers.
	totalInflight int
	// flushing blocks new addRow calls while a Flush is draining.
	flushing bool
	closed   bool
	hasher   hash.Hash32
}

func newPendingBuffer() *pendingBuffer {
	pb := &pendingBuffer{
		keyMessages: make(map[int][]pendingEvent),
		hasher:      crc32.New(crc32.MakeTable(crc32.IEEE)),
	}
	pb.cond = sync.NewCond(&pb.mu)
	return pb
}

func (pb *pendingBuffer) computeKeyHash(topicName string, key []byte) int {
	pb.hasher.Reset()
	pb.hasher.Write([]byte(topicName))
	pb.hasher.Write(key)
	return int(pb.hasher.Sum32())
}

// addRow adds an event to the buffer and signals waiting workers.
// Blocks while a Flush is draining the buffer.
func (pb *pendingBuffer) addRow(topicName string, e pendingEvent) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	for pb.flushing && !pb.closed {
		pb.cond.Wait()
	}

	h := pb.computeKeyHash(topicName, e.key)
	_, exists := pb.keyMessages[h]
	pb.keyMessages[h] = append(pb.keyMessages[h], e)
	pb.totalEvents++

	// Only push to heap if this key is new (not already queued or inflight).
	// Use the event's mvcc timestamp for age-based ordering.
	if !exists && !pb.inflight.Contains(h) {
		heap.Push(&pb.keyHeap, keyHeapEntry{keyHash: h, mvcc: e.mvcc})
	}

	pb.cond.Signal()
}

// hasActionableWork returns true if there are non-inflight keys with
// pending events.
func (pb *pendingBuffer) hasActionableWork() bool {
	return pb.keyHeap.Len() > 0
}

// getBatch blocks until a batch of events is available, then returns
// it. Returns nil if the buffer is closed. The caller must call
// completeBatch when done processing.
func (pb *pendingBuffer) getBatch(maxMessages, maxBytes int) *pendingBatch {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	for !pb.hasActionableWork() && !pb.closed {
		pb.cond.Wait()
	}
	if pb.closed {
		return nil
	}

	batch := &pendingBatch{}
	totalBytes := 0

	for pb.keyHeap.Len() > 0 {
		entry := heap.Pop(&pb.keyHeap).(keyHeapEntry)
		events := pb.keyMessages[entry.keyHash]
		if len(events) == 0 {
			delete(pb.keyMessages, entry.keyHash)
			continue
		}

		taken := 0
		for _, ev := range events {
			evSize := len(ev.key) + len(ev.val)
			if maxMessages > 0 && len(batch.events) >= maxMessages {
				break
			}
			if maxBytes > 0 && totalBytes+evSize > maxBytes && len(batch.events) > 0 {
				break
			}
			batch.events = append(batch.events, ev)
			totalBytes += evSize
			taken++
		}

		batch.keys.Add(entry.keyHash)
		pb.inflight.Add(entry.keyHash)

		if taken < len(events) {
			pb.keyMessages[entry.keyHash] = events[taken:]
		} else {
			delete(pb.keyMessages, entry.keyHash)
		}

		if maxMessages > 0 && len(batch.events) >= maxMessages {
			break
		}
		if maxBytes > 0 && totalBytes >= maxBytes {
			break
		}
	}

	pb.totalEvents -= len(batch.events)
	pb.totalInflight += len(batch.events)

	return batch
}

// completeBatch marks the keys in the batch as no longer inflight.
// If any of those keys have new pending events, they're re-added to
// the heap.
func (pb *pendingBuffer) completeBatch(batch *pendingBatch) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	batch.keys.ForEach(func(h int) {
		pb.inflight.Remove(h)
		if events, ok := pb.keyMessages[h]; ok && len(events) > 0 {
			heap.Push(&pb.keyHeap, keyHeapEntry{keyHash: h, mvcc: events[0].mvcc})
		}
	})

	pb.totalInflight -= len(batch.events)
	pb.cond.Broadcast()
}

// close signals all waiting goroutines to stop.
func (pb *pendingBuffer) close() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.closed = true
	pb.cond.Broadcast()
}

// noLingerSink implements the Sink interface using a pull-based model
// where idle workers pull batches from a shared pendingBuffer.
type noLingerSink struct {
	client       SinkClient
	topicNamer   *TopicNamer
	concreteType sinkType

	buf *pendingBuffer

	maxMessages int
	maxBytes    int
	ioWorkers   int
	retryOpts   retry.Options

	metrics        metricsRecorder
	noLingerMetric noLingerMetricsRecorder
	settings       *cluster.Settings

	termErr error // guarded by buf.mu

	wg     ctxgroup.Group
	cancel context.CancelFunc
}

var _ Sink = (*noLingerSink)(nil)

// Dial implements the Sink interface.
func (s *noLingerSink) Dial() error {
	return s.client.CheckConnection(context.TODO())
}

// Close implements the Sink interface.
func (s *noLingerSink) Close() error {
	s.buf.close()
	s.cancel()
	_ = s.wg.Wait()
	return s.client.Close()
}

// getConcreteType implements the Sink interface.
func (s *noLingerSink) getConcreteType() sinkType {
	return s.concreteType
}

func (s *noLingerSink) getTermErr() error {
	s.buf.mu.Lock()
	defer s.buf.mu.Unlock()
	return s.termErr
}

func (s *noLingerSink) setTermErr(err error) {
	s.buf.mu.Lock()
	defer s.buf.mu.Unlock()
	if s.termErr == nil {
		s.termErr = err
	}
}

// EmitRow implements the Sink interface.
func (s *noLingerSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
	headers rowHeaders,
) error {
	if err := s.getTermErr(); err != nil {
		return err
	}

	var topicName string
	if s.topicNamer != nil {
		var err error
		topicName, err = s.topicNamer.Name(topic)
		if err != nil {
			return err
		}
	}

	s.metrics.recordMessageSize(int64(len(key) + len(value)))

	e := pendingEvent{
		topic: topic,
		key:   key,
		val:   value,
		attrs: attributes{
			tableName: topic.GetTableName(),
			headers:   headers,
			mvcc:      mvcc,
		},
		alloc: alloc,
		mvcc:  mvcc,
	}
	s.buf.addRow(topicName, e)
	s.noLingerMetric.recordPendingRows(1)
	return nil
}

// Flush implements the Sink interface. It blocks new EmitRow calls and
// waits until all pending and inflight events have been flushed.
// Workers continue pulling batches during Flush.
func (s *noLingerSink) Flush(ctx context.Context) error {
	defer s.metrics.recordFlushRequestCallback()()

	s.buf.mu.Lock()
	defer s.buf.mu.Unlock()

	if s.termErr != nil {
		return s.termErr
	}

	// Block new addRow calls while draining.
	s.buf.flushing = true
	defer func() {
		s.buf.flushing = false
		s.buf.cond.Broadcast()
	}()

	// Wait until all pending and inflight events are drained.
	for (s.buf.totalInflight > 0 || s.buf.totalEvents > 0) &&
		s.termErr == nil && !s.buf.closed {
		s.buf.cond.Wait()
	}

	return s.termErr
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *noLingerSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	if err = s.Flush(ctx); err != nil {
		return err
	}
	return s.client.FlushResolvedPayload(
		ctx, data, s.topicNamer.Each, s.retryOpts,
	)
}

// Topics implements SinkWithTopics.
func (s *noLingerSink) Topics() []string {
	if s.topicNamer == nil {
		return nil
	}
	return s.topicNamer.DisplayNamesSlice()
}

var _ SinkWithTopics = (*noLingerSink)(nil)

// runWorker is the main loop for each IO worker goroutine.
func (s *noLingerSink) runWorker(ctx context.Context) error {
	for {
		waitStart := timeutil.Now()
		batch := s.buf.getBatch(s.maxMessages, s.maxBytes)
		if batch == nil {
			return nil // closed
		}
		s.noLingerMetric.recordGetBatchWait(timeutil.Since(waitStart))
		s.noLingerMetric.recordPendingRows(-int64(len(batch.events)))

		if s.maxMessages > 0 {
			pct := int64(len(batch.events)) * 100 / int64(s.maxMessages)
			s.noLingerMetric.recordBatchFillPct(pct)
		}

		err := s.processBatch(ctx, batch)

		// Release allocs regardless of success.
		for i := range batch.events {
			batch.events[i].alloc.Release(ctx)
		}

		if err != nil {
			s.setTermErr(err)
			s.buf.completeBatch(batch)
			s.buf.close()
			return err
		}

		s.buf.completeBatch(batch)
	}
}

// processBatch creates a BatchBuffer, appends all events, and flushes.
func (s *noLingerSink) processBatch(ctx context.Context, batch *pendingBatch) error {
	bb := s.client.MakeBatchBuffer()
	for _, ev := range batch.events {
		var topicName string
		if s.topicNamer != nil {
			var err error
			topicName, err = s.topicNamer.Name(ev.topic)
			if err != nil {
				return err
			}
		}
		bb.Append(ctx, topicName, ev.key, ev.val, ev.attrs)
	}

	payload, err := bb.Close()
	if err != nil {
		return err
	}

	return s.retryOpts.Do(ctx, func(ctx context.Context) error {
		return s.client.Flush(ctx, payload)
	})
}

func makeNoLingerSink(
	ctx context.Context,
	concreteType sinkType,
	client SinkClient,
	retryOpts retry.Options,
	numWorkers int,
	maxMessages int,
	maxBytes int,
	topicNamer *TopicNamer,
	metrics metricsRecorder,
	settings *cluster.Settings,
) *noLingerSink {
	ctx, cancel := context.WithCancel(ctx)

	sink := &noLingerSink{
		client:         client,
		topicNamer:     topicNamer,
		concreteType:   concreteType,
		buf:            newPendingBuffer(),
		maxMessages:    maxMessages,
		maxBytes:       maxBytes,
		ioWorkers:      numWorkers,
		retryOpts:      retryOpts,
		metrics:        metrics,
		noLingerMetric: metrics.newNoLingerMetricsRecorder(),
		settings:       settings,
		cancel:         cancel,
	}

	sink.wg = ctxgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		sink.wg.GoCtx(sink.runWorker)
	}

	// Watch for context cancellation and close the buffer.
	sink.wg.GoCtx(func(ctx context.Context) error {
		<-ctx.Done()
		sink.buf.close()
		return nil
	})

	return sink
}
