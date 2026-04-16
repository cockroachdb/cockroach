// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// --- Mock SinkClient and BatchBuffer ---

type mockBatchBuffer struct {
	mu       sync.Mutex
	messages []mockMessage
}

type mockMessage struct {
	topic string
	key   []byte
	val   []byte
}

func (b *mockBatchBuffer) Append(
	_ context.Context, topic string, key []byte, value []byte, _ attributes,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = append(b.messages, mockMessage{topic: topic, key: key, val: value})
}

func (b *mockBatchBuffer) ShouldFlush() bool { return false }
func (b *mockBatchBuffer) Close() (SinkPayload, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	msgs := make([]mockMessage, len(b.messages))
	copy(msgs, b.messages)
	return msgs, nil
}

type mockSinkClient struct {
	mu       sync.Mutex
	flushed  [][]mockMessage
	flushErr error

	// flushCh, if non-nil, blocks Flush until a value is received.
	flushCh chan struct{}

	// flushStartCh, if non-nil, receives a signal when Flush starts.
	flushStartCh chan struct{}
}

func (c *mockSinkClient) MakeBatchBuffer() BatchBuffer {
	return &mockBatchBuffer{}
}

func (c *mockSinkClient) Flush(ctx context.Context, payload SinkPayload) error {
	if c.flushStartCh != nil {
		select {
		case c.flushStartCh <- struct{}{}:
		default:
		}
	}
	if c.flushCh != nil {
		select {
		case <-c.flushCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.flushErr != nil {
		return c.flushErr
	}
	msgs := payload.([]mockMessage)
	c.flushed = append(c.flushed, msgs)
	return nil
}

func (c *mockSinkClient) FlushResolvedPayload(
	ctx context.Context,
	body []byte,
	forEachTopic func(func(topic string) error) error,
	retryOpts retry.Options,
) error {
	return nil
}

func (c *mockSinkClient) Close() error                            { return nil }
func (c *mockSinkClient) CheckConnection(_ context.Context) error { return nil }

func (c *mockSinkClient) getFlushed() [][]mockMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([][]mockMessage, len(c.flushed))
	copy(result, c.flushed)
	return result
}

func (c *mockSinkClient) allFlushedMessages() []mockMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	var all []mockMessage
	for _, batch := range c.flushed {
		all = append(all, batch...)
	}
	return all
}

// --- Helpers ---

// topicN creates a TopicDescriptor with a specific table ID, allowing
// tests to create distinct topics that map to different targets.
func topicN(name string, id int) *tableDescriptorTopic {
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: name,
		ID:   descpb.ID(id),
	}).BuildImmutableTable()
	spec := changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		DescID:            tableDesc.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(name),
	}
	return &tableDescriptorTopic{Metadata: makeMetadata(tableDesc), spec: spec}
}

func makeTestNoLingerSink(
	t *testing.T,
	client *mockSinkClient,
	numWorkers int,
	maxMessages int,
	topics ...*tableDescriptorTopic,
) *noLingerSink {
	t.Helper()

	targets := changefeedbase.Targets{}
	for _, td := range topics {
		targets.Add(td.spec)
	}
	topicNamer, err := MakeTopicNamer(targets,
		WithSanitizeFn(changefeedbase.SQLNameToKafkaName))
	require.NoError(t, err)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()

	return makeNoLingerSink(
		ctx,
		sinkTypeKafka,
		client,
		retry.Options{MaxRetries: 1, InitialBackoff: time.Millisecond},
		numWorkers,
		maxMessages,
		0, // maxBytes
		topicNamer,
		(*sliMetrics)(nil),
		settings,
	)
}

// --- PendingBuffer Tests ---

func TestPendingBuffer_AddAndGetSingle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	e := pendingEvent{key: []byte("k1"), val: []byte("v1")}
	pb.addRow("t", e)

	batch := pb.getBatch(0, 0)
	require.NotNil(t, batch)
	require.Len(t, batch.events, 1)
	require.Equal(t, []byte("k1"), batch.events[0].key)
	require.Equal(t, []byte("v1"), batch.events[0].val)

	pb.completeBatch(batch)
	require.Equal(t, 0, pb.totalEvents)
	require.Equal(t, 0, pb.totalInflight)
}

func TestPendingBuffer_FIFO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	for i, v := range []string{"v1", "v2", "v3"} {
		_ = i
		pb.addRow("t", pendingEvent{key: []byte("k"), val: []byte(v)})
	}

	batch := pb.getBatch(0, 0)
	require.NotNil(t, batch)
	require.Len(t, batch.events, 3)
	require.Equal(t, []byte("v1"), batch.events[0].val)
	require.Equal(t, []byte("v2"), batch.events[1].val)
	require.Equal(t, []byte("v3"), batch.events[2].val)

	pb.completeBatch(batch)
}

func TestPendingBuffer_ConflictAvoidance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A1")})
	pb.addRow("t", pendingEvent{key: []byte("B"), val: []byte("B1")})
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A2")})

	batch1 := pb.getBatch(0, 0)
	require.NotNil(t, batch1)
	// Should get A1, A2, and B1 — all keys available.
	require.Len(t, batch1.events, 3)

	// Now add A3 while A is inflight.
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A3")})

	// Second getBatch should block since all pending keys (A) are inflight.
	// Use a goroutine to test this.
	done := make(chan *pendingBatch, 1)
	go func() {
		done <- pb.getBatch(0, 0)
	}()

	// Give it a moment — should not return yet.
	select {
	case <-done:
		t.Fatal("getBatch should block while A is inflight")
	case <-time.After(50 * time.Millisecond):
	}

	// Complete the first batch, which should unblock.
	pb.completeBatch(batch1)

	select {
	case batch2 := <-done:
		require.NotNil(t, batch2)
		require.Len(t, batch2.events, 1)
		require.Equal(t, []byte("A3"), batch2.events[0].val)
		pb.completeBatch(batch2)
	case <-time.After(2 * time.Second):
		t.Fatal("getBatch should have unblocked after completeBatch")
	}
}

func TestPendingBuffer_MultipleKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A1")})
	pb.addRow("t", pendingEvent{key: []byte("B"), val: []byte("B1")})
	pb.addRow("t", pendingEvent{key: []byte("C"), val: []byte("C1")})

	// First getBatch grabs everything.
	batch1 := pb.getBatch(2, 0) // limit to 2 keys worth
	require.NotNil(t, batch1)
	require.Len(t, batch1.events, 2)

	// Second getBatch should get the remaining key.
	batch2 := pb.getBatch(0, 0)
	require.NotNil(t, batch2)
	require.Len(t, batch2.events, 1)

	// Verify no key overlap.
	var keys1, keys2 []string
	for _, e := range batch1.events {
		keys1 = append(keys1, string(e.key))
	}
	for _, e := range batch2.events {
		keys2 = append(keys2, string(e.key))
	}
	for _, k1 := range keys1 {
		for _, k2 := range keys2 {
			require.NotEqual(t, k1, k2, "batches should not share keys")
		}
	}

	pb.completeBatch(batch1)
	pb.completeBatch(batch2)
}

func TestPendingBuffer_GetBatchBlocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()

	done := make(chan *pendingBatch, 1)
	go func() {
		done <- pb.getBatch(0, 0)
	}()

	// Should block.
	select {
	case <-done:
		t.Fatal("getBatch should block on empty buffer")
	case <-time.After(50 * time.Millisecond):
	}

	// addRow should unblock it.
	pb.addRow("t", pendingEvent{key: []byte("k"), val: []byte("v")})

	select {
	case batch := <-done:
		require.NotNil(t, batch)
		require.Len(t, batch.events, 1)
		pb.completeBatch(batch)
	case <-time.After(2 * time.Second):
		t.Fatal("getBatch should have unblocked after addRow")
	}
}

func TestPendingBuffer_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()

	done := make(chan *pendingBatch, 1)
	go func() {
		done <- pb.getBatch(0, 0)
	}()

	// Should block.
	select {
	case <-done:
		t.Fatal("getBatch should block on empty buffer")
	case <-time.After(50 * time.Millisecond):
	}

	pb.close()

	select {
	case batch := <-done:
		require.Nil(t, batch)
	case <-time.After(2 * time.Second):
		t.Fatal("getBatch should have unblocked after close")
	}
}

func TestPendingBuffer_MaxMessages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	for i := 0; i < 10; i++ {
		pb.addRow("t", pendingEvent{
			key: []byte("k"),
			val: []byte("v"),
		})
	}

	batch1 := pb.getBatch(3, 0)
	require.NotNil(t, batch1)
	require.Len(t, batch1.events, 3)

	// Key is inflight, so completeBatch first.
	pb.completeBatch(batch1)

	batch2 := pb.getBatch(3, 0)
	require.NotNil(t, batch2)
	require.Len(t, batch2.events, 3)
	pb.completeBatch(batch2)
}

func TestPendingBuffer_MaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	// Each event: key(2) + val(10) = 12 bytes.
	for i := 0; i < 5; i++ {
		pb.addRow("t", pendingEvent{
			key: []byte("kk"),
			val: []byte("vvvvvvvvvv"),
		})
	}

	// MaxBytes=25 should allow 2 events (24 bytes) but not 3 (36 bytes).
	batch := pb.getBatch(0, 25)
	require.NotNil(t, batch)
	require.Len(t, batch.events, 2)
	pb.completeBatch(batch)
}

func TestPendingBuffer_KeyGrouping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	// Add events: A x3, B x2, C x1 — interleaved.
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A1")})
	pb.addRow("t", pendingEvent{key: []byte("B"), val: []byte("B1")})
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A2")})
	pb.addRow("t", pendingEvent{key: []byte("C"), val: []byte("C1")})
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A3")})
	pb.addRow("t", pendingEvent{key: []byte("B"), val: []byte("B2")})

	batch := pb.getBatch(0, 0)
	require.NotNil(t, batch)
	require.Len(t, batch.events, 6)

	// Events for the same key should be grouped together and in order.
	keyOrder := make(map[string][]string)
	for _, e := range batch.events {
		k := string(e.key)
		keyOrder[k] = append(keyOrder[k], string(e.val))
	}
	require.Equal(t, []string{"A1", "A2", "A3"}, keyOrder["A"])
	require.Equal(t, []string{"B1", "B2"}, keyOrder["B"])
	require.Equal(t, []string{"C1"}, keyOrder["C"])

	pb.completeBatch(batch)
}

func TestPendingBuffer_CompleteBatchRequeues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A1")})

	batch1 := pb.getBatch(0, 0)
	require.Len(t, batch1.events, 1)

	// Add more events for same key while inflight.
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A2")})
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A3")})

	// Complete the first batch — should make A available again.
	pb.completeBatch(batch1)

	batch2 := pb.getBatch(0, 0)
	require.NotNil(t, batch2)
	require.Len(t, batch2.events, 2)
	require.Equal(t, []byte("A2"), batch2.events[0].val)
	require.Equal(t, []byte("A3"), batch2.events[1].val)
	pb.completeBatch(batch2)
}

func TestPendingBuffer_DrainWithInflight(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()
	pb.addRow("t", pendingEvent{key: []byte("A"), val: []byte("A1")})

	// Take a batch to create inflight work.
	batch := pb.getBatch(0, 0)
	require.NotNil(t, batch)
	require.Equal(t, 1, pb.totalInflight)

	// completeBatch clears inflight.
	pb.completeBatch(batch)
	require.Equal(t, 0, pb.totalInflight)
	require.Equal(t, 0, pb.totalEvents)
}

func TestPendingBuffer_RequeueByMVCC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pb := newPendingBuffer()

	// Key A: 6 events with mvcc timestamps 1-6.
	for i := 1; i <= 6; i++ {
		pb.addRow("t", pendingEvent{
			key:  []byte("A"),
			val:  []byte("A"),
			mvcc: hlc.Timestamp{WallTime: int64(i)},
		})
	}
	// Key B: 1 event with a much higher mvcc timestamp.
	pb.addRow("t", pendingEvent{
		key:  []byte("B"),
		val:  []byte("B"),
		mvcc: hlc.Timestamp{WallTime: 10},
	})

	// First batch: maxMessages=3. Should take A's first 3 events.
	batch1 := pb.getBatch(3, 0)
	require.Len(t, batch1.events, 3)
	require.Equal(t, []byte("A"), batch1.events[0].key)

	// Complete the batch. Key A has 3 remaining (mvcc=4,5,6).
	// Key B has mvcc=10. Key A's oldest remaining (mvcc=4) is older
	// than key B (mvcc=10), so key A should be served next.
	pb.completeBatch(batch1)

	batch2 := pb.getBatch(3, 0)
	require.Len(t, batch2.events, 3)
	require.Equal(t, []byte("A"), batch2.events[0].key,
		"key A (mvcc=4) should be served before key B (mvcc=10)")

	pb.completeBatch(batch2)

	// Now key B should finally be served.
	batch3 := pb.getBatch(3, 0)
	require.Len(t, batch3.events, 1)
	require.Equal(t, []byte("B"), batch3.events[0].key)
	pb.completeBatch(batch3)
}

// --- NoLingerSink Tests ---

func TestNoLingerSink_EmitAndFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{}
	td := topic("t")
	sink := makeTestNoLingerSink(t, client, 1, 0, td)
	defer func() { require.NoError(t, sink.Close()) }()

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		require.NoError(t, sink.EmitRow(
			ctx, td, []byte("k"), []byte("v"), zeroTS, zeroTS, zeroAlloc, nil,
		))
	}

	require.NoError(t, sink.Flush(ctx))

	msgs := client.allFlushedMessages()
	require.Len(t, msgs, 3)
}

func TestNoLingerSink_Ordering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{}
	td := topic("t")
	sink := makeTestNoLingerSink(t, client, 1, 0, td)
	defer func() { require.NoError(t, sink.Close()) }()

	ctx := context.Background()

	for _, v := range []string{"v1", "v2", "v3"} {
		require.NoError(t, sink.EmitRow(
			ctx, td, []byte("same-key"), []byte(v), zeroTS, zeroTS, zeroAlloc, nil,
		))
	}

	require.NoError(t, sink.Flush(ctx))

	msgs := client.allFlushedMessages()
	require.Len(t, msgs, 3)
	require.Equal(t, "v1", string(msgs[0].val))
	require.Equal(t, "v2", string(msgs[1].val))
	require.Equal(t, "v3", string(msgs[2].val))
}

func TestNoLingerSink_Parallelism(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	flushCh := make(chan struct{})
	flushStartCh := make(chan struct{}, 4)
	client := &mockSinkClient{flushCh: flushCh, flushStartCh: flushStartCh}
	td := topic("t")
	// maxMessages=1 so each worker gets exactly 1 event.
	sink := makeTestNoLingerSink(t, client, 4, 1, td)
	defer func() {
		close(flushCh)
		require.NoError(t, sink.Close())
	}()

	ctx := context.Background()

	// Emit 4 events with different keys.
	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, sink.EmitRow(
			ctx, td, []byte(k), []byte("v"), zeroTS, zeroTS, zeroAlloc, nil,
		))
	}

	// Wait for 4 concurrent flushes to start.
	var started int
	timeout := time.After(5 * time.Second)
	for started < 4 {
		select {
		case <-flushStartCh:
			started++
		case <-timeout:
			t.Fatalf("only %d/4 concurrent flushes started", started)
		}
	}

	// All 4 should be running concurrently now.
	require.Equal(t, 4, started)
}

func TestNoLingerSink_FlushDrainsInflight(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	flushCh := make(chan struct{})
	client := &mockSinkClient{flushCh: flushCh}
	td := topic("t")
	sink := makeTestNoLingerSink(t, client, 1, 0, td)
	defer func() { require.NoError(t, sink.Close()) }()

	ctx := context.Background()

	require.NoError(t, sink.EmitRow(
		ctx, td, []byte("k"), []byte("v"), zeroTS, zeroTS, zeroAlloc, nil,
	))

	// Flush should block until the worker completes.
	var flushed atomic.Bool
	go func() {
		_ = sink.Flush(ctx)
		flushed.Store(true)
	}()

	time.Sleep(50 * time.Millisecond)
	require.False(t, flushed.Load())

	// Unblock the worker.
	flushCh <- struct{}{}

	require.Eventually(t, flushed.Load, 2*time.Second, 10*time.Millisecond)
}

func TestNoLingerSink_EmitResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{}
	td := topic("t")
	sink := makeTestNoLingerSink(t, client, 1, 0, td)
	defer func() { require.NoError(t, sink.Close()) }()

	ctx := context.Background()
	var e testEncoder
	require.NoError(t, sink.EmitResolvedTimestamp(ctx, e, hlc.Timestamp{WallTime: 1}))
}

func TestNoLingerSink_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{}
	sink := makeTestNoLingerSink(t, client, 4, 0, topic("t"))
	require.NoError(t, sink.Close())
}

func TestNoLingerSink_FlushError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{
		flushErr: errors.New("boom"),
	}
	td := topic("t")
	sink := makeTestNoLingerSink(t, client, 1, 0, td)
	defer func() { _ = sink.Close() }()

	ctx := context.Background()
	require.NoError(t, sink.EmitRow(
		ctx, td, []byte("k"), []byte("v"), zeroTS, zeroTS, zeroAlloc, nil,
	))

	// The error should eventually surface.
	require.Eventually(t, func() bool {
		err := sink.Flush(ctx)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestNoLingerSink_AllocRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{}
	td := topic("t")
	sink := makeTestNoLingerSink(t, client, 1, 0, td)
	defer func() { require.NoError(t, sink.Close()) }()

	ctx := context.Background()

	var pool testAllocPool
	for i := 0; i < 5; i++ {
		require.NoError(t, sink.EmitRow(
			ctx, td, []byte("k"), []byte("v"), zeroTS, zeroTS, pool.alloc(), nil,
		))
	}

	require.NoError(t, sink.Flush(ctx))
	require.EqualValues(t, 0, pool.used())
}

func TestNoLingerSink_MultiTopic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	client := &mockSinkClient{}
	td1 := topicN("t1", 1)
	td2 := topicN("t2", 2)
	sink := makeTestNoLingerSink(t, client, 1, 0, td1, td2)
	defer func() { require.NoError(t, sink.Close()) }()

	ctx := context.Background()

	require.NoError(t, sink.EmitRow(
		ctx, td1, []byte("k1"), []byte("v1"), zeroTS, zeroTS, zeroAlloc, nil,
	))
	require.NoError(t, sink.EmitRow(
		ctx, td2, []byte("k2"), []byte("v2"), zeroTS, zeroTS, zeroAlloc, nil,
	))

	require.NoError(t, sink.Flush(ctx))

	msgs := client.allFlushedMessages()
	require.Len(t, msgs, 2)

	topics := make(map[string]bool)
	for _, m := range msgs {
		topics[m.topic] = true
	}
	require.True(t, topics["t1"])
	require.True(t, topics["t2"])
}
