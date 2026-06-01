// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// recordingSinkClient captures payloads passed to Flush. errs, if
// non-empty, is consumed one entry per Flush call: a non-nil entry
// makes that Flush fail without recording the payload. alwaysErr,
// if non-nil, takes precedence over errs and makes every Flush fail
// with the same error. holdFlush, if non-nil, blocks Flush after
// recording state until the channel is closed -- used to pin a
// worker inside Flush for tests that need to assert behavior while
// a flush is in progress.
type recordingSinkClient struct {
	mu         syncutil.Mutex
	flushed    [][]byte      // successful payloads, in arrival order
	attempts   int           // total Flush call count, regardless of outcome
	errs       []error       // optional sequence of errors to return
	alwaysErr  error         // optional permanent failure
	holdFlush  chan struct{} // optional gate; Flush blocks on receive
	flushDelay time.Duration // optional artificial latency per Flush
	// collectLatencies, when true, makes Flush parse the
	// 16-hex-char emit-time prefix that emitKVTimed embeds in
	// values and append per-event latencies (computed AFTER
	// flushDelay has elapsed, so it reflects EmitRow-to-delivery
	// time as observed by the sink layer).
	collectLatencies bool
	latencies        []time.Duration
}

func (r *recordingSinkClient) MakeBatchBuffer(string) BatchBuffer {
	return &recordingBatchBuffer{}
}

func (r *recordingSinkClient) FlushResolvedPayload(
	context.Context, []byte, func(func(string) error) error, retry.Options,
) error {
	return nil
}

func (r *recordingSinkClient) Flush(_ context.Context, p SinkPayload) error {
	hold, err := r.recordFlush(p)
	if err != nil {
		return err
	}
	if hold != nil {
		<-hold
	}
	r.mu.Lock()
	delay := r.flushDelay
	collect := r.collectLatencies
	r.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if collect {
		r.recordLatencies(p.([]byte))
	}
	return nil
}

// recordLatencies parses each "k=<16-hex-nanos><suffix>\n" line in
// payload, extracts the emit-time, and appends time.Since(emitTime)
// to r.latencies. Lines that don't match the format are skipped.
func (r *recordingSinkClient) recordLatencies(payload []byte) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range bytes.Split(payload, []byte{'\n'}) {
		eq := bytes.IndexByte(line, '=')
		if eq < 0 || len(line) < eq+1+16 {
			continue
		}
		nanos, err := strconv.ParseUint(string(line[eq+1:eq+1+16]), 16, 64)
		if err != nil {
			continue
		}
		r.latencies = append(r.latencies, now.Sub(time.Unix(0, int64(nanos))))
	}
}

// recordFlush is the locked half of Flush: records the attempt /
// payload / error, returns the holdFlush channel (if any) for the
// caller to block on outside the lock.
func (r *recordingSinkClient) recordFlush(p SinkPayload) (chan struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	if r.alwaysErr != nil {
		return nil, r.alwaysErr
	}
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	r.flushed = append(r.flushed, append([]byte(nil), p.([]byte)...))
	return r.holdFlush, nil
}

func (r *recordingSinkClient) Close() error                          { return nil }
func (r *recordingSinkClient) CheckConnection(context.Context) error { return nil }

// recordingBatchBuffer accumulates "key=value\n" lines into a payload.
type recordingBatchBuffer struct {
	payload []byte
}

func (b *recordingBatchBuffer) Append(_ context.Context, k, v []byte, _ attributes) {
	b.payload = append(b.payload, k...)
	b.payload = append(b.payload, '=')
	b.payload = append(b.payload, v...)
	b.payload = append(b.payload, '\n')
}
func (b *recordingBatchBuffer) ShouldFlush() bool           { return false }
func (b *recordingBatchBuffer) Close() (SinkPayload, error) { return b.payload, nil }

// makeRecordingSink constructs a noLingerSink wired to the given
// recording client. Caller should defer sink.Close(). retryOpts
// defaults to no retries (MaxRetries: 0 = single attempt) when not
// provided.
func makeRecordingSink(t *testing.T, rec *recordingSinkClient, retryOpts ...retry.Options) Sink {
	t.Helper()
	opts := retry.Options{}
	if len(retryOpts) > 0 {
		opts = retryOpts[0]
	}
	settings := cluster.MakeTestingClusterSettings()
	changefeedbase.NoLingerSinkEnabled.Override(context.Background(), &settings.SV, true)
	return makeBatchingOrNoLingerSink(
		context.Background(), sinkTypeWebhook, rec, 0, opts, 1, nil,
		func() *admission.Pacer { return nil },
		timeutil.DefaultTimeSource{}, nilMetricsRecorderBuilder(true), settings,
	)
}

// makeUncappedNoLingerSink constructs a noLingerSink directly with a
// pendingBuffer configured for very large batches and a large buffer
// limit, so the comparison vs batchingSink (whose recording-stub
// BatchBuffer.ShouldFlush returns false, allowing arbitrarily large
// batches) is a fair fight rather than artificially capped on the
// noLinger side.
func makeUncappedNoLingerSink(t *testing.T, rec *recordingSinkClient, numWorkers int) Sink {
	t.Helper()
	s := &noLingerSink{
		client:       rec,
		concreteType: sinkTypeWebhook,
		metrics:      nilMetricsRecorderBuilder(true),
		buffer: newPendingBuffer(pendingBufferConfig{
			maxMessages: 1 << 20,
			maxBytes:    1 << 30,
			bufferLimit: 1 << 20,
		}),
		wg: ctxgroup.WithContext(context.Background()),
	}
	for i := 0; i < numWorkers; i++ {
		s.wg.GoCtx(func(ctx context.Context) error {
			s.runWorker(ctx)
			return nil
		})
	}
	return s
}

// makeRecordingBatchingSink constructs the legacy batchingSink wired
// to the given recording client, with the requested
// minFlushFrequency (the linger timer that the noLingerSink design
// removes).
func makeRecordingBatchingSink(
	t *testing.T, rec *recordingSinkClient, minFlushFreq time.Duration, numWorkers int,
) Sink {
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	// Setting is off by default -- dispatcher returns batchingSink.
	return makeBatchingOrNoLingerSink(
		context.Background(), sinkTypeWebhook, rec, minFlushFreq, retry.Options{}, numWorkers, nil,
		func() *admission.Pacer { return nil },
		timeutil.DefaultTimeSource{}, nilMetricsRecorderBuilder(true), settings,
	)
}

// stubEncoder satisfies the Encoder interface with no-op stubs;
// only EncodeResolvedTimestamp is meaningfully exercised by the
// noLingerSink tests.
type stubEncoder struct{}

func (stubEncoder) EncodeKey(context.Context, cdcevent.Row) ([]byte, error) {
	return nil, nil
}
func (stubEncoder) EncodeValue(
	context.Context, eventContext, cdcevent.Row, cdcevent.Row,
) ([]byte, error) {
	return nil, nil
}
func (stubEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, _ hlc.Timestamp,
) ([]byte, error) {
	return []byte("resolved"), nil
}

// emitKV is shorthand for sink.EmitRow with a stubTopic.
func emitKV(t *testing.T, sink Sink, topic, key, value string) {
	t.Helper()
	require.NoError(t, sink.EmitRow(context.Background(), stubTopic{name: topic},
		[]byte(key), []byte(value), nil, hlc.Timestamp{}, hlc.Timestamp{},
		kvevent.Alloc{}, nil))
}

// emitKVTimed emits with a 16-hex-char nanosecond emit-time prefix
// in the value, so recordingSinkClient (with collectLatencies=true)
// can compute end-to-end delivery latency at Flush time. Use only
// in workloads that don't assert on raw value bytes.
func emitKVTimed(t *testing.T, sink Sink, topic, key, suffix string) {
	t.Helper()
	value := []byte(fmt.Sprintf("%016x%s", time.Now().UnixNano(), suffix))
	require.NoError(t, sink.EmitRow(context.Background(), stubTopic{name: topic},
		[]byte(key), value, nil, hlc.Timestamp{}, hlc.Timestamp{},
		kvevent.Alloc{}, nil))
}

// percentile returns the p-th percentile (0.0-1.0) of latencies.
// Returns 0 if latencies is empty.
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// stubTopic is a minimal TopicDescriptor whose GetTableName returns
// the name we set. Other methods return zero values; only
// GetTableName is exercised by the multi-topic guard.
type stubTopic struct{ name string }

func (t stubTopic) GetNameComponents() (changefeedbase.StatementTimeName, []string) {
	return changefeedbase.StatementTimeName(t.name), nil
}
func (t stubTopic) GetTopicIdentifier() TopicIdentifier           { return TopicIdentifier{} }
func (t stubTopic) GetVersion() descpb.DescriptorVersion          { return 0 }
func (t stubTopic) GetTargetSpecification() changefeedbase.Target { return changefeedbase.Target{} }
func (t stubTopic) GetTableName() string                          { return t.name }

// noopBatchBuffer is a BatchBuffer whose methods do nothing. Used by
// stub sinks that don't care about the flush path.
type noopBatchBuffer struct{}

func (noopBatchBuffer) Append(context.Context, []byte, []byte, attributes) {}
func (noopBatchBuffer) ShouldFlush() bool                                  { return false }
func (noopBatchBuffer) Close() (SinkPayload, error)                        { return nil, nil }

// stubSinkClient is a minimal SinkClient used to drive
// makeBatchingOrNoLingerSink in tests. Its methods are no-ops.
type stubSinkClient struct{}

func (stubSinkClient) MakeBatchBuffer(string) BatchBuffer { return noopBatchBuffer{} }
func (stubSinkClient) FlushResolvedPayload(
	context.Context, []byte, func(func(string) error) error, retry.Options,
) error {
	return nil
}
func (stubSinkClient) Flush(context.Context, SinkPayload) error { return nil }
func (stubSinkClient) Close() error                             { return nil }
func (stubSinkClient) CheckConnection(context.Context) error    { return nil }

// makeStubSink invokes the dispatcher with a stub SinkClient and the
// test's cluster settings. Caller should defer sink.Close().
func makeStubSink(t *testing.T, settings *cluster.Settings) Sink {
	t.Helper()
	return makeBatchingOrNoLingerSink(
		context.Background(),
		sinkTypeWebhook,
		stubSinkClient{},
		0, // minFlushFrequency
		retry.Options{},
		1,   // numWorkers
		nil, // topicNamer
		func() *admission.Pacer { return nil },
		timeutil.DefaultTimeSource{},
		nilMetricsRecorderBuilder(true),
		settings,
	)
}

func TestNoLingerSinkDispatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("disabled returns batchingSink", func(t *testing.T) {
		settings := cluster.MakeTestingClusterSettings()
		sink := makeStubSink(t, settings)
		defer func() { require.NoError(t, sink.Close()) }()
		require.IsType(t, &batchingSink{}, sink)
	})

	t.Run("enabled returns noLingerSink", func(t *testing.T) {
		settings := cluster.MakeTestingClusterSettings()
		changefeedbase.NoLingerSinkEnabled.Override(ctx, &settings.SV, true)
		sink := makeStubSink(t, settings)
		defer func() { require.NoError(t, sink.Close()) }()
		require.IsType(t, &noLingerSink{}, sink)
	})
}

func TestNoLingerSinkRejectsMultiTopic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	changefeedbase.NoLingerSinkEnabled.Override(ctx, &settings.SV, true)
	sink := makeStubSink(t, settings).(*noLingerSink)
	defer func() { require.NoError(t, sink.Close()) }()

	emit := func(topicName string) error {
		return sink.EmitRow(ctx, stubTopic{name: topicName},
			[]byte("k"), []byte("v"), nil, hlc.Timestamp{}, hlc.Timestamp{},
			kvevent.Alloc{}, nil)
	}

	// First topic is accepted; same topic again is accepted.
	require.NoError(t, emit("foo"))
	require.NoError(t, emit("foo"))

	// Different topic is rejected with a clear error.
	err := emit("bar")
	require.Error(t, err)
	require.Contains(t, err.Error(), "multi-topic")
	require.Contains(t, err.Error(), "foo")
	require.Contains(t, err.Error(), "bar")
}

// TestNoLingerSinkBasicHappyPath is the end-to-end happy-path check for
// noLingerSink against a real webhook test feed. Insert rows with the
// setting on; expect their payloads to arrive at the sink.
//
// Until the worker pool lands, this test times out in assertPayloads
// because noLingerSink accumulates events in pendingBuffer with no
// consumer.
func TestNoLingerSinkBasicHappyPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.no_linger_sink.enabled = true`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

		// TODO: drop this WITH clause once the webhook test feed
		// handles multi-message payloads. Messages=1 is the workaround
		// (test feed only reads payload[0]); Frequency must accompany
		// Messages to satisfy webhook config validation, even though
		// noLingerSink doesn't use the linger timer.
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH webhook_sink_config = '{"Flush":{"Messages":1,"Frequency":"100ms"}}'`)
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
		assertPayloads(t, foo, []string{
			`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("webhook"))
}

// TestNoLingerSinkResolvedWaitsForDrain pins the resolved-timestamp
// contract deterministically: EmitResolvedTimestamp must not return
// while any row event added before it is still in flight, otherwise
// the sink could ship the resolved before the rows it covers.
//
// The setup pins a worker inside client.Flush via holdFlush, then
// invokes EmitResolvedTimestamp in a goroutine. While the worker is
// blocked, EmitResolvedTimestamp must also be blocked. After we
// release the worker, EmitResolvedTimestamp must complete.
//
// EXPECTED TO FAIL until M3 commit 4 (real Flush drain in
// EmitResolvedTimestamp) lands -- today EmitResolvedTimestamp goes
// straight to client.FlushResolvedPayload regardless of pending
// row work.
func TestNoLingerSinkResolvedWaitsForDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rec := &recordingSinkClient{holdFlush: make(chan struct{})}
	sink := makeRecordingSink(t, rec)

	emitKV(t, sink, "foo", "k1", "v1")

	// Wait for the worker to be stuck inside client.Flush.
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.attempts >= 1
	}, time.Second, 5*time.Millisecond, "worker never entered Flush")

	done := make(chan error, 1)
	go func() {
		done <- sink.EmitResolvedTimestamp(ctx, stubEncoder{}, hlc.Timestamp{WallTime: 1})
	}()

	// EmitResolvedTimestamp must NOT return while the worker is still
	// flushing the row.
	select {
	case err := <-done:
		t.Fatalf("EmitResolvedTimestamp returned (err=%v) without draining the buffer", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Releasing the worker unblocks the drain; EmitResolvedTimestamp
	// should complete promptly afterwards.
	close(rec.holdFlush)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("EmitResolvedTimestamp did not complete after worker drained")
	}

	require.NoError(t, sink.Close())
}

// TestNoLingerSinkCloseDrains pins the contract that all events
// successfully passed to EmitRow before Close are flushed before Close
// returns. A regression that re-introduces eager ctx-cancel on Close
// would silently drop in-flight drains and fail this test.
func TestNoLingerSinkCloseDrains(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rec := &recordingSinkClient{}
	sink := makeRecordingSink(t, rec)

	const n = 10
	for i := 0; i < n; i++ {
		emitKV(t, sink, "foo", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	require.NoError(t, sink.Close())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	combined := bytes.Join(rec.flushed, nil)
	for i := 0; i < n; i++ {
		require.Contains(t, string(combined), fmt.Sprintf("k%d=v%d", i, i),
			"event %d missing from delivered payloads", i)
	}
}

// TestNoLingerSinkRetriesTransientFailures pins the contract that a
// Flush call that fails transiently is retried, and the event is
// eventually delivered. EXPECTED TO FAIL until M3 commit 3 (retries)
// lands.
func TestNoLingerSinkRetriesTransientFailures(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// First two Flush calls fail; third succeeds.
	rec := &recordingSinkClient{errs: []error{
		errors.New("transient 1"),
		errors.New("transient 2"),
	}}
	sink := makeRecordingSink(t, rec, retry.Options{
		InitialBackoff: time.Microsecond,
		MaxBackoff:     time.Millisecond,
		MaxRetries:     5,
	})

	emitKV(t, sink, "foo", "k1", "v1")
	require.NoError(t, sink.Close())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.GreaterOrEqual(t, rec.attempts, 3, "expected at least 3 Flush attempts (2 failures + 1 success)")
	combined := bytes.Join(rec.flushed, nil)
	require.Contains(t, string(combined), "k1=v1",
		"event should have been delivered after retries succeeded")
}

// TestNoLingerSinkWorkerSurvivesFlushError pins the contract that
// when a transient Flush failure is masked by a retry success, the
// worker is not driven into a terminal state -- subsequent EmitRow
// calls keep working and their events get delivered. This is the
// counterpart to TestNoLingerSinkPropagatesTerminalFlushError, which
// covers the case where retries are exhausted.
func TestNoLingerSinkWorkerSurvivesFlushError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// First Flush fails; the retry succeeds.
	rec := &recordingSinkClient{errs: []error{errors.New("transient")}}
	sink := makeRecordingSink(t, rec, retry.Options{
		InitialBackoff: time.Microsecond,
		MaxBackoff:     time.Millisecond,
		MaxRetries:     2,
	})

	emitKV(t, sink, "foo", "k1", "v1")

	// Wait until the first batch has been delivered (1 failure + 1
	// retry success = at least 2 attempts).
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.attempts >= 2 && len(rec.flushed) >= 1
	}, time.Second, 5*time.Millisecond, "first batch never delivered")

	// The worker should still be alive and accepting work.
	emitKV(t, sink, "foo", "k2", "v2")
	require.NoError(t, sink.Close())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	combined := bytes.Join(rec.flushed, nil)
	require.Contains(t, string(combined), "k1=v1",
		"first event was not delivered after retry")
	require.Contains(t, string(combined), "k2=v2",
		"second event was not delivered (worker may have terminated)")
}

// TestNoLingerSinkPropagatesTerminalFlushError pins the contract that
// when a worker exhausts retries on Flush, the terminal error
// surfaces to subsequent EmitRow callers so the changefeed processor
// can restart instead of silently losing events. EXPECTED TO FAIL
// until termErr propagation lands.
func TestNoLingerSinkPropagatesTerminalFlushError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rec := &recordingSinkClient{alwaysErr: errors.New("permanent failure")}
	sink := makeRecordingSink(t, rec) // default MaxRetries: 0 -> single attempt
	defer func() { _ = sink.Close() }()

	// First emit succeeds at addRow time -- the worker hasn't observed
	// the failure yet.
	emitKV(t, sink, "foo", "k1", "v1")

	// Eventually the worker attempts the flush, fails, and sets
	// termErr. From then on, EmitRow returns the terminal error.
	require.Eventually(t, func() bool {
		err := sink.EmitRow(context.Background(), stubTopic{name: "foo"},
			[]byte("k2"), []byte("v2"), nil, hlc.Timestamp{}, hlc.Timestamp{},
			kvevent.Alloc{}, nil)
		return err != nil &&
			(errors.Is(err, rec.alwaysErr) ||
				bytes.Contains([]byte(err.Error()), []byte("permanent failure")))
	}, time.Second, 5*time.Millisecond,
		"EmitRow never returned the terminal Flush error")
}

// TestNoLingerSinkBatchingEmerges demonstrates the latency /
// throughput decoupling that motivates the noLingerSink design:
// the same sink configuration handles both low-rate and high-rate
// workloads well, with no min_flush_frequency knob to tune.
//
// At low rate: events drain as soon as the worker is free; the batch
// is small and end-to-end is bounded by the SinkClient's Flush
// latency, not by any timer.
//
// At high rate: events accumulate while the worker is busy in
// Flush; the next getBatch returns a large batch naturally,
// pushing throughput up without raising per-event latency by a
// timer interval.
//
// Skipped under -short; run with `./dev test pkg/ccl/changefeedccl
// -f TestNoLingerSinkBatchingEmerges -v` to see the numbers.
func TestNoLingerSinkBatchingEmerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderShort(t, "perf demo; run with -v")

	// Run a single workload against a sink and report the resulting
	// batch shape, throughput, and per-event latency. burstSize controls
	// the emit pattern: events are emitted as fast as possible in groups
	// of burstSize, with a sleep of emitGap between groups. burstSize=1
	// + emitGap>0 yields a steady low rate (one event per emitGap);
	// burstSize=N + emitGap=0 yields a saturating workload;
	// burstSize=N + emitGap>0 yields a bursty workload.
	type sinkBuilder func(*testing.T, *recordingSinkClient) Sink
	runWorkload := func(t *testing.T, build sinkBuilder, label string,
		burstSize int, emitGap, flushLatency time.Duration, totalEvents int) {
		rec := &recordingSinkClient{
			flushDelay:       flushLatency,
			collectLatencies: true,
		}
		sink := build(t, rec)

		start := time.Now()
		for i := 0; i < totalEvents; i++ {
			emitKVTimed(t, sink, "foo", fmt.Sprintf("k%d", i), "v")
			if (i+1)%burstSize == 0 && emitGap > 0 {
				time.Sleep(emitGap)
			}
		}
		emitElapsed := time.Since(start)
		require.NoError(t, sink.Flush(context.Background()))
		totalElapsed := time.Since(start)
		require.NoError(t, sink.Close())

		rec.mu.Lock()
		defer rec.mu.Unlock()
		batches := len(rec.flushed)
		events := 0
		for _, p := range rec.flushed {
			events += bytes.Count(p, []byte{'\n'})
		}
		avgBatch := float64(events) / float64(batches)
		throughput := float64(events) / totalElapsed.Seconds()
		p50 := percentile(rec.latencies, 0.50)
		p99 := percentile(rec.latencies, 0.99)
		t.Logf("%-40s emit=%v drain=%v events=%d batches=%d avg_batch=%.1f "+
			"throughput=%.0f ev/s p50=%v p99=%v",
			label, emitElapsed, totalElapsed, events, batches, avgBatch,
			throughput, p50, p99)
	}

	noLinger := func(numWorkers int) sinkBuilder {
		return func(t *testing.T, rec *recordingSinkClient) Sink {
			return makeUncappedNoLingerSink(t, rec, numWorkers)
		}
	}
	batching := func(freq time.Duration, numWorkers int) sinkBuilder {
		return func(t *testing.T, rec *recordingSinkClient) Sink {
			return makeRecordingBatchingSink(t, rec, freq, numWorkers)
		}
	}

	// 8 workers approximates a real production sink (parallelism is
	// usually 8-16 for kafka/webhook v2). The flush stub takes 1ms.
	const numWorkers = 8
	const flushLatency = time.Millisecond

	startTrace := func(t *testing.T, name string) {
		t.Helper()
		f, err := os.Create(filepath.Join(os.TempDir(), name+".trace"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		require.NoError(t, trace.Start(f))
		t.Cleanup(trace.Stop)
		t.Logf("trace: %s", f.Name())
	}

	t.Run("low_rate_no_linger", func(t *testing.T) {
		startTrace(t, "low_rate_no_linger")
		runWorkload(t, noLinger(numWorkers), "noLingerSink            ",
			1, 10*time.Millisecond, flushLatency, 1000)
	})
	t.Run("low_rate_min_freq=1ms", func(t *testing.T) {
		startTrace(t, "low_rate_min_freq_1ms")
		runWorkload(t, batching(time.Millisecond, numWorkers), "batchingSink min_freq=1ms  ",
			1, 10*time.Millisecond, flushLatency, 1000)
	})
	t.Run("low_rate_min_freq=100ms", func(t *testing.T) {
		startTrace(t, "low_rate_min_freq_100ms")
		runWorkload(t, batching(100*time.Millisecond, numWorkers), "batchingSink min_freq=100ms",
			1, 10*time.Millisecond, flushLatency, 1000)
	})

	t.Run("high_rate_no_linger", func(t *testing.T) {
		startTrace(t, "high_rate_no_linger")
		runWorkload(t, noLinger(numWorkers), "noLingerSink            ",
			1, 0, flushLatency, 100000)
	})
	t.Run("high_rate_min_freq=1ms", func(t *testing.T) {
		startTrace(t, "high_rate_min_freq_1ms")
		runWorkload(t, batching(time.Millisecond, numWorkers), "batchingSink min_freq=1ms  ",
			1, 0, flushLatency, 100000)
	})
	t.Run("high_rate_min_freq=100ms", func(t *testing.T) {
		startTrace(t, "high_rate_min_freq_100ms")
		runWorkload(t, batching(100*time.Millisecond, numWorkers), "batchingSink min_freq=100ms",
			1, 0, flushLatency, 100000)
	})

	// Bursty: 1000 events as fast as possible, then 50ms idle, repeated
	// 100 times (total 100k events, ~5s wall). Models a workload where
	// rangefeed deliveries arrive in bursts (e.g. a leaseholder change
	// or a backfill catch-up) interleaved with quiet periods.
	t.Run("bursty_no_linger", func(t *testing.T) {
		startTrace(t, "bursty_no_linger")
		runWorkload(t, noLinger(numWorkers), "noLingerSink            ",
			1000, 50*time.Millisecond, flushLatency, 100000)
	})
	t.Run("bursty_min_freq=1ms", func(t *testing.T) {
		startTrace(t, "bursty_min_freq_1ms")
		runWorkload(t, batching(time.Millisecond, numWorkers), "batchingSink min_freq=1ms  ",
			1000, 50*time.Millisecond, flushLatency, 100000)
	})
	t.Run("bursty_min_freq=100ms", func(t *testing.T) {
		startTrace(t, "bursty_min_freq_100ms")
		runWorkload(t, batching(100*time.Millisecond, numWorkers), "batchingSink min_freq=100ms",
			1000, 50*time.Millisecond, flushLatency, 100000)
	})
}

// TestNoLingerSinkFlushDrains pins that a direct Sink.Flush() call
// (the path the changefeed processor uses at checkpoint boundaries)
// drains in-flight workers before returning -- not just the
// EmitResolvedTimestamp wrapper. EXPECTED TO FAIL until M3 commit 4
// lands.
func TestNoLingerSinkFlushDrains(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rec := &recordingSinkClient{holdFlush: make(chan struct{})}
	sink := makeRecordingSink(t, rec)

	emitKV(t, sink, "foo", "k1", "v1")

	// Wait for the worker to be stuck inside client.Flush.
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.attempts >= 1
	}, time.Second, 5*time.Millisecond, "worker never entered Flush")

	done := make(chan error, 1)
	go func() { done <- sink.Flush(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("Flush returned (err=%v) without draining the buffer", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(rec.holdFlush)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Flush did not complete after worker drained")
	}

	require.NoError(t, sink.Close())
}

// TestNoLingerSinkFlushReturnsTermErr pins that when a worker
// latches a terminal error, a subsequent Sink.Flush() returns it
// rather than reporting success. EXPECTED TO FAIL until M3 commit 4
// lands -- today Flush is a no-op and always returns nil.
func TestNoLingerSinkFlushReturnsTermErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rec := &recordingSinkClient{alwaysErr: errors.New("permanent failure")}
	sink := makeRecordingSink(t, rec) // MaxRetries: 0 -> single attempt
	defer func() { _ = sink.Close() }()

	emitKV(t, sink, "foo", "k1", "v1")

	// Eventually the worker sets termErr; after that Flush must
	// surface it.
	require.Eventually(t, func() bool {
		err := sink.Flush(ctx)
		return err != nil && bytes.Contains([]byte(err.Error()), []byte("permanent failure"))
	}, time.Second, 5*time.Millisecond,
		"Flush never returned the terminal error")
}
