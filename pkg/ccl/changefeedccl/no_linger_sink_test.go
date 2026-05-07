// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
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
// with the same error.
type recordingSinkClient struct {
	mu        syncutil.Mutex
	flushed   [][]byte // successful payloads, in arrival order
	attempts  int      // total Flush call count, regardless of outcome
	errs      []error  // optional sequence of errors to return
	alwaysErr error    // optional permanent failure
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	if r.alwaysErr != nil {
		return r.alwaysErr
	}
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			return err
		}
	}
	r.flushed = append(r.flushed, append([]byte(nil), p.([]byte)...))
	return nil
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

// emitKV is shorthand for sink.EmitRow with a stubTopic.
func emitKV(t *testing.T, sink Sink, topic, key, value string) {
	t.Helper()
	require.NoError(t, sink.EmitRow(context.Background(), stubTopic{name: topic},
		[]byte(key), []byte(value), nil, hlc.Timestamp{}, hlc.Timestamp{},
		kvevent.Alloc{}, nil))
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
