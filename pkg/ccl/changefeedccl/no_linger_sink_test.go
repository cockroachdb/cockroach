// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"sync"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// recordingSinkClient captures payloads passed to Flush. errs, if
// non-empty, is consumed one entry per Flush call: a non-nil entry
// makes that Flush fail without recording the payload.
type recordingSinkClient struct {
	mu       sync.Mutex
	flushed  [][]byte // successful payloads, in arrival order
	attempts int      // total Flush call count, regardless of outcome
	errs     []error  // optional sequence of errors to return
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
// recording client. Caller should defer sink.Close().
func makeRecordingSink(t *testing.T, rec *recordingSinkClient) Sink {
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	changefeedbase.NoLingerSinkEnabled.Override(context.Background(), &settings.SV, true)
	return makeBatchingOrNoLingerSink(
		context.Background(), sinkTypeWebhook, rec, 0, retry.Options{}, 1, nil,
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

// TestNoLingerSinkWorkerSurvivesFlushError pins the contract that a
// transient Flush failure does not exit the worker. We arrange the
// failure with errs[0]; a subsequent EmitRow after the failure has
// been observed must still be delivered.
func TestNoLingerSinkWorkerSurvivesFlushError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rec := &recordingSinkClient{errs: []error{errors.New("transient")}}
	sink := makeRecordingSink(t, rec)

	emitKV(t, sink, "foo", "k1", "v1")

	// Wait until the first batch has been attempted (and failed).
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.attempts >= 1
	}, time.Second, 5*time.Millisecond, "first Flush attempt never observed")

	emitKV(t, sink, "foo", "k2", "v2")
	require.NoError(t, sink.Close())

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.GreaterOrEqual(t, rec.attempts, 2, "worker exited after first failure")
	combined := bytes.Join(rec.flushed, nil)
	require.Contains(t, string(combined), "k2=v2",
		"event after the failed flush was not delivered")
	// k1's batch was the failed one; with no retry (commit 2 still
	// pending) it is not expected to appear.
	require.NotContains(t, string(combined), "k1=v1",
		"unexpectedly delivered the failed event (retry must not be implemented yet)")
}
