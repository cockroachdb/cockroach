// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"

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
	"github.com/stretchr/testify/require"
)

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

// stubSinkClient is a minimal SinkClient used to drive
// makeBatchingOrNoLingerSink in tests. Its methods are no-ops.
type stubSinkClient struct{}

func (stubSinkClient) MakeBatchBuffer(string) BatchBuffer { return nil }
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

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo`)
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
