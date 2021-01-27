// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type interceptableStreamClient interface {
	streamclient.Client

	RegisterInterception(func(event streamingccl.Event))
}

// mockStreamClient will return the slice of events associated to the stream
// partition being consumed. Stream partitions are identified by unique
// partition addresses.
type mockStreamClient struct {
	partitionEvents map[streamingccl.PartitionAddress][]streamingccl.Event
}

var _ streamclient.Client = &mockStreamClient{}

// GetTopology implements the StreamClient interface.
func (m *mockStreamClient) GetTopology(
	_ streamingccl.StreamAddress,
) (streamingccl.Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the StreamClient interface.
func (m *mockStreamClient) ConsumePartition(
	_ context.Context, address streamingccl.PartitionAddress, _ time.Time,
) (chan streamingccl.Event, error) {
	var events []streamingccl.Event
	var ok bool
	if events, ok = m.partitionEvents[address]; !ok {
		return nil, errors.Newf("no events found for paritition %s", address)
	}

	eventCh := make(chan streamingccl.Event, len(events))

	for _, event := range events {
		eventCh <- event
	}
	close(eventCh)

	return eventCh, nil
}

// Close implements the StreamClient interface.
func (m *mockStreamClient) Close() {}

func TestStreamIngestionProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	// Inject a mock client.
	v := roachpb.MakeValueFromString("value_1")
	v.Timestamp = hlc.Timestamp{WallTime: 1}
	sampleKV := roachpb.KeyValue{Key: roachpb.Key("key_1"), Value: v}
	events := []streamingccl.Event{
		streamingccl.MakeKVEvent(sampleKV),
		streamingccl.MakeKVEvent(sampleKV),
		streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
		streamingccl.MakeKVEvent(sampleKV),
		streamingccl.MakeKVEvent(sampleKV),
		streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
	}
	pa1 := streamingccl.PartitionAddress("partition1")
	pa2 := streamingccl.PartitionAddress("partition2")
	mockClient := &mockStreamClient{
		partitionEvents: map[streamingccl.PartitionAddress][]streamingccl.Event{pa1: events, pa2: events},
	}

	startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	out, err := runStreamIngestionProcessor(ctx, t, kvDB, "some://stream", startTime,
		nil /* interceptors */, mockClient)
	require.NoError(t, err)

	// Compare the set of results since the ordering is not guaranteed.
	expectedRows := map[string]struct{}{
		"partition1{-\\x00} 0.000000001,0": {},
		"partition1{-\\x00} 0.000000004,0": {},
		"partition2{-\\x00} 0.000000001,0": {},
		"partition2{-\\x00} 0.000000004,0": {},
	}
	actualRows := make(map[string]struct{})
	for {
		row := out.NextNoMeta(t)
		if row == nil {
			break
		}
		datum := row[0].Datum
		protoBytes, ok := datum.(*tree.DBytes)
		require.True(t, ok)

		var resolvedSpan jobspb.ResolvedSpan
		require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &resolvedSpan))

		actualRows[fmt.Sprintf("%s %s", resolvedSpan.Span, resolvedSpan.Timestamp)] = struct{}{}
	}

	require.Equal(t, expectedRows, actualRows)
}

// TestRandomClientGeneration tests the ingestion processor against a random
// stream workload.
func TestRandomClientGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	makeTestStreamURI := func(
		tableID string,
		valueRange, kvsPerResolved int,
		kvFrequency time.Duration,
	) string {
		return "test://" + tableID + "?VALUE_RANGE=" + strconv.Itoa(valueRange) +
			"&KV_FREQUENCY=" + strconv.Itoa(int(kvFrequency)) +
			"&KVS_PER_RESOLVED=" + strconv.Itoa(kvsPerResolved)
	}

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Create the expected table for the random stream to ingest into.
	sqlDB.Exec(t, streamclient.RandomStreamSchema)
	tableID := sqlDB.QueryStr(t, `SELECT id FROM system.namespace WHERE name = 'test'`)[0][0]

	// TODO: Consider testing variations on these parameters.
	valueRange := 100
	kvsPerResolved := 1_000
	kvFrequency := 50 * time.Nanosecond
	streamAddr := makeTestStreamURI(tableID, valueRange, kvsPerResolved, kvFrequency)

	startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	ctx, cancel := context.WithCancel(ctx)
	// Cancel the flow after emitting 1000 checkpoint events from the client.
	cancelAfterCheckpoints := makeCheckpointEventCounter(1_000, cancel)
	out, err := runStreamIngestionProcessor(ctx, t, kvDB, streamAddr, startTime,
		cancelAfterCheckpoints, nil /* mockClient */)
	require.NoError(t, err)

	p1Key := roachpb.Key("partition1")
	p2Key := roachpb.Key("partition2")
	p1Span := roachpb.Span{Key: p1Key, EndKey: p1Key.Next()}
	p2Span := roachpb.Span{Key: p2Key, EndKey: p2Key.Next()}
	numResolvedEvents := 0
	for {
		row, meta := out.Next()
		if meta != nil {
			// The flow may fail with a context cancellation error if the processor
			// was cut of during flushing.
			if !testutils.IsError(meta.Err, "context canceled") {
				t.Fatalf("unexpected meta error %v", meta.Err)
			}
		}
		if row == nil {
			break
		}
		datum := row[0].Datum
		protoBytes, ok := datum.(*tree.DBytes)
		require.True(t, ok)

		var resolvedSpan jobspb.ResolvedSpan
		require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &resolvedSpan))

		if resolvedSpan.Span.String() != p1Span.String() && resolvedSpan.Span.String() != p2Span.String() {
			t.Fatalf("expected resolved span %v to be either %v or %v", resolvedSpan.Span, p1Span, p2Span)
		}

		// All resolved timestamp events should be greater than the start time.
		require.Less(t, startTime.WallTime, resolvedSpan.Timestamp.WallTime)
		numResolvedEvents++
	}

	// Check that some rows have been ingested and that we've emitted some resolved events.
	numRows, err := strconv.Atoi(sqlDB.QueryStr(t, `SELECT count(*) FROM defaultdb.test`)[0][0])
	require.NoError(t, err)
	require.Greater(t, numRows, 0, "at least 1 row ingested expected")

	require.Greater(t, numResolvedEvents, 0, "at least 1 resolved event expected")
}

func runStreamIngestionProcessor(
	ctx context.Context,
	t *testing.T,
	kvDB *kv.DB,
	streamAddr string,
	startTime hlc.Timestamp,
	interceptEvents func(streamingccl.Event),
	mockClient streamclient.Client,
) (*distsqlutils.RowBuffer, error) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DB:          kvDB,
			DiskMonitor: testDiskMonitor,
		},
		EvalCtx: &evalCtx,
	}

	out := &distsqlutils.RowBuffer{}
	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	spec.StreamAddress = streamingccl.StreamAddress(streamAddr)

	spec.PartitionAddresses = []streamingccl.PartitionAddress{"partition1", "partition2"}
	spec.StartTime = startTime
	processorID := int32(0)
	proc, err := newStreamIngestionDataProcessor(&flowCtx, processorID, spec, &post, out)
	require.NoError(t, err)
	sip, ok := proc.(*streamIngestionProcessor)
	if !ok {
		t.Fatal("expected the processor that's created to be a split and scatter processor")
	}

	if mockClient != nil {
		sip.client = mockClient
	}

	if interceptableClient, ok := sip.client.(interceptableStreamClient); ok {
		interceptableClient.RegisterInterception(interceptEvents)
		// TODO: Inject an interceptor here that keeps track of generated events so
		// we can compare.
	} else if interceptEvents != nil {
		t.Fatalf("interceptor specified, but client %T does not implement interceptableStreamClient",
			sip.client)
	}

	sip.Run(ctx)

	// Ensure that all the outputs are properly closed.
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	return out, err
}

// makeCheckpointEventCounter runs f after seeing `threshold` number of
// checkpoint events.
func makeCheckpointEventCounter(threshold int, f func()) func(streamingccl.Event) {
	numCheckpointEventsGenerated := 0
	return func(event streamingccl.Event) {
		switch event.Type() {
		case streamingccl.CheckpointEvent:
			numCheckpointEventsGenerated++
			if numCheckpointEventsGenerated > threshold {
				f()
			}
		}
	}
}
