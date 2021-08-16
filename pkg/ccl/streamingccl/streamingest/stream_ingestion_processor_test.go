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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockStreamClient will return the slice of events associated to the stream
// partition being consumed. Stream partitions are identified by unique
// partition addresses.
type mockStreamClient struct {
	partitionEvents map[streamingccl.PartitionAddress][]streamingccl.Event
}

var _ streamclient.Client = &mockStreamClient{}

// GetTopology implements the Client interface.
func (m *mockStreamClient) GetTopology(
	_ streamingccl.StreamAddress,
) (streamingccl.Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the Client interface.
func (m *mockStreamClient) ConsumePartition(
	_ context.Context, address streamingccl.PartitionAddress, _ hlc.Timestamp,
) (chan streamingccl.Event, chan error, error) {
	var events []streamingccl.Event
	var ok bool
	if events, ok = m.partitionEvents[address]; !ok {
		return nil, nil, errors.Newf("no events found for paritition %s", address)
	}

	eventCh := make(chan streamingccl.Event, len(events))

	for _, event := range events {
		eventCh <- event
	}
	close(eventCh)

	return eventCh, nil, nil
}

// errorStreamClient always returns an error when consuming a partition.
type errorStreamClient struct{}

var _ streamclient.Client = &errorStreamClient{}

// GetTopology implements the streamclient.Client interface.
func (m *errorStreamClient) GetTopology(
	_ streamingccl.StreamAddress,
) (streamingccl.Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the streamclient.Client interface.
func (m *errorStreamClient) ConsumePartition(
	_ context.Context, _ streamingccl.PartitionAddress, _ hlc.Timestamp,
) (chan streamingccl.Event, chan error, error) {
	return nil, nil, errors.New("this client always returns an error")
}

func TestStreamIngestionProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)

	t.Run("finite stream client", func(t *testing.T) {
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
		partitionAddresses := []streamingccl.PartitionAddress{"partition1", "partition2"}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB, "randomgen://test/",
			partitionAddresses, startTime, nil /* interceptEvents */, mockClient)
		require.NoError(t, err)

		actualRows := make(map[string]struct{})
		for {
			row := out.NextNoMeta(t)
			if row == nil {
				break
			}
			datum := row[0].Datum
			protoBytes, ok := datum.(*tree.DBytes)
			require.True(t, ok)

			var resolvedSpans jobspb.ResolvedSpans
			require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &resolvedSpans))
			for _, resolvedSpan := range resolvedSpans.ResolvedSpans {
				actualRows[fmt.Sprintf("%s %s", resolvedSpan.Span, resolvedSpan.Timestamp)] = struct{}{}
			}
		}

		// Only compare the latest advancement, since not all intermediary resolved
		// timestamps might be flushed (due to the minimum flush interval setting in
		// the ingestion processor).
		require.Contains(t, actualRows, "partition1{-\\x00} 0.000000004,0",
			"partition 1 should advance to timestamp 4")
		require.Contains(t, actualRows, "partition2{-\\x00} 0.000000004,0",
			"partition 2 should advance to timestamp 4")
	})

	t.Run("error stream client", func(t *testing.T) {
		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		partitionAddresses := []streamingccl.PartitionAddress{"partition1", "partition2"}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB, "randomgen://test",
			partitionAddresses, startTime, nil /* interceptEvents */, &errorStreamClient{})
		require.NoError(t, err)

		// Expect no rows, and just the error.
		row, meta := out.Next()
		require.Nil(t, row)
		testutils.IsError(meta.Err, "this client always returns an error")
	})
}

func getPartitionSpanToTableID(
	t *testing.T, partitionAddresses []streamingccl.PartitionAddress,
) map[string]int {
	pSpanToTableID := make(map[string]int)

	// Aggregate the table IDs which should have been ingested.
	for _, pa := range partitionAddresses {
		pKey := roachpb.Key(pa)
		pSpan := roachpb.Span{Key: pKey, EndKey: pKey.Next()}
		paURL, err := pa.URL()
		require.NoError(t, err)
		id, err := strconv.Atoi(paURL.Host)
		require.NoError(t, err)
		pSpanToTableID[pSpan.String()] = id
	}
	return pSpanToTableID
}

// assertEqualKVs iterates over the store in `tc` and compares the MVCC KVs
// against the in-memory copy of events stored in the `streamValidator`. This
// ensures that the stream ingestion processor ingested at least as much data as
// was streamed up until partitionTimestamp.
func assertEqualKVs(
	t *testing.T,
	tc *testcluster.TestCluster,
	streamValidator *streamClientValidator,
	tableID int,
	partitionTimestamp hlc.Timestamp,
) {
	key := keys.TODOSQLCodec.TablePrefix(uint32(tableID))

	// Iterate over the store.
	store := tc.GetFirstStoreFromServer(t, 0)
	it := store.Engine().NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: key,
		UpperBound: key.PrefixEnd(),
	})
	defer it.Close()
	var prevKey roachpb.Key
	var valueTimestampTuples []roachpb.KeyValue
	var err error
	for it.SeekGE(storage.MVCCKey{Key: key}); ; it.Next() {
		if ok, err := it.Valid(); !ok {
			if err != nil {
				t.Fatal(err)
			}
			break
		}

		// We only want to process MVCC KVs with a ts less than or equal to the max
		// resolved ts for this partition.
		if partitionTimestamp.Less(it.Key().Timestamp) {
			continue
		}

		newKey := (prevKey != nil && !it.Key().Key.Equal(prevKey)) || prevKey == nil
		prevKey = it.Key().Key

		if newKey {
			// All value ts should have been drained at this point, otherwise there is
			// a mismatch between the streamed and ingested data.
			require.Equal(t, 0, len(valueTimestampTuples))
			valueTimestampTuples, err = streamValidator.getValuesForKeyBelowTimestamp(
				string(it.Key().Key), partitionTimestamp)
			require.NoError(t, err)
		}

		require.Greater(t, len(valueTimestampTuples), 0)
		// Since the iterator goes from latest to older versions, we compare
		// starting from the end of the slice that is sorted by timestamp.
		latestVersionInChain := valueTimestampTuples[len(valueTimestampTuples)-1]
		require.Equal(t, roachpb.KeyValue{
			Key: it.Key().Key,
			Value: roachpb.Value{
				RawBytes:  it.Value(),
				Timestamp: it.Key().Timestamp,
			},
		}, latestVersionInChain)
		// Truncate the latest version which we just checked against in preparation
		// for the next iteration.
		valueTimestampTuples = valueTimestampTuples[0 : len(valueTimestampTuples)-1]
	}
}

func makeTestStreamURI(
	valueRange, kvsPerResolved, numPartitions, tenantID int,
	kvFrequency time.Duration,
	dupProbability float64,
) string {
	return streamclient.RandomGenScheme + ":///" + "?VALUE_RANGE=" + strconv.Itoa(valueRange) +
		"&EVENT_FREQUENCY=" + strconv.Itoa(int(kvFrequency)) +
		"&KVS_PER_CHECKPOINT=" + strconv.Itoa(kvsPerResolved) +
		"&NUM_PARTITIONS=" + strconv.Itoa(numPartitions) +
		"&DUP_PROBABILITY=" + strconv.FormatFloat(dupProbability, 'f', -1, 32) +
		"&TENANT_ID=" + strconv.Itoa(tenantID)
}

// TestRandomClientGeneration tests the ingestion processor against a random
// stream workload.
func TestRandomClientGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 61287, "flaky test")
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	kvDB := tc.Server(0).DB()
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// TODO: Consider testing variations on these parameters.
	streamAddr := getTestRandomClientURI(int(roachpb.SystemTenantID.ToUint64()))

	// The random client returns system and table data partitions.
	streamClient, err := streamclient.NewStreamClient(streamingccl.StreamAddress(streamAddr))
	require.NoError(t, err)
	topo, err := streamClient.GetTopology(streamingccl.StreamAddress(streamAddr))
	require.NoError(t, err)
	// One system and two table data partitions.
	require.Equal(t, 2 /* numPartitions */, len(topo.Partitions))

	startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	ctx, cancel := context.WithCancel(ctx)
	// Cancel the flow after emitting 1000 checkpoint events from the client.
	mu := syncutil.Mutex{}
	cancelAfterCheckpoints := makeCheckpointEventCounter(&mu, 1000, cancel)
	streamValidator := newStreamClientValidator()
	validator := registerValidatorWithClient(streamValidator)
	out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB, streamAddr, topo.Partitions,
		startTime, []streamclient.InterceptFn{cancelAfterCheckpoints, validator}, nil /* mockClient */)
	require.NoError(t, err)

	partitionSpanToTableID := getPartitionSpanToTableID(t, topo.Partitions)
	numResolvedEvents := 0
	maxResolvedTimestampPerPartition := make(map[string]hlc.Timestamp)
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

		var resolvedSpans jobspb.ResolvedSpans
		require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &resolvedSpans))

		for _, resolvedSpan := range resolvedSpans.ResolvedSpans {
			if _, ok := partitionSpanToTableID[resolvedSpan.Span.String()]; !ok {
				t.Fatalf("expected resolved span %v to be either in one of the supplied partition"+
					" addresses %v", resolvedSpan.Span, topo.Partitions)
			}

			// All resolved timestamp events should be greater than the start time.
			require.Greater(t, resolvedSpan.Timestamp.WallTime, startTime.WallTime)

			// Track the max resolved timestamp per partition.
			if ts, ok := maxResolvedTimestampPerPartition[resolvedSpan.Span.String()]; !ok ||
				ts.Less(resolvedSpan.Timestamp) {
				maxResolvedTimestampPerPartition[resolvedSpan.Span.String()] = resolvedSpan.Timestamp
			}
			numResolvedEvents++
		}
	}

	// Ensure that no errors were reported to the validator.
	for _, failure := range streamValidator.failures() {
		t.Error(failure)
	}

	for pSpan, id := range partitionSpanToTableID {
		numRows, err := strconv.Atoi(sqlDB.QueryStr(t, fmt.Sprintf(
			`SELECT count(*) FROM defaultdb.%s%d`, streamclient.IngestionTablePrefix, id))[0][0])
		require.NoError(t, err)
		require.Greater(t, numRows, 0, "at least 1 row ingested expected")

		// Scan the store for KVs ingested by this partition, and compare the MVCC
		// KVs against the KVEvents streamed up to the max ingested timestamp for
		// the partition.
		assertEqualKVs(t, tc, streamValidator, id, maxResolvedTimestampPerPartition[pSpan])
	}
	require.Greater(t, numResolvedEvents, 0, "at least 1 resolved event expected")
}

func runStreamIngestionProcessor(
	ctx context.Context,
	t *testing.T,
	registry *jobs.Registry,
	kvDB *kv.DB,
	streamAddr string,
	partitionAddresses []streamingccl.PartitionAddress,
	startTime hlc.Timestamp,
	interceptEvents []streamclient.InterceptFn,
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
			JobRegistry: registry,
		},
		EvalCtx:     &evalCtx,
		DiskMonitor: testDiskMonitor,
	}

	out := &distsqlutils.RowBuffer{}
	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	spec.StreamAddress = streamAddr

	spec.PartitionAddresses = make([]string, len(partitionAddresses))
	for i, pa := range partitionAddresses {
		spec.PartitionAddresses[i] = string(pa)
	}
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

	if interceptable, ok := sip.client.(streamclient.InterceptableStreamClient); ok {
		for _, interceptor := range interceptEvents {
			interceptable.RegisterInterception(interceptor)
		}
	}

	sip.Run(ctx)

	// Ensure that all the outputs are properly closed.
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	return out, err
}

func registerValidatorWithClient(
	validator *streamClientValidator,
) func(event streamingccl.Event, pa streamingccl.PartitionAddress) {
	return func(event streamingccl.Event, pa streamingccl.PartitionAddress) {
		switch event.Type() {
		case streamingccl.CheckpointEvent:
			resolvedTS := *event.GetResolved()
			err := validator.noteResolved(string(pa), resolvedTS)
			if err != nil {
				panic(err.Error())
			}
		case streamingccl.KVEvent:
			kv := *event.GetKV()

			err := validator.noteRow(string(pa), string(kv.Key), string(kv.Value.RawBytes),
				kv.Value.Timestamp)
			if err != nil {
				panic(err.Error())
			}
		}
	}
}

// makeCheckpointEventCounter runs f after seeing `threshold` number of
// checkpoint events.
func makeCheckpointEventCounter(
	mu *syncutil.Mutex, threshold int, f func(),
) func(streamingccl.Event, streamingccl.PartitionAddress) {
	mu.Lock()
	defer mu.Unlock()
	numCheckpointEventsGenerated := 0
	return func(event streamingccl.Event, _ streamingccl.PartitionAddress) {
		mu.Lock()
		defer mu.Unlock()
		switch event.Type() {
		case streamingccl.CheckpointEvent:
			numCheckpointEventsGenerated++
			if numCheckpointEventsGenerated == threshold {
				f()
			}
		}
	}
}
