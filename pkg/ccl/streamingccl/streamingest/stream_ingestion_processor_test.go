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
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/streaming"
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
	partitionEvents map[string][]streamingccl.Event
}

var _ streamclient.Client = &mockStreamClient{}

// Create implements the Client interface.
func (m *mockStreamClient) Create(
	ctx context.Context, target roachpb.TenantID,
) (streaming.StreamID, error) {
	panic("unimplemented")
}

// Heartbeat implements the Client interface.
func (m *mockStreamClient) Heartbeat(
	ctx context.Context, ID streaming.StreamID, _ hlc.Timestamp,
) error {
	panic("unimplemented")
}

// Plan implements the Client interface.
func (m *mockStreamClient) Plan(
	ctx context.Context, _ streaming.StreamID,
) (streamclient.Topology, error) {
	panic("unimplemented mock method")
}

type mockSubscription struct {
	eventsCh chan streamingccl.Event
}

// Subscribe implements the Subscription interface.
func (m *mockSubscription) Subscribe(ctx context.Context) error {
	return nil
}

// Events implements the Subscription interface.
func (m *mockSubscription) Events() <-chan streamingccl.Event {
	return m.eventsCh
}

// Err implements the Subscription interface.
func (m *mockSubscription) Err() error {
	return nil
}

// Subscribe implements the Client interface.
func (m *mockStreamClient) Subscribe(
	ctx context.Context,
	stream streaming.StreamID,
	spec streamclient.SubscriptionToken,
	checkpoint hlc.Timestamp,
) (streamclient.Subscription, error) {
	var events []streamingccl.Event
	var ok bool
	if events, ok = m.partitionEvents[string(spec)]; !ok {
		return nil, errors.Newf("no events found for paritition %s", string(spec))
	}

	log.Infof(ctx, "%q emitting %d events", string(spec), len(events))
	eventCh := make(chan streamingccl.Event, len(events))
	for _, event := range events {
		log.Infof(ctx, "%q emitting event %v", string(spec), event)
		eventCh <- event
	}
	log.Infof(ctx, "%q done emitting %d events", string(spec), len(events))
	close(eventCh)
	return &mockSubscription{eventsCh: eventCh}, nil
}

// Close implements the Client interface.
func (m *mockStreamClient) Close() error {
	return nil
}

// Complete implements the streamclient.Client interface.
func (m *mockStreamClient) Complete(ctx context.Context, streamID streaming.StreamID) error {
	return nil
}

// errorStreamClient always returns an error when consuming a partition.
type errorStreamClient struct{ mockStreamClient }

var _ streamclient.Client = &errorStreamClient{}

// ConsumePartition implements the streamclient.Client interface.
func (m *errorStreamClient) Subscribe(
	ctx context.Context,
	stream streaming.StreamID,
	spec streamclient.SubscriptionToken,
	checkpoint hlc.Timestamp,
) (streamclient.Subscription, error) {
	return nil, errors.New("this client always returns an error")
}

// Complete implements the streamclient.Client interface.
func (m *errorStreamClient) Complete(ctx context.Context, streamID streaming.StreamID) error {
	return nil
}

func TestStreamIngestionProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	const tenantID = 20
	tenantRekey := execinfrapb.TenantRekey{
		OldID: roachpb.MakeTenantID(tenantID),
		NewID: roachpb.MakeTenantID(tenantID + 10),
	}

	t.Run("finite stream client", func(t *testing.T) {
		v := roachpb.MakeValueFromString("value_1")
		v.Timestamp = hlc.Timestamp{WallTime: 1}
		sampleKV := func() roachpb.KeyValue {
			key, err := keys.RewriteKeyToTenantPrefix(roachpb.Key("key_1"),
				keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID)))
			require.NoError(t, err)
			return roachpb.KeyValue{Key: key, Value: v}
		}
		//sampleKV := roachpb.KeyValue{Key: roachpb.Key("key_1"), Value: v}
		events := func() []streamingccl.Event {
			return []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
			}
		}
		p1 := streamclient.SubscriptionToken("p1")
		p2 := streamclient.SubscriptionToken("p2")
		mockClient := &mockStreamClient{
			partitionEvents: map[string][]streamingccl.Event{string(p1): events(), string(p2): events()},
		}

		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1},
			{ID: "2", SubscriptionToken: p2},
		}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB, "randomgen://test/",
			partitions, startTime, nil /* interceptEvents */, tenantRekey, mockClient)
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
		require.Contains(t, actualRows, "1{-\\x00} 0.000000004,0",
			"partition 1 should advance to timestamp 4")
		require.Contains(t, actualRows, "2{-\\x00} 0.000000004,0",
			"partition 2 should advance to timestamp 4")
	})

	t.Run("error stream client", func(t *testing.T) {
		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		partitions := []streamclient.PartitionInfo{
			{SubscriptionToken: streamclient.SubscriptionToken("1")},
			{SubscriptionToken: streamclient.SubscriptionToken("2")},
		}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB, "randomgen://test",
			partitions, startTime, nil /* interceptEvents */, tenantRekey, &errorStreamClient{})
		require.NoError(t, err)

		// Expect no rows, and just the error.
		row, meta := out.Next()
		require.Nil(t, row)
		testutils.IsError(meta.Err, "this client always returns an error")
	})

	t.Run("stream ingestion processor shuts down gracefully on losing client connection", func(t *testing.T) {
		events := []streamingccl.Event{streamingccl.MakeGenerationEvent()}
		mockClient := &mockStreamClient{
			partitionEvents: map[string][]streamingccl.Event{"foo": events},
		}

		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		partitions := []streamclient.PartitionInfo{{SubscriptionToken: streamclient.SubscriptionToken("foo")}}

		processEventCh := make(chan struct{})
		defer close(processEventCh)
		streamingTestingKnob := &sql.StreamingTestingKnobs{RunAfterReceivingEvent: func(ctx context.Context) {
			processEventCh <- struct{}{}
		}}
		sip, out, err := getStreamIngestionProcessor(ctx, t, registry, kvDB, "randomgen://test/",
			partitions, startTime, nil /* interceptEvents */, tenantRekey, mockClient, streamingTestingKnob)
		defer func() {
			require.NoError(t, sip.forceClientForTests.Close())
		}()
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			sip.Run(ctx)
		}()

		// The channel will block on read if the event has not been intercepted yet.
		// Once it unblocks, we are guaranteed that the mockClient has sent the
		// GenerationEvent and the processor has read it.
		<-processEventCh

		// The sip processor has received a GenerationEvent and is thus
		// waiting for a cutover signal, so let's send one!
		sip.cutoverCh <- struct{}{}

		wg.Wait()
		// Ensure that all the outputs are properly closed.
		if !out.ProducerClosed() {
			t.Fatalf("output RowReceiver not closed")
		}

		for {
			// No metadata should have been produced since the processor
			// should have been moved to draining state with a nil error.
			row := out.NextNoMeta(t)
			if row == nil {
				break
			}
			t.Fatalf("more output rows than expected")
		}
	})
}

func getPartitionSpanToTableID(
	t *testing.T, partitions []streamclient.PartitionInfo,
) map[string]int {
	pSpanToTableID := make(map[string]int)

	// Aggregate the table IDs which should have been ingested.
	for _, pa := range partitions {
		pKey := roachpb.Key(pa.SubscriptionToken)
		pSpan := roachpb.Span{Key: pKey, EndKey: pKey.Next()}
		paURL, err := url.Parse(string(pa.SubscriptionToken))
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
	valueRange, kvsPerResolved, numPartitions int, kvFrequency time.Duration, dupProbability float64,
) string {
	return streamclient.RandomGenScheme + ":///" + "?VALUE_RANGE=" + strconv.Itoa(valueRange) +
		"&EVENT_FREQUENCY=" + strconv.Itoa(int(kvFrequency)) +
		"&KVS_PER_CHECKPOINT=" + strconv.Itoa(kvsPerResolved) +
		"&NUM_PARTITIONS=" + strconv.Itoa(numPartitions) +
		"&DUP_PROBABILITY=" + strconv.FormatFloat(dupProbability, 'f', -1, 32)
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
	streamAddr := getTestRandomClientURI()

	// The random client returns system and table data partitions.
	streamClient, err := streamclient.NewStreamClient(streamingccl.StreamAddress(streamAddr))
	require.NoError(t, err)
	const tenantID = 20
	id, err := streamClient.Create(ctx, roachpb.MakeTenantID(tenantID))
	require.NoError(t, err)

	topo, err := streamClient.Plan(ctx, id)
	require.NoError(t, err)
	// One system and two table data partitions.
	require.Equal(t, 2 /* numPartitions */, len(topo))

	startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	ctx, cancel := context.WithCancel(ctx)
	// Cancel the flow after emitting 1000 checkpoint events from the client.
	mu := syncutil.Mutex{}
	cancelAfterCheckpoints := makeCheckpointEventCounter(&mu, 1000, cancel)
	tenantRekey := execinfrapb.TenantRekey{
		OldID: roachpb.MakeTenantID(tenantID),
		NewID: roachpb.MakeTenantID(tenantID + 10),
	}
	rekeyer, err := backupccl.MakeKeyRewriterFromRekeys(keys.MakeSQLCodec(roachpb.MakeTenantID(tenantID)),
		nil /* tableRekeys */, []execinfrapb.TenantRekey{tenantRekey}, true /* restoreTenantFromStream */)
	require.NoError(t, err)
	streamValidator := newStreamClientValidator(rekeyer)
	validator := registerValidatorWithClient(streamValidator)
	out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB, streamAddr, topo,
		startTime, []streamclient.InterceptFn{cancelAfterCheckpoints, validator}, tenantRekey, nil /* mockClient */)
	require.NoError(t, err)

	partitionSpanToTableID := getPartitionSpanToTableID(t, topo)
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
					" addresses %v", resolvedSpan.Span, topo)
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
	partitions streamclient.Topology,
	startTime hlc.Timestamp,
	interceptEvents []streamclient.InterceptFn,
	tenantRekey execinfrapb.TenantRekey,
	mockClient streamclient.Client,
) (*distsqlutils.RowBuffer, error) {
	sip, out, err := getStreamIngestionProcessor(ctx, t, registry, kvDB, streamAddr,
		partitions, startTime, interceptEvents, tenantRekey, mockClient, nil /* streamingTestingKnobs */)
	require.NoError(t, err)

	sip.Run(ctx)

	// Ensure that all the outputs are properly closed.
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	if err := sip.forceClientForTests.Close(); err != nil {
		return nil, err
	}
	return out, err
}

func getStreamIngestionProcessor(
	ctx context.Context,
	t *testing.T,
	registry *jobs.Registry,
	kvDB *kv.DB,
	streamAddr string,
	partitions streamclient.Topology,
	startTime hlc.Timestamp,
	interceptEvents []streamclient.InterceptFn,
	tenantRekey execinfrapb.TenantRekey,
	mockClient streamclient.Client,
	streamingTestingKnobs *sql.StreamingTestingKnobs,
) (*streamIngestionProcessor, *distsqlutils.RowBuffer, error) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:     st,
			DB:           kvDB,
			JobRegistry:  registry,
			TestingKnobs: execinfra.TestingKnobs{StreamingTestingKnobs: streamingTestingKnobs},
		},
		EvalCtx:     &evalCtx,
		DiskMonitor: testDiskMonitor,
	}

	out := &distsqlutils.RowBuffer{}
	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	spec.StreamAddress = streamAddr
	spec.TenantRekey = tenantRekey
	spec.PartitionIds = make([]string, len(partitions))
	spec.PartitionAddresses = make([]string, len(partitions))
	spec.PartitionSpecs = make([]string, len(partitions))
	for i, pa := range partitions {
		spec.PartitionIds[i] = pa.ID
		spec.PartitionAddresses[i] = string(pa.SrcAddr)
		spec.PartitionSpecs[i] = string(pa.SubscriptionToken)
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
		sip.forceClientForTests = mockClient
	}

	if interceptable, ok := sip.forceClientForTests.(streamclient.InterceptableStreamClient); ok {
		for _, interceptor := range interceptEvents {
			interceptable.RegisterInterception(interceptor)
		}
	}
	return sip, out, err
}

func registerValidatorWithClient(
	validator *streamClientValidator,
) func(event streamingccl.Event, spec streamclient.SubscriptionToken) {
	return func(event streamingccl.Event, spec streamclient.SubscriptionToken) {
		switch event.Type() {
		case streamingccl.CheckpointEvent:
			resolvedTS := *event.GetResolved()
			err := validator.noteResolved(string(spec), resolvedTS)
			if err != nil {
				panic(err.Error())
			}
		case streamingccl.KVEvent:
			kv := *event.GetKV()

			if validator.rekeyer != nil {
				rekey, _, err := validator.rekeyer.RewriteKey(kv.Key)
				if err != nil {
					panic(err.Error())
				}
				kv.Key = rekey
				kv.Value.ClearChecksum()
				kv.Value.InitChecksum(kv.Key)
			}
			err := validator.noteRow(string(spec), string(kv.Key), string(kv.Value.RawBytes),
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
) func(streamingccl.Event, streamclient.SubscriptionToken) {
	mu.Lock()
	defer mu.Unlock()
	numCheckpointEventsGenerated := 0
	return func(event streamingccl.Event, _ streamclient.SubscriptionToken) {
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
