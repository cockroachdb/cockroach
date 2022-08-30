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
	"math"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
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
	doneCh          chan struct{}
}

var _ streamclient.Client = &mockStreamClient{}

// Create implements the Client interface.
func (m *mockStreamClient) Create(
	ctx context.Context, target roachpb.TenantID,
) (streaming.StreamID, error) {
	panic("unimplemented")
}

// Dial implements the Client interface.
func (m *mockStreamClient) Dial(ctx context.Context) error {
	panic("unimplemented")
}

// Heartbeat implements the Client interface.
func (m *mockStreamClient) Heartbeat(
	ctx context.Context, ID streaming.StreamID, _ hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
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
	token streamclient.SubscriptionToken,
	startTime hlc.Timestamp,
) (streamclient.Subscription, error) {
	var events []streamingccl.Event
	var ok bool
	if events, ok = m.partitionEvents[string(token)]; !ok {
		return nil, errors.Newf("no events found for partition %s", string(token))
	}
	log.Infof(ctx, "%q beginning subscription from time %v ", string(token), startTime)

	log.Infof(ctx, "%q emitting %d events", string(token), len(events))
	eventCh := make(chan streamingccl.Event, len(events))
	for _, event := range events {
		log.Infof(ctx, "%q emitting event %v", string(token), event)
		eventCh <- event
	}
	log.Infof(ctx, "%q done emitting %d events", string(token), len(events))
	go func() {
		if m.doneCh != nil {
			log.Infof(ctx, "%q waiting for doneCh", string(token))
			<-m.doneCh
			log.Infof(ctx, "%q received event on doneCh", string(token))
		}
		close(eventCh)
	}()
	return &mockSubscription{eventsCh: eventCh}, nil
}

// Close implements the Client interface.
func (m *mockStreamClient) Close(ctx context.Context) error {
	return nil
}

// Complete implements the streamclient.Client interface.
func (m *mockStreamClient) Complete(
	ctx context.Context, streamID streaming.StreamID, successfulIngestion bool,
) error {
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
func (m *errorStreamClient) Complete(
	ctx context.Context, streamID streaming.StreamID, successfulIngestion bool,
) error {
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

	p1 := streamclient.SubscriptionToken("p1")
	p2 := streamclient.SubscriptionToken("p2")
	v := roachpb.MakeValueFromString("value_1")
	v.Timestamp = hlc.Timestamp{WallTime: 1}
	p1Key := roachpb.Key("key_1")
	p1Span := roachpb.Span{Key: p1Key, EndKey: p1Key.Next()}
	p2Key := roachpb.Key("key_2")
	p2Span := roachpb.Span{Key: p2Key, EndKey: p2Key.Next()}

	sampleKV := func() roachpb.KeyValue {
		key, err := keys.RewriteKeyToTenantPrefix(p1Key,
			keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID)))
		require.NoError(t, err)
		return roachpb.KeyValue{Key: key, Value: v}
	}
	sampleCheckpoint := func(span roachpb.Span, ts int64) []jobspb.ResolvedSpan {
		return []jobspb.ResolvedSpan{{Span: span, Timestamp: hlc.Timestamp{WallTime: ts}}}
	}

	readRows := func(streamOut *distsqlutils.RowBuffer) map[string]struct{} {
		actualRows := make(map[string]struct{})
		for {
			row := streamOut.NextNoMeta(t)
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
		return actualRows
	}

	t.Run("finite stream client", func(t *testing.T) {
		events := func() []streamingccl.Event {
			return []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 2)),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 4)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p2Span, 5)),
			}
		}
		mockClient := &mockStreamClient{
			partitionEvents: map[string][]streamingccl.Event{string(p1): events(), string(p2): events()},
		}

		startTime := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
			{ID: "2", SubscriptionToken: p2, Spans: []roachpb.Span{p2Span}},
		}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB,
			partitions, startTime, []jobspb.ResolvedSpan{}, nil /* interceptEvents */, tenantRekey, mockClient, nil /* cutoverProvider */, nil /* streamingTestingKnobs */)
		require.NoError(t, err)

		emittedRows := readRows(out)

		// Only compare the latest advancement, since not all intermediary resolved
		// timestamps might be flushed (due to the minimum flush interval setting in
		// the ingestion processor).
		require.Contains(t, emittedRows, "key_1{-\\x00} 0.000000004,0",
			"partition 1 should advance to timestamp 4")
		require.Contains(t, emittedRows, "key_2{-\\x00} 0.000000005,0",
			"partition 2 should advance to timestamp 5")
	})

	// Two partitions, checkpoint for each, client start time for each should match
	t.Run("resume from checkpoint", func(t *testing.T) {
		events := func() []streamingccl.Event {
			return []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p2Span, 2)),
				streamingccl.MakeKVEvent(sampleKV()),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 6)),
			}
		}
		mockClient := &mockStreamClient{
			partitionEvents: map[string][]streamingccl.Event{string(p1): events(), string(p2): events()},
		}

		startTime := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
			{ID: "2", SubscriptionToken: p2, Spans: []roachpb.Span{p2Span}},
		}
		checkpoint := []jobspb.ResolvedSpan{
			{Span: p1Span, Timestamp: hlc.Timestamp{WallTime: 4}},
			{Span: p2Span, Timestamp: hlc.Timestamp{WallTime: 5}},
		}

		lastClientStart := make(map[string]hlc.Timestamp)
		streamingTestingKnobs := &sql.StreamingTestingKnobs{BeforeClientSubscribe: func(addr string, token string, clientStartTime hlc.Timestamp) {
			lastClientStart[token] = clientStartTime
		}}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB,
			partitions, startTime, checkpoint, nil /* interceptEvents */, tenantRekey, mockClient, nil /* cutoverProvider */, streamingTestingKnobs)
		require.NoError(t, err)

		emittedRows := readRows(out)

		// Only compare the latest advancement, since not all intermediary resolved
		// timestamps might be flushed (due to the minimum flush interval setting in
		// the ingestion processor).
		require.Contains(t, emittedRows, "key_1{-\\x00} 0.000000004,0",
			"partition 1 should advance to timestamp 4")
		require.Contains(t, emittedRows, "key_2{-\\x00} 0.000000005,0",
			"partition 2 should advance to timestamp 5")
		require.Equal(t, lastClientStart[string(p1)], hlc.Timestamp{WallTime: 4})
		require.Equal(t, lastClientStart[string(p2)], hlc.Timestamp{WallTime: 5})
	})

	t.Run("error stream client", func(t *testing.T) {
		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		partitions := []streamclient.PartitionInfo{
			{SubscriptionToken: streamclient.SubscriptionToken("1")},
			{SubscriptionToken: streamclient.SubscriptionToken("2")},
		}
		out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB,
			partitions, startTime, []jobspb.ResolvedSpan{}, nil /* interceptEvents */, tenantRekey, &errorStreamClient{}, nil /* cutoverProvider */, nil /* streamingTestingKnobs */)
		require.NoError(t, err)

		// Expect no rows, and just the error.
		row, meta := out.Next()
		require.Nil(t, row)
		testutils.IsError(meta.Err, "this client always returns an error")
	})
}

func getPartitionSpanToTableID(
	t *testing.T, partitions []streamclient.PartitionInfo,
) map[string]int {
	pSpanToTableID := make(map[string]int)

	// Aggregate the table IDs which should have been ingested.
	for _, pa := range partitions {
		pKey := roachpb.Key(pa.ID)
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
	valueRange, kvsPerResolved, numPartitions int,
	kvFrequency time.Duration,
	dupProbability float64,
	tenantID int,
) string {
	return streamclient.RandomGenScheme + ":///" + "?VALUE_RANGE=" + strconv.Itoa(valueRange) +
		"&EVENT_FREQUENCY=" + strconv.Itoa(int(kvFrequency)) +
		"&KVS_PER_CHECKPOINT=" + strconv.Itoa(kvsPerResolved) +
		"&NUM_PARTITIONS=" + strconv.Itoa(numPartitions) +
		"&DUP_PROBABILITY=" + strconv.FormatFloat(dupProbability, 'f', -1, 32) +
		"&TENANT_ID=" + strconv.Itoa(tenantID)
}

type noCutover struct{}

func (n noCutover) cutoverReached(context.Context) (bool, error) { return false, nil }

// TestRandomClientGeneration tests the ingestion processor against a random
// stream workload.
func TestRandomClientGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	kvDB := tc.Server(0).DB()

	// TODO: Consider testing variations on these parameters.
	const tenantID = 20
	streamAddr := getTestRandomClientURI(tenantID)

	// The random client returns system and table data partitions.
	streamClient, err := streamclient.NewStreamClient(ctx, streamingccl.StreamAddress(streamAddr))
	require.NoError(t, err)
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

	out, err := runStreamIngestionProcessor(ctx, t, registry, kvDB,
		topo, startTime, []jobspb.ResolvedSpan{}, []streamclient.InterceptFn{cancelAfterCheckpoints, validator}, tenantRekey,
		streamClient, noCutover{}, nil /* streamingTestingKnobs*/)
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

		latestResolvedTimestamp := hlc.MinTimestamp
		for _, resolvedSpan := range resolvedSpans.ResolvedSpans {
			if latestResolvedTimestamp.Less(resolvedSpan.Timestamp) {
				latestResolvedTimestamp = resolvedSpan.Timestamp
			}

			// Track the max resolved timestamp per partition.
			if ts, ok := maxResolvedTimestampPerPartition[resolvedSpan.Span.String()]; !ok ||
				ts.Less(resolvedSpan.Timestamp) {
				maxResolvedTimestampPerPartition[resolvedSpan.Span.String()] = resolvedSpan.Timestamp
			}
			numResolvedEvents++
		}

		// We must have at least some progress across the frontier
		require.Greater(t, latestResolvedTimestamp.WallTime, startTime.WallTime)
	}

	// Ensure that no errors were reported to the validator.
	for _, failure := range streamValidator.failures() {
		t.Error(failure)
	}

	for pSpan, id := range partitionSpanToTableID {
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
	partitions streamclient.Topology,
	startTime hlc.Timestamp,
	checkpoint []jobspb.ResolvedSpan,
	interceptEvents []streamclient.InterceptFn,
	tenantRekey execinfrapb.TenantRekey,
	mockClient streamclient.Client,
	cutoverProvider cutoverProvider,
	streamingTestingKnobs *sql.StreamingTestingKnobs,
) (*distsqlutils.RowBuffer, error) {
	sip, out, err := getStreamIngestionProcessor(ctx, t, registry, kvDB,
		partitions, startTime, checkpoint, interceptEvents, tenantRekey, mockClient, cutoverProvider, streamingTestingKnobs)
	require.NoError(t, err)

	sip.Run(ctx)

	// Ensure that all the outputs are properly closed.
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}
	if err := sip.forceClientForTests.Close(ctx); err != nil {
		return nil, err
	}
	return out, err
}

func getStreamIngestionProcessor(
	ctx context.Context,
	t *testing.T,
	registry *jobs.Registry,
	kvDB *kv.DB,
	partitions streamclient.Topology,
	startTime hlc.Timestamp,
	checkpoint []jobspb.ResolvedSpan,
	interceptEvents []streamclient.InterceptFn,
	tenantRekey execinfrapb.TenantRekey,
	mockClient streamclient.Client,
	cutoverProvider cutoverProvider,
	streamingTestingKnobs *sql.StreamingTestingKnobs,
) (*streamIngestionProcessor, *distsqlutils.RowBuffer, error) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	if mockClient == nil {
		return nil, nil, errors.AssertionFailedf("non-nil streamclient required")
	}

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:          st,
			DB:                kvDB,
			JobRegistry:       registry,
			TestingKnobs:      execinfra.TestingKnobs{StreamingTestingKnobs: streamingTestingKnobs},
			BulkSenderLimiter: limit.MakeConcurrentRequestLimiter("test", math.MaxInt),
		},
		EvalCtx:     &evalCtx,
		DiskMonitor: testDiskMonitor,
	}

	out := &distsqlutils.RowBuffer{}
	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	spec.StreamAddress = "http://unused"
	spec.TenantRekey = tenantRekey
	spec.PartitionSpecs = make(map[string]execinfrapb.StreamIngestionPartitionSpec)
	for _, pa := range partitions {
		spec.PartitionSpecs[pa.ID] = execinfrapb.StreamIngestionPartitionSpec{
			PartitionID:       pa.ID,
			Address:           string(pa.SrcAddr),
			SubscriptionToken: string(pa.SubscriptionToken),
			Spans:             pa.Spans,
		}
	}
	spec.StartTime = startTime
	spec.Checkpoint.ResolvedSpans = checkpoint
	processorID := int32(0)
	proc, err := newStreamIngestionDataProcessor(&flowCtx, processorID, spec, &post, out)
	require.NoError(t, err)
	sip, ok := proc.(*streamIngestionProcessor)
	if !ok {
		t.Fatal("expected the processor that's created to be a split and scatter processor")
	}

	sip.forceClientForTests = mockClient
	if cutoverProvider != nil {
		sip.cutoverProvider = cutoverProvider
	}

	if interceptable, ok := sip.forceClientForTests.(streamclient.InterceptableStreamClient); ok {
		for _, interceptor := range interceptEvents {
			interceptable.RegisterInterception(interceptor)
		}
	}
	return sip, out, err
}

func resolvedSpansMinTS(resolvedSpans []jobspb.ResolvedSpan) hlc.Timestamp {
	minTS := hlc.MaxTimestamp
	for _, rs := range resolvedSpans {
		if rs.Timestamp.Less(minTS) {
			minTS = rs.Timestamp
		}
	}
	return minTS
}

func registerValidatorWithClient(
	validator *streamClientValidator,
) func(event streamingccl.Event, spec streamclient.SubscriptionToken) {
	return func(event streamingccl.Event, spec streamclient.SubscriptionToken) {
		switch event.Type() {
		case streamingccl.CheckpointEvent:
			resolvedTS := resolvedSpansMinTS(*event.GetResolvedSpans())
			err := validator.noteResolved(string(spec), resolvedTS)
			if err != nil {
				panic(err.Error())
			}
		case streamingccl.KVEvent:
			keyVal := *event.GetKV()
			if validator.rekeyer != nil {
				rekey, _, err := validator.rekeyer.RewriteKey(keyVal.Key)
				if err != nil {
					panic(err.Error())
				}
				keyVal.Key = rekey
				keyVal.Value.ClearChecksum()
				keyVal.Value.InitChecksum(keyVal.Key)
			}
			err := validator.noteRow(string(spec), string(keyVal.Key), string(keyVal.Value.RawBytes),
				keyVal.Value.Timestamp)
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
