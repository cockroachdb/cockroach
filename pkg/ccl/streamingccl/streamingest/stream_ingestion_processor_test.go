// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
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
	heartbeatErr    error
	heartbeatStatus streampb.StreamReplicationStatus
	onHeartbeat     func() (streampb.StreamReplicationStatus, error)
}

var _ streamclient.Client = &mockStreamClient{}

// Create implements the Client interface.
func (m *mockStreamClient) Create(
	_ context.Context, _ roachpb.TenantName, _ streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	panic("unimplemented")
}

// Dial implements the Client interface.
func (m *mockStreamClient) Dial(_ context.Context) error {
	panic("unimplemented")
}

// Heartbeat implements the Client interface.
func (m *mockStreamClient) Heartbeat(
	_ context.Context, _ streampb.StreamID, _ hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	if m.onHeartbeat != nil {
		return m.onHeartbeat()
	}
	return m.heartbeatStatus, m.heartbeatErr
}

// Plan implements the Client interface.
func (m *mockStreamClient) Plan(
	_ context.Context, _ streampb.StreamID,
) (streamclient.Topology, error) {
	panic("unimplemented mock method")
}

type mockSubscription struct {
	eventsCh chan streamingccl.Event
}

// Subscribe implements the Subscription interface.
func (m *mockSubscription) Subscribe(_ context.Context) error {
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
	_ streampb.StreamID,
	_ int32,
	token streamclient.SubscriptionToken,
	initialScanTime hlc.Timestamp,
	_ span.Frontier,
) (streamclient.Subscription, error) {
	var events []streamingccl.Event
	var ok bool
	if events, ok = m.partitionEvents[string(token)]; !ok {
		return nil, errors.Newf("no events found for partition %s", string(token))
	}
	log.Infof(ctx, "%q beginning subscription from time %v ", string(token), initialScanTime)

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
func (m *mockStreamClient) Close(_ context.Context) error {
	return nil
}

// Complete implements the streamclient.Client interface.
func (m *mockStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}

// PriorReplicationDetails implements the streamclient.Client interface.
func (m *mockStreamClient) PriorReplicationDetails(
	_ context.Context, _ roachpb.TenantName,
) (string, string, hlc.Timestamp, error) {
	return "", "", hlc.Timestamp{}, nil
}

// errorStreamClient always returns an error when consuming a partition.
type errorStreamClient struct{ mockStreamClient }

var _ streamclient.Client = &errorStreamClient{}

// ConsumePartition implements the streamclient.Client interface.
func (m *errorStreamClient) Subscribe(
	_ context.Context,
	_ streampb.StreamID,
	_ int32,
	_ streamclient.SubscriptionToken,
	_ hlc.Timestamp,
	_ span.Frontier,
) (streamclient.Subscription, error) {
	return nil, errors.New("this client always returns an error")
}

// Complete implements the streamclient.Client interface.
func (m *errorStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}

func TestStreamIngestionProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Perhaps it would be possible to make this
			// run in a secondary tenant, but the test
			// would need to be completely rewritten to be
			// even further from real-world operation.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)
	st := cluster.MakeTestingClusterSettings()
	quantize.Override(ctx, &st.SV, 0)

	db := tc.Server(0).InternalDB().(descs.DB)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)

	const tenantID = 20
	tenantRekey := execinfrapb.TenantRekey{
		OldID: roachpb.MustMakeTenantID(tenantID),
		NewID: roachpb.MustMakeTenantID(tenantID + 10),
	}

	p1 := streamclient.SubscriptionToken("p1")
	p2 := streamclient.SubscriptionToken("p2")
	p1Key := roachpb.Key("key_1")
	p1Span := roachpb.Span{Key: p1Key, EndKey: p1Key.Next()}
	p2Key := roachpb.Key("key_2")
	p2Span := roachpb.Span{Key: p2Key, EndKey: p2Key.Next()}

	sampleKV := func() []roachpb.KeyValue {
		key, err := keys.RewriteKeyToTenantPrefix(p1Key,
			keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenantID)))
		require.NoError(t, err)
		v := roachpb.MakeValueFromString("value_1")
		v.Timestamp = hlc.Timestamp{WallTime: 1}
		return []roachpb.KeyValue{{Key: key, Value: v}}
	}
	sampleCheckpoint := func(span roachpb.Span, ts int64) []jobspb.ResolvedSpan {
		return []jobspb.ResolvedSpan{{Span: span, Timestamp: hlc.Timestamp{WallTime: ts}}}
	}

	readRow := func(streamOut execinfra.RowSource) []string {
		row, meta := streamOut.Next()
		require.Nil(t, meta, "unexpected non-nil meta on stream ingestion processor output: %v", meta)
		if row == nil {
			return nil
		}
		datum := row[0].Datum
		protoBytes, ok := datum.(*tree.DBytes)
		require.True(t, ok)

		var resolvedSpans jobspb.ResolvedSpans
		require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &resolvedSpans))
		ret := make([]string, 0, len(resolvedSpans.ResolvedSpans))
		for _, resolvedSpan := range resolvedSpans.ResolvedSpans {
			ret = append(ret, fmt.Sprintf("%s %s", resolvedSpan.Span, resolvedSpan.Timestamp))
		}
		return ret
	}

	readRows := func(streamOut *distsqlutils.RowBuffer) map[string]struct{} {
		actualRows := make(map[string]struct{})
		for {
			resolved := readRow(streamOut)
			if resolved == nil {
				break
			}
			for _, resolvedSpan := range resolved {
				actualRows[resolvedSpan] = struct{}{}
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

		initialScanTimestamp := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
			{ID: "2", SubscriptionToken: p2, Spans: []roachpb.Span{p2Span}},
		}
		topology := streamclient.Topology{
			Partitions: partitions,
		}
		out, err := runStreamIngestionProcessor(ctx, t, registry, db,
			topology, initialScanTimestamp, []jobspb.ResolvedSpan{}, tenantRekey,
			mockClient, nil /* cutoverProvider */, nil /* streamingTestingKnobs */, st)
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
	t.Run("time-based-flush", func(t *testing.T) {
		events := func() []streamingccl.Event {
			return []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 2)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 4)),
			}
		}
		mockClient := &mockStreamClient{
			doneCh:          make(chan struct{}),
			partitionEvents: map[string][]streamingccl.Event{string(p1): events()},
		}

		initialScanTimestamp := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
		}
		topology := streamclient.Topology{
			Partitions: partitions,
		}

		g := ctxgroup.WithContext(ctx)
		sip, err := getStreamIngestionProcessor(ctx, t, registry, db,
			topology, initialScanTimestamp, []jobspb.ResolvedSpan{}, tenantRekey, mockClient,
			nil /* cutoverProvider */, nil /* streamingTestingKnobs */, st)

		require.NoError(t, err)
		minimumFlushInterval.Override(ctx, &st.SV, 5*time.Millisecond)
		quantize.Override(ctx, &st.SV, 0)
		out := &execinfra.RowChannel{}
		out.InitWithNumSenders(sip.OutputTypes(), 1)
		out.Start(ctx)
		g.Go(func() error {
			sip.Run(ctx, out)
			return sip.forceClientForTests.Close(ctx)
		})

		emittedRows := readRow(out)
		require.Equal(t, []string{"key_1{-\\x00} 0.000000002,0"}, emittedRows, "partition 1 should advance to timestamp 2")
		emittedRows = readRow(out)
		require.Equal(t, []string{"key_1{-\\x00} 0.000000004,0"}, emittedRows, "partition 1 should advance to timestamp 4")
		close(mockClient.doneCh)
		require.NoError(t, g.Wait())
	})
	t.Run("kv-size-based-flush", func(t *testing.T) {
		events := func() []streamingccl.Event {
			return []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 2)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 4)),
				streamingccl.MakeKVEvent(sampleKV()),
			}
		}
		mockClient := &mockStreamClient{
			doneCh:          make(chan struct{}),
			partitionEvents: map[string][]streamingccl.Event{string(p1): events()},
		}

		initialScanTimestamp := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
		}
		topology := streamclient.Topology{
			Partitions: partitions,
		}

		g := ctxgroup.WithContext(ctx)
		sip, err := getStreamIngestionProcessor(ctx, t, registry, db,
			topology, initialScanTimestamp, []jobspb.ResolvedSpan{}, tenantRekey, mockClient,
			nil /* cutoverProvider */, nil /* streamingTestingKnobs */, st)
		require.NoError(t, err)

		minimumFlushInterval.Override(ctx, &st.SV, 50*time.Minute)
		quantize.Override(ctx, &st.SV, 0)
		maxKVBufferSize.Override(ctx, &st.SV, 1)
		out := &execinfra.RowChannel{}
		out.InitWithNumSenders(sip.OutputTypes(), 1)
		out.Start(ctx)
		g.Go(func() error {
			sip.Run(ctx, out)
			return sip.forceClientForTests.Close(ctx)
		})

		emittedRows := readRow(out)
		require.Equal(t, []string{"key_1{-\\x00} 0.000000002,0"}, emittedRows, "partition 1 should advance to timestamp 2")
		emittedRows = readRow(out)
		require.Equal(t, []string{"key_1{-\\x00} 0.000000004,0"}, emittedRows, "partition 1 should advance to timestamp 4")
		close(mockClient.doneCh)
		require.NoError(t, g.Wait())
	})
	t.Run("range-kv-size-based-flush", func(t *testing.T) {
		key, err := keys.RewriteKeyToTenantPrefix(p1Key, keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenantID)))
		require.NoError(t, err)
		events := func() []streamingccl.Event {
			return []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 2)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(p1Span, 4)),
				streamingccl.MakeDeleteRangeEvent(kvpb.RangeFeedDeleteRange{
					Span:      roachpb.Span{Key: key, EndKey: key.Next()},
					Timestamp: hlc.Timestamp{WallTime: 5},
				}),
			}
		}
		mockClient := &mockStreamClient{
			doneCh:          make(chan struct{}),
			partitionEvents: map[string][]streamingccl.Event{string(p1): events()},
		}

		initialScanTimestamp := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
		}
		topology := streamclient.Topology{
			Partitions: partitions,
		}

		g := ctxgroup.WithContext(ctx)
		sip, err := getStreamIngestionProcessor(ctx, t, registry, db,
			topology, initialScanTimestamp, []jobspb.ResolvedSpan{}, tenantRekey, mockClient,
			nil /* cutoverProvider */, nil /* streamingTestingKnobs */, st)
		require.NoError(t, err)

		minimumFlushInterval.Override(ctx, &st.SV, 50*time.Minute)
		maxRangeKeyBufferSize.Override(ctx, &st.SV, 1)
		quantize.Override(ctx, &st.SV, 0)

		out := &execinfra.RowChannel{}
		out.InitWithNumSenders(sip.OutputTypes(), 1)
		out.Start(ctx)
		g.Go(func() error {
			sip.Run(ctx, out)
			return sip.forceClientForTests.Close(ctx)
		})

		emittedRows := readRow(out)
		require.Equal(t, []string{"key_1{-\\x00} 0.000000002,0"}, emittedRows, "partition 1 should advance to timestamp 2")
		emittedRows = readRow(out)
		require.Equal(t, []string{"key_1{-\\x00} 0.000000004,0"}, emittedRows, "partition 1 should advance to timestamp 4")
		close(mockClient.doneCh)
		require.NoError(t, g.Wait())
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

		initialScanTimestamp := hlc.Timestamp{WallTime: 1}
		partitions := []streamclient.PartitionInfo{
			{ID: "1", SubscriptionToken: p1, Spans: []roachpb.Span{p1Span}},
			{ID: "2", SubscriptionToken: p2, Spans: []roachpb.Span{p2Span}},
		}
		topology := streamclient.Topology{
			Partitions: partitions,
		}
		checkpoint := []jobspb.ResolvedSpan{
			{Span: p1Span, Timestamp: hlc.Timestamp{WallTime: 4}},
			{Span: p2Span, Timestamp: hlc.Timestamp{WallTime: 5}},
		}

		lastClientStart := make(map[string]hlc.Timestamp)
		streamingTestingKnobs := &sql.StreamingTestingKnobs{BeforeClientSubscribe: func(addr string, token string, clientStartTimes span.Frontier) {
			sp := p1Span
			if token == string(p2) {
				sp = p2Span
			}
			clientStartTimes.SpanEntries(sp, func(s roachpb.Span, t hlc.Timestamp) (done span.OpResult) {
				lastClientStart[token] = t
				return span.StopMatch
			})
		}}
		out, err := runStreamIngestionProcessor(ctx, t, registry, db,
			topology, initialScanTimestamp, checkpoint, tenantRekey, mockClient,
			nil /* cutoverProvider */, streamingTestingKnobs, st)
		require.NoError(t, err)

		emittedRows := readRows(out)

		// Only compare the latest advancement, since not all intermediary resolved
		// timestamps might be flushed (due to the minimum flush interval setting in
		// the ingestion processor).
		require.Contains(t, emittedRows, "key_1{-\\x00} 0.000000006,0",
			"partition 1 should advance to timestamp 6")
		require.Contains(t, emittedRows, "key_2{-\\x00} 0.000000005,0",
			"partition 2 should advance to timestamp 5")
		require.Equal(t, hlc.Timestamp{WallTime: 4}, lastClientStart[string(p1)])
		require.Equal(t, hlc.Timestamp{WallTime: 5}, lastClientStart[string(p2)])
	})

	t.Run("error stream client", func(t *testing.T) {
		initialScanTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		partitions := []streamclient.PartitionInfo{
			{SubscriptionToken: streamclient.SubscriptionToken("1")},
			{SubscriptionToken: streamclient.SubscriptionToken("2")},
		}
		topology := streamclient.Topology{
			Partitions: partitions,
		}
		out, err := runStreamIngestionProcessor(ctx, t, registry, db,
			topology, initialScanTimestamp, []jobspb.ResolvedSpan{}, tenantRekey, &errorStreamClient{},
			nil /* cutoverProvider */, nil /* streamingTestingKnobs */, st)
		require.NoError(t, err)

		// Expect no rows, and just the error.
		row, meta := out.Next()
		require.Nil(t, row)
		testutils.IsError(meta.Err, "this client always returns an error")
	})
}

func TestFrontierForSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		spanAB = roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
		spanCD = roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
		spanEF = roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}
		spanXZ = roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("z")}
	)

	t.Run("returns the lowest timestamp for the matched spans", func(t *testing.T) {
		f, err := span.MakeFrontier(spanAB, spanCD, spanEF)
		require.NoError(t, err)
		_, err = f.Forward(spanAB, hlc.Timestamp{WallTime: 1})
		require.NoError(t, err)
		_, err = f.Forward(spanCD, hlc.Timestamp{WallTime: 2})
		require.NoError(t, err)
		_, err = f.Forward(spanEF, hlc.Timestamp{WallTime: 3})
		require.NoError(t, err)
		require.Equal(t, hlc.Timestamp{WallTime: 1}, frontierForSpans(f, spanAB, spanCD, spanEF))
		require.Equal(t, hlc.Timestamp{WallTime: 2}, frontierForSpans(f, spanCD, spanEF))
		require.Equal(t, hlc.Timestamp{WallTime: 3}, frontierForSpans(f, spanEF))
		require.Equal(t, hlc.Timestamp{WallTime: 1}, frontierForSpans(f, spanAB, spanEF))
	})
	t.Run("returns zero if none of the spans overlap", func(t *testing.T) {
		f, err := span.MakeFrontierAt(hlc.Timestamp{WallTime: 1}, spanAB, spanCD, spanEF)
		require.NoError(t, err)
		require.Equal(t, hlc.Timestamp{}, frontierForSpans(f, spanXZ))
	})
	t.Run("returns zero if one of the spans is zero", func(t *testing.T) {
		f, err := span.MakeFrontier(spanAB, spanCD, spanEF)
		require.NoError(t, err)
		_, err = f.Forward(spanAB, hlc.Timestamp{WallTime: 1})
		require.NoError(t, err)
		// spanCD should still be at zero
		_, err = f.Forward(spanEF, hlc.Timestamp{WallTime: 3})
		require.NoError(t, err)
		require.Equal(t, hlc.Timestamp{}, frontierForSpans(f, spanAB, spanCD, spanEF))
	})
}

// getPartitionSpanToTableID maps a partiton's span to the tableID it covers in
// the source keyspace. It assumes the source used a random_stream_client, which generates keys for
// a single table span per partition.
func getPartitionSpanToTableID(
	t *testing.T, partitions []streamclient.PartitionInfo,
) map[string]int {
	pSpanToTableID := make(map[string]int)

	// Aggregate the table IDs which should have been ingested.
	for _, pa := range partitions {
		require.Equal(t, 1, len(pa.Spans), "unexpected number of spans in the partition")
		pSpan := pa.Spans[0]
		paURL, err := url.Parse(string(pa.SubscriptionToken))
		require.NoError(t, err)
		id, err := strconv.Atoi(paURL.Host)
		require.NoError(t, err)
		pSpanToTableID[pSpan.String()] = id
		t.Logf("Partition Span %s; Partition ID %d", pSpan.String(), id)
	}
	return pSpanToTableID
}

// assertEqualKVs iterates over the store in `tc` and compares the MVCC KVs
// against the in-memory copy of events stored in the `streamValidator`. This
// ensures that the stream ingestion processor ingested at least as much data as
// was streamed up until partitionTimestamp. The function returns true if it
// validated at least one streamed kv.
func assertEqualKVs(
	t *testing.T,
	srv serverutils.TestServerInterface,
	streamValidator *streamClientValidator,
	targetSpan roachpb.Span,
	partitionTimestamp hlc.Timestamp,
) (foundKVs bool) {
	t.Logf("target span %s; partition ts %s", targetSpan, partitionTimestamp)
	if partitionTimestamp.WallTime == 0 {
		// Implies this span never got a checkpoint
		return foundKVs
	}
	// Iterate over the store.
	store, err := srv.StorageLayer().GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
	require.NoError(t, err)
	it, err := store.TODOEngine().NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: targetSpan.Key,
		UpperBound: targetSpan.EndKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var prevKey roachpb.Key
	var valueTimestampTuples []roachpb.KeyValue
	for it.SeekGE(storage.MVCCKey{Key: targetSpan.Key}); ; it.Next() {
		if ok, err := it.Valid(); !ok {
			if err != nil {
				t.Fatal(err)
			}
			break
		}

		// We only want to process MVCC KVs with a ts less than or equal to the max
		// resolved ts for this partition.
		if partitionTimestamp.Less(it.UnsafeKey().Timestamp) {
			continue
		}
		foundKVs = true
		newKey := (prevKey != nil && !it.UnsafeKey().Key.Equal(prevKey)) || prevKey == nil
		prevKey = it.UnsafeKey().Clone().Key
		descriptiveErrorMsg := fmt.Sprintf("Key %s; Ts %s: Is new Key %t; Partition Ts %s", it.UnsafeKey().Key, it.UnsafeKey().Timestamp, newKey, partitionTimestamp)
		if newKey {
			// All value ts should have been drained at this point, otherwise there is
			// a mismatch between the streamed and ingested data.
			require.Equal(t, 0, len(valueTimestampTuples))
			valueTimestampTuples, err = streamValidator.getValuesForKeyBelowTimestamp(
				string(it.UnsafeKey().Key), partitionTimestamp)
			require.NoError(t, err, descriptiveErrorMsg)
		}
		// Implies there exists a key in the store that was not logged by the stream validator.
		require.Greater(t, len(valueTimestampTuples), 0, descriptiveErrorMsg)
		// Since the iterator goes from latest to older versions, we compare
		// starting from the end of the slice that is sorted by timestamp.
		latestVersionInChain := valueTimestampTuples[len(valueTimestampTuples)-1]
		v, err := it.Value()
		require.NoError(t, err)
		require.Equal(t, roachpb.KeyValue{
			Key: it.UnsafeKey().Key,
			Value: roachpb.Value{
				RawBytes:  v,
				Timestamp: it.UnsafeKey().Timestamp,
			},
		}, latestVersionInChain)
		// Truncate the latest version which we just checked against in preparation
		// for the next iteration.
		valueTimestampTuples = valueTimestampTuples[0 : len(valueTimestampTuples)-1]
	}
	return foundKVs
}

func makeTestStreamURI(
	valueRange, kvsPerResolved, numPartitions int,
	kvFrequency time.Duration,
	dupProbability float64,
	tenantID roachpb.TenantID,
	tenantName roachpb.TenantName,
) string {
	return streamclient.RandomGenScheme + ":///" + "?VALUE_RANGE=" + strconv.Itoa(valueRange) +
		"&EVENT_FREQUENCY=" + strconv.Itoa(int(kvFrequency)) +
		"&KVS_PER_CHECKPOINT=" + strconv.Itoa(kvsPerResolved) +
		"&NUM_PARTITIONS=" + strconv.Itoa(numPartitions) +
		"&DUP_PROBABILITY=" + strconv.FormatFloat(dupProbability, 'f', -1, 32) +
		"&TENANT_ID=" + strconv.Itoa(int(tenantID.ToUint64())) +
		"&TENANT_NAME=" + string(tenantName)
}

type noCutover struct{}

func (n noCutover) cutoverReached(context.Context) (bool, error) { return false, nil }

// TestRandomClientGeneration tests the ingestion processor against a random
// stream workload.
func TestRandomClientGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	quantize.Override(ctx, &srv.SystemLayer().ClusterSettings().SV, 0)
	ts := srv.SystemLayer()

	registry := ts.JobRegistry().(*jobs.Registry)

	// TODO: Consider testing variations on these parameters.
	tenantID := roachpb.MustMakeTenantID(20)
	tenantName := roachpb.TenantName("20")
	streamAddr := getTestRandomClientURI(tenantID, tenantName)

	// The random client returns system and table data partitions.
	streamClient, err := streamclient.NewStreamClient(ctx, streamingccl.StreamAddress(streamAddr), nil)
	require.NoError(t, err)

	randomStreamClient, ok := streamClient.(*streamclient.RandomStreamClient)
	require.True(t, ok)
	rps, err := randomStreamClient.Create(ctx, tenantName, streampb.ReplicationProducerRequest{})
	require.NoError(t, err)

	topo, err := randomStreamClient.Plan(ctx, rps.StreamID)
	require.NoError(t, err)
	require.Equal(t, 2 /* numPartitions */, len(topo.Partitions))

	initialScanTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	ctx, cancel := context.WithCancel(ctx)
	// Cancel the flow after emitting 1000 checkpoint events from the client.
	mu := syncutil.Mutex{}
	cancelAfterCheckpoints := makeCheckpointEventCounter(&mu, 1000, cancel)

	tenantRekey := execinfrapb.TenantRekey{
		OldID: tenantID,
		NewID: roachpb.MustMakeTenantID(tenantID.ToUint64() + 10),
	}
	rekeyer, err := backupccl.MakeKeyRewriterFromRekeys(keys.MakeSQLCodec(tenantID),
		nil /* tableRekeys */, []execinfrapb.TenantRekey{tenantRekey}, true /* restoreTenantFromStream */)
	require.NoError(t, err)
	streamValidator := newStreamClientValidator(rekeyer)

	randomStreamClient.ClearInterceptors()
	randomStreamClient.RegisterSSTableGenerator(func(keyValues []roachpb.KeyValue) kvpb.RangeFeedSSTable {
		return replicationtestutils.SSTMaker(t, keyValues)
	})
	randomStreamClient.RegisterInterception(cancelAfterCheckpoints)
	randomStreamClient.RegisterInterception(validateFnWithValidator(t, streamValidator))

	st := cluster.MakeTestingClusterSettings()
	quantize.Override(ctx, &st.SV, 0)
	out, err := runStreamIngestionProcessor(ctx, t, registry, ts.InternalDB().(descs.DB),
		topo, initialScanTimestamp, []jobspb.ResolvedSpan{}, tenantRekey,
		randomStreamClient, noCutover{}, nil /* streamingTestingKnobs*/, st)
	require.NoError(t, err)

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

			// Track the max resolved timestamp per partition. Note that resolved
			// spans are mapped to the source tenant keyspace.
			if maxResolvedTimestampPerPartition[resolvedSpan.Span.String()].Less(resolvedSpan.Timestamp) {
				maxResolvedTimestampPerPartition[resolvedSpan.Span.String()] = resolvedSpan.Timestamp
			}
			numResolvedEvents++
		}

		// We must have at least some progress across the frontier
		require.Greater(t, latestResolvedTimestamp.WallTime, initialScanTimestamp.WallTime)
	}

	// Ensure that no errors were reported to the validator.
	for _, failure := range streamValidator.failures() {
		t.Error(failure)
	}
	foundKVs := false
	ingestionCodec := keys.MakeSQLCodec(tenantRekey.NewID)
	for pSpan, id := range getPartitionSpanToTableID(t, topo.Partitions) {
		// Scan the store for KVs ingested by this partition, and compare the MVCC
		// KVs against the KVEvents streamed up to the max ingested timestamp for
		// the partition.
		//
		// Note that target span must be rekeyed to the destination
		// tenant key space.
		startKey := ingestionCodec.TablePrefix(uint32(id))
		targetSpan := roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()}
		if assertEqualKVs(t, srv, streamValidator, targetSpan,
			maxResolvedTimestampPerPartition[pSpan]) {
			foundKVs = true
		}
	}
	// Note: we only assert that KVs were found over all partitions instead of in
	// each partition because it is possible for a partition to not send any
	// checkpoint events. This stream ingestion processor only terminates once a
	// total number of checkpoints have been reached and makes no guarantees that
	// each partition gets a checkpoint.
	require.True(t, foundKVs, "expected to find and assert equal kvs")
	require.Greater(t, numResolvedEvents, 0, "at least 1 resolved event expected")
}

func runStreamIngestionProcessor(
	ctx context.Context,
	t *testing.T,
	registry *jobs.Registry,
	db descs.DB,
	partitions streamclient.Topology,
	initialScanTimestamp hlc.Timestamp,
	checkpoint []jobspb.ResolvedSpan,
	tenantRekey execinfrapb.TenantRekey,
	mockClient streamclient.Client,
	cutoverProvider cutoverProvider,
	streamingTestingKnobs *sql.StreamingTestingKnobs,
	st *cluster.Settings,
) (*distsqlutils.RowBuffer, error) {
	sip, err := getStreamIngestionProcessor(ctx, t, registry, db,
		partitions, initialScanTimestamp, checkpoint, tenantRekey, mockClient, cutoverProvider, streamingTestingKnobs, st)
	require.NoError(t, err)

	out := &distsqlutils.RowBuffer{}
	sip.Run(ctx, out)

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
	db descs.DB,
	partitions streamclient.Topology,
	initialScanTimestamp hlc.Timestamp,
	checkpoint []jobspb.ResolvedSpan,
	tenantRekey execinfrapb.TenantRekey,
	mockClient streamclient.Client,
	cutoverProvider cutoverProvider,
	streamingTestingKnobs *sql.StreamingTestingKnobs,
	st *cluster.Settings,
) (*streamIngestionProcessor, error) {
	evalCtx := eval.MakeTestingEvalContext(st)
	if mockClient == nil {
		return nil, errors.AssertionFailedf("non-nil streamclient required")
	}

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:          st,
			DB:                db,
			JobRegistry:       registry,
			TestingKnobs:      execinfra.TestingKnobs{StreamingTestingKnobs: streamingTestingKnobs},
			BulkSenderLimiter: limit.MakeConcurrentRequestLimiter("test", math.MaxInt),
		},
		NodeID:      base.TestingIDContainer,
		EvalCtx:     &evalCtx,
		Mon:         evalCtx.TestingMon,
		DiskMonitor: testDiskMonitor,
	}

	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	spec.StreamAddress = "http://unused"
	spec.TenantRekey = tenantRekey
	spec.PartitionSpecs = make(map[string]execinfrapb.StreamIngestionPartitionSpec)
	for _, pa := range partitions.Partitions {
		spec.PartitionSpecs[pa.ID] = execinfrapb.StreamIngestionPartitionSpec{
			PartitionID:       pa.ID,
			Address:           string(pa.SrcAddr),
			SubscriptionToken: string(pa.SubscriptionToken),
			Spans:             pa.Spans,
		}
	}
	spec.InitialScanTimestamp = initialScanTimestamp
	spec.Checkpoint.ResolvedSpans = checkpoint
	processorID := int32(0)
	proc, err := newStreamIngestionDataProcessor(ctx, &flowCtx, processorID, spec, &post)
	require.NoError(t, err)
	sip, ok := proc.(*streamIngestionProcessor)
	if !ok {
		t.Fatal("expected the processor that's created to be a split and scatter processor")
	}

	sip.forceClientForTests = mockClient
	if cutoverProvider != nil {
		sip.cutoverProvider = cutoverProvider
	}

	return sip, err
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

func noteKeyVal(
	validator *streamClientValidator, keyVals []roachpb.KeyValue, spec streamclient.SubscriptionToken,
) {
	for _, keyVal := range keyVals {
		if validator.rekeyer != nil {
			rekey, _, err := validator.rekeyer.RewriteTenant(keyVal.Key)
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

func validateFnWithValidator(
	t *testing.T, validator *streamClientValidator,
) func(event streamingccl.Event, spec streamclient.SubscriptionToken) {
	return func(event streamingccl.Event, spec streamclient.SubscriptionToken) {
		switch event.Type() {
		case streamingccl.CheckpointEvent:
			resolvedTS := resolvedSpansMinTS(event.GetResolvedSpans())
			err := validator.noteResolved(string(spec), resolvedTS)
			if err != nil {
				panic(err.Error())
			}
		case streamingccl.SSTableEvent:
			kvs := storageutils.ScanSST(t, event.GetSSTable().Data)
			for _, keyVal := range kvs.MVCCKeyValues() {
				noteKeyVal(validator, []roachpb.KeyValue{{
					Key: keyVal.Key.Key,
					Value: roachpb.Value{
						RawBytes:  keyVal.Value,
						Timestamp: keyVal.Key.Timestamp,
					},
				}}, spec)
			}
		case streamingccl.KVEvent:
			noteKeyVal(validator, event.GetKVs(), spec)
		case streamingccl.DeleteRangeEvent:
			panic(errors.New("unsupported event type"))
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
