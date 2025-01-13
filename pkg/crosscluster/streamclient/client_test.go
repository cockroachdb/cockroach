// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testStreamClient struct{}

var _ Client = testStreamClient{}

// Dial implements Client interface.
func (sc testStreamClient) Dial(_ context.Context) error {
	return nil
}

func (testStreamClient) PlanLogicalReplication(
	ctx context.Context, req streampb.LogicalReplicationPlanRequest,
) (LogicalReplicationPlan, error) {
	return LogicalReplicationPlan{}, errors.AssertionFailedf("unimplemented")
}

func (testStreamClient) CreateForTables(
	ctx context.Context, req *streampb.ReplicationProducerRequest,
) (*streampb.ReplicationProducerSpec, error) {
	return nil, errors.AssertionFailedf("unimplemented")
}

// CreateForTenant implements the Client interface.
func (sc testStreamClient) CreateForTenant(
	_ context.Context, _ roachpb.TenantName, _ streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	return streampb.ReplicationProducerSpec{
		StreamID:             streampb.StreamID(1),
		ReplicationStartTime: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
	}, nil
}

// PlanForPhysicalReplication implements the Client interface.
func (sc testStreamClient) PlanPhysicalReplication(
	_ context.Context, _ streampb.StreamID,
) (Topology, error) {
	return Topology{
		Partitions: []PartitionInfo{
			{
				ConnUri: MakeTestClusterUri(url.URL{
					Scheme: "test",
					Host:   "host1",
				}),
			},
			{
				ConnUri: MakeTestClusterUri(url.URL{
					Scheme: "test",
					Host:   "host2",
				}),
			},
		},
	}, nil
}

// Heartbeat implements the Client interface.
func (sc testStreamClient) Heartbeat(
	_ context.Context, _ streampb.StreamID, _ hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	return streampb.StreamReplicationStatus{}, nil
}

// Close implements the Client interface.
func (sc testStreamClient) Close(_ context.Context) error {
	return nil
}

// Subscribe implements the Client interface.
func (sc testStreamClient) Subscribe(
	_ context.Context,
	_ streampb.StreamID,
	_, _ int32,
	_ SubscriptionToken,
	_ hlc.Timestamp,
	_ span.Frontier,
	_ ...SubscribeOption,
) (Subscription, error) {
	sampleKV := roachpb.KeyValue{
		Key: []byte("key_1"),
		Value: roachpb.Value{
			RawBytes:  []byte("value_1"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}

	sampleResolvedSpan := jobspb.ResolvedSpan{
		Span:      roachpb.Span{Key: sampleKV.Key, EndKey: sampleKV.Key.Next()},
		Timestamp: hlc.Timestamp{WallTime: 100},
	}

	events := make(chan crosscluster.Event, 2)
	events <- crosscluster.MakeKVEventFromKVs([]roachpb.KeyValue{sampleKV})
	events <- crosscluster.MakeCheckpointEvent(&streampb.StreamEvent_StreamCheckpoint{
		ResolvedSpans: []jobspb.ResolvedSpan{sampleResolvedSpan},
	})
	close(events)

	return &testStreamSubscription{
		eventCh: events,
	}, nil
}

// Complete implements the Client interface.
func (sc testStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}

func (sc testStreamClient) ExecStatement(
	_ context.Context, _ string, _ string, _ ...interface{},
) error {
	return nil
}

// PriorReplicationDetails implements the streamclient.Client interface.
func (sc testStreamClient) PriorReplicationDetails(
	_ context.Context, _ roachpb.TenantName,
) (string, string, hlc.Timestamp, error) {
	return "", "", hlc.Timestamp{}, nil
}

type testStreamSubscription struct {
	eventCh chan crosscluster.Event
}

// Subscribe implements the Subscription interface.
func (t testStreamSubscription) Subscribe(_ context.Context) error {
	return nil
}

// Events implements the Subscription interface.
func (t testStreamSubscription) Events() <-chan crosscluster.Event {
	return t.eventCh
}

// Err implements the Subscription interface.
func (t testStreamSubscription) Err() error {
	return nil
}

func TestGetFirstActiveClientEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var streamAddresses []ClusterUri
	activeClient, err := GetFirstActiveClient(context.Background(), streamAddresses, nil)
	require.ErrorContains(t, err, "failed to connect, no connection uris")
	require.Nil(t, activeClient)

	activeSpanConfigClient, err := GetFirstActiveSpanConfigClient(context.Background(), streamAddresses, nil)
	require.ErrorContains(t, err, "failed to connect, no connection uris")
	require.Nil(t, activeSpanConfigClient)
}

func TestPlannedPartitionBackwardCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("old source, new client", func(t *testing.T) {
		partitionSpec := streampb.StreamPartitionSpec{
			Spans: []roachpb.Span{keys.MakeTenantSpan(serverutils.TestTenantID())},
			Config: streampb.StreamPartitionSpec_ExecutionConfig{
				MinCheckpointFrequency: 10 * time.Millisecond,
			},
		}
		encodedSpec, err := protoutil.Marshal(&partitionSpec)
		require.NoError(t, err)

		var sourcePartition = streampb.SourcePartition{}
		err = protoutil.Unmarshal(encodedSpec, &sourcePartition)
		require.NoError(t, err)
		require.Equal(t, partitionSpec.Spans, sourcePartition.Spans)
	})

	t.Run("new source, old client", func(t *testing.T) {
		var sourcePartition = streampb.SourcePartition{
			Spans: []roachpb.Span{keys.MakeTenantSpan(serverutils.TestTenantID())},
		}
		encodedSpec, err := protoutil.Marshal(&sourcePartition)
		require.NoError(t, err)

		partitionSpec := streampb.StreamPartitionSpec{}
		err = protoutil.Unmarshal(encodedSpec, &partitionSpec)
		require.NoError(t, err)
		require.Equal(t, sourcePartition.Spans, partitionSpec.Spans)
	})
}

// ExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func ExampleClient() {
	client := testStreamClient{}
	ctx := context.Background()
	defer func() {
		_ = client.Close(ctx)
	}()

	prs, err := client.CreateForTenant(ctx, "system", streampb.ReplicationProducerRequest{})
	if err != nil {
		panic(err)
	}
	id := prs.StreamID

	frontier, _ := span.MakeFrontier(roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax})
	ingested := span.MakeConcurrentFrontier(frontier)

	done := make(chan struct{})

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(func(ctx context.Context) error {
		ticker := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-done:
				return nil
			case <-ticker.C:
				ingested.Frontier()

				if _, err := client.Heartbeat(ctx, id, frontier.Frontier()); err != nil {
					return err
				}
			}
		}
	})

	grp.GoCtx(func(ctx context.Context) error {
		defer close(done)

		topology, err := client.PlanPhysicalReplication(ctx, id)
		if err != nil {
			panic(err)
		}

		for _, partition := range topology.Partitions {
			// TODO(dt): use Subscribe helper and partition.SrcAddr
			sub, err := client.Subscribe(ctx, id, 0, 0, partition.SubscriptionToken, hlc.Timestamp{}, ingested)
			if err != nil {
				panic(err)
			}

			// This example looks for the closing of the channel to terminate the test,
			// but an ingestion job should look for another event such as the user
			// cutting over to the new cluster to move to the next stage.
			for event := range sub.Events() {
				switch event.Type() {
				case crosscluster.KVEvent:
					kvs := event.GetKVs()
					for _, kv := range kvs {
						fmt.Printf("kv: %s->%s@%d\n", kv.KeyValue.Key.String(), string(kv.KeyValue.Value.RawBytes), kv.KeyValue.Value.Timestamp.WallTime)
					}
				case crosscluster.SSTableEvent:
					sst := event.GetSSTable()
					fmt.Printf("sst: %s->%s@%d\n", sst.Span.String(), string(sst.Data), sst.WriteTS.WallTime)
				case crosscluster.DeleteRangeEvent:
					delRange := event.GetDeleteRange()
					fmt.Printf("delRange: %s@%d\n", delRange.Span.String(), delRange.Timestamp.WallTime)
				case crosscluster.CheckpointEvent:
					minTS := hlc.MaxTimestamp
					for _, rs := range event.GetCheckpoint().ResolvedSpans {
						if rs.Timestamp.Less(minTS) {
							minTS = rs.Timestamp
						}
					}
					_, _ = ingested.Forward(roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}, minTS)
					fmt.Printf("resolved %d\n", minTS.WallTime)
				default:
					panic(fmt.Sprintf("unexpected event type %v", event.Type()))
				}
			}
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		panic(err)
	}

	// Output:
	// kv: "key_1"->value_1@1
	// resolved 100
	// kv: "key_1"->value_1@1
	// resolved 100
}
