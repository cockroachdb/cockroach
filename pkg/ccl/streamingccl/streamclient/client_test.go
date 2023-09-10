// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// Create implements the Client interface.
func (sc testStreamClient) Create(
	_ context.Context, _ roachpb.TenantName,
) (streampb.ReplicationProducerSpec, error) {
	return streampb.ReplicationProducerSpec{
		StreamID:             streampb.StreamID(1),
		ReplicationStartTime: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
	}, nil
}

// Plan implements the Client interface.
func (sc testStreamClient) Plan(_ context.Context, _ streampb.StreamID) (Topology, error) {
	return Topology{
		Partitions: []PartitionInfo{
			{
				SrcAddr: "test://host1",
			},
			{
				SrcAddr: "test://host2",
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
	_ context.Context, _ streampb.StreamID, _ SubscriptionToken, _ hlc.Timestamp, _ hlc.Timestamp,
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

	events := make(chan streamingccl.Event, 2)
	events <- streamingccl.MakeKVEvent(sampleKV)
	events <- streamingccl.MakeCheckpointEvent([]jobspb.ResolvedSpan{sampleResolvedSpan})
	close(events)

	return &testStreamSubscription{
		eventCh: events,
	}, nil
}

// Complete implements the streamclient.Client interface.
func (sc testStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}

type testStreamSubscription struct {
	eventCh chan streamingccl.Event
}

// Subscribe implements the Subscription interface.
func (t testStreamSubscription) Subscribe(_ context.Context) error {
	return nil
}

// Events implements the Subscription interface.
func (t testStreamSubscription) Events() <-chan streamingccl.Event {
	return t.eventCh
}

// Err implements the Subscription interface.
func (t testStreamSubscription) Err() error {
	return nil
}

func TestGetFirstActiveClientEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var streamAddresses []string
	activeClient, err := GetFirstActiveClient(context.Background(), streamAddresses, nil)
	require.ErrorContains(t, err, "failed to connect, no addresses")
	require.Nil(t, activeClient)

	activeSpanConfigClient, err := GetFirstActiveSpanConfigClient(context.Background(), streamAddresses, nil)
	require.ErrorContains(t, err, "failed to connect, no addresses")
	require.Nil(t, activeSpanConfigClient)

}

func TestGetFirstActiveClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	client := GetRandomStreamClientSingletonForTesting()
	defer func() {
		require.NoError(t, client.Close(context.Background()))
	}()

	streamAddresses := []string{
		"randomgen://test0/",
		"<invalid-url-test1>",
		"randomgen://test2/",
		"invalidScheme://test3",
		"randomgen://test4/",
		"randomgen://test5/",
		"randomgen://test6/",
	}
	addressDialCount := map[string]int{}
	for _, addr := range streamAddresses {
		addressDialCount[addr] = 0
	}

	// Track dials and error for all but test3 and test4
	client.RegisterDialInterception(func(streamURL *url.URL) error {
		addr := streamURL.String()
		addressDialCount[addr]++
		if addr != streamAddresses[3] && addr != streamAddresses[4] {
			return errors.Errorf("injected dial error")
		}
		return nil
	})

	activeClient, err := GetFirstActiveClient(context.Background(), streamAddresses, nil)
	require.NoError(t, err)

	// Should've dialed the valid schemes up to the 5th one where it should've
	// succeeded
	require.Equal(t, 1, addressDialCount[streamAddresses[0]])
	require.Equal(t, 0, addressDialCount[streamAddresses[1]])
	require.Equal(t, 1, addressDialCount[streamAddresses[2]])
	require.Equal(t, 0, addressDialCount[streamAddresses[3]])
	require.Equal(t, 1, addressDialCount[streamAddresses[4]])
	require.Equal(t, 0, addressDialCount[streamAddresses[5]])
	require.Equal(t, 0, addressDialCount[streamAddresses[6]])

	// The 5th should've succeded as it was a valid scheme and succeeded Dial
	require.Equal(t, activeClient.(*RandomStreamClient).streamURL.String(), streamAddresses[4])
}

// ExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func ExampleClient() {
	client := testStreamClient{}
	ctx := context.Background()
	defer func() {
		_ = client.Close(ctx)
	}()

	prs, err := client.Create(ctx, "system")
	if err != nil {
		panic(err)
	}
	id := prs.StreamID

	var ingested struct {
		ts hlc.Timestamp
		syncutil.Mutex
	}

	done := make(chan struct{})

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(func(ctx context.Context) error {
		ticker := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-done:
				return nil
			case <-ticker.C:
				ingested.Lock()
				ts := ingested.ts
				ingested.Unlock()

				if _, err := client.Heartbeat(ctx, id, ts); err != nil {
					return err
				}
			}
		}
	})

	grp.GoCtx(func(ctx context.Context) error {
		defer close(done)
		ingested.Lock()
		ts := ingested.ts
		ingested.Unlock()

		topology, err := client.Plan(ctx, id)
		if err != nil {
			panic(err)
		}

		for _, partition := range topology.Partitions {
			// TODO(dt): use Subscribe helper and partition.SrcAddr
			sub, err := client.Subscribe(ctx, id, partition.SubscriptionToken, hlc.Timestamp{}, ts)
			if err != nil {
				panic(err)
			}

			// This example looks for the closing of the channel to terminate the test,
			// but an ingestion job should look for another event such as the user
			// cutting over to the new cluster to move to the next stage.
			for event := range sub.Events() {
				switch event.Type() {
				case streamingccl.KVEvent:
					kv := event.GetKV()
					fmt.Printf("kv: %s->%s@%d\n", kv.Key.String(), string(kv.Value.RawBytes), kv.Value.Timestamp.WallTime)
				case streamingccl.SSTableEvent:
					sst := event.GetSSTable()
					fmt.Printf("sst: %s->%s@%d\n", sst.Span.String(), string(sst.Data), sst.WriteTS.WallTime)
				case streamingccl.DeleteRangeEvent:
					delRange := event.GetDeleteRange()
					fmt.Printf("delRange: %s@%d\n", delRange.Span.String(), delRange.Timestamp.WallTime)
				case streamingccl.CheckpointEvent:
					ingested.Lock()
					minTS := hlc.MaxTimestamp
					for _, rs := range event.GetResolvedSpans() {
						if rs.Timestamp.Less(minTS) {
							minTS = rs.Timestamp
						}
					}
					ingested.ts.Forward(minTS)
					ingested.Unlock()
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
