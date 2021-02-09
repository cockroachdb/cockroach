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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testStreamClient struct{}

var _ Client = testStreamClient{}

// GetTopology implements the Client interface.
func (sc testStreamClient) GetTopology(
	_ streamingccl.StreamAddress,
) (streamingccl.Topology, error) {
	return streamingccl.Topology{Partitions: []streamingccl.PartitionAddress{
		"s3://my_bucket/my_stream/partition_1",
		"s3://my_bucket/my_stream/partition_2",
	}}, nil
}

// ConsumePartition implements the Client interface.
func (sc testStreamClient) ConsumePartition(
	_ context.Context, pa streamingccl.PartitionAddress, _ time.Time,
) (chan streamingccl.Event, error) {
	sampleKV := roachpb.KeyValue{
		Key: []byte("key_1"),
		Value: roachpb.Value{
			RawBytes:  []byte("value_1"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}

	events := make(chan streamingccl.Event, 2)
	events <- streamingccl.MakeKVEvent(sampleKV)
	events <- streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 100})
	close(events)

	return events, nil
}

// ExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func ExampleClient() {
	client := testStreamClient{}
	topology, err := client.GetTopology("s3://my_bucket/my_stream")
	if err != nil {
		panic(err)
	}

	startTimestamp := timeutil.Now()

	for _, partition := range topology.Partitions {
		eventCh, err := client.ConsumePartition(context.Background(), partition, startTimestamp)
		if err != nil {
			panic(err)
		}

		// This example looks for the closing of the channel to terminate the test,
		// but an ingestion job should look for another event such as the user
		// cutting over to the new cluster to move to the next stage.
		for event := range eventCh {
			switch event.Type() {
			case streamingccl.KVEvent:
				kv := event.GetKV()
				fmt.Printf("%s->%s@%d\n", kv.Key.String(), string(kv.Value.RawBytes), kv.Value.Timestamp.WallTime)
			case streamingccl.CheckpointEvent:
				fmt.Printf("resolved %d\n", event.GetResolved().WallTime)
			default:
				panic(fmt.Sprintf("unexpected event type %v", event.Type()))
			}
		}
	}

	// Output:
	// "key_1"->value_1@1
	// resolved 100
	// "key_1"->value_1@1
	// resolved 100
}

// Ensure that all implementations specified in this test properly close the
// eventChannel when the given context is canceled.
func TestImplementationsCloseChannel(t *testing.T) {
	streamURL, err := url.Parse("test://52")
	require.NoError(t, err)
	randomClient, err := newRandomStreamClient(streamURL)
	require.NoError(t, err)

	// TODO: Add SQL client and file client here when implemented.
	impls := []Client{
		&mockClient{},
		randomClient,
	}

	for _, impl := range impls {
		ctx, cancel := context.WithCancel(context.Background())
		eventCh, err := impl.ConsumePartition(ctx, "test://53/", timeutil.Now())
		require.NoError(t, err)

		// Ensure that the eventCh closes when the context is canceled.
		cancel()
		for range eventCh {
		}
	}
}
