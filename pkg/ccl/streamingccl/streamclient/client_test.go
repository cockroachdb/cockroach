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

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	_ context.Context, _ streamingccl.PartitionAddress, _ hlc.Timestamp,
) (chan streamingccl.Event, chan error, error) {
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

	return events, nil, nil
}

// ExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func ExampleClient() {
	client := testStreamClient{}
	topology, err := client.GetTopology("s3://my_bucket/my_stream")
	if err != nil {
		panic(err)
	}

	startTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	for _, partition := range topology.Partitions {
		eventCh, _ /* errCh */, err := client.ConsumePartition(context.Background(), partition, startTimestamp)
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
