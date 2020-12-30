// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testStreamClient struct{}

var _ Client = testStreamClient{}

// GetTopology implements the Client interface.
func (sc testStreamClient) GetTopology(_ StreamAddress) (Topology, error) {
	return Topology{Partitions: []PartitionAddress{
		"s3://my_bucket/my_stream/partition_1",
		"s3://my_bucket/my_stream/partition_2",
	}}, nil
}

// ConsumePartition implements the Client interface.
func (sc testStreamClient) ConsumePartition(_ PartitionAddress, _ time.Time) (chan Event, error) {
	sampleKV := roachpb.KeyValue{
		Key: []byte("key_1"),
		Value: roachpb.Value{
			RawBytes:  []byte("value 1"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}

	events := make(chan Event, 100)
	events <- MakeKVEvent(sampleKV)
	events <- MakeCheckpointEvent(timeutil.Now())
	close(events)

	return events, nil
}

// TestExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func TestExampleClientUsage(t *testing.T) {
	client := testStreamClient{}
	sa := StreamAddress("s3://my_bucket/my_stream")
	topology, err := client.GetTopology(sa)
	require.NoError(t, err)

	startTimestamp := timeutil.Now()
	numReceivedEvents := 0

	for _, partition := range topology.Partitions {
		eventCh, err := client.ConsumePartition(partition, startTimestamp)
		require.NoError(t, err)

		// This example looks for the closing of the channel to terminate the test,
		// but an ingestion job should look for another event such as the user
		// cutting over to the new cluster to move to the next stage.
		for {
			_, ok := <-eventCh
			if !ok {
				break
			}
			numReceivedEvents++
		}
	}

	// We expect 4 events, 2 from each partition.
	require.Equal(t, 4, numReceivedEvents)
}
