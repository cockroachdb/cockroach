// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stream_client

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type mockStreamClient struct{}

var _ StreamClient = mockStreamClient{}

// GetTopology implements the StreamClient interface.
func (sc mockStreamClient) GetTopology(_ StreamAddress) (Topology, error) {
	return Topology{Partitions: []PartitionAddress{
		"s3://my_bucket/my_stream/partition_1",
		"s3://my_bucket/my_stream/partition_2",
	}}, nil
}

// ConsumePartition implements the StreamClient interface.
func (sc mockStreamClient) ConsumePartition(
	_ PartitionAddress, _ time.Time,
) (chan streamingccl.Event, error) {
	sampleKV := roachpb.KeyValue{
		Key: []byte("key_1"),
		Value: roachpb.Value{
			RawBytes:  []byte("value 1"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}

	events := make(chan streamingccl.Event, 100)
	events <- streamingccl.MakeKVEvent(sampleKV)
	events <- streamingccl.MakeResolvedEvent(timeutil.Now())
	close(events)

	return events, nil
}

// TestExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func TestExampleClientUsage(t *testing.T) {
	client := mockStreamClient{}
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
		done := false
		for {
			select {
			case _, ok := <-eventCh:
				if !ok {
					done = true
					break
				}
				numReceivedEvents += 1
			}

			if done {
				break
			}
		}
	}

	// We expect 4 events, 2 from each partition.
	require.Equal(t, 4, numReceivedEvents)
}
