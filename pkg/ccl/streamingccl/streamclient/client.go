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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
)

// Client provides a way for the stream ingestion job to consume a
// specified stream.
// TODO(57427): The stream client does not yet support the concept of
//  generations in a stream.
type Client interface {
	// GetTopology returns the Topology of a stream.
	GetTopology(address streamingccl.StreamAddress) (streamingccl.Topology, error)

	// ConsumePartition returns a channel on which we can start listening for
	// events from a given partition that occur after a startTime.
	//
	// Canceling the context will stop reading the partition and close the event
	// channel.
	ConsumePartition(ctx context.Context, address streamingccl.PartitionAddress, startTime time.Time) (chan streamingccl.Event, chan error, error)
}

// NewStreamClient creates a new stream client based on the stream
// address.
func NewStreamClient(streamAddress streamingccl.StreamAddress) (Client, error) {
	var streamClient Client
	streamURL, err := streamAddress.URL()
	if err != nil {
		return streamClient, err
	}

	switch streamURL.Scheme {
	case TestScheme:
		streamClient, err = newRandomStreamClient(streamURL)
		if err != nil {
			return streamClient, err
		}
	default:
		streamClient = &mockClient{}
	}

	return streamClient, nil
}
