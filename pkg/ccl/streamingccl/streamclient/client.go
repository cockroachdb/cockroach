// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
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
	ConsumePartition(address streamingccl.PartitionAddress, startTime time.Time) (chan streamingccl.Event, error)
}
