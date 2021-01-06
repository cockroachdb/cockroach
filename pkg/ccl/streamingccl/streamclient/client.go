// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import "time"

// StreamAddress is the location of the stream. The topology of a stream should
// be resolvable given a stream address.
type StreamAddress string

// PartitionAddress is the address where the stream client should be able to
// read the events produced by a partition of a stream.
//
// Each partition will emit events for a fixed span of keys.
type PartitionAddress string

// Topology is a configuration of stream partitions. These are particular to a
// stream. It specifies the number and addresses of partitions of the stream.
//
// There are a fixed number of Partitions in a Topology.
type Topology struct {
	Partitions []PartitionAddress
}

// Client provides a way for the stream ingestion job to consume a
// specified stream.
// TODO(57427): The stream client does not yet support the concept of
//  generations in a stream.
type Client interface {
	// GetTopology returns the Topology of a stream.
	GetTopology(address StreamAddress) (Topology, error)

	// ConsumePartition returns a channel on which we can start listening for
	// events from a given partition that occur after a startTime.
	ConsumePartition(address PartitionAddress, startTime time.Time) (chan Event, error)
}
