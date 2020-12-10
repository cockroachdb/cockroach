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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
)

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

// StreamClient provides a way for the stream ingestion job to consume a
// specified stream.
// TODO(57427): The stream client does not yet support the concept of
//  generations in a stream.
type StreamClient interface {
	// GetTopology returns the Topology of a stream.
	GetTopology(address StreamAddress) (Topology, error)

	// ConsumePartition returns a channel on which we can start listening for
	// events from a given partition that occur after a startTime.
	ConsumePartition(address PartitionAddress, startTime time.Time) (chan streamingccl.Event, error)
}
