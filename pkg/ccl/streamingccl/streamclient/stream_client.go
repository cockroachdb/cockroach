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

// client is a mock stream client.
type client struct{}

var _ Client = &client{}

// NewStreamClient returns a new mock stream client.
func NewStreamClient() Client {
	return &client{}
}

// GetTopology implements the Client interface.
func (m *client) GetTopology(_ streamingccl.StreamAddress) (streamingccl.Topology, error) {
	return streamingccl.Topology{
		Partitions: []streamingccl.PartitionAddress{"some://address"},
	}, nil
}

// ConsumePartition implements the Client interface.
func (m *client) ConsumePartition(
	_ streamingccl.PartitionAddress, _ time.Time,
) (chan streamingccl.Event, error) {
	eventCh := make(chan streamingccl.Event)
	return eventCh, nil
}
