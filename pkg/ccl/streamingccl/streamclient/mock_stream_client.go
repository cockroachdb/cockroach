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

// mockClient is a mock stream client.
type mockClient struct{}

var _ Client = &mockClient{}

// GetTopology implements the Client interface.
func (m *mockClient) GetTopology(_ streamingccl.StreamAddress) (streamingccl.Topology, error) {
	return streamingccl.Topology{
		Partitions: []streamingccl.PartitionAddress{"some://address"},
	}, nil
}

// ConsumePartition implements the Client interface.
func (m *mockClient) ConsumePartition(
	ctx context.Context, _ streamingccl.PartitionAddress, _ time.Time,
) (chan streamingccl.Event, chan error, error) {
	eventCh := make(chan streamingccl.Event)
	go func() {
		<-ctx.Done()
		close(eventCh)
	}()
	return eventCh, nil, nil
}
