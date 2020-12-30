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

// MockStreamClient is a mock stream client.
type MockStreamClient struct{}

var _ StreamClient = &MockStreamClient{}

// NewMockStreamClient returns a new mock stream client.
func NewMockStreamClient() *MockStreamClient {
	return &MockStreamClient{}
}

// GetTopology implements the StreamClient interface.
func (m *MockStreamClient) GetTopology(address StreamAddress) (Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the StreamClient interface.
func (m *MockStreamClient) ConsumePartition(
	address PartitionAddress, startTime time.Time,
) (chan streamingccl.Event, error) {
	panic("unimplemented mock method")
}
