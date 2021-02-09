// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import "net/url"

// StreamAddress is the location of the stream. The topology of a stream should
// be resolvable given a stream address.
type StreamAddress string

// URL parses the stream address as a URL.
func (sa StreamAddress) URL() (*url.URL, error) {
	return url.Parse(string(sa))
}

// PartitionAddress is the address where the stream client should be able to
// read the events produced by a partition of a stream.
//
// Each partition will emit events for a fixed span of keys.
type PartitionAddress string

// URL parses the partition address as a URL.
func (pa PartitionAddress) URL() (*url.URL, error) {
	return url.Parse(string(pa))
}

// Topology is a configuration of stream partitions. These are particular to a
// stream. It specifies the number and addresses of partitions of the stream.
//
// There are a fixed number of Partitions in a Topology.
type Topology struct {
	Partitions []PartitionAddress
}
