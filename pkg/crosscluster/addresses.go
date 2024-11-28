// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package crosscluster

import "net/url"

// StreamAddress is the location of the stream. The topology of a stream should
// be resolvable given a stream address.
type StreamAddress string

// URL parses the stream address as a URL.
func (sa StreamAddress) URL() (*url.URL, error) {
	return url.Parse(string(sa))
}

// String returns only the Host of the StreamAddress in order to avoid leaking
// credentials.  If the URL is invalid, "<invalidURL>" is returned.
func (sa StreamAddress) String() string {
	streamURL, parseErr := sa.URL()
	if parseErr != nil {
		return "<invalidURL>"
	}
	return streamURL.Host
}

// PartitionAddress is the address where the stream client should be able to
// read the events produced by a partition of a stream.
//
// Each partition will emit events for a fixed span of keys.
type PartitionAddress string
