// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package crosscluster

import "net/url"

// SourceClusterUri is a connection uri for the source of the stream. The
// topology of a stream should be resolvable given a source cluster connection
// uri.
type SourceClusterUri string

// URL parses the uri into a URL.
func (sa SourceClusterUri) URL() (*url.URL, error) {
	return url.Parse(string(sa))
}

// String returns only the Host of the StreamAddress in order to avoid leaking
// credentials.  If the URL is invalid, "<invalidURL>" is returned.
func (sa SourceClusterUri) String() string {
	streamURL, parseErr := sa.URL()
	if parseErr != nil {
		return "<invalidURL>"
	}
	return streamURL.Host
}

// PartitionUri is a connection where the stream client should be able to read
// the events produced by a partition of a stream.
//
// Each partition will emit events for a fixed span of keys.
type PartitionUri string
