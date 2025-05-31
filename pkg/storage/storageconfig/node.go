// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

// Node contains all the node-level storage related configuration.
type Node struct {
	// WALFailover enables and configures automatic WAL failover when latency to
	// a store's primary WAL increases.
	WALFailover WALFailover
	// SharedStorage is specified to enable disaggregated shared storage. It is
	// enabled if the uri is set.
	SharedStorage SharedStorage
}

// SharedStorage specifies the properties of the shared storage.
type SharedStorage struct {
	// URI is the base location to read and write shared storage files.
	URI string
	// Cache is the size of the secondary cache used to store blocks from
	// disaggregated shared storage.
	Cache SizeSpec
}
