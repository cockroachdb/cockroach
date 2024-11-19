// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvclient

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// NodeDescStore stores a collection of NodeDescriptors and StoreDescriptors.
//
// Implementations of the interface are expected to be threadsafe.
type NodeDescStore interface {
	// GetNodeDescriptor looks up the node descriptor by node ID.
	// It returns an error if the node is not known by the cache.
	GetNodeDescriptor(roachpb.NodeID) (*roachpb.NodeDescriptor, error)
	// GetNodeDescriptorCount get the number of node descriptors in the cache.
	// TODO(ewall): Replace this function with a new KV endpoint for 23.1.
	//  See https://github.com/cockroachdb/cockroach/issues/93549.
	GetNodeDescriptorCount() int
	// GetStoreDescriptor looks up the store descriptor by store ID.
	// It returns an error if the store is not known by the cache.
	GetStoreDescriptor(roachpb.StoreID) (*roachpb.StoreDescriptor, error)
}
