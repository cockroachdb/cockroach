// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

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
