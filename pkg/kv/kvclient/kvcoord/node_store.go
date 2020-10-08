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

// NodeDescStore stores a collection of NodeDescriptors.
//
// Implementations of the interface are expected to be threadsafe.
type NodeDescStore interface {
	// GetNodeDescriptor looks up the descriptor of the node by ID.
	// It returns an error if the node is not known by the store.
	GetNodeDescriptor(roachpb.NodeID) (*roachpb.NodeDescriptor, error)
}
