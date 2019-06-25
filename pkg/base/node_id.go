// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// NodeIDContainer is used to share a single roachpb.NodeID instance between
// multiple layers. It allows setting and getting the value. Once a value is
// set, the value cannot change.
type NodeIDContainer struct {
	_ util.NoCopy

	// nodeID is atomically updated under the mutex; it can be read atomically
	// without the mutex.
	nodeID int32
}

// String returns the node ID, or "?" if it is unset.
func (n *NodeIDContainer) String() string {
	val := n.Get()
	if val == 0 {
		return "?"
	}
	return strconv.Itoa(int(val))
}

// Get returns the current node ID; 0 if it is unset.
func (n *NodeIDContainer) Get() roachpb.NodeID {
	return roachpb.NodeID(atomic.LoadInt32(&n.nodeID))
}

// Set sets the current node ID. If it is already set, the value must match.
func (n *NodeIDContainer) Set(ctx context.Context, val roachpb.NodeID) {
	if val <= 0 {
		log.Fatalf(ctx, "trying to set invalid NodeID: %d", val)
	}
	oldVal := atomic.SwapInt32(&n.nodeID, int32(val))
	if oldVal == 0 {
		if log.V(2) {
			log.Infof(ctx, "NodeID set to %d", val)
		}
	} else if oldVal != int32(val) {
		log.Fatalf(ctx, "different NodeIDs set: %d, then %d", oldVal, val)
	}
}

// Reset changes the NodeID regardless of the old value.
//
// Should only be used in testing code.
func (n *NodeIDContainer) Reset(val roachpb.NodeID) {
	atomic.StoreInt32(&n.nodeID, int32(val))
}
