// Copyright 2017 The Cockroach Authors.
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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ClusterIDContainer is used to share a single Cluster ID instance between
// multiple layers. It allows setting and getting the value. Once a value is
// set, the value cannot change.
//
// The cluster ID is determined on startup as follows:
// - If there are existing stores, their cluster ID is used.
// - If the node is bootstrapping, a new UUID is generated.
// - Otherwise, it is determined via gossip with other nodes.
type ClusterIDContainer struct {
	clusterID atomic.Value // uuid.UUID
}

// String returns the cluster ID, or "?" if it is unset.
func (c *ClusterIDContainer) String() string {
	val := c.Get()
	if val == uuid.Nil {
		return "?"
	}
	return val.String()
}

// Get returns the current cluster ID; uuid.Nil if it is unset.
func (c *ClusterIDContainer) Get() uuid.UUID {
	v := c.clusterID.Load()
	if v == nil {
		return uuid.Nil
	}
	return v.(uuid.UUID)
}

// Set sets the current cluster ID. If it is already set, the value must match.
func (c *ClusterIDContainer) Set(ctx context.Context, val uuid.UUID) {
	// NOTE: this compare-and-swap is intentionally racy and won't catch all
	// cases where two different cluster IDs are set. That's ok, as this is
	// just a sanity check. But if we decide to care, we can use the new
	// (*atomic.Value).CompareAndSwap API introduced in go1.17.
	cur := c.Get()
	if cur == uuid.Nil {
		c.clusterID.Store(val)
		if log.V(2) {
			log.Infof(ctx, "ClusterID set to %s", val)
		}
	} else if cur != val {
		log.Fatalf(ctx, "different ClusterIDs set: %s, then %s", cur, val)
	}
}

// Reset changes the ClusterID regardless of the old value.
//
// Should only be used in testing code.
func (c *ClusterIDContainer) Reset(val uuid.UUID) {
	c.clusterID.Store(val)
}
