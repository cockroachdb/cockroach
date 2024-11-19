// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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
	// OnSet, if non-nil, is called after the ID is set with the new value.
	OnSet func(uuid.UUID)
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
	} else if cur != val {
		// NB: we are avoiding log.Fatal here because we want to avoid a dependency
		// on the log package. Also, this assertion would denote a serious bug and
		// we may as well panic.
		panic(errors.AssertionFailedf("different ClusterIDs set: %s, then %s", cur, val))
	}
	if c.OnSet != nil {
		c.OnSet(val)
	}
}

// Reset changes the ClusterID regardless of the old value.
//
// Should only be used in testing code.
func (c *ClusterIDContainer) Reset(val uuid.UUID) {
	c.clusterID.Store(val)
}
