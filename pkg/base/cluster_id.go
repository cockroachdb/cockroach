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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	syncutil.Mutex

	clusterID uuid.UUID
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
	c.Lock()
	defer c.Unlock()
	return c.clusterID
}

// Set sets the current cluster ID. If it is already set, the value must match.
func (c *ClusterIDContainer) Set(ctx context.Context, val uuid.UUID) {
	c.Lock()
	defer c.Unlock()
	if c.clusterID == uuid.Nil {
		c.clusterID = val
		if log.V(2) {
			log.Infof(ctx, "ClusterID set to %s", val)
		}
	} else if c.clusterID != val {
		log.Fatalf(ctx, "different ClusterIDs set: %s, then %s", c.clusterID, val)
	}
}

// Reset changes the ClusterID regardless of the old value.
//
// Should only be used in testing code.
func (c *ClusterIDContainer) Reset(val uuid.UUID) {
	c.Lock()
	defer c.Unlock()
	c.clusterID = val
}
