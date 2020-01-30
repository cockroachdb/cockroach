// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcluster

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cluster"
)

// Cluster implements the cluster.Info interface for testing purposes.
type Cluster struct {
	neighborhoods []Neighborhood
}

var _ cluster.Info = &Cluster{}

// New creates a new empty instance of the test cluster.
func New() *Cluster {
	return &Cluster{}
}

// NeighborhoodCount is part of the cluster.Info interface.
func (tc *Cluster) NeighborhoodCount() int {
	return len(tc.neighborhoods)
}

// Neighborhood is part of the cluster.Info interface.
func (tc *Cluster) Neighborhood(i int) cluster.Neighborhood {
	return &tc.neighborhoods[i]
}

// NeighborhoodFromNode is part of the cluster.Info interface.
func (tc *Cluster) NeighborhoodFromNode(id roachpb.NodeID) cluster.NeighborhoodID {
	for _, hood := range tc.neighborhoods {
		for _, node := range hood.nodes {
			if id == node.id {
				return hood.id
			}
		}
	}
	return 0
}

// Neighborhood implements cluster.Neighborhood.
type Neighborhood struct {
	id    cluster.NeighborhoodID
	nodes []node
}

var _ cluster.Neighborhood = &Neighborhood{}

// ID is part of the cluster.Neighborhood interface.
func (on *Neighborhood) ID() cluster.NeighborhoodID {
	return on.id
}

// SatisfiesConstraints is part of the cluster.Neighborhood interface.
func (on *Neighborhood) SatisfiesConstraints(constraints cat.ConstraintSet) bool {
	for i := range on.nodes {
		if on.nodes[i].nodeSatisfiesConstraints(constraints) {
			return true
		}
	}
	return false
}

// node contains information about a node in a neighborhood.
type node struct {
	id       roachpb.NodeID
	locality roachpb.Locality
	attrs    roachpb.Attributes
}

// nodeSatisfiesConstraints checks whether a node satisfies all of the given
// constraints. If a constraint is of the REQUIRED type, satisfying it means
// the node should match the constraint's spec. If a constraint is of the
// PROHIBITED type, satisfying it means the node should not match the
// constraint's spec.
func (n *node) nodeSatisfiesConstraints(constraints cat.ConstraintSet) bool {
	for i, cnt := 0, constraints.ConstraintCount(); i < cnt; i++ {
		c := constraints.Constraint(i)
		hasConstraint := n.nodeMatchesConstraint(c)
		required := c.IsRequired()
		if (required && !hasConstraint) || (!required && hasConstraint) {
			return false
		}
	}
	return true
}

// nodeMatchesConstraint returns whether the node matches a constraint's
// spec. It notably ignores whether the constraint is required or prohibited.
// Also see nodeSatisfiesConstraints().
func (n *node) nodeMatchesConstraint(c cat.Constraint) bool {
	if c.GetKey() == "" {
		for _, attr := range n.attrs.Attrs {
			if attr == c.GetValue() {
				return true
			}
		}
		return false
	}
	for _, tier := range n.locality.Tiers {
		if c.GetKey() == tier.Key && c.GetValue() == tier.Value {
			return true
		}
	}
	return false
}
