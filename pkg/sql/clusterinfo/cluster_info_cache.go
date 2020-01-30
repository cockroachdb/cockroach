// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterinfo

import (
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Cache implements the cluster.Info interface. It is used to cache information
// about neighborhoods in the cluster. The information is used by the optimizer
// to make better planning decisions by taking into account how data is
// distributed across the cluster.
//
// Neighborhoods are groups of nodes that are physically close to each other
// (e.g., part of the same region or availability zone). The number of nodes in
// each neighborhood can vary depending on the cluster size and topology.
type Cache struct {
	mu struct {
		syncutil.Mutex

		// neighborhoods contains information about every neighborhood in the
		// CockroachDB cluster.
		neighborhoods []optNeighborhood
	}
	Gossip *gossip.Gossip
}

var _ cluster.Info = &Cache{}

func New(g *gossip.Gossip) *Cache {
	c := &Cache{Gossip: g}

	var descriptors []roachpb.NodeDescriptor
	if err := g.IterateInfos(gossip.KeyNodeIDPrefix, func(key string, i gossip.Info) error {
		bytes, err := i.Value.GetBytes()
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to extract bytes for key %q", key)
		}

		var d roachpb.NodeDescriptor
		if err := protoutil.Unmarshal(bytes, &d); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", key)
		}

		// Don't use node descriptors with NodeID 0, because that's meant to
		// indicate that the node has been removed from the cluster.
		if d.NodeID != 0 {
			descriptors = append(descriptors, d)
		}
		return nil
	}); err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "while getting node descriptors"))
	}
	c.mu.neighborhoods = make([]optNeighborhood, len(descriptors))
	for i, desc := range descriptors {
		// TODO(rytaft): This currently constructs one neighborhood per node. Add
		// logic to adapt the number of nodes per neighborhood based on the cluster
		// size and topology.
		c.mu.neighborhoods[i] = optNeighborhood{
			id:    cluster.NeighborhoodID(desc.NodeID),
			nodes: []node{{id: desc.NodeID, locality: desc.Locality, attrs: desc.Attrs}},
		}
	}

	// TODO(rytaft): add gossip callback for whenever a node is added or removed
	// from the cluster.

	return c
}

// NeighborhoodCount is part of the cluster.Info interface.
func (c *Cache) NeighborhoodCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.mu.neighborhoods)
}

// Neighborhood is part of the cluster.Info interface.
func (c *Cache) Neighborhood(i int) cluster.Neighborhood {
	c.mu.Lock()
	defer c.mu.Unlock()

	return &c.mu.neighborhoods[i]
}

// NeighborhoodFromNode is part of the cluster.Info interface.
func (c *Cache) NeighborhoodFromNode(id roachpb.NodeID) cluster.NeighborhoodID {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, hood := range c.mu.neighborhoods {
		for _, node := range hood.nodes {
			if id == node.id {
				return hood.id
			}
		}
	}
	return 0
}

// optNeighborhood implements cluster.Neighborhood.
type optNeighborhood struct {
	id    cluster.NeighborhoodID
	nodes []node
}

var _ cluster.Neighborhood = &optNeighborhood{}

// ID is part of the cluster.Neighborhood interface.
func (on *optNeighborhood) ID() cluster.NeighborhoodID {
	return on.id
}

// SatisfiesConstraints is part of the cluster.Neighborhood interface.
func (on *optNeighborhood) SatisfiesConstraints(constraints cat.ConstraintSet) bool {
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
