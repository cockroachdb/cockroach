// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physical

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Partitioning represents the physical partitioning of data for a relational
// operator. Currently it is only used for scans, but in the future it will
// be used for all operators to describe where the data will be physically
// located during execution. This information will enable more accurate costing
// of each operator by taking latency and network bandwidth into account.
type Partitioning struct {
	// partitions is the set of disjoint partitions that make up this
	// Partitioning. They do not need to be in any particular order.
	partitions []partition
}

// partition represents a subset of rows for a relational operator that are
// known to reside on a particular subset of nodes in the cluster.
type partition struct {
	// constraint defines the subset of rows in this partition.
	constraint constraint.Constraint

	// nodes is the set of all nodes where replicas of this partition may reside,
	// as defined by the index zone configurations.
	nodes util.FastIntSet

	// leasePreferences is the set of nodes that satisfy the first lease
	// preference for this partition, as defined by the index zone configurations.
	// leasePreferences should be a subset of nodes, and indicates the nodes where
	// the leaseholder replica for this partition is most likely to reside.
	leasePreferences util.FastIntSet
}

// NewPartitioning creates a new Partitioning based on the catalog index
// partitioning.
func NewPartitioning(evalCtx *tree.EvalContext, md *opt.Metadata, index cat.Index) *Partitioning {
	var p Partitioning
	p.init(evalCtx, md, index, nil)
	return &p
}

// NewConstrainedPartitioning creates a new Partitioning based on the
// catalog index partitioning, and filters it according to the given
// constraint.
func NewConstrainedPartitioning(
	evalCtx *tree.EvalContext, md *opt.Metadata, index cat.Index, constraint *constraint.Constraint,
) *Partitioning {
	var p Partitioning
	p.init(evalCtx, md, index, constraint)
	return &p
}

func (p *Partitioning) init(
	evalCtx *tree.EvalContext, md *opt.Metadata, index cat.Index, c *constraint.Constraint,
) {
	// Make the key context for the constraints.
	var keyCtx *constraint.KeyContext
	if c != nil {
		// TODO(rytaft): check that columns of c are a prefix of the index columns.
		keyCtx = &constraint.KeyContext{EvalCtx: evalCtx, Columns: c.Columns}
	} else {
		cols := make([]opt.OrderingColumn, index.KeyColumnCount())
		tab := md.TableByStableID(index.Table().ID())
		for i := range cols {
			col := index.Column(i)
			colID := tab.MetaID.ColumnID(col.Ordinal)
			cols[i] = opt.MakeOrderingColumn(colID, col.Descending)
		}
		var constraintCols constraint.Columns
		constraintCols.Init(cols)
		newKeyCtx := constraint.MakeKeyContext(&constraintCols, evalCtx)
		keyCtx = &newKeyCtx
	}

	// Add a single-span constraint for each partition, as well as nodes matching
	// the index's zone config.
	partitionCount := index.PartitionCount()
	p.partitions = make([]partition, 0, partitionCount)
	for i := 0; i < partitionCount; i++ {
		catPart := index.Partition(i)
		var span constraint.Span
		startBoundary, endBoundary := constraint.IncludeBoundary, constraint.ExcludeBoundary
		if catPart.To.Len() == 0 {
			// Empty boundaries must be inclusive.
			endBoundary = constraint.IncludeBoundary
		}
		span.Init(
			constraint.MakeCompositeKey(catPart.From...),
			startBoundary,
			constraint.MakeCompositeKey(catPart.To...),
			endBoundary,
		)
		var cs constraint.Constraint
		cs.InitSingleSpan(keyCtx, &span)
		if c != nil {
			// Filter according to the provided constraint. If this partition is
			// filtered out, it will be detected below with cs.IsContradiction().
			cs.IntersectWith(evalCtx, c)
		}
		if !cs.IsContradiction() {
			p.partitions = append(p.partitions, partition{
				constraint:       cs,
				nodes:            nodesFromZone(md.AllNodes(), catPart.Zone),
				leasePreferences: leasePrefFromZone(md.AllNodes(), catPart.Zone),
			})
		}
	}
}

// Nodes returns the set of all nodes where data for this relational operator
// may reside.
func (p *Partitioning) Nodes() util.FastIntSet {
	var nodes util.FastIntSet
	for i := range p.partitions {
		nodes.UnionWith(p.partitions[i].nodes)
	}
	return nodes
}

// nodesFromZone returns all the nodes that satisfy at least one of the replica
// constraints in the given zone.
func nodesFromZone(allNodes []opt.NodeMeta, zone cat.Zone) util.FastIntSet {
	var nodes util.FastIntSet
	for i := range allNodes {
		for j, n := 0, zone.ReplicaConstraintsCount(); j < n; j++ {
			if nodeSatisfiesConstraints(&allNodes[i], zone.ReplicaConstraints(j)) {
				nodes.Add(int(allNodes[i].MetaID))
				break
			}
		}
	}
	return nodes
}

// leasePrefFromZone returns all the nodes that satisfy the first lease
// preference in the given zone.
func leasePrefFromZone(allNodes []opt.NodeMeta, zone cat.Zone) util.FastIntSet {
	var leasePreferences util.FastIntSet
	for i := range allNodes {
		if zone.LeasePreferenceCount() > 0 {
			// Only use the first lease preference if available since others only apply
			// in edge cases.
			if nodeSatisfiesConstraints(&allNodes[i], zone.LeasePreference(0)) {
				leasePreferences.Add(int(allNodes[i].MetaID))
			}
		}
	}
	return leasePreferences
}

// nodeSatisfiesConstraints checks whether a node satisfies all of the given
// constraints. If a constraint is of the PROHIBITED type, satisfying it means
// the node should not match the constraint's spec.
func nodeSatisfiesConstraints(node *opt.NodeMeta, constraints cat.ConstraintSet) bool {
	for i, n := 0, constraints.ConstraintCount(); i < n; i++ {
		c := constraints.Constraint(i)
		hasConstraint := nodeMatchesConstraint(node, c)
		required := c.IsRequired()
		if (required && !hasConstraint) || (!required && hasConstraint) {
			return false
		}
	}
	return true
}

// nodeMatchesConstraint returns whether a node matches a constraint's
// spec. It notably ignores whether the constraint is required or prohibited.
// Also see nodeSatisfiesConstraints().
func nodeMatchesConstraint(node *opt.NodeMeta, c cat.Constraint) bool {
	if c.GetKey() == "" {
		for _, attr := range node.Attrs.Attrs {
			if attr == c.GetValue() {
				return true
			}
		}
		return false
	}
	for _, tier := range node.Locality.Tiers {
		if c.GetKey() == tier.Key && c.GetValue() == tier.Value {
			return true
		}
	}
	return false
}
