// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgradecluster

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// Node captures the relevant bits of each node as it pertains to the upgrade
// infrastructure.
type Node struct {
	ID    roachpb.NodeID
	Epoch int64
}

// Nodes is a collection of node objects.
type Nodes []Node

// NodesFromNodeLiveness returns the IDs and epochs for all nodes that are
// currently part of the cluster (i.e. they haven't been decommissioned away)
// and any nodes which are currently unavailable. Migrations have the
// pre-requisite that all nodes are up and running so that we're able to
// execute all relevant node-level operations on them.
//
// It's important to note that this makes no guarantees about new nodes
// being added to the cluster. It's entirely possible for that to happen
// concurrently with the retrieval of the current set of nodes. Appropriate
// usage of this entails wrapping it under a stabilizing loop, like we do in
// EveryNode.
func NodesFromNodeLiveness(
	ctx context.Context, nl livenesspb.NodeVitalityInterface,
) (live, unavailable Nodes, _ error) {
	ls, err := nl.ScanNodeVitalityFromKV(ctx)
	if err != nil {
		return nil, nil, err
	}
	for id, n := range ls {
		if n.IsDecommissioned() {
			continue
		}
		if !n.IsLive(livenesspb.Upgrade) {
			unavailable = append(unavailable, Node{ID: id, Epoch: n.GenLiveness().Epoch})
		}
		// TODO(baptist): Stop using Epoch, need to determine an alternative.
		live = append(live, Node{ID: id, Epoch: n.GenLiveness().Epoch})

	}
	// Tests assume the nodes are sorted, so sort by node id first.
	sort.Slice(live, func(i, j int) bool { return live[i].ID < live[j].ID })
	sort.Slice(unavailable, func(i, j int) bool { return unavailable[i].ID < unavailable[j].ID })
	return live, unavailable, nil
}

// Identical returns whether or not two lists of Nodes are identical as sets,
// and if not, what changed (in terms of cluster membership operations and epoch
// changes). The textual diffs are only to be used for logging purposes.
func (ns Nodes) Identical(other Nodes) (ok bool, _ []redact.RedactableString) {
	a, b := ns, other

	type ent struct {
		node         Node
		count        int
		epochChanged bool
	}
	m := map[roachpb.NodeID]ent{}
	for _, node := range a {
		m[node.ID] = ent{count: 1, node: node, epochChanged: false}
	}
	for _, node := range b {
		e, ok := m[node.ID]
		e.count--
		if ok && e.node.Epoch != node.Epoch {
			e.epochChanged = true
		}
		m[node.ID] = e
	}

	var diffs []redact.RedactableString
	for id, e := range m {
		if e.epochChanged {
			diffs = append(diffs, redact.Sprintf("n%d's Epoch changed", id))
		}
		if e.count > 0 {
			diffs = append(diffs, redact.Sprintf("n%d was decommissioned", id))
		}
		if e.count < 0 {
			diffs = append(diffs, redact.Sprintf("n%d joined the cluster", id))
		}
	}

	return len(diffs) == 0, diffs
}

func (ns Nodes) String() string {
	return redact.StringWithoutMarkers(ns)
}

// SafeFormat implements redact.SafeFormatter.
func (ns Nodes) SafeFormat(s redact.SafePrinter, _ rune) {
	s.SafeString("n{")
	if len(ns) > 0 {
		s.Printf("%d", ns[0].ID)
		for _, node := range ns[1:] {
			s.Printf(",%d", node.ID)
		}
	}
	s.SafeString("}")
}
