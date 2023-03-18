// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgradecluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
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
// currently part of the cluster (i.e. they haven't been decommissioned away).
// Migrations have the pre-requisite that all nodes are up and running so that
// we're able to execute all relevant node-level operations on them. If any of
// the nodes are found to be unavailable, an error is returned.
//
// It's important to note that this makes no guarantees about new nodes
// being added to the cluster. It's entirely possible for that to happen
// concurrently with the retrieval of the current set of nodes. Appropriate
// usage of this entails wrapping it under a stabilizing loop, like we do in
// EveryNode.
func NodesFromNodeLiveness(ctx context.Context, nl NodeLiveness) (Nodes, error) {
	nodeList := nl.NotDecommissionedList()

	var ns []Node
	for _, nodeId := range nodeList {
		status := nl.GetNodeStatus(nodeId)
		if !status.IsAlive() {
			return nil, errors.Newf("n%d required, but unavailable", nodeId)
		}
		ns = append(ns, Node{ID: nodeId})
	}
	return ns, nil
}

// Identical returns whether or not two lists of Nodes are identical as sets,
// and if not, what changed (in terms of cluster membership operations).
// The textual diffs are only to be used for logging purposes.
func (ns Nodes) Identical(other Nodes) (ok bool, _ []redact.RedactableString) {
	a, b := ns, other

	type ent struct {
		node  Node
		count int
	}
	m := map[roachpb.NodeID]ent{}
	for _, node := range a {
		m[node.ID] = ent{count: 1, node: node}
	}
	for _, node := range b {
		e, ok := m[node.ID]
		if ok {
			e.count--
			m[node.ID] = ent{count: 0, node: node}
		} else {
			m[node.ID] = ent{count: -1, node: node}
		}
	}

	var diffs []redact.RedactableString
	for id, e := range m {
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
