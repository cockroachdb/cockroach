// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationcluster

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// Node captures the relevant bits of each node as it pertains to the migration
// infrastructure.
type Node struct {
	ID    roachpb.NodeID
	Epoch int64
}

// Nodes is a collection of node objects.
type Nodes []Node

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
