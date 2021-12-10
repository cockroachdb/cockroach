// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraph

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// Edge represents a relationship between two Nodes.
//
// TODO(ajwerner): Consider hiding Node pointers behind an interface to clarify
// mutability.
type Edge interface {
	From() *scpb.Node
	To() *scpb.Node
}

// OpEdge represents an edge changing the state of a target with an op.
type OpEdge struct {
	from, to   *scpb.Node
	op         []scop.Op
	typ        scop.Type
	revertible bool
	minPhase   scop.Phase
}

// From implements the Edge interface.
func (oe *OpEdge) From() *scpb.Node { return oe.from }

// To implements the Edge interface.
func (oe *OpEdge) To() *scpb.Node { return oe.to }

// Op returns the scop.Op for execution that is associated with the op edge.
func (oe *OpEdge) Op() []scop.Op { return oe.op }

// Revertible returns if the dependency edge is revertible
func (oe *OpEdge) Revertible() bool { return oe.revertible }

// Type returns the types of operations associated with this edge.
func (oe *OpEdge) Type() scop.Type {
	return oe.typ
}

// IsPhaseSatisfied returns true iff the operations can run in the given phase.
func (oe *OpEdge) IsPhaseSatisfied(phase scop.Phase) bool {
	return phase >= oe.minPhase
}

// String returns a string representation of this edge
func (oe *OpEdge) String() string {
	from := screl.NodeString(oe.from)
	nonRevertible := ""
	if !oe.revertible {
		nonRevertible = "non-revertible"
	}
	return fmt.Sprintf("%s -op-%s-> %s", from, nonRevertible, oe.to.Status)
}

// DepEdgeKind indicates the kind of constraint enforced by the edge.
type DepEdgeKind int

//go:generate stringer -type DepEdgeKind

const (
	_ DepEdgeKind = iota

	// Precedence indicates that the source (from) of the edge must be
	// reached before the destination (to), possibly doing so in the same stage.
	Precedence

	// SameStagePrecedence indicates that the source (from) of the edge must
	// be reached before the destination (to), and _must_ do so in the same stage.
	SameStagePrecedence
)

// DepEdge represents a dependency between two nodes. A dependency
// implies that the To() node cannot be reached before the From() node. It
// can be reached concurrently.
type DepEdge struct {
	from, to *scpb.Node
	kind     DepEdgeKind

	// TODO(ajwerner): Deal with the possibility that multiple rules could
	// generate the same edge.
	rule string
}

// From implements the Edge interface.
func (de *DepEdge) From() *scpb.Node { return de.from }

// To implements the Edge interface.
func (de *DepEdge) To() *scpb.Node { return de.to }

// Name returns the name of the rule which generated this edge.
func (de *DepEdge) Name() string { return de.rule }

// Kind returns the kind of the DepEdge.
func (de *DepEdge) Kind() DepEdgeKind { return de.kind }

// String returns a string representation of this edge
func (de *DepEdge) String() string {
	from := screl.NodeString(de.from)
	to := screl.NodeString(de.to)
	return fmt.Sprintf("%s -dep-%s-> %s", from, de.kind, to)
}
