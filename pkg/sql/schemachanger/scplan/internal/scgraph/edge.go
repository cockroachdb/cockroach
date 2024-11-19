// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scgraph

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// Edge represents a relationship between two Nodes.
//
// TODO(ajwerner): Consider hiding Node pointers behind an interface to clarify
// mutability.
type Edge interface {
	From() *screl.Node
	To() *screl.Node
}

// OpEdge represents an edge changing the state of a target with an op.
type OpEdge struct {
	from, to *screl.Node
	op       []scop.Op
	typ      scop.Type

	// canFail indicates that a backfill or validation operation for this
	// target has yet to be run as of the originating node for this edge.
	canFail bool

	// revertible indicates that no operation which destroys information
	// permanently or publishes new information externally has yet been
	// run for this target.
	revertible bool
}

// From implements the Edge interface.
func (oe *OpEdge) From() *screl.Node { return oe.from }

// To implements the Edge interface.
func (oe *OpEdge) To() *screl.Node { return oe.to }

// Op returns the scop.Op for execution that is associated with the op edge.
func (oe *OpEdge) Op() []scop.Op { return oe.op }

// Revertible returns if the dependency edge is revertible.
func (oe *OpEdge) Revertible() bool { return oe.revertible }

// CanFail returns if the dependency edge corresponds to a target in a state
// which has yet to undergo all of its validation and backfill operations
// before reaching its targeted status.
func (oe *OpEdge) CanFail() bool { return oe.canFail }

// Type returns the types of operations associated with this edge.
func (oe *OpEdge) Type() scop.Type {
	return oe.typ
}

// String returns a string representation of this edge
func (oe *OpEdge) String() string {
	from := screl.NodeString(oe.from)
	nonRevertible := ""
	if !oe.revertible {
		nonRevertible = "non-revertible"
	}
	return fmt.Sprintf("%s -op-%s-> %s", from, nonRevertible, oe.to.CurrentStatus)
}

// DepEdgeKind indicates the kind of constraint enforced by the edge.
type DepEdgeKind int

//go:generate stringer -type DepEdgeKind

const (
	_ DepEdgeKind = iota

	// Precedence indicates that the source (from) of the edge must be reached
	// before the destination (to), whether in a previous stage or the same stage.
	Precedence

	// SameStagePrecedence indicates that the source (from) of the edge must
	// be reached before the destination (to), and _must_ do so in the same stage.
	SameStagePrecedence

	// PreviousStagePrecedence indicates that the source (from) of the edge must
	// be reached before the destination (to), and _must_ do so in a previous
	// stage.
	//
	// This edge kind is only maintained for compatibility with the 23.1 and 22.2
	// releases via the release_22_2 and release_23_1 rulesets and should not be
	// used elsewhere.
	// Deprecated
	PreviousStagePrecedence

	// PreviousTransactionPrecedence indicates that the source (from) of the edge
	// must be reached before the destination (to), and _must_ do so in a previous
	// transaction.
	//
	// This edge kind is used to enforce the two-version invariant.
	PreviousTransactionPrecedence
)

// DepEdge represents a dependency between two nodes. A dependency
// implies that the To() node cannot be reached before the From() node. It
// can be reached concurrently.
type DepEdge struct {
	from, to *screl.Node
	kind     DepEdgeKind

	rules []Rule
}

// Rule describes a reason for a DepEdge to exist.
type Rule struct {
	Name RuleName
	Kind DepEdgeKind
}

// RuleNames is a slice of RuleName.
type RuleNames []RuleName

// String makes RuleNames a fmt.Stringer.
func (rn RuleNames) String() string {
	var sb strings.Builder
	if len(rn) == 1 {
		sb.WriteString(string(rn[0]))
	} else {
		sb.WriteString("[")
		for i, r := range rn {
			if i > 0 {
				sb.WriteString("; ")
			}
			sb.WriteString(string(r))
		}
		sb.WriteString("]")
	}
	return sb.String()
}

// From implements the Edge interface.
func (de *DepEdge) From() *screl.Node { return de.from }

// To implements the Edge interface.
func (de *DepEdge) To() *screl.Node { return de.to }

// RuleNames returns the names of the rules which generated this edge.
func (de *DepEdge) RuleNames() RuleNames {
	ret := make(RuleNames, len(de.rules))
	for i, r := range de.rules {
		ret[i] = r.Name
	}
	return ret
}

// Rules returns the metadata about the rules which generated this edge.
func (de *DepEdge) Rules() []Rule { return de.rules }

// Kind returns the kind of the DepEdge. Note that it returns the strongest
// kind implied by a rule; if one rule which created this edge is Precedence,
// and another is SameStagePrecedence or PreviousStagePrecedence, then it
// returns the latter.
func (de *DepEdge) Kind() DepEdgeKind { return de.kind }

// String returns a string representation of this edge
func (de *DepEdge) String() string {
	from := screl.NodeString(de.from)
	to := screl.NodeString(de.to)
	return fmt.Sprintf("%s -dep-%s-> %s", from, de.kind, to)
}
