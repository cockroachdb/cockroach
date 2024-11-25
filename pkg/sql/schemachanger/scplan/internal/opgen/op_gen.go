// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type registry struct {
	targets   []target
	targetMap map[targetKey]int
}

type targetKey struct {
	elType       reflect.Type
	targetStatus scpb.Status
}

var opRegistry = &registry{
	targetMap: make(map[targetKey]int),
}

// Transition is used to introspect the operation transitions for elements.
type Transition interface {
	From() scpb.Status
	To() scpb.Status
	OpType() scop.Type
}

// HasTransient returns true if the element of this type has
// Transient transitions
func HasTransient(elType scpb.Element) bool {
	return hasTarget(elType, scpb.Transient)
}

// HasPublic returns true if the element of this type has
// ToPublic transitions
func HasPublic(elType scpb.Element) bool {
	return hasTarget(elType, scpb.ToPublic)
}

func hasTarget(elType scpb.Element, s scpb.TargetStatus) bool {
	_, ok := findTarget(elType, s.Status())
	return ok
}

// IterateTransitions iterates the transitions for a given element and
// TargetStatus.
func IterateTransitions(
	elType scpb.Element, target scpb.TargetStatus, fn func(t Transition) error,
) error {
	t, ok := findTarget(elType, target.Status())
	if !ok {
		return errors.Errorf("failed to find table %T to %v", elType, target)
	}
	for i := range t.transitions {
		if err := fn(&t.transitions[i]); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// BuildGraph constructs a graph with operation edges populated from an initial
// state.
func BuildGraph(
	ctx context.Context, activeVersion clusterversion.ClusterVersion, cs scpb.CurrentState,
) (*scgraph.Graph, error) {
	return opRegistry.buildGraph(ctx, activeVersion, cs)
}

func (r *registry) buildGraph(
	ctx context.Context, activeVersion clusterversion.ClusterVersion, cs scpb.CurrentState,
) (_ *scgraph.Graph, err error) {
	start := timeutil.Now()
	defer func() {
		if err != nil || !log.ExpensiveLogEnabled(ctx, 2) {
			return
		}
		log.Infof(ctx, "operation graph generation took %v", timeutil.Since(start))
	}()
	g, err := scgraph.New(cs)
	if err != nil {
		return nil, err
	}
	// Iterate through each match of initial state target's to target rules
	// and apply the relevant op edges to the graph. Copy out the elements
	// to not mutate the database in place.
	type toAdd struct {
		transition
		n *screl.Node
	}
	var edgesToAdd []toAdd
	md := makeOpgenContext(activeVersion, cs)
	for _, t := range r.targets {
		edgesToAdd = edgesToAdd[:0]
		if err := t.iterateFunc(g.Database(), func(n *screl.Node) error {
			status := n.CurrentStatus
			for _, op := range t.transitions {
				if op.from == status {
					edgesToAdd = append(edgesToAdd, toAdd{
						transition: op,
						n:          n,
					})
					status = op.to
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		for _, e := range edgesToAdd {
			var ops []scop.Op
			if e.ops != nil {
				ops = e.ops(e.n.Element(), &md)
			}
			// Operations are revertible unless stated otherwise.
			revertible := true
			if e.revertible != nil {
				// If a callback function exists invoke it to find out.
				revertible = e.revertible(e.n.Element(), &md)
			}
			if err := g.AddOpEdges(
				e.n.Target, e.from, e.to, revertible, e.canFail, ops...,
			); err != nil {
				return nil, err
			}
		}

	}
	return g, nil
}

// InitialStatus returns the status at the source of an op-edge path.
func InitialStatus(e scpb.Element, target scpb.Status) scpb.Status {
	if t, found := findTarget(e, target); found {
		for _, tstn := range t.transitions {
			if tstn.isEquiv {
				continue
			}
			return tstn.from
		}
		// All transitions results from `equiv(xxx)` specs. Return `to` of the last transition.
		return target
	}
	return scpb.Status_UNKNOWN
}

// NextStatus returns the status succeeding the current one for the element
// and target status, if the corresponding op edge exists.
func NextStatus(e scpb.Element, target, current scpb.Status) scpb.Status {
	if t, found := findTarget(e, target); found {
		for _, tt := range t.transitions {
			if tt.from == current {
				return tt.to
			}
		}
	}
	return scpb.Status_UNKNOWN
}

func findTarget(e scpb.Element, s scpb.Status) (_ target, found bool) {
	idx, ok := opRegistry.targetMap[makeTargetKey(reflect.TypeOf(e), s)]
	if !ok {
		return target{}, false
	}
	return opRegistry.targets[idx], true
}
