// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// A Phase represents the context in which an op is executed within a schema
// change. Different phases require different dependencies for the execution of
// the ops to be plumbed in.
//
// Today, we support the phases corresponding to async schema changes initiated
// and partially executed in the user transaction. This will change as we
// transition to transactional schema changes.
type Phase int

const (
	// StatementPhase refers to execution of ops occurring during statement
	// execution during the user transaction.
	StatementPhase Phase = iota
	// PreCommitPhase refers to execution of ops occurring during the user
	// transaction immediately before commit.
	PreCommitPhase
	// PostCommitPhase refers to execution of ops occurring after the user
	// transaction has committed (i.e., in the async schema change job).
	PostCommitPhase
)

// Params holds the arguments for planning.
type Params struct {
	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase Phase
	// CreatedDescriptorIDs contains IDs for new descriptors created by the same
	// schema changer (i.e., earlier in the same transaction). New descriptors
	// can have most of their schema changes fully executed in the same
	// transaction.
	//
	// This doesn't do anything right now.
	CreatedDescriptorIDs catalog.DescriptorIDSet
}

// A Plan is a schema change plan, primarily containing ops to be executed that
// are partitioned into stages.
type Plan struct {
	Params       Params
	InitialNodes []*scpb.Node
	Graph        *scgraph.Graph
	Stages       []Stage
}

// A Stage is a sequence of ops to be executed "together" as part of a schema
// change.
//
// Stages also contain their corresponding targets and states before and after
// the execution of the ops in the stage, reflecting the fact that any set of
// ops can be thought of as a transition from a set of target states to another.
type Stage struct {
	Before, After []*scpb.Node
	Ops           scop.Ops
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial states for a set of targets.
func MakePlan(initialStates []*scpb.Node, params Params) (_ Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	g, err := scgraph.New(initialStates)
	if err != nil {
		return Plan{}, err
	}
	// TODO(ajwerner): Generate the stages for all of the phases as it will make
	// debugging easier.
	for _, ts := range initialStates {
		p[reflect.TypeOf(ts.Element())].ops(g, ts.Target, ts.State, params)
	}
	if err := g.ForEachNode(func(n *scpb.Node) error {
		d, ok := p[reflect.TypeOf(n.Element())]
		if !ok {
			return errors.Errorf("not implemented for %T", n.Target)
		}
		d.deps(g, n.Target, n.State)
		return nil
	}); err != nil {
		return Plan{}, err
	}
	stages := buildStages(initialStates, g)
	return Plan{
		Params:       params,
		InitialNodes: initialStates,
		Graph:        g,
		Stages:       stages,
	}, nil
}

func buildStages(init []*scpb.Node, g *scgraph.Graph) []Stage {
	// TODO(ajwerner): deal with the case where the target state was
	// fulfilled by something that preceded the initial state.
	cur := init
	fulfilled := map[*scpb.Node]struct{}{}
	filterUnsatisfiedEdgesStep := func(edges []*scgraph.OpEdge) ([]*scgraph.OpEdge, bool) {
		candidates := make(map[*scpb.Node]struct{})
		for _, e := range edges {
			candidates[e.To()] = struct{}{}
		}
		// Check to see if the current set of edges will have their dependencies met
		// if they are all run. Any which will not must be pruned. This greedy
		// algorithm works, but a justification is in order.
		failed := map[*scgraph.OpEdge]struct{}{}
		for _, e := range edges {
			_ = g.ForEachDepEdgeFrom(e.To(), func(de *scgraph.DepEdge) error {
				_, isFulfilled := fulfilled[de.To()]
				_, isCandidate := candidates[de.To()]
				if isFulfilled || isCandidate {
					return nil
				}
				failed[e] = struct{}{}
				return iterutil.StopIteration()
			})
		}
		if len(failed) == 0 {
			return edges, true
		}
		truncated := edges[:0]
		for _, e := range edges {
			if _, found := failed[e]; !found {
				truncated = append(truncated, e)
			}
		}
		return truncated, false
	}
	filterUnsatisfiedEdges := func(edges []*scgraph.OpEdge) ([]*scgraph.OpEdge, bool) {
		for len(edges) > 0 {
			if filtered, done := filterUnsatisfiedEdgesStep(edges); !done {
				edges = filtered
			} else {
				return filtered, true
			}
		}
		return edges, false
	}
	buildStageType := func(edges []*scgraph.OpEdge) (Stage, bool) {
		edges, ok := filterUnsatisfiedEdges(edges)
		if !ok {
			return Stage{}, false
		}
		next := append(cur[:0:0], cur...)
		var ops []scop.Op
		for i, ts := range cur {
			for _, e := range edges {
				if e.From() == ts {
					next[i] = e.To()
					ops = append(ops, e.Op())
					break
				}
			}
		}
		return Stage{
			Before: cur,
			After:  next,
			Ops:    scop.MakeOps(ops...),
		}, true
	}

	var stages []Stage
	for {
		// Note that the current nodes are fulfilled for the sake of dependency
		// checking.
		for _, ts := range cur {
			fulfilled[ts] = struct{}{}
		}

		// Extract the set of op edges for the current stage.
		var opEdges []*scgraph.OpEdge
		for _, t := range cur {
			// TODO(ajwerner): improve the efficiency of this lookup.
			// Look for an opEdge from this node. Then, for the other side
			// of the opEdge, look for dependencies.
			if oe, ok := g.GetOpEdgeFrom(t); ok {
				opEdges = append(opEdges, oe)
			}
		}

		// Group the op edges a per-type basis.
		opTypes := make(map[scop.Type][]*scgraph.OpEdge)
		for _, oe := range opEdges {
			opTypes[oe.Op().Type()] = append(opTypes[oe.Op().Type()], oe)
		}

		// Greedily attempt to find a stage which can be executed. This is sane
		// because once a dependency is met, it never becomes unmet.
		var didSomething bool
		var s Stage
		for _, typ := range []scop.Type{
			scop.MutationType,
			scop.BackfillType,
			scop.ValidationType,
		} {
			if s, didSomething = buildStageType(opTypes[typ]); didSomething {
				break
			}
		}
		if !didSomething {
			break
		}
		stages = append(stages, s)
		cur = s.After
	}
	return stages
}
