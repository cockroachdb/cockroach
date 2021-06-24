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
	Params  Params
	Initial scpb.State
	Graph   *scgraph.Graph
	Stages  []Stage
}

// A Stage is a sequence of ops to be executed "together" as part of a schema
// change.
//
// Stages also contain the state before and after the execution of the ops in
// the stage, reflecting the fact that any set of ops can be thought of as a
// transition from one state to another.
type Stage struct {
	Before, After scpb.State
	Ops           scop.Ops
	Revertible    bool
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial state for a set of targets.
func MakePlan(initial scpb.State, params Params) (_ Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	g, err := scgraph.New(initial)
	if err != nil {
		return Plan{}, err
	}
	// TODO(ajwerner): Generate the stages for all of the phases as it will make
	// debugging easier.
	for _, ts := range initial {
		p[reflect.TypeOf(ts.Element())].ops(g, ts.Target, ts.Status, params)
	}
	if err := g.ForEachNode(func(n *scpb.Node) error {
		d, ok := p[reflect.TypeOf(n.Element())]
		if !ok {
			return errors.Errorf("not implemented for %T", n.Target)
		}
		d.deps(g, n.Target, n.Status)
		return nil
	}); err != nil {
		return Plan{}, err
	}
	stages := buildStages(initial, g, params)
	return Plan{
		Params:  params,
		Initial: initial,
		Graph:   g,
		Stages:  stages,
	}, nil
}

func buildStages(init scpb.State, g *scgraph.Graph, params Params) []Stage {
	// TODO(ajwerner): deal with the case where the target status was
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
		isStageRevertible := true
		var ops []scop.Op
		for revertible := 1; revertible >= 0; revertible-- {
			isStageRevertible = revertible == 1
			for i, ts := range cur {
				for _, e := range edges {
					if e.From() == ts && isStageRevertible == e.Revertible() {
						next[i] = e.To()
						ops = append(ops, e.Op()...)
						break
					}
				}
			}
			// If we added non-revertible stages
			// then this stage is done
			if len(ops) != 0 {
				break
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
			for _, op := range oe.Op() {
				opTypes[op.Type()] = append(opTypes[op.Type()], oe)
			}
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
		// Sort ops based on graph dependencies.
		sortOps(g, s.Ops.Slice())
		stages = append(stages, s)
		cur = s.After
	}
	return stages
}

// Check if some route exists from curr to the
// target node
func doesPathExistToNode(graph *scgraph.Graph, start *scpb.Node, target *scpb.Node) bool {
	nodesToVisit := []*scpb.Node{start}
	visitedNodes := map[*scpb.Node]struct{}{}
	for len(nodesToVisit) > 0 {
		curr := nodesToVisit[0]
		if curr == target {
			return true
		}
		nodesToVisit = nodesToVisit[1:]
		if _, ok := visitedNodes[curr]; !ok {
			visitedNodes[curr] = struct{}{}
			edges, ok := graph.GetDepEdgesFrom(curr)
			if !ok {
				return false
			}
			// Append all of the nodes to visit
			for _, currEdge := range edges {
				nodesToVisit = append(nodesToVisit, currEdge.To())
			}
		}
	}
	return false
}

// sortOps sorts the operations into order based on
// graph dependencies
func sortOps(graph *scgraph.Graph, ops []scop.Op) {
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0; j-- {
			if compareOps(graph, ops[j], ops[j-1]) {
				tmp := ops[j]
				ops[j] = ops[j-1]
				ops[j-1] = tmp
			}
		}
	}
	// Sanity: Graph order is sane across all of
	// the ops.
	for i := 0; i < len(ops); i++ {
		for j := i + 1; j < len(ops); j++ {
			if !compareOps(graph, ops[i], ops[j]) && // Greater, but not equal (if equal opposite comparison would match).
				compareOps(graph, ops[j], ops[i]) {
				panic(errors.AssertionFailedf("Operators are not completely sorted %d %d", i, j))
			} else if compareOps(graph, ops[j], ops[i]) {
				compareOps(graph, ops[j], ops[i])
				panic(errors.AssertionFailedf("Operators are not completely sorted %d %d", i, j))
			}
		}
	}
}

// compareOps compares operations and orders them based on
// followed by the graph dependencies.
func compareOps(graph *scgraph.Graph, firstOp scop.Op, secondOp scop.Op) (less bool) {
	// Otherwise, lets compare attributes
	firstNode := graph.GetNodeFromOp(firstOp)
	secondNode := graph.GetNodeFromOp(secondOp)
	if firstNode == secondNode {
		return false // Equal
	}
	firstExists := doesPathExistToNode(graph, firstNode, secondNode)
	secondExists := doesPathExistToNode(graph, secondNode, firstNode)
	if firstExists && secondExists {
		if firstNode.Target.Direction == scpb.Target_DROP {
			return true
		} else if secondNode.Target.Direction == scpb.Target_DROP {
			return false
		} else {
			panic(errors.AssertionFailedf("A potential cycle exists in plan the graph, without any"+
				"nodes transitioning in opposite directions\n %s\n%s\n",
				firstNode,
				secondNode))
		}
	}

	// Path exists from first to second, so we depend on second.
	return firstExists
}
