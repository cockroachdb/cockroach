package compiler

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

type Stage struct {
	Ops         []ops.Op
	NextTargets []targets.TargetState
}

type ExecutionPhase int

const (
	PostStatementPhase ExecutionPhase = iota
	PreCommitPhase
	PostCommitPhase
)

type compileFlags struct {
	ExecutionPhase       ExecutionPhase
	CreatedDescriptorIDs descIDSet
}

type descIDSet struct {
	set util.FastIntSet
}

func makeDescIDSet(ids ...descpb.ID) descIDSet {
	s := descIDSet{}
	for _, id := range ids {
		s.add(id)
	}
	return s
}

func (d *descIDSet) add(id descpb.ID) {
	d.set.Add(int(id))
}

func (d *descIDSet) contains(id descpb.ID) bool {
	return d.set.Contains(int(id))
}

type opEdge struct {
	from, to *targets.TargetState
	op       ops.Op
}

func (o *opEdge) start() *targets.TargetState {
	return o.from
}

func (o *opEdge) end() *targets.TargetState {
	return o.to
}

var _ edge = (*opEdge)(nil)
var _ edge = (*depEdge)(nil)

// depEdge represents a dependency of a TargetState to another.
// A dependency implies that the from TargetState must be reached on or
// before the to TargetState.
//
// It is illegal for from and to to refer to the same target.
type depEdge struct {
	from, to *targets.TargetState
}

func (d depEdge) start() *targets.TargetState {
	return d.from
}

func (d depEdge) end() *targets.TargetState {
	return d.to
}

type edge interface {
	start() *targets.TargetState
	end() *targets.TargetState
}

type targetStateGraph struct {
	initialTargetStates []*targets.TargetState

	targets      []targets.Target
	targetStates []map[targets.State]*targets.TargetState
	targetIdxMap map[targets.Target]int

	// TODO(ajwerner): Store the set of targets and intern the TargetState values
	edges []edge
}

func (g *targetStateGraph) getOrCreateTargetState(
	t targets.Target, s targets.State,
) *targets.TargetState {
	targetStates := g.getOrCreateTargetStates(t)
	if ts, ok := targetStates[s]; ok {
		return ts
	}
	ts := &targets.TargetState{
		Target: t,
		State:  s,
	}
	targetStates[s] = ts
	return ts
}

func (g *targetStateGraph) getOrCreateTargetStates(
	t targets.Target,
) map[targets.State]*targets.TargetState {
	idx, ok := g.targetIdxMap[t]
	if ok {
		return g.targetStates[idx]
	}
	idx = len(g.targets)
	g.targetIdxMap[t] = idx
	g.targets = append(g.targets, t)
	g.targetStates = append(g.targetStates, map[targets.State]*targets.TargetState{})
	return g.targetStates[idx]
}

func (g *targetStateGraph) String() string {
	drawn, err := drawGraph(g.edges)
	if err != nil {
		panic(err)
	}
	return drawn
}

func (g *targetStateGraph) containsTarget(target targets.Target) bool {
	_, exists := g.targetIdxMap[target]
	return exists
}

// addOpEdge adds an opEdge for the given target with the provided op.
// Returns the next state (for convenience).
func (g *targetStateGraph) addOpEdge(
	t targets.Target, cur, next targets.State, op ops.Op,
) targets.State {
	g.edges = append(g.edges, &opEdge{
		from: g.getOrCreateTargetState(t, cur),
		to:   g.getOrCreateTargetState(t, next),
		op:   op,
	})
	return next
}

func (g *targetStateGraph) addDepEdge(
	fromTarget targets.Target,
	fromState targets.State,
	toTarget targets.Target,
	toState targets.State,
) {
	g.edges = append(g.edges, &depEdge{
		from: g.getOrCreateTargetState(fromTarget, fromState),
		to:   g.getOrCreateTargetState(toTarget, toState),
	})
}

func buildGraph(
	initialStates []*targets.TargetState, flags compileFlags,
) (*targetStateGraph, error) {
	g := targetStateGraph{
		targetIdxMap: map[targets.Target]int{},
	}
	for _, ts := range initialStates {
		if g.containsTarget(ts.Target) {
			return nil, errors.Errorf("invalid initial states contains duplicate target: %v", ts.Target)
		}
		g.initialTargetStates = append(g.initialTargetStates,
			g.getOrCreateTargetState(ts.Target, ts.State))
	}
	for _, ts := range initialStates {
		if err := generateOpEdges(&g, ts.Target, ts.State, flags); err != nil {
			return nil, err
		}
	}
	if err := generateDepEdges(&g); err != nil {
		return nil, err
	}

	return &g, nil
}

func compile(t []*targets.TargetState, flags compileFlags) ([]Stage, error) {
	// We want to create a sequence of TargetStates and ops along the edges.

	// We'll start with a process of producing a graph of edges.
	// We'll also want to create a dependency graph.
	// Then we'll fill it down.
	panic("unimplemented")
}

func generateDepEdges(g *targetStateGraph) error {
	// We want to generate the dependencies between target states.

	// TODO(ajwerner): refactor, this initial pass is incredibly imperative.
	// We want to iterate over the set of nodes and then iterate over the set of
	// targets which might be associated with those nodes and then add relevant
	// dep edges (we may need to synthesize nodes).
	for idx, ts := range g.targetStates {
		for s := range ts {
			if err := generateTargetStateDepEdges(g, g.targets[idx], s); err != nil {
				return err
			}
		}
	}
	return nil
}

// Now we need a way to talk about dependencies.
// * A column cannot be made public until all of the indexes using it are backfilled.
// * A column cannot be made public until all column constraints are public
// * A primary index cannot be made DeleteAndWriteOnly until another primary index
//   is in DeleteAndWriteOnly.
// *

func columnsContainsID(haystack []descpb.ColumnID, needle descpb.ColumnID) bool {
	for _, id := range haystack {
		if id == needle {
			return true
		}
	}
	return false
}
