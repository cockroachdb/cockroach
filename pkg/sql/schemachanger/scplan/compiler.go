package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

type StageI interface {
	Ops() scop.Ops
	Targets() []scpb.Target
	NextStates() []scpb.State
}

type Stage struct {
	Ops         []scop.Op
	NextTargets []scpb.TargetState
}

type ExecutionPhase int

const (
	PostStatementPhase ExecutionPhase = iota
	PreCommitPhase
	PostCommitPhase
)

type CompileFlags struct {
	ExecutionPhase       ExecutionPhase
	CreatedDescriptorIDs catalog.DescriptorIDSet
}

type opEdge struct {
	from, to *scpb.TargetState
	op       scop.Op
}

func (o *opEdge) start() *scpb.TargetState {
	return o.from
}

func (o *opEdge) end() *scpb.TargetState {
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
	from, to *scpb.TargetState
}

func (d depEdge) start() *scpb.TargetState {
	return d.from
}

func (d depEdge) end() *scpb.TargetState {
	return d.to
}

type edge interface {
	start() *scpb.TargetState
	end() *scpb.TargetState
}

type SchemaChange struct {
	flags               CompileFlags
	initialTargetStates []*scpb.TargetState

	targets      []scpb.Target
	targetStates []map[scpb.State]*scpb.TargetState
	targetIdxMap map[scpb.Target]int

	// targetStateOpEdges maps a TargetState to an opEdge that proceeds
	// from it. A targetState may have at most one opEdge from it.
	targetStateOpEdges map[*scpb.TargetState]*opEdge

	// targetStateDepEdges maps a TargetState to its dependencies.
	// A targetState dependency is another target state which must be
	// reached before or concurrently with this targetState.
	targetStateDepEdges map[*scpb.TargetState][]*depEdge

	edges []edge

	stages []stage
}

func (g *SchemaChange) forEach(it nodeFunc) error {
	for _, m := range g.targetStates {
		for i := 0; i < scpb.NumStates; i++ {
			if ts, ok := m[scpb.State(i)]; ok {
				if err := it(ts.Target, ts.State); err != nil {
					if iterutil.Done(err) {
						err = nil
					}
					return err
				}
			}
		}
	}
	return nil
}

func (g *SchemaChange) forEachTarget(it targetFunc) error {
	for _, t := range g.targets {
		if err := it(t); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}

func (g *SchemaChange) getOrCreateTargetState(t scpb.Target, s scpb.State) *scpb.TargetState {
	targetStates := g.getTargetStatesMap(t)
	if ts, ok := targetStates[s]; ok {
		return ts
	}
	ts := &scpb.TargetState{
		Target: t,
		State:  s,
	}
	targetStates[s] = ts
	return ts
}

func (g *SchemaChange) getTargetStatesMap(t scpb.Target) map[scpb.State]*scpb.TargetState {
	return g.targetStates[g.targetIdxMap[t]]
}

// DrawStageGraph returns a graphviz string of the stages of the compiled
// SchemaChange.
func (g *SchemaChange) DrawStageGraph() (string, error) {
	gv, err := g.drawStages()
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

// D returns a graphviz string of the stages of the compiled
// SchemaChange.
func (g *SchemaChange) DrawDepGraph() (string, error) {
	gv, err := g.drawDeps()
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

func (g *SchemaChange) containsTarget(target scpb.Target) bool {
	_, exists := g.targetIdxMap[target]
	return exists
}

// addOpEdge adds an opEdge for the given target with the provided op.
// Returns the next state (for convenience).
func (g *SchemaChange) addOpEdge(t scpb.Target, cur, next scpb.State, op scop.Op) scpb.State {
	oe := &opEdge{
		from: g.getOrCreateTargetState(t, cur),
		to:   g.getOrCreateTargetState(t, next),
		op:   op,
	}
	if existing, exists := g.targetStateOpEdges[oe.from]; exists {
		panic(errors.Errorf("duplicate outbound op edge %v and %v",
			oe, existing))
	}
	g.edges = append(g.edges, oe)
	g.targetStateOpEdges[oe.from] = oe
	return next
}

func (g *SchemaChange) addDepEdge(
	fromTarget scpb.Target, fromState scpb.State, toTarget scpb.Target, toState scpb.State,
) {
	de := &depEdge{
		from: g.getOrCreateTargetState(fromTarget, fromState),
		to:   g.getOrCreateTargetState(toTarget, toState),
	}
	g.edges = append(g.edges, de)
	g.targetStateDepEdges[de.from] = append(g.targetStateDepEdges[de.from], de)
}

type stage struct {
	ops  []scop.Op
	next []*scpb.TargetState
}

func buildStages(g *SchemaChange, flags CompileFlags) error {
	// TODO(ajwerner): deal with the case where the target state was
	// fulfilled by something that preceded the initial state.
	fulfilled := map[*scpb.TargetState]struct{}{}
	cur := g.initialTargetStates
	for {
		for _, ts := range cur {
			fulfilled[ts] = struct{}{}
		}
		// candidates is a map of ends of opEdges to their opEdge.
		var opEdges []*opEdge
		for _, t := range cur {
			// TODO(ajwerner): improve the efficiency of this lookup.
			// Look for an opEdge from this node. Then, for the other side
			// of the opEdge, look for dependencies.
			if oe := g.targetStateOpEdges[t]; oe != nil {
				opEdges = append(opEdges, oe)
			}
		}
		// Find out what opEdges we have on a per-type basis and then
		// figure out if we have the dependencies fulfilled.
		opTypes := make(map[scop.Type][]*opEdge)
		for _, oe := range opEdges {
			opTypes[oe.op.Type()] = append(opTypes[oe.op.Type()], oe)
		}

		// It's not clear that greedy is going to do it here but let's
		// assume that it will and press on. The reason it's valid is
		// that so long as we make progress, everything is okay.
		var didSomething bool
		for _, typ := range []scop.Type{
			scop.DescriptorMutationType,
			scop.BackfillType,
			scop.ValidationType,
		} {
			edges := opTypes[typ]
			for len(edges) > 0 {
				candidates := make(map[*scpb.TargetState]struct{})
				for _, e := range edges {
					candidates[e.to] = struct{}{}
				}
				// See if we can apply all of them. Otherwise what? remove all
				// that cannot be applied.
				failed := map[*opEdge]struct{}{}
				for _, e := range edges {
					for _, d := range g.targetStateDepEdges[e.to] {
						if _, ok := fulfilled[d.to]; ok {
							continue
						}
						if _, ok := candidates[d.to]; ok {
							continue
						}
						failed[e] = struct{}{}
						break
					}
				}
				if len(failed) == 0 {
					break
				}
				truncated := edges[:0]
				for _, e := range edges {
					if _, found := failed[e]; !found {
						truncated = append(truncated, e)
					}
				}
				edges = truncated
			}
			if len(edges) == 0 {
				continue
			}
			next := append(cur[:0:0], cur...)
			var s stage
			for i, ts := range cur {
				for _, e := range edges {
					if e.from == ts {
						next[i] = e.to
						s.ops = append(s.ops, e.op)
						break
					}
				}
			}
			s.next = next
			g.stages = append(g.stages, s)
			cur = next
			didSomething = true
			break
			// For each edge, we want to see if the
		}
		if !didSomething {
			break
		}
	}
	return nil
}

func Compile(t []scpb.TargetState, flags CompileFlags) (*SchemaChange, error) {
	// We want to create a sequence of TargetStates and ops along the edges.

	// We'll start with a process of producing a graph of edges.
	// We'll also want to create a dependency graph.
	// Then we'll fill it down.

	// We want to walk the states and add edges to the current stage
	// so long as they have their dependencies met or can have all of
	// their dependencies met.
	return buildGraph(t, flags)
}

func (g *SchemaChange) Stages() []Stage {
	ret := make([]Stage, 0, len(g.stages))
	for i := range g.stages {
		ret = append(ret, Stage{
			Ops: g.stages[i].ops,
			NextTargets: func() (next []scpb.TargetState) {
				next = make([]scpb.TargetState, len(g.stages[i].next))
				for i, ts := range g.stages[i].next {
					next[i] = *ts
				}
				return next
			}(),
		})
	}
	return ret
}
