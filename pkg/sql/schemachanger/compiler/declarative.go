package compiler

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/errors"
)

func buildGraph(initialStates []targets.TargetState, flags CompileFlags) (*SchemaChange, error) {
	g := SchemaChange{
		targetIdxMap:        map[targets.Target]int{},
		targetStateOpEdges:  map[*targets.TargetState]*opEdge{},
		targetStateDepEdges: map[*targets.TargetState][]*depEdge{},
	}

	// TODO: add validation of targets to ensure no two overlap in their
	// referenced elements.
	for _, ts := range initialStates {
		if existing, ok := g.targetIdxMap[ts.Target]; ok {
			return nil, errors.Errorf("invalid initial states contains duplicate target: %v and %v", ts, initialStates[existing])
		}
		idx := len(g.targets)
		g.targetIdxMap[ts.Target] = idx
		g.targets = append(g.targets, ts.Target)
		g.targetStates = append(g.targetStates, map[targets.State]*targets.TargetState{})
		g.initialTargetStates = append(g.initialTargetStates,
			g.getOrCreateTargetState(ts.Target, ts.State))
	}
	// TODO(ajwerner): Generate the stages for all of the phases as it will make
	// debugging easier.

	for _, ts := range initialStates {
		p[reflect.TypeOf(ts.Target)].forward(&g, ts.Target, ts.State, flags)
	}
	g.forEach(func(target targets.Target, s targets.State) error {
		d, ok := p[reflect.TypeOf(target)]
		if !ok {
			panic(errors.Errorf("not implemented for %T", target))
		}
		d.deps(&g, target, s)
		return nil
	})
	if err := buildStages(&g, flags); err != nil {
		return nil, err
	}
	return &g, nil
}

var _ graphBuilder = (*SchemaChange)(nil)

type depMatcher struct {
	s       targets.State
	matcher interface{}
}

type decOpEdge struct {
	nextState targets.State
	predicate interface{}
	op        interface{}
}

type targetRules struct {
	deps               targetDepRules
	forward, backwards targetOpRules
}

type targetDepRules map[targets.State][]depMatcher

type targetOpRules map[targets.State][]decOpEdge

var p = buildSchemaChangePlanner(rules)

type opGenFunc func(builder graphBuilder, t targets.Target, s targets.State, flags CompileFlags)
type depGenFunc func(g graphBuilder, t targets.Target, s targets.State)

type schemaChangeTargetPlanner struct {
	forward, backwards opGenFunc
	deps               depGenFunc
}

type schemaChangePlanner map[reflect.Type]schemaChangeTargetPlanner

func buildSchemaChangePlanner(m map[targets.Target]targetRules) schemaChangePlanner {
	tp := make(map[reflect.Type]schemaChangeTargetPlanner)
	for t, r := range m {
		tp[reflect.TypeOf(t)] = schemaChangeTargetPlanner{
			forward:   buildSchemaChangeOpGenFunc(t, r.forward),
			backwards: buildSchemaChangeOpGenFunc(t, r.backwards),
			deps:      buildSchemaChangeDepGenFunc(t, r.deps),
		}
	}
	return tp
}

func buildSchemaChangeDepGenFunc(t targets.Target, deps targetDepRules) depGenFunc {
	// We want to walk all of the edges and ensure that they have the proper
	// signature.
	tTyp := reflect.TypeOf(t)
	matchers := map[targets.State]map[reflect.Type][]func(a, b targets.Target) (bool, targets.State){}
	for s, rules := range deps {
		for i, rule := range rules {
			mt := reflect.TypeOf(rule.matcher)
			if mt.NumIn() != 2 {
				panic(errors.Errorf("expected two args, got %d for (%T,%s)[%d]", mt.NumIn(), t, s, i))
			}
			if got := mt.In(0); got != tTyp {
				panic(errors.Errorf("expected %T, got %v for (%T,%s)[%d]", t, got, t, s, i))
			}
			other := mt.In(1)
			if !other.Implements(targetType) {
				panic(errors.Errorf("expected %T to implement %v for (%T,%s)[%d]", other, targetType, t, s, i))
			}
			if matchers[s] == nil {
				matchers[s] = map[reflect.Type][]func(a targets.Target, b targets.Target) (bool, targets.State){}
			}
			rule := rule
			f := func(a, b targets.Target) (bool, targets.State) {
				out := reflect.ValueOf(rule.matcher).Call([]reflect.Value{reflect.ValueOf(a), reflect.ValueOf(b)})
				if out[0].Bool() {
					return true, rule.s
				}
				return false, 0
			}
			matchers[s][other] = append(matchers[s][other], f)
		}
	}
	return func(g graphBuilder, this targets.Target, thisState targets.State) {
		for t, funcs := range matchers[thisState] {
			if err := g.forEach(func(that targets.Target, thatState targets.State) error {
				if reflect.TypeOf(that) != t {
					return nil
				}
				for i, f := range funcs {
					func() {
						defer func() {
							if r := recover(); r != nil {
								panic(errors.Errorf("%T %v  %T %v %d %p: %v", this, t, that, thisState, i, f, r))
							}
						}()
						if ok, thatState := f(this, that); ok {
							g.addDepEdge(this, thisState, that, thatState)
						}
					}()
				}
				return nil
			}); err != nil {
				panic(err)
			}
		}
	}
}

var (
	compileFlagsTyp = reflect.TypeOf((*CompileFlags)(nil)).Elem()
	opsType         = reflect.TypeOf((*ops.Op)(nil)).Elem()
	boolType        = reflect.TypeOf((*bool)(nil)).Elem()
	targetType      = reflect.TypeOf((*targets.Target)(nil)).Elem()
)

func buildSchemaChangeOpGenFunc(t targets.Target, forward targetOpRules) opGenFunc {
	// We want to walk all of the edges and ensure that they have the proper
	// signature.
	tTyp := reflect.TypeOf(t)
	predicateTyp := reflect.FuncOf(
		[]reflect.Type{tTyp, compileFlagsTyp},
		[]reflect.Type{boolType},
		false, /* variadic */
	)
	opType := reflect.FuncOf(
		[]reflect.Type{tTyp},
		[]reflect.Type{opsType},
		false, /* variadic */
	)
	for s, rules := range forward {
		for i, rule := range rules {
			if rule.nextState == s {
				panic(errors.Errorf("detected rule into same state: %s for %T[%d]", s, t, i))
			}
			if rule.predicate != nil {
				if pt := reflect.TypeOf(rule.predicate); pt != predicateTyp {
					panic(errors.Errorf("invalid predicate with signature %v != %v for %T[%d]", pt, predicateTyp, t, i))
				}
			}
			if rule.nextState == targets.State_UNKNOWN {
				if rule.op != nil {
					panic(errors.Errorf("invalid stopping rule with non-nil op func for %T[%d]", t, i))
				}
				continue
			}
			if rule.nextState != targets.State_UNKNOWN && rule.op == nil {
				panic(errors.Errorf("invalid nil op with next state %s for %T[%d]", rule.nextState, t, i))
			}
			if ot := reflect.TypeOf(rule.op); ot != opType {
				panic(errors.Errorf("invalid ops with signature %v != %v %p %p for (%T, %s)[%d]", ot, opType, ot, opsType, t, s, i))
			}
		}
	}

	return func(builder graphBuilder, t targets.Target, s targets.State, flags CompileFlags) {
		cur := s
		tv := reflect.ValueOf(t)
		flagsV := reflect.ValueOf(flags)
		predicateArgs := []reflect.Value{tv, flagsV}
		opsArgs := []reflect.Value{tv}
	outer:
		for {
			rules := forward[cur]
			for _, rule := range rules {
				if rule.predicate != nil {
					if out := reflect.ValueOf(rule.predicate).Call(predicateArgs); !out[0].Bool() {
						continue
					}
				}
				if rule.nextState == targets.State_UNKNOWN {
					return
				}
				out := reflect.ValueOf(rule.op).Call(opsArgs)
				builder.addOpEdge(t, cur, rule.nextState, out[0].Interface().(ops.Op))
				cur = rule.nextState
				continue outer
			}
			break
		}
	}
}

type nodeFunc func(target targets.Target, s targets.State) error

type graph interface {
	forEach(it nodeFunc) error
}

type graphBuilder interface {
	graph
	addOpEdge(
		t targets.Target,
		cur, next targets.State,
		op ops.Op,
	) (nextState targets.State)
	addDepEdge(
		fromTarget targets.Target,
		fromState targets.State,
		toTarget targets.Target,
		toState targets.State,
	)
}
