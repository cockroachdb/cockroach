// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

// target represents the operation generation rules for a given Target.
type target struct {
	e           scpb.Element
	status      scpb.Status
	transitions []transition
	iterateFunc func(*rel.Database, func(*screl.Node) error) error
}

// transition represents a transition from one status to the next towards a
// Target.
type transition struct {
	from, to   scpb.Status
	revertible RevertibleFn
	canFail    bool
	ops        opsFunc
	opType     scop.Type
	isEquiv    bool // True if this transition comes from a `equiv` spec.
}

func (t transition) OpType() scop.Type {
	return t.opType
}

func (t transition) From() scpb.Status {
	return t.from
}

func (t transition) To() scpb.Status {
	return t.to
}

func makeTarget(e scpb.Element, spec targetSpec) (t target, err error) {
	defer func() {
		err = errors.Wrapf(err, "target %s", spec.to)
	}()
	t = target{
		e:      e,
		status: spec.to,
	}
	t.transitions, err = makeTransitions(e, spec)
	if err != nil {
		return t, err
	}

	// Make iterator function for traversing graph nodes with this target.
	var element, target, node, targetStatus rel.Var = "element", "target", "node", "target-status"
	q, err := rel.NewQuery(screl.Schema,
		element.Type(e),
		targetStatus.Eq(spec.to),
		screl.JoinTargetNode(element, target, node),
		target.AttrEqVar(screl.TargetStatus, targetStatus),
	)
	if err != nil {
		return t, errors.Wrap(err, "failed to construct query")
	}
	t.iterateFunc = func(database *rel.Database, f func(*screl.Node) error) error {
		return q.Iterate(database, nil, func(r rel.Result) error {
			return f(r.Var(node).(*screl.Node))
		})
	}

	return t, nil
}

// makeTransitions constructs a slice of transitions from the transition spec, as specified in `opgen_xx.go` file.
// An example can nicely explain how it works.
// Example 1:
//
//	Input: toAbsent(PUBLIC, equiv(VALIDATED), to(WRITE_ONLY), to(ABSENT))
//	Output: [VALIDATED, PUBLIC], [PUBLIC, WRITE_ONLY], [WRITE_ONLY, ABSENT]
//
// Example 2:
//
//	Input: toPublic(ABSENT, to(DELETE_ONLY), to(WRITE_ONLY), to(PUBLIC))
//	Output: [ABSENT, DELETE_ONLY], [DELETE_ONLY, WRITE_ONLY], [WRITE_ONLY, PUBLIC].
//
// Right, nothing too surprising except that the trick we use for `equiv(s)` is to encode it
// as a transition from `s` to whatever the current status is.
func makeTransitions(e scpb.Element, spec targetSpec) (ret []transition, err error) {
	tbs := makeTransitionBuildState(spec.from)

	// lastTransitionWhichCanFail tracks the index in ret of the last transition
	// corresponding to a backfill or validation. We'll use this to add an
	// annotation to the transitions indicating whether such an operation exists
	// in the current or any subsequent transitions.
	lastTransitionWhichCanFail := -1
	for i, s := range spec.transitionSpecs {
		var t transition
		if s.from == scpb.Status_UNKNOWN {
			// Construct a transition `tbs.from --> s.to`, which comes from a `to(...)` spec.
			t.from = tbs.from
			t.to = s.to
			if err := tbs.withTransition(s, i == 0 /* isFirst */); err != nil {
				return nil, errors.Wrapf(
					err, "invalid transition %s -> %s", t.from, t.to,
				)
			}
			if len(s.emitFns) > 0 {
				t.ops, t.opType, err = makeOpsFunc(e, s.emitFns)
				if err != nil {
					return nil, errors.Wrapf(
						err, "making ops func for transition %s -> %s", t.from, t.to,
					)
				}
			}
		} else {
			// Construct a transition `s.from --> tbs.from`, which comes from a `equiv(...)` spec.
			t.from = s.from
			t.to = tbs.from
			if err := tbs.withEquivTransition(s); err != nil {
				return nil, errors.Wrapf(
					err, "invalid no-op transition %s -> %s", t.from, t.to,
				)
			}
			t.isEquiv = true
		}
		t.revertible = tbs.isRevertible
		if t.opType != scop.MutationType && t.opType != 0 {
			lastTransitionWhichCanFail = i
		}
		ret = append(ret, t)
	}

	// Mark the transitions which can fail or precede something which can fail.
	for i := 0; i <= lastTransitionWhichCanFail; i++ {
		ret[i].canFail = true
	}

	// Check that the final status has been reached.
	if tbs.from != spec.to {
		return nil, errors.Errorf(
			"expected %s as the final status, instead found %s", spec.to, tbs.from,
		)
	}

	return ret, nil
}

type transitionBuildState struct {
	from         scpb.Status
	isRevertible RevertibleFn

	isEquivMapped map[scpb.Status]bool
	isTo          map[scpb.Status]bool
	isFrom        map[scpb.Status]bool
}

func makeTransitionBuildState(from scpb.Status) transitionBuildState {
	return transitionBuildState{
		from:          from,
		isRevertible:  nil,
		isEquivMapped: map[scpb.Status]bool{from: true},
		isTo:          map[scpb.Status]bool{},
		isFrom:        map[scpb.Status]bool{},
	}
}

func (tbs *transitionBuildState) withTransition(s transitionSpec, isFirst bool) error {
	// Check validity of target status.
	if s.to == scpb.Status_UNKNOWN {
		return errors.Errorf("invalid 'to' status")
	}
	if tbs.isTo[s.to] {
		return errors.Errorf("%s was featured as 'to' in a previous transition", s.to)
	} else if tbs.isEquivMapped[s.to] {
		return errors.Errorf("%s was featured as 'from' in a previous equivalence mapping", s.to)
	}

	if tbs.isRevertible != nil && s.revertible != nil {
		oldRevertibleFn := tbs.isRevertible
		tbs.isRevertible = func(e scpb.Element, state *opGenContext) bool {
			return oldRevertibleFn(e, state) && s.revertible(e, state)
		}
	} else if tbs.isRevertible == nil {
		tbs.isRevertible = s.revertible
	}
	tbs.isEquivMapped[tbs.from] = true
	tbs.isTo[s.to] = true
	tbs.isFrom[tbs.from] = true
	tbs.from = s.to
	return nil
}

func (tbs *transitionBuildState) withEquivTransition(s transitionSpec) error {
	// Check validity of status pair.
	if s.to != scpb.Status_UNKNOWN {
		return errors.Errorf("invalid 'to' status %s", s.to)
	}

	// Check validity of origin status.
	if tbs.isTo[s.from] {
		return errors.Errorf("%s was featured as 'to' in a previous transition", s.from)
	} else if tbs.isEquivMapped[s.from] {
		return errors.Errorf("%s was featured as 'from' in a previous equivalence mapping", s.from)
	}

	// The transition above is equivalent, so it cannot override the revertibility
	// in any way.
	if s.revertible != nil {
		return errors.Errorf("must be revertible")
	}

	tbs.isEquivMapped[s.from] = true
	return nil
}
