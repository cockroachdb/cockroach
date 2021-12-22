// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	iterateFunc func(*rel.Database, func(*scpb.Node) error) error
}

// transition represents a transition of a target to a new status.
type transition struct {
	from, to   scpb.Status
	revertible bool
	ops        opsFunc
	minPhase   scop.Phase
}

func makeTarget(e scpb.Element, status scpb.Status, specs ...transitionSpec) target {
	defer decoratePanickedError(func(err error) error {
		return errors.Wrapf(err, "making target %T:%s", e, status.String())
	})()
	return target{
		e:           e,
		status:      status,
		transitions: makeTransitions(e, specs),
		iterateFunc: makeQuery(e, status),
	}
}

func makeTransitions(e scpb.Element, specs []transitionSpec) []transition {
	transitions := make([]transition, 0, len(specs))
	for _, s := range specs {
		fn, err := makeOpsFunc(e, s.emitFns)
		if err != nil {
			panic(errors.Wrapf(err, "building transition from %v->%v", s.from, s.to))
		}
		transitions = append(transitions, transition{
			from:       s.from,
			to:         s.to,
			revertible: s.revertible,
			ops:        fn,
			minPhase:   s.minPhase,
		})
	}
	return transitions
}

func makeQuery(e scpb.Element, ts scpb.Status) func(*rel.Database, func(*scpb.Node) error) error {
	var element, target, node, targetStatus rel.Var = "element", "target", "node", "target-status"
	q, err := rel.NewQuery(screl.Schema,
		element.Type(e),
		targetStatus.Eq(ts),
		screl.JoinTargetNode(element, target, node),
		target.AttrEqVar(screl.TargetStatus, targetStatus),
	)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to construct query"))
	}
	return func(database *rel.Database, f func(*scpb.Node) error) error {
		return q.Iterate(database, func(r rel.Result) error {
			return f(r.Var(node).(*scpb.Node))
		})
	}
}

func decoratePanickedError(f func(error) error) func() {
	return func() {
		var err error
		switch r := recover().(type) {
		case nil:
			return
		case error:
			err = r
		default:
			err = errors.AssertionFailedf("%v", r)
		}
		panic(f(err))
	}
}
