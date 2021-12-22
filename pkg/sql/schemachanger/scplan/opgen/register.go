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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type addSpec struct {
	transitionSpecs []transitionSpec
}

type dropSpec struct {
	transitionSpecs []transitionSpec
}

// equiv defines an equivalence mapping.
func equiv(from, to scpb.Status) transitionSpec {
	return transitionSpec{
		from: from,
		to:   to,
	}
}

func notImplemented(e scpb.Element) *scop.NotImplemented {
	return &scop.NotImplemented{
		ElementType: reflect.ValueOf(e).Type().Elem().String(),
	}
}

func buildTransitionSpecs(initial, final scpb.Status, specs []transitionSpec) []transitionSpec {
	ts := make([]transitionSpec, 0, len(specs))
	from := initial
	isEquivMapped := map[scpb.Status]bool{from: true}
	seqTransitionTo := map[scpb.Status]transitionSpec{}
	seqTransitionFrom := map[scpb.Status]transitionSpec{}
	var currentMinPhase scop.Phase
	isRevertible := true
	for _, s := range specs {
		if s.from == scpb.Status_UNKNOWN {
			// Check validity of target status.
			if s.to == scpb.Status_UNKNOWN {
				panic(errors.Errorf("invalid transition to %s: invalid target status %s",
					s.to, s.to))
			}
			if _, found := seqTransitionTo[s.to]; found {
				panic(errors.Errorf("invalid transition to %s: %s was featured as 'to' in a previous transition",
					s.to, s.to))
			} else if isEquivMapped[s.to] {
				panic(errors.Errorf("invalid transition to %s: %s was featured as 'from' in a previous equivalence mapping",
					s.to, s.to))
			}
			// Check that the transition spec will emit stuff.
			if len(s.emitFns) == 0 {
				panic(errors.Errorf("invalid transition to %s: not emitting anything",
					s.to))
			}
			// Check that the minimum phase is monotonically increasing.
			if s.minPhase > 0 && s.minPhase < currentMinPhase {
				panic(errors.Errorf("invalid transition to %s: minimum phase %s is less than inherited mininum phase %s",
					s.to, s.minPhase.String(), currentMinPhase.String()))
			}

			isRevertible = isRevertible && s.revertible
			if s.minPhase > currentMinPhase {
				currentMinPhase = s.minPhase
			}
			s.from = from
			s.minPhase = currentMinPhase
			s.revertible = isRevertible
			ts = append(ts, s)
			from = s.to
			isEquivMapped[from] = true
			seqTransitionTo[s.to] = s
			seqTransitionFrom[s.from] = s
		} else {
			// Check validity of status pair.
			if s.from == s.to {
				panic(errors.Errorf("invalid equivalence mapping %s -> %s: statuses are identical",
					s.from, s.to))
			} else if s.to == scpb.Status_UNKNOWN {
				panic(errors.Errorf("invalid equivalence mapping %s -> %s: invalid target status %s",
					s.from, s.to, s.to))
			}
			// Check validity of origin status.
			if _, found := seqTransitionTo[s.from]; found {
				panic(errors.Errorf("invalid equivalence mapping %s -> %s: %s was featured as 'to' in a previous transition",
					s.from, s.to, s.from))
			} else if isEquivMapped[s.from] {
				panic(errors.Errorf("invalid equivalence mapping %s -> %s: %s was featured as 'from' in a previous equivalence mapping",
					s.from, s.to, s.from))
			}
			// Check validity of target status.
			spec, found := seqTransitionFrom[s.to]
			if !found {
				panic(errors.Errorf("invalid equivalence mapping %s -> %s: %s was not featured as 'from' in any previous transition",
					s.from, s.to, s.to))
			}

			spec.from = s.from
			ts = append(ts, spec)
			isEquivMapped[s.from] = true
		}
	}

	// Check that the final status has been reached.
	if from != final {
		panic(errors.Errorf("expected %s as the final status, instead found %s", final, from))
	}

	return ts
}

func add(specs ...transitionSpec) addSpec {
	return addSpec{
		transitionSpecs: buildTransitionSpecs(scpb.Status_ABSENT, scpb.Status_PUBLIC, specs),
	}
}

func drop(specs ...transitionSpec) dropSpec {
	return dropSpec{
		transitionSpecs: buildTransitionSpecs(scpb.Status_PUBLIC, scpb.Status_ABSENT, specs),
	}
}

// register constructs the add and drop operations edges for a given element.
// Intended to be called during init, register panics on any error.
func (r *registry) register(e scpb.Element, add addSpec, drop dropSpec) {
	type statusPair struct {
		from, to scpb.Status
	}

	edges := map[statusPair]int{}
	allStatuses := map[scpb.Status]bool{}
	traverseSpecs := func(specs []transitionSpec, m map[scpb.Status]bool) {
		for _, ts := range specs {
			m[ts.from] = true
			allStatuses[ts.from] = true
			m[ts.to] = true
			allStatuses[ts.to] = true
			edge := statusPair{from: ts.from, to: ts.to}
			edges[edge] = edges[edge] + 1
		}
	}

	validAddStatuses := map[scpb.Status]bool{}
	traverseSpecs(add.transitionSpecs, validAddStatuses)
	validDropStatuses := map[scpb.Status]bool{}
	traverseSpecs(drop.transitionSpecs, validDropStatuses)
	for s := range allStatuses {
		if !validAddStatuses[s] {
			panic(errors.Errorf("status %s is featured in drop spec but not in add spec", s))
		}
		if !validDropStatuses[s] {
			panic(errors.Errorf("status %s is featured in add spec but not in drop spec", s))
		}
	}
	for edge, n := range edges {
		if n > 1 {
			panic(errors.Errorf("edge %s -> %s is featured %d times", edge.from, edge.to, n))
		}
	}

	r.targets = append(r.targets,
		makeTarget(e, scpb.Status_PUBLIC, add.transitionSpecs...),
		makeTarget(e, scpb.Status_ABSENT, drop.transitionSpecs...),
	)
}
