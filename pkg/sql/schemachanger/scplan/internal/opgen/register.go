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

// equiv defines the from status as being equivalent to the current status.
func equiv(from scpb.Status) transitionSpec {
	return transitionSpec{from: from, revertible: true}
}

func notImplemented(e scpb.Element) *scop.NotImplemented {
	return &scop.NotImplemented{
		ElementType: reflect.ValueOf(e).Type().Elem().String(),
	}
}

func toPublic(initialStatus scpb.Status, specs ...transitionSpec) targetSpec {
	return asTargetSpec(scpb.Status_PUBLIC, initialStatus, specs...)
}

func toAbsent(initialStatus scpb.Status, specs ...transitionSpec) targetSpec {
	return asTargetSpec(scpb.Status_ABSENT, initialStatus, specs...)
}

func toTransientAbsent(initalStatus scpb.Status, specs ...transitionSpec) targetSpec {
	return asTargetSpec(scpb.Status_TRANSIENT_ABSENT, initalStatus, specs...)
}

func asTargetSpec(to, from scpb.Status, specs ...transitionSpec) targetSpec {
	return targetSpec{from: from, to: to, transitionSpecs: specs}
}

// register constructs all operations edges for a given element.
// Intended to be called during init, register panics on any error.
func (r *registry) register(e scpb.Element, targetSpecs ...targetSpec) {
	onErrPanic := func(err error) {
		if err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "element %T", e))
		}
	}

	onErrPanic(expandTransientAbsentSpec(targetSpecs))
	targets, err := buildTargets(e, targetSpecs)
	onErrPanic(err)
	onErrPanic(validateTargets(targets))
	r.targets = append(r.targets, targets...)
}

// Expand the definition of the TRANSIENT_ABSENT targetSpec according to
// the transitions in the ABSENT targetSpec if they exist.
func expandTransientAbsentSpec(targetSpecs []targetSpec) error {
	toAbsent, transient, err := findToAbsentAndTransient(targetSpecs)
	if err != nil || transient == nil {
		return err
	}
	return populateTransientAbsent(toAbsent, transient)
}

// populateTransientAbsent takes the targetSpecs to ABSENT and TRANSIENT_ABSENT
// and arranges for the operations to ABSENT to occur also in the plans for the
// elements to TRANSIENT_ABSENT. The idea is that for each of the to ABSENT
// edges, there will be equivalent edges for the target to TRANSIENT_ABSENT
// with the status of the latter sequence containing the TRANSIENT_ prefix.
//
// Note that this function directly mutates the passed targetSpecs.
func populateTransientAbsent(toAbsent, transient *targetSpec) error {

	// Begin by finding the position in the ABSENT transition specs which match
	// the end of the existing TRANSIENT_ABSENT spec.
	tts := transient.transitionSpecs
	ats := toAbsent.transitionSpecs
	var startIdx int
	if initial := tts[len(tts)-1].to; toAbsent.from != initial {
		startIdx = findTransitionTo(ats, initial) + 1
		if startIdx == 0 {
			return errors.AssertionFailedf(
				"cannot find transition starting in %v in the transitionSpecs for ABSENT",
				initial,
			)
		}
	}

	// atsWithEquiv is an augmented copy of the toAbsent transitions which will
	// be populated additionally with equiv transitions for the relevant
	// TRANSIENT_ statuses we're adding for the target to TRANSIENT_ABSENT.
	atsWithEquiv := ats[:startIdx:startIdx]

	// Fill out both the tail of atsWithEquiv and tts with the appropriate
	// transitionSpecs.
	for i := startIdx; i < len(ats); i++ {
		next := ats[i]
		atsWithEquiv = append(atsWithEquiv, next)

		// NOTE: If the toAbsent transitions have any equiv definitions
		// we'll fail to find them because we won't find a transient equivalent.
		// For now, this is fine, but we may later need to decide to skip them or
		// add them in some other way.
		nextTo, ok := scpb.GetTransientEquivalent(next.to)
		if !ok {
			return errors.AssertionFailedf(
				"failed to find transient equivalent for %v", next.to,
			)
		}
		next.to = nextTo
		tts = append(tts, next)
		atsWithEquiv = append(atsWithEquiv, equiv(next.to))
	}

	transient.transitionSpecs = tts
	toAbsent.transitionSpecs = atsWithEquiv
	return nil
}

// findToAbsentAndTransient searches the slice of targetSpec for the two
// targets to ABSENT and TRANSIENT_ABSENT. An error is returned if either
// such target is defined more than once of if TRANSIENT_ABSENT is defined
// but ABSENT is not. If TRANSIENT_ABSENT is not defined, neither targetSpec
// will be returned; it is not an error for just ABSENT to be defined or
// for neither to be defined.
func findToAbsentAndTransient(specs []targetSpec) (toAbsent, transient *targetSpec, err error) {
	findTargetSpec := func(to scpb.Status) (*targetSpec, error) {
		i := findTargetSpecTo(specs, to)
		if i == -1 {
			return nil, nil
		}
		if also := findTargetSpecTo(specs[i+1:], to); also != -1 {
			return nil, errors.Errorf("duplicate spec to %v", to)
		}
		return &specs[i], nil
	}
	if transient, err = findTargetSpec(
		scpb.Status_TRANSIENT_ABSENT,
	); err != nil || transient == nil {
		return nil, nil, err
	}
	if toAbsent, err = findTargetSpec(scpb.Status_ABSENT); err != nil {
		return nil, nil, err
	}
	if toAbsent == nil {
		return nil, nil, errors.AssertionFailedf(
			"cannot have %v target without a %v target",
			scpb.Status_TRANSIENT_ABSENT, scpb.Status_ABSENT,
		)
	}
	return toAbsent, transient, nil
}

func findTargetSpecTo(haystack []targetSpec, needle scpb.Status) int {
	for i, spec := range haystack {
		if spec.to == needle {
			return i
		}
	}
	return -1
}

func findTransitionTo(haystack []transitionSpec, needle scpb.Status) int {
	for i, transition := range haystack {
		if transition.from == needle {
			return i
		}
	}
	return -1
}

func buildTargets(e scpb.Element, targetSpecs []targetSpec) ([]target, error) {
	targets := make([]target, len(targetSpecs))
	for i, spec := range targetSpecs {
		var err error
		if targets[i], err = makeTarget(e, spec); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func validateTargets(targets []target) error {
	allStatuses := map[scpb.Status]bool{}
	targetStatuses := make([]map[scpb.Status]bool, len(targets))
	for i, tgt := range targets {
		m := map[scpb.Status]bool{}
		for _, t := range tgt.transitions {
			m[t.from] = true
			allStatuses[t.from] = true
			m[t.to] = true
			allStatuses[t.to] = true
		}
		targetStatuses[i] = m
	}

	for i, tgt := range targets {
		m := targetStatuses[i]
		for s := range allStatuses {
			if !m[s] {
				return errors.Errorf("target %s: status %s is missing here but is featured in other targets",
					tgt.status.String(), s.String())
			}
		}
	}
	return nil
}
