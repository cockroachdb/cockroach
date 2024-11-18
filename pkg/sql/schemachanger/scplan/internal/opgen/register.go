// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

// equiv defines the from status as being equivalent to the current status.
func equiv(from scpb.Status) transitionSpec {
	return transitionSpec{from: from, revertible: nil}
}

func notImplemented(e scpb.Element) *scop.NotImplemented {
	return &scop.NotImplemented{
		ElementType: reflect.ValueOf(e).Type().Elem().String(),
	}
}

func notImplementedForPublicObjects(e scpb.Element) *scop.NotImplementedForPublicObjects {
	return &scop.NotImplementedForPublicObjects{
		ElementType: reflect.ValueOf(e).Type().Elem().String(),
		DescID:      screl.GetDescID(e),
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

func toTransientAbsentLikePublic() targetSpec {
	return asTargetSpec(scpb.Status_TRANSIENT_ABSENT, scpb.Status_ABSENT)
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
	fullTargetSpecs, err := populateAndValidateSpecs(targetSpecs)
	onErrPanic(err)
	targets, err := buildTargets(e, fullTargetSpecs)
	onErrPanic(err)
	onErrPanic(validateTargets(targets))
	start := len(r.targets)
	r.targets = append(r.targets, targets...)
	elType := reflect.TypeOf(e)
	for i := range targets {
		r.targetMap[makeTargetKey(elType, targets[i].status)] = start + i
	}

}

func makeTargetKey(elType reflect.Type, status scpb.Status) targetKey {
	return targetKey{
		elType:       elType,
		targetStatus: status,
	}
}

func populateAndValidateSpecs(targetSpecs []targetSpec) ([]targetSpec, error) {
	var absentSpec, publicSpec, transientSpec *targetSpec
	for i := range targetSpecs {
		s := &targetSpecs[i]
		var p **targetSpec
		switch s.to {
		case scpb.Status_ABSENT:
			p = &absentSpec
		case scpb.Status_PUBLIC:
			p = &publicSpec
		case scpb.Status_TRANSIENT_ABSENT:
			p = &transientSpec
		default:
			return nil, errors.Errorf("unsupported target %s", s.to)
		}
		if *p != nil {
			return nil, errors.Errorf("duplicate %s spec", s.to)
		}
		if s.to != scpb.Status_ABSENT && s.from != scpb.Status_ABSENT {
			return nil, errors.Errorf("expected %s spec to start in ABSENT, not %s", s.to, s.from)
		}
		*p = s
	}
	if absentSpec == nil {
		return nil, errors.Errorf("ABSENT spec is missing but required")
	}
	if transientSpec != nil {
		if publicSpec != nil && len(transientSpec.transitionSpecs) == 0 {
			// Here we want the transient spec to be a copy of the public spec.
			transientSpec.transitionSpecs = append(transientSpec.transitionSpecs, publicSpec.transitionSpecs...)
		}
		if err := populateTransientAbsent(absentSpec, transientSpec); err != nil {
			return nil, err
		}
	}
	specs := make([]targetSpec, 1, 3)
	specs[0] = *absentSpec
	if publicSpec != nil {
		specs = append(specs, *publicSpec)
	}
	if transientSpec != nil {
		specs = append(specs, *transientSpec)
	}
	for _, s := range specs {
		if len(s.transitionSpecs) == 0 {
			return nil, errors.Errorf("no transition specs found for %s spec", s.to)
		}
	}
	return specs, nil
}

// populateTransientAbsent takes the targetSpecs to ABSENT and TRANSIENT_ABSENT
// and arranges for the operations to ABSENT to occur also in the plans for the
// elements to TRANSIENT_ABSENT. The idea is that for each of the to ABSENT
// edges, there will be equivalent edges for the target to TRANSIENT_ABSENT
// with the status of the latter sequence containing the TRANSIENT_ prefix.
//
// Note that this function directly mutates the passed targetSpecs.
func populateTransientAbsent(absentSpec, transientSpec *targetSpec) error {

	// Begin by finding the position in the ABSENT transition specs which match
	// the end of the existing TRANSIENT_ABSENT spec.
	ats := absentSpec.transitionSpecs
	tts := transientSpec.transitionSpecs
	var startIdx int
	if initial := tts[len(tts)-1].to; absentSpec.from != initial {
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
		if next.to == scpb.Status_UNKNOWN {
			nextFrom, ok := scpb.GetTransientEquivalent(next.from)
			if !ok {
				return errors.AssertionFailedf(
					"failed to find transient equivalent for 'from' %s", next.from,
				)
			}
			next.from = nextFrom
			atsWithEquiv = append(atsWithEquiv, equiv(next.from))
		} else {
			nextTo, ok := scpb.GetTransientEquivalent(next.to)
			if !ok {
				return errors.AssertionFailedf(
					"failed to find transient equivalent for 'to' %s", next.to,
				)
			}
			next.to = nextTo
			atsWithEquiv = append(atsWithEquiv, equiv(next.to))
		}
		tts = append(tts, next)
	}

	transientSpec.transitionSpecs = tts
	absentSpec.transitionSpecs = atsWithEquiv
	return nil
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
	absentStatuses := map[scpb.Status]bool{}
	nonAbsentStatuses := map[scpb.Status]bool{}
	for i, tgt := range targets {
		var m map[scpb.Status]bool
		if i == 0 {
			m = absentStatuses
		} else {
			m = nonAbsentStatuses
		}
		for _, t := range tgt.transitions {
			m[t.from] = true
			allStatuses[t.from] = true
			m[t.to] = true
			allStatuses[t.to] = true
		}
	}

	for s := range allStatuses {
		if nonAbsentStatuses[s] && !absentStatuses[s] {
			return errors.Errorf("status %s is featured in non-ABSENT targets but not in the ABSENT target", s)
		}
		if absentStatuses[s] && !nonAbsentStatuses[s] {
			return errors.Errorf("status %s is featured in ABSENT target but not in any non-ABSENT targets", s)
		}
	}
	return nil
}
