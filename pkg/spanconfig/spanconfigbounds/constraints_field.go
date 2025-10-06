// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type constraintsConjunctionField int

var _ field[[]roachpb.ConstraintsConjunction] = constraintsConjunctionField(0)

func (f constraintsConjunctionField) String() string {
	return config.Field(f).String()
}

func (f constraintsConjunctionField) SafeFormat(s redact.SafePrinter, verb rune) {
	s.Print(config.Field(f))
}

func (f constraintsConjunctionField) FieldValue(sc *roachpb.SpanConfig) Value {
	if v := f.fieldValue(sc); v != nil {
		return (*constraintsConjunctionValue)(v)
	}
	return nil
}

func (f constraintsConjunctionField) FieldBound(b *Bounds) ValueBounds {
	if b.ConstraintBounds == nil {
		return unbounded{}
	}
	switch f {
	case constraints, voterConstraints:
		return (*constraintsConjunctionBounds)(b.ConstraintBounds)
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field spanConfigBound %s", f))
	}
}

func (f constraintsConjunctionField) fieldValue(
	c *roachpb.SpanConfig,
) *[]roachpb.ConstraintsConjunction {
	switch f {
	case voterConstraints:
		return &c.VoterConstraints
	case constraints:
		return &c.Constraints
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field %s", f))
	}
}

type constraintsConjunctionBounds tenantcapabilitiespb.SpanConfigBounds_ConstraintBounds

func (c *constraintsConjunctionBounds) String() string {
	return redact.Sprint(c).StripMarkers()
}

func formatConstraintsConjunction(
	s redact.SafePrinter, conj []tenantcapabilitiespb.SpanConfigBounds_ConstraintsConjunction,
) {
	s.Printf("[")
	for i, cc := range conj {
		if i > 0 {
			s.Printf(", ")
		}
		formatConstraints(s, cc.Constraints)
	}
	s.Printf("]")
}

func formatConstraints(s redact.SafePrinter, c []roachpb.Constraint) {
	s.Printf("[")
	for i, b := range c {
		if i > 0 {
			s.Printf(", ")
		}
		s.Printf("{")
		switch b.Type {
		case roachpb.Constraint_REQUIRED:
			s.Printf("+")
		case roachpb.Constraint_PROHIBITED:
			s.Printf("-")
		default:
			_, _ = fmt.Fprintf(s, "(unknown type %d)", b.Type)
		}
		if b.Key != "" {
			s.Printf("%s=", b.Key)
		}
		s.Printf("%s}", b.Value)
	}
	s.Printf("]")
}

func (c *constraintsConjunctionBounds) SafeFormat(s redact.SafePrinter, verb rune) {
	s.Printf("{allowed: ")
	formatConstraints(s, c.Allowed)
	s.Printf(", fallback: ")
	formatConstraintsConjunction(s, c.Fallback)
	s.Printf("}")
}

func (c *constraintsConjunctionBounds) conforms(t *roachpb.SpanConfig, f Field) (conforms bool) {
	s := sortedConstraints(c.Allowed)
	constraints := f.(field[[]roachpb.ConstraintsConjunction]).fieldValue(t)
	for i := range *constraints {
		if !s.conjunctionConforms((*constraints)[i].Constraints) {
			return false
		}
	}
	return len(*constraints) > 0 || len(c.Fallback) == 0
}

func (c *constraintsConjunctionBounds) clamp(t *roachpb.SpanConfig, f Field) (changed bool) {
	if c.conforms(t, f) {
		return false
	}

	// This is a weird special case where we have no fallback constraints.
	// It's not clear why this would happen, but the semantics are going to
	// be that it means that the data is now unconstrained.
	if len(c.Fallback) == 0 {
		*f.(field[[]roachpb.ConstraintsConjunction]).fieldValue(t) = nil
		return true
	}

	quorumMinusOne := func(v int32) int32 {
		ret := v / 2
		if ret%2 == 0 {
			ret--
		}
		return ret
	}
	distributeFallbackConstraints := func(toDistribute int32) []roachpb.ConstraintsConjunction {
		rem := toDistribute
		regions := len(c.Fallback)
		ret := make([]roachpb.ConstraintsConjunction, 0, regions)
		for i, r := range c.Fallback {
			numReplicas := rem / int32(regions-i)
			rem -= numReplicas
			ret = append(ret, roachpb.ConstraintsConjunction{
				NumReplicas: numReplicas,
				Constraints: r.Constraints,
			})
		}
		return ret
	}

	// Otherwise, we're going to use the fallback. Here we'll consult the
	// field to determine the policy.
	switch f {
	case voterConstraints:
		// When controlled by the multi-region locality syntax, voter constraints
		// either attempt to state exactly one region, either for all replicas or
		// for quorum less 1. We'll treat this situation as something special, and
		// we'll use the first constraint in fallback to mirror this policy.
		existing := t.VoterConstraints
		switch len(existing) {
		case 1:
			switch existing[0].NumReplicas {
			case 0:
				t.VoterConstraints = []roachpb.ConstraintsConjunction{
					{Constraints: c.Fallback[0].Constraints},
				}
				return true
			default:
				// We're going to assume that if there's one entry in
				// voter_constraints, and it has a required number of replicas, this
				// is attempting to a subset of replicas smaller than a quorum here.
				// We'll mirror that intention, but for the first region in fallbacks.

				t.VoterConstraints = []roachpb.ConstraintsConjunction{
					{
						Constraints: c.Fallback[0].Constraints,
						NumReplicas: quorumMinusOne(t.NumVoters),
					},
				}
				return true
			}
		default:
			// If there's more than one value in the constraints, we're going to
			// assume that the intention is to spread the replicas over the allowed
			// regions. This doesn't happen with the MR primitives, but we need a
			// well-defined behavior.
			t.VoterConstraints = distributeFallbackConstraints(t.NumVoters)
			return true
		}
	case constraints:
		// Normally we wouldn't constrain more than one replica in a given region.
		// It's not great to be so prescriptive, but we're forced into it because
		// if we don't place all the replicas, the allocator will place the extra
		// replicas in regions outside the fallback. That's not okay.
		t.Constraints = distributeFallbackConstraints(t.NumReplicas)
		return true
	default:
		panic(errors.AssertionFailedf("failed to clamp constraints in unknown field %v", f))
	}
}

type sortedConstraints []roachpb.Constraint

func (s sortedConstraints) conjunctionConforms(conj []roachpb.Constraint) (conforms bool) {
	for i := range conj {
		if !s.constraintConforms(&conj[i]) {
			return false
		}
	}
	return true
}

func (s sortedConstraints) constraintConforms(c *roachpb.Constraint) (conforms bool) {
	idx := sort.Search(len(s), func(i int) bool {
		return !compareConstraints(&s[i], c)
	})
	return idx < len(s) && !compareConstraints(c, &s[idx])
}

var _ sort.Interface = (sortedConstraints)(nil)

func (s sortedConstraints) Len() int { return len(s) }
func (s sortedConstraints) Less(i, j int) bool {
	return compareConstraints(&s[i], &s[j])
}
func (s sortedConstraints) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func compareConstraints(a, b *roachpb.Constraint) bool {
	switch {
	case a.Type != b.Type:
		return a.Type < b.Type
	case a.Key != b.Key:
		return a.Key < b.Key
	default:
		return a.Value < b.Value
	}
}
