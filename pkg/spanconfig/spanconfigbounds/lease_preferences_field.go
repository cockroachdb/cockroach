// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type leasePreferencesField int

var _ field[[]roachpb.LeasePreference] = leasePreferencesField(0)

func (f leasePreferencesField) String() string {
	return config.Field(f).String()
}

func (f leasePreferencesField) SafeFormat(s redact.SafePrinter, verb rune) {
	s.Print(config.Field(f))
}

func (f leasePreferencesField) FieldValue(sc *roachpb.SpanConfig) Value {
	if v := f.fieldValue(sc); v != nil {
		return (*leasePreferencesValue)(v)
	}
	return nil
}

func (f leasePreferencesField) FieldBound(b *Bounds) ValueBounds {
	if b.ConstraintBounds == nil {
		return unbounded{}
	}
	switch f {
	case leasePreferences:
		return (*leasePreferencesBound)(b.ConstraintBounds)
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field spanConfigBound %s", f))
	}
}

func (f leasePreferencesField) fieldValue(c *roachpb.SpanConfig) *[]roachpb.LeasePreference {
	switch f {
	case leasePreferences:
		return &c.LeasePreferences
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field %s", f))
	}
}

type leasePreferencesBound tenantcapabilitiespb.SpanConfigBounds_ConstraintBounds

func (c *leasePreferencesBound) SafeFormat(s redact.SafePrinter, verb rune) {
	(*constraintsConjunctionBounds)(c).SafeFormat(s, verb)
}

func (c *leasePreferencesBound) String() string {
	return (*constraintsConjunctionBounds)(c).String()
}

func (c *leasePreferencesBound) conforms(t *roachpb.SpanConfig, f Field) (conforms bool) {
	s := sortedConstraints(c.Allowed)
	v := f.(field[[]roachpb.LeasePreference]).fieldValue(t)
	for i := range *v {
		if !s.conjunctionConforms((*v)[i].Constraints) {
			return false
		}
	}
	return true
}

func (c *leasePreferencesBound) clamp(t *roachpb.SpanConfig, f Field) (changed bool) {
	// The clamping logic for lease preferences removes all entries from the
	// slice which do not conform with the bounds.
	s := sortedConstraints(c.Allowed)
	v := f.(field[[]roachpb.LeasePreference]).fieldValue(t)
	var truncated []roachpb.LeasePreference
	for i := range *v {
		conforms := s.conjunctionConforms((*v)[i].Constraints)
		if conforms && truncated != nil {
			truncated = append(truncated, (*v)[i])
		} else if !conforms && truncated == nil {
			truncated = (*v)[:i]
		}
	}
	if truncated != nil {
		*v = truncated
		return true
	}
	return false
}
