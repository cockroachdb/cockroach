// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package spanconfigbounds provides tools to enforce bounds on spanconfigs.
package spanconfigbounds

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TODO(ajwerner): Add benchmarking.

// Bounds wraps the tenantcapabilities.SpanConfigBounds and utilizes its
// policy to interact with SpanConfigs.
type Bounds struct {
	b *tenantcapabilitiespb.SpanConfigBounds
}

// MakeBounds constructs a Bounds from its serialization.
func MakeBounds(b *tenantcapabilitiespb.SpanConfigBounds) Bounds {
	if cb := b.ConstraintBounds; cb != nil {
		sort.Sort(sortedConstraints(cb.Allowed))
	}
	return Bounds{b: b}
}

// Clamp will update the SpanConfig in place, clamping any properties
// which do not conform to the valueBound. It will return true if
// any properties were changed.
//
// An invariant on Clamp is that if it returns true, Conforms would have
// returned false, and now will return true.
func (b Bounds) Clamp(c *roachpb.SpanConfig) (changed bool) {
	return b.clamp(c, nil)
}

// Conforms returns true if the SpanConfig conforms to the specified bounds.
func (b Bounds) Conforms(c *roachpb.SpanConfig) bool {
	for _, f := range fields {
		if !f.FieldBound(b).conforms(c, f) {
			return false
		}
	}
	return true
}

// Check will check the SpanConfig for bounds violations and report them.
// Note that it is less efficient than Clamp or Conforms, and it will allocate
// memory. Use this when a detailed error is desired. Violations can be
// transformed into an error via its AsError() method.
func (b Bounds) Check(c *roachpb.SpanConfig) Violations {
	if b.Conforms(c) {
		return nil
	}
	clone := protoutil.Clone(c).(*roachpb.SpanConfig)
	var ret []Violation
	b.clamp(clone, func(f Field) {
		ret = append(ret, Violation{
			Field:     f,
			Bounds:    f.FieldBound(b),
			Value:     f.FieldValue(c),
			ClampedTo: f.FieldValue(clone),
		})
	})
	return ret
}

func (b Bounds) clamp(c *roachpb.SpanConfig, reporter func(Field)) (changed bool) {
	for _, f := range fields {
		if b := f.FieldBound(b); !b.clamp(c, f) {
			continue
		}
		changed = true
		if reporter != nil {
			reporter(f)
		}
	}
	return changed
}
