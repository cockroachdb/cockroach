// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// TODO(ajwerner): Add benchmarking.

// Bounds wraps the tenantcapabilities.SpanConfigBounds and utilizes its
// policy to interact with SpanConfigs.
type Bounds tenantcapabilitiespb.SpanConfigBounds

func (b *Bounds) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf("spanconfigbounds.Bounds{")
	for i, field := range fields {
		delim := redact.SafeString(", ")
		if i == 0 {
			delim = ""
		}
		fieldID := config.Field(i)
		s.Printf("%v%v: %v", delim, fieldID, field.FieldBound(b))
	}
	s.Printf("}")
}

var _ redact.SafeFormatter = (*Bounds)(nil)

// New constructs a Bounds from its serialization.
func New(b *tenantcapabilitiespb.SpanConfigBounds) *Bounds {
	if cb := b.ConstraintBounds; cb != nil {
		sort.Sort(sortedConstraints(cb.Allowed))
	}
	return (*Bounds)(b)
}

func (b *Bounds) String() string {
	var buf strings.Builder
	sep := ""
	for _, field := range fields {
		_, _ = fmt.Fprintf(&buf, "%s%s: %s", sep, field, field.FieldBound(b))
		sep = "\n"
	}
	return buf.String()
}

// Clamp will update the SpanConfig in place, clamping any properties
// which do not conform to the valueBound. It will return true if
// any properties were changed.
//
// An invariant on Clamp is that if it returns true, Conforms would have
// returned false, and now will return true.
func (b *Bounds) Clamp(c *roachpb.SpanConfig) (changed bool) {
	return b.clamp(c, nil)
}

// Conforms returns true if the SpanConfig conforms to the specified bounds.
func (b *Bounds) Conforms(c *roachpb.SpanConfig) bool {
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
func (b *Bounds) Check(c *roachpb.SpanConfig) Violations {
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

func (b *Bounds) clamp(c *roachpb.SpanConfig, reporter func(Field)) (changed bool) {
	for _, f := range fields {
		if bb := f.FieldBound(b); !bb.clamp(c, f) {
			continue
		}
		changed = true
		if reporter != nil {
			reporter(f)
		}
	}
	return changed
}
