// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Violation represents a violation of the SpanConfigBound
type Violation struct {
	Field            Field
	Bounds           ValueBounds
	Value, ClampedTo Value
}

func (v Violation) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.Printf("%v: %v does not conform to %v, will be clamped to %v",
		v.Field, v.Value, v.Bounds, v.ClampedTo)
}

type Violations []Violation

func (v Violations) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.SafeRune('[')
	for i, violation := range v {
		if i > 0 {
			s.SafeString("; ")
		}
		s.Printf("%v", violation)
	}
	s.SafeRune(']')
}

func (v Violations) String() string {
	return redact.Sprint(v).StripMarkers()
}

var _ redact.SafeFormatter = Violations(nil)
var _ redact.SafeFormatter = (*Violation)(nil)

type ViolationError struct {
	Violations Violations
}

func (b *ViolationError) Format(f fmt.State, verb rune) {
	errors.FormatError(b, f, verb)
}

func (b *ViolationError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("span config bounds violated for fields: ")
	for i, v := range b.Violations {
		if i > 0 {
			p.Printf(", ")
		}
		p.Print(v.Field)
	}
	if !p.Detail() {
		return nil
	}
	for i, v := range b.Violations {
		if i > 0 {
			p.Printf("\n")
		}
		p.Print(v)
	}
	return nil
}

func (b *ViolationError) Error() string {
	return fmt.Sprintf("%v", b)
}

var _ errors.SafeFormatter = (*ViolationError)(nil)
var _ fmt.Formatter = (*ViolationError)(nil)

func (v Violations) AsError() error {
	return &ViolationError{
		Violations: v,
	}
}
