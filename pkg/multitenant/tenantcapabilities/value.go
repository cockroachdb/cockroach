// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilities

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Value is a generic interface to the value of capabilities of
// various underlying Go types. It enables processing capabilities
// without concern for the specific underlying type, such as in SHOW
// TENANT WITH CAPABILITIES.
type Value interface {
	fmt.Stringer
	redact.SafeFormatter
}

// TypedValue is a convenience interface for specialized implementations of
// Value.
type TypedValue[T any] interface {
	Value
	Get() T
	Set(T)
}

type (
	BoolValue            = TypedValue[bool]
	SpanConfigBoundValue = TypedValue[*spanconfigbounds.Bounds]
)

// boolValue is a wrapper around bool that ensures that values can
// be included in reportables.
type boolValue bool

var _ BoolValue = (*boolValue)(nil)

func (b *boolValue) Get() bool      { return bool(*b) }
func (b *boolValue) Set(val bool)   { *b = boolValue(val) }
func (b *boolValue) String() string { return strconv.FormatBool(bool(*b)) }
func (b *boolValue) SafeFormat(p redact.SafePrinter, verb rune) {
	p.Print(bool(*b))
}

// invertedBoolCap is an accessor struct for boolean capabilities that are
// stored as "disabled" in the underlying proto. Layers above this package
// interact are oblivious to this detail.
type invertedBoolValue bool

func (b *invertedBoolValue) Get() bool      { return bool(!*b) }
func (b *invertedBoolValue) Set(val bool)   { *b = invertedBoolValue(!val) }
func (b *invertedBoolValue) String() string { return strconv.FormatBool(bool(!*b)) }
func (b *invertedBoolValue) SafeFormat(p redact.SafePrinter, verb rune) {
	p.Print(bool(!*b))
}

type spanConfigBoundsValue struct {
	// Double-indirection is used because the Set method will overwrite the
	// pointer with a new pointer.
	b **tenantcapabilitiespb.SpanConfigBounds
}

func (s *spanConfigBoundsValue) Get() *spanconfigbounds.Bounds {
	if *s.b == nil {
		return nil
	}
	return spanconfigbounds.New(*s.b)
}

func (s *spanConfigBoundsValue) Set(t *spanconfigbounds.Bounds) {
	*s.b = (*tenantcapabilitiespb.SpanConfigBounds)(t)
}

var _ SpanConfigBoundValue = (*spanConfigBoundsValue)(nil)

func (s *spanConfigBoundsValue) String() string {
	if *s.b == nil {
		return "{}"
	}
	return fmt.Sprint(spanconfigbounds.New(*s.b))
}

func (s *spanConfigBoundsValue) SafeFormat(p interfaces.SafePrinter, verb rune) {
	if *s.b == nil {
		p.Printf("{}")
	}
	p.Print(spanconfigbounds.New(*s.b))
}
