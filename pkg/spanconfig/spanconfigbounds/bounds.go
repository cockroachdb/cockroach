// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// ValueBounds represents the bounds on a given value. It corresponds
// to a member in the SpanConfigBounds.
type ValueBounds interface {
	fmt.Stringer
	redact.SafeFormatter

	// clamp will modify the value to make it conform.
	clamp(*roachpb.SpanConfig, Field) (changed bool)

	// conforms checks whether the value conforms. It does not modify the value.
	// If it returns true, clamp must also return true. After clamp has returned
	// true, conforms should return false.
	conforms(*roachpb.SpanConfig, Field) (conforms bool)
}

// unbounded is a ValueBounds which accepts every value.
type unbounded struct{}

var _ ValueBounds = unbounded{}

func (u unbounded) String() string { return "*" }
func (u unbounded) SafeFormat(s redact.SafePrinter, verb rune) {
	s.SafeString("*")
}
func (u unbounded) clamp(*roachpb.SpanConfig, Field) (changed bool) {
	return false
}
func (u unbounded) conforms(*roachpb.SpanConfig, Field) (conforms bool) {
	return true
}

type int32Range tenantcapabilitiespb.SpanConfigBounds_Int32Range

var _ ValueBounds = (*int32Range)(nil)

func (i *int32Range) String() string {
	return formatRange(i.Start, i.End)
}
func (i *int32Range) SafeFormat(s redact.SafePrinter, verb rune) {
	s.SafeString(redact.SafeString(formatRange(i.Start, i.End)))
}
func (i *int32Range) clamp(c *roachpb.SpanConfig, f Field) (changed bool) {
	return clampRange(f.(field[int32]).fieldValue(c), i.Start, i.End)
}
func (i *int32Range) conforms(c *roachpb.SpanConfig, f Field) (conforms bool) {
	return checkRange(f.(field[int32]).fieldValue(c), i.Start, i.End)
}

type int64Range tenantcapabilitiespb.SpanConfigBounds_Int64Range

var _ ValueBounds = (*int64Range)(nil)

func (i *int64Range) String() string {
	return formatRange(i.Start, i.End)
}
func (i *int64Range) SafeFormat(s redact.SafePrinter, verb rune) {
	s.SafeString(redact.SafeString(formatRange(i.Start, i.End)))
}
func (i *int64Range) clamp(c *roachpb.SpanConfig, f Field) (changed bool) {
	return clampRange(f.(field[int64]).fieldValue(c), i.Start, i.End)
}
func (i *int64Range) conforms(c *roachpb.SpanConfig, f Field) (conforms bool) {
	return checkRange(f.(field[int64]).fieldValue(c), i.Start, i.End)
}
