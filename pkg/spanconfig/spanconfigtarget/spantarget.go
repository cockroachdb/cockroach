// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtarget

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
)

type SpanTarget struct {
	roachpb.Span
}

var _ spanconfig.Target = &SpanTarget{}

// NewSpanTarget returns a new spanconfigtarget.SpanTarget.
func NewSpanTarget(sp roachpb.Span) *SpanTarget {
	return &SpanTarget{
		Span: sp,
	}
}

// IsSystemTarget implements the spanconfig.Target interface.
func (s *SpanTarget) IsSystemTarget() bool {
	return false
}

// IsSpanTarget implements the spanconfig.Target interface.
func (s *SpanTarget) IsSpanTarget() bool {
	return true
}

// Encode implements the spanconfig.Target interface.
func (s *SpanTarget) Encode() roachpb.Span {
	return s.Span
}

// Less implements the spanconfig.Target interface.
func (s *SpanTarget) Less(o spanconfig.Target) bool {
	if o.IsSystemTarget() {
		return true
	}

	// We're dealing with 2 span targets.
	return s.Span.Key.Compare(o.Encode().Key) < 0
}

// Equal implements the spanconfig.Target interface.
func (s *SpanTarget) Equal(o spanconfig.Target) bool {
	if o.IsSystemTarget() {
		return false
	}

	return s.Span.Equal(o.Encode())
}

func (t *SpanTarget) TargetProto() *roachpb.SpanConfigTarget {
	return &roachpb.SpanConfigTarget{
		Union: &roachpb.SpanConfigTarget_Span{
			Span: &t.Span,
		},
	}
}
