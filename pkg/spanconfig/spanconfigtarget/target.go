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

func New(target roachpb.SpanConfigTarget) spanconfig.Target {
	switch target.Union.(type) {
	case *roachpb.SpanConfigTarget_Span:
		return NewSpanTarget(*target.GetSpan())
	case *roachpb.SpanConfigTarget_SystemSpanConfigTarget:
		t, err := NewSystemTarget(target.GetSystemSpanConfigTarget().SourceTenantID, target.GetSystemSpanConfigTarget().TargetTenantID)
		if err != nil {
			panic(err)
		}
		return t
	default:
		panic("unknown span config target")
	}
}

func Decode(span roachpb.Span) spanconfig.Target {
	if conformsToSystemTargetEncoding(span) {
		t, err := DecodeSystemTarget(span)
		if err != nil {
			panic(err)
		}
		return t
	}
	return &SpanTarget{
		Span: span,
	}
}
