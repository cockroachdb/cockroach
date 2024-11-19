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

type int64Field int

var _ field[int64] = int64Field(0)

func (f int64Field) String() string {
	return config.Field(f).String()
}

func (f int64Field) SafeFormat(s redact.SafePrinter, verb rune) {
	s.Print(config.Field(f))
}

func (f int64Field) FieldBound(b *Bounds) ValueBounds {
	getBound := func() *tenantcapabilitiespb.SpanConfigBounds_Int64Range {
		switch f {
		case rangeMaxBytes:
			return b.RangeMaxBytes
		case rangeMinBytes:
			return b.RangeMinBytes
		default:
			// This is safe because we test that all the fields in the proto have
			// a corresponding field, and we call this for each of them, and the user
			// never provides the input to this function.
			panic(errors.AssertionFailedf("failed to look up field spanConfigBound %s", f))
		}
	}
	if r := getBound(); r != nil {
		return (*int64Range)(r)
	}
	return unbounded{}
}

func (f int64Field) FieldValue(c *roachpb.SpanConfig) Value {
	return (*int64Value)(f.fieldValue(c))
}

func (f int64Field) fieldValue(c *roachpb.SpanConfig) *int64 {
	switch f {
	case rangeMaxBytes:
		return &c.RangeMaxBytes
	case rangeMinBytes:
		return &c.RangeMinBytes
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field %s", f))
	}
}
