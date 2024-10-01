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

type int32Field int

var _ field[int32] = int32Field(0)

func (f int32Field) String() string { return config.Field(f).String() }

func (f int32Field) SafeFormat(s redact.SafePrinter, verb rune) {
	s.Print(config.Field(f))
}

func (f int32Field) FieldBound(b *Bounds) ValueBounds {
	getBound := func() *tenantcapabilitiespb.SpanConfigBounds_Int32Range {
		switch f {
		case numReplicas:
			return b.NumReplicas
		case numVoters:
			return b.NumVoters
		case gcTTLSeconds:
			return b.GCTTLSeconds
		default:
			// This is safe because we test that all the fields in the proto have
			// a corresponding field, and we call this for each of them, and the user
			// never provides the input to this function.
			panic(errors.AssertionFailedf("failed to look up field spanConfigBound %s", f))
		}
	}
	if r := getBound(); r != nil {
		return (*int32Range)(r)
	}
	return unbounded{}
}

func (f int32Field) FieldValue(c *roachpb.SpanConfig) Value {
	return (*int32Value)(f.fieldValue(c))
}

func (f int32Field) fieldValue(c *roachpb.SpanConfig) *int32 {
	switch f {
	case numReplicas:
		return &c.NumReplicas
	case numVoters:
		return &c.NumVoters
	case gcTTLSeconds:
		return &c.GCPolicy.TTLSeconds
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field %s", f))
	}
}
