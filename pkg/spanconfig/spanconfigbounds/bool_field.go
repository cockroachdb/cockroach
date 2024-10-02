// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

import (
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type boolField int

var _ field[bool] = boolField(0)

func (f boolField) SafeFormat(s redact.SafePrinter, verb rune) {
	s.Printf("%s", config.Field(f))
}

func (f boolField) String() string {
	return config.Field(f).String()
}

func (f boolField) FieldBound(b *Bounds) ValueBounds {
	return unbounded{}
}

func (f boolField) FieldValue(c *roachpb.SpanConfig) Value {
	return (*boolValue)(f.fieldValue(c))
}

func (f boolField) fieldValue(c *roachpb.SpanConfig) *bool {
	switch f {
	case globalReads:
		return &c.GlobalReads

		// TODO(ajwerner): Decide what to do about these fields which do not exist
		// zone configurations. For now, they can be set by the tenant.
	/*
		case excludeDataFromBackup:
			return &c.ExcludeDataFromBackup
		case rangeFeedEnabled:
			return &c.RangeFeedEnabled
	*/
	default:
		// This is safe because we test that all the fields in the proto have
		// a corresponding field, and we call this for each of them, and the user
		// never provides the input to this function.
		panic(errors.AssertionFailedf("failed to look up field %s", f))
	}
}
