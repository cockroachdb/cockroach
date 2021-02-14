// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestUnionType(t *testing.T) {
	testCases := []struct {
		left, right, expected *types.T
	}{
		{
			left:     types.Unknown,
			right:    types.Int,
			expected: types.Int,
		},
		{
			left:     types.Int,
			right:    types.Unknown,
			expected: types.Int,
		},
		{
			left:     types.Int4,
			right:    types.Int,
			expected: types.Int,
		},
		{
			left:     types.Int4,
			right:    types.Int2,
			expected: types.Int4,
		},
		{
			left:     types.Float4,
			right:    types.Float,
			expected: types.Float,
		},
		{
			left:     types.MakeDecimal(12 /* precision */, 5 /* scale */),
			right:    types.MakeDecimal(10 /* precision */, 7 /* scale */),
			expected: types.MakeDecimal(10 /* precision */, 7 /* scale */),
		},
		{
			// At the same scale, we use the left type.
			left:     types.MakeDecimal(10 /* precision */, 1 /* scale */),
			right:    types.MakeDecimal(12 /* precision */, 1 /* scale */),
			expected: types.MakeDecimal(10 /* precision */, 1 /* scale */),
		},
		{
			left:     types.Int4,
			right:    types.Decimal,
			expected: types.Decimal,
		},
		{
			left:     types.Decimal,
			right:    types.Float,
			expected: types.Decimal,
		},
		{
			// Error.
			left:     types.Float,
			right:    types.String,
			expected: nil,
		},
	}

	for _, tc := range testCases {
		result := func() *types.T {
			defer func() {
				// Swallow any error and return nil.
				_ = recover()
			}()
			return determineUnionType(tc.left, tc.right, "test")
		}()
		toStr := func(t *types.T) string {
			if t == nil {
				return "<nil>"
			}
			return t.SQLString()
		}
		if toStr(result) != toStr(tc.expected) {
			t.Errorf(
				"left: %s  right: %s  expected: %s  got: %s",
				toStr(tc.left), toStr(tc.right), toStr(tc.expected), toStr(result),
			)
		}
	}
}
