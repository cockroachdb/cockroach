// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package invertedexpr

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestUnionKeySpansToSpanExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		uks      geoindex.UnionKeySpans
		expected string
	}
	cases := []testCase{
		{
			uks: []geoindex.KeySpan{{Start: 5, End: 5}, {Start: 10, End: 10}, {Start: 1, End: 3}},
			expected: "spans_to_read:<start:\"B\\215\" end:\"B\\216\" > " +
				"spans_to_read:<start:\"B\\222\" end:\"B\\223\" > " +
				"spans_to_read:<start:\"B\\211\" end:\"B\\214\" > " +
				"node:<" +
				"factored_union_spans:<start:\"B\\215\" end:\"B\\216\" > " +
				"factored_union_spans:<start:\"B\\222\" end:\"B\\223\" > " +
				"factored_union_spans:<start:\"B\\211\" end:\"B\\214\" > > ",
		},
	}
	for _, c := range cases {
		spanExpr := GeoUnionKeySpansToSpanExpr(c.uks).(*inverted.SpanExpression)
		require.Equal(t, c.expected, spanExpr.ToProto().String())
	}

	// Test with nil union key spans.
	expr := GeoUnionKeySpansToSpanExpr(nil)
	require.Equal(t, inverted.NonInvertedColExpression{}, expr)
}

func TestRPKeyExprToSpanExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		rpx      geoindex.RPKeyExpr
		expected string
		err      string
	}
	cases := []testCase{
		{
			// Union of two keys.
			rpx: []geoindex.RPExprElement{geoindex.Key(5), geoindex.Key(10), geoindex.RPSetUnion},
			expected: "spans_to_read:<start:\"B\\215\" end:\"B\\216\" > " +
				"spans_to_read:<start:\"B\\222\" end:\"B\\223\" > " +
				"node:<" +
				"factored_union_spans:<start:\"B\\215\" end:\"B\\216\" > " +
				"factored_union_spans:<start:\"B\\222\" end:\"B\\223\" > > ",
		},
		{
			// Intersection of two keys.
			rpx: []geoindex.RPExprElement{geoindex.Key(5), geoindex.Key(10), geoindex.RPSetIntersection},
			expected: "spans_to_read:<start:\"B\\215\" end:\"B\\216\" > " +
				"spans_to_read:<start:\"B\\222\" end:\"B\\223\" > " +
				"node:<" +
				"operator:SetIntersection " +
				"left:<factored_union_spans:<start:\"B\\222\" end:\"B\\223\" > > " +
				"right:<factored_union_spans:<start:\"B\\215\" end:\"B\\216\" > > > ",
		},
		{
			// Single key.
			rpx: []geoindex.RPExprElement{geoindex.Key(5)},
			expected: "spans_to_read:<start:\"B\\215\" end:\"B\\216\" > " +
				"node:<factored_union_spans:<start:\"B\\215\" end:\"B\\216\" > > ",
		},
		{
			// Malformed.
			rpx: []geoindex.RPExprElement{geoindex.Key(5), geoindex.RPSetUnion},
			err: "malformed expression: F0/L30/000000000000000000000000000002 \\U",
		},
		{
			// Expression as represented in the proto: 7 ∩ (5 U 10 U 3 U 4 U (2 ∩ 1))
			// Unions have been collapsed wherever possible into a single slice of
			// FactoredUnionSpans.
			rpx: []geoindex.RPExprElement{
				geoindex.Key(5), geoindex.Key(10), geoindex.RPSetUnion,
				geoindex.Key(1), geoindex.Key(2), geoindex.RPSetIntersection,
				geoindex.RPSetUnion,
				geoindex.Key(3), geoindex.Key(4), geoindex.RPSetUnion,
				geoindex.RPSetUnion,
				geoindex.Key(7),
				geoindex.RPSetIntersection,
			},
			expected: "spans_to_read:<start:\"B\\211\" end:\"B\\212\" > " +
				"spans_to_read:<start:\"B\\212\" end:\"B\\213\" > " +
				"spans_to_read:<start:\"B\\213\" end:\"B\\214\" > " +
				"spans_to_read:<start:\"B\\214\" end:\"B\\215\" > " +
				"spans_to_read:<start:\"B\\215\" end:\"B\\216\" > " +
				"spans_to_read:<start:\"B\\217\" end:\"B\\220\" > " +
				"spans_to_read:<start:\"B\\222\" end:\"B\\223\" > " +
				"node:<operator:SetIntersection " +
				"left:<factored_union_spans:<start:\"B\\217\" end:\"B\\220\" > > " +
				"right:<" +
				"factored_union_spans:<start:\"B\\213\" end:\"B\\214\" > " +
				"factored_union_spans:<start:\"B\\214\" end:\"B\\215\" > " +
				"factored_union_spans:<start:\"B\\215\" end:\"B\\216\" > " +
				"factored_union_spans:<start:\"B\\222\" end:\"B\\223\" > " +
				"operator:SetIntersection " +
				"left:<factored_union_spans:<start:\"B\\212\" end:\"B\\213\" > > " +
				"right:<factored_union_spans:<start:\"B\\211\" end:\"B\\212\" > > > > ",
		},
	}
	for _, c := range cases {
		rpx, err := GeoRPKeyExprToSpanExpr(c.rpx)
		if len(c.err) == 0 {
			require.NoError(t, err)
			require.Equal(t, c.expected, rpx.(*inverted.SpanExpression).ToProto().String())
		} else {
			require.Equal(t, c.err, err.Error())
		}
	}

	// Test with nil RPKeyExpr.
	expr, err := GeoRPKeyExprToSpanExpr(nil)
	require.NoError(t, err)
	require.Equal(t, inverted.NonInvertedColExpression{}, expr)
}
