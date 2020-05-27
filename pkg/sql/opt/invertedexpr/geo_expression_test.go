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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestUnionKeySpansToProto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		uks      geoindex.UnionKeySpans
		expected string
	}
	cases := []testCase{
		{
			uks: []geoindex.KeySpan{{Start: 5, End: 6}, {Start: 10, End: 11}, {Start: 1, End: 3}},
			expected: "spans_to_read:<start:\"\\215\" end:\"\\216\" > " +
				"spans_to_read:<start:\"\\222\" end:\"\\223\" > " +
				"spans_to_read:<start:\"\\211\" end:\"\\213\" > " +
				"node:<" +
				"factored_union_spans:<start:\"\\215\" end:\"\\216\" > " +
				"factored_union_spans:<start:\"\\222\" end:\"\\223\" > " +
				"factored_union_spans:<start:\"\\211\" end:\"\\213\" > > ",
		},
		{
			uks:      nil,
			expected: "<nil>",
		},
	}
	for _, c := range cases {
		require.Equal(t, c.expected, proto.CompactTextString(GeoUnionKeySpansToProto(c.uks)))
	}
}

func TestRPKeyExprToProto(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		rpx      geoindex.RPKeyExpr
		expected string
		err      string
	}
	cases := []testCase{
		{
			rpx:      nil,
			expected: "<nil>",
		},
		{
			// Union of two keys.
			rpx: []geoindex.RPExprElement{geoindex.Key(5), geoindex.Key(10), geoindex.RPSetUnion},
			expected: "spans_to_read:<start:\"\\215\" end:\"\\216\" > " +
				"spans_to_read:<start:\"\\222\" end:\"\\223\" > " +
				"node:<" +
				"factored_union_spans:<start:\"\\215\" end:\"\\216\" > " +
				"factored_union_spans:<start:\"\\222\" end:\"\\223\" > > ",
		},
		{
			// Intersection of two keys.
			rpx: []geoindex.RPExprElement{geoindex.Key(5), geoindex.Key(10), geoindex.RPSetIntersection},
			expected: "spans_to_read:<start:\"\\215\" end:\"\\216\" > " +
				"spans_to_read:<start:\"\\222\" end:\"\\223\" > " +
				"node:<" +
				"operator:SetIntersection " +
				"left:<factored_union_spans:<start:\"\\222\" end:\"\\223\" > > " +
				"right:<factored_union_spans:<start:\"\\215\" end:\"\\216\" > > > ",
		},
		{
			// Single key.
			rpx: []geoindex.RPExprElement{geoindex.Key(5)},
			expected: "spans_to_read:<start:\"\\215\" end:\"\\216\" > " +
				"node:<factored_union_spans:<start:\"\\215\" end:\"\\216\" > > ",
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
			expected: "spans_to_read:<start:\"\\215\" end:\"\\216\" > " +
				"spans_to_read:<start:\"\\222\" end:\"\\223\" > " +
				"spans_to_read:<start:\"\\211\" end:\"\\212\" > " +
				"spans_to_read:<start:\"\\212\" end:\"\\213\" > " +
				"spans_to_read:<start:\"\\213\" end:\"\\214\" > " +
				"spans_to_read:<start:\"\\214\" end:\"\\215\" > " +
				"spans_to_read:<start:\"\\217\" end:\"\\220\" > " +
				"node:<operator:SetIntersection " +
				"left:<factored_union_spans:<start:\"\\217\" end:\"\\220\" > > " +
				"right:<" +
				"factored_union_spans:<start:\"\\215\" end:\"\\216\" > " +
				"factored_union_spans:<start:\"\\222\" end:\"\\223\" > " +
				"factored_union_spans:<start:\"\\213\" end:\"\\214\" > " +
				"factored_union_spans:<start:\"\\214\" end:\"\\215\" > " +
				"operator:SetIntersection " +
				"left:<factored_union_spans:<start:\"\\212\" end:\"\\213\" > > " +
				"right:<factored_union_spans:<start:\"\\211\" end:\"\\212\" > > > > ",
		},
	}
	for _, c := range cases {
		rpxProto, err := GeoRPKeyExprToProto(c.rpx)
		if len(c.err) == 0 {
			require.NoError(t, err)
			require.Equal(t, c.expected, proto.CompactTextString(rpxProto))
		} else {
			require.Equal(t, c.err, err.Error())
		}
	}
}
