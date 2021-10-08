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
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// This file contains functions to encode geoindex.{UnionKeySpans, RPKeyExpr}
// into a SpanExpression. These functions are in this package since they
// need to use sqlbase.EncodeTableKey to convert geoindex.Key to
// invertedexpr.EncVal and that cannot be done in the geoindex package
// as it introduces a circular dependency.
//
// TODO(sumeer): change geoindex to produce SpanExpressions directly.

func geoKeyToEncInvertedVal(k geoindex.Key, end bool, b []byte) (inverted.EncVal, []byte) {
	// geoindex.KeySpan.End is inclusive, while InvertedSpan.end is exclusive.
	// For all but k == math.MaxUint64, we can account for this before the key
	// encoding. For k == math.MaxUint64, we must PrefixEnd after, which incurs
	// a separate memory allocation.
	prefixEnd := false
	if end {
		if k < math.MaxUint64 {
			k++
		} else {
			prefixEnd = true
		}
	}
	prev := len(b)
	b = encoding.EncodeGeoInvertedAscending(b)
	b = encoding.EncodeUvarintAscending(b, uint64(k))
	// Set capacity so that the caller appending does not corrupt later keys.
	enc := b[prev:len(b):len(b)]
	if prefixEnd {
		enc = roachpb.Key(enc).PrefixEnd()
	}
	return enc, b
}

func geoToSpan(span geoindex.KeySpan, b []byte) (inverted.Span, []byte) {
	start, b := geoKeyToEncInvertedVal(span.Start, false, b)
	end, b := geoKeyToEncInvertedVal(span.End, true, b)
	return inverted.Span{Start: start, End: end}, b
}

// GeoUnionKeySpansToSpanExpr converts geoindex.UnionKeySpans to a
// SpanExpression.
func GeoUnionKeySpansToSpanExpr(ukSpans geoindex.UnionKeySpans) inverted.Expression {
	if len(ukSpans) == 0 {
		return inverted.NonInvertedColExpression{}
	}
	// Avoid per-span heap allocations. Each of the 2 keys in a span is the
	// geoInvertedIndexMarker (1 byte) followed by a varint.
	b := make([]byte, 0, len(ukSpans)*(2*encoding.MaxVarintLen+2))
	spans := make([]inverted.Span, len(ukSpans))
	for i, ukSpan := range ukSpans {
		spans[i], b = geoToSpan(ukSpan, b)
	}
	return &inverted.SpanExpression{
		SpansToRead:        spans,
		FactoredUnionSpans: spans,
	}
}

// GeoRPKeyExprToSpanExpr converts geoindex.RPKeyExpr to SpanExpression.
func GeoRPKeyExprToSpanExpr(rpExpr geoindex.RPKeyExpr) (inverted.Expression, error) {
	if len(rpExpr) == 0 {
		return inverted.NonInvertedColExpression{}, nil
	}
	spansToRead := make([]inverted.Span, 0, len(rpExpr))
	var b []byte // avoid per-expr heap allocations
	var stack []*inverted.SpanExpression
	for _, elem := range rpExpr {
		switch e := elem.(type) {
		case geoindex.Key:
			var span inverted.Span
			span, b = geoToSpan(geoindex.KeySpan{Start: e, End: e}, b)
			// The keys in the RPKeyExpr are unique, so simply append to spansToRead.
			spansToRead = append(spansToRead, span)
			stack = append(stack, &inverted.SpanExpression{
				FactoredUnionSpans: []inverted.Span{span},
			})
		case geoindex.RPSetOperator:
			if len(stack) < 2 {
				return nil, errors.Errorf("malformed expression: %s", rpExpr)
			}
			node0, node1 := stack[len(stack)-1], stack[len(stack)-2]
			var node *inverted.SpanExpression
			stack = stack[:len(stack)-2]
			switch e {
			case geoindex.RPSetIntersection:
				node = makeSpanExpression(inverted.SetIntersection, node0, node1)
			case geoindex.RPSetUnion:
				if node0.Operator == inverted.None {
					node0, node1 = node1, node0
				}
				if node1.Operator == inverted.None {
					// node1 can be discarded after unioning its FactoredUnionSpans.
					node = node0
					// Union into the one with the larger capacity. This optimizes
					// the case of many unions. We will sort the spans later.
					if cap(node.FactoredUnionSpans) < cap(node1.FactoredUnionSpans) {
						node.FactoredUnionSpans = append(node1.FactoredUnionSpans, node.FactoredUnionSpans...)
					} else {
						node.FactoredUnionSpans = append(node.FactoredUnionSpans, node1.FactoredUnionSpans...)
					}
				} else {
					node = makeSpanExpression(inverted.SetUnion, node0, node1)
				}
			}
			stack = append(stack, node)
		}
	}
	if len(stack) != 1 {
		return inverted.NonInvertedColExpression{}, errors.Errorf("malformed expression: %s", rpExpr)
	}
	spanExpr := *stack[0]
	spanExpr.SpansToRead = spansToRead
	sort.Sort(spanExpr.SpansToRead)
	// Sort the FactoredUnionSpans of the root. The others are already sorted
	// in makeSpanExpression.
	sort.Sort(spanExpr.FactoredUnionSpans)
	return &spanExpr, nil
}

func makeSpanExpression(
	op inverted.SetOperator, n0 *inverted.SpanExpression, n1 *inverted.SpanExpression,
) *inverted.SpanExpression {
	sort.Sort(n0.FactoredUnionSpans)
	sort.Sort(n1.FactoredUnionSpans)
	return &inverted.SpanExpression{
		Operator: op,
		Left:     n0,
		Right:    n1,
	}
}
