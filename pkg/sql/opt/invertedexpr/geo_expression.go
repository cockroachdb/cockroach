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

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// This file contains functions to encode geoindex.{UnionKeySpans, RPKeyExpr}
// into a SpanExpression. These functions are in this package since they
// need to use sqlbase.EncodeTableKey to convert geoindex.Key to
// invertedexpr.EncInvertedVal and that cannot be done in the geoindex package
// as it introduces a circular dependency.
//
// TODO(sumeer): change geoindex to produce SpanExpressions directly.

func geoKeyToEncInvertedVal(k geoindex.Key, end bool, b []byte) (EncInvertedVal, []byte) {
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
	b = encoding.EncodeUvarintAscending(b, uint64(k))
	enc := b[prev:]
	if prefixEnd {
		enc = roachpb.Key(enc).PrefixEnd()
	}
	return enc, b
}

func geoToSpan(span geoindex.KeySpan, b []byte) (InvertedSpan, []byte) {
	start, b := geoKeyToEncInvertedVal(span.Start, false, b)
	end, b := geoKeyToEncInvertedVal(span.End, true, b)
	return InvertedSpan{Start: start, End: end}, b
}

// GeoUnionKeySpansToSpanExpr converts geoindex.UnionKeySpans to a
// SpanExpression.
func GeoUnionKeySpansToSpanExpr(ukSpans geoindex.UnionKeySpans) *SpanExpression {
	if len(ukSpans) == 0 {
		return nil
	}
	spans := make([]InvertedSpan, len(ukSpans))
	var b []byte // avoid per-span heap allocations
	for i, ukSpan := range ukSpans {
		spans[i], b = geoToSpan(ukSpan, b)
	}
	return &SpanExpression{
		SpansToRead:        spans,
		FactoredUnionSpans: spans,
	}
}

// GeoRPKeyExprToSpanExpr converts geoindex.RPKeyExpr to SpanExpression.
func GeoRPKeyExprToSpanExpr(rpExpr geoindex.RPKeyExpr) (*SpanExpression, error) {
	if len(rpExpr) == 0 {
		return nil, nil
	}
	spansToRead := make([]InvertedSpan, 0, len(rpExpr))
	var b []byte // avoid per-expr heap allocations
	for _, elem := range rpExpr {
		// The keys in the RPKeyExpr are unique.
		if key, ok := elem.(geoindex.Key); ok {
			var span InvertedSpan
			span, b = geoToSpan(geoindex.KeySpan{Start: key, End: key}, b)
			spansToRead = append(spansToRead, span)
		}
	}
	var stack []*SpanExpression
	for _, elem := range rpExpr {
		switch e := elem.(type) {
		case geoindex.Key:
			var span InvertedSpan
			span, b = geoToSpan(geoindex.KeySpan{Start: e, End: e}, b)
			stack = append(stack, &SpanExpression{
				FactoredUnionSpans: []InvertedSpan{span},
			})
		case geoindex.RPSetOperator:
			if len(stack) < 2 {
				return nil, errors.Errorf("malformed expression: %s", rpExpr)
			}
			node0, node1 := stack[len(stack)-1], stack[len(stack)-2]
			var node *SpanExpression
			stack = stack[:len(stack)-2]
			switch e {
			case geoindex.RPSetIntersection:
				node = &SpanExpression{
					Operator: SetIntersection,
					Left:     node0,
					Right:    node1,
				}
			case geoindex.RPSetUnion:
				if node0.Operator == None {
					node0, node1 = node1, node0
				}
				if node1.Operator == None {
					node = node0
					node.FactoredUnionSpans = append(node.FactoredUnionSpans, node1.FactoredUnionSpans...)
				} else {
					node = &SpanExpression{
						Operator: SetUnion,
						Left:     node0,
						Right:    node1,
					}
				}
			}
			stack = append(stack, node)
		}
	}
	if len(stack) != 1 {
		return nil, errors.Errorf("malformed expression: %s", rpExpr)
	}
	spanExpr := *stack[0]
	spanExpr.SpansToRead = spansToRead
	return &spanExpr, nil
}
