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
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

func geoKeyToEncInvertedVal(k geoindex.Key, end bool) EncInvertedVal {
	dint := tree.DInt(k)
	encoded, err := sqlbase.EncodeTableKey(nil, &dint, encoding.Ascending)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected encoding error: %d", k))
	}
	if end {
		// geoindex.KeySpan.End is inclusive, while InvertedSpan.end is exclusive.
		encoded = roachpb.Key(encoded).PrefixEnd()
	}
	return encoded
}

func geoToSpan(span geoindex.KeySpan) InvertedSpan {
	return InvertedSpan{
		Start: geoKeyToEncInvertedVal(span.Start, false),
		End:   geoKeyToEncInvertedVal(span.End, true),
	}
}

// GeoUnionKeySpansToSpanExpr converts geoindex.UnionKeySpans to a
// SpanExpression.
func GeoUnionKeySpansToSpanExpr(ukSpans geoindex.UnionKeySpans) *SpanExpression {
	if len(ukSpans) == 0 {
		return nil
	}
	spans := make([]InvertedSpan, len(ukSpans))
	for i, ukSpan := range ukSpans {
		spans[i] = geoToSpan(ukSpan)
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
	for _, elem := range rpExpr {
		// The keys in the RPKeyExpr are unique.
		if key, ok := elem.(geoindex.Key); ok {
			spansToRead = append(spansToRead, geoToSpan(geoindex.KeySpan{Start: key, End: key}))
		}
	}
	var stack []*SpanExpression
	for _, elem := range rpExpr {
		switch e := elem.(type) {
		case geoindex.Key:
			stack = append(stack, &SpanExpression{
				FactoredUnionSpans: []InvertedSpan{
					geoToSpan(geoindex.KeySpan{Start: e, End: e})},
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
