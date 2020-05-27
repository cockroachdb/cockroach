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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// This file contains functions to encode geoindex.{UnionKeySpans, RPKeyExpr}
// into a SpanExpressionProto. These functions are in this package since they
// need to use sqlbase.EncodeTableKey to convert geoindex.Key to
// invertedexpr.EncInvertedVal and that cannot be done in the geoindex package
// as it introduces a circular dependency.

func geoKeyToEncInvertedVal(k geoindex.Key) EncInvertedVal {
	dint := tree.DInt(k)
	encoded, err := sqlbase.EncodeTableKey(nil, &dint, encoding.Ascending)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "unexpected encoding error: %d", k))
	}
	return encoded
}

func geoToSpan(span geoindex.KeySpan) SpanExpressionProto_Span {
	return SpanExpressionProto_Span{
		Start: geoKeyToEncInvertedVal(span.Start),
		End:   geoKeyToEncInvertedVal(span.End),
	}
}

// GeoUnionKeySpansToProto converts geoindex.UnionKeySpans to
// SpanExpressionProto.
func GeoUnionKeySpansToProto(ukSpans geoindex.UnionKeySpans) *SpanExpressionProto {
	if len(ukSpans) == 0 {
		return nil
	}
	spans := make([]SpanExpressionProto_Span, len(ukSpans))
	for i, ukSpan := range ukSpans {
		spans[i] = geoToSpan(ukSpan)
	}
	return &SpanExpressionProto{
		SpansToRead: spans,
		Node: SpanExpressionProto_Node{
			FactoredUnionSpans: spans,
		},
	}
}

// GeoRPKeyExprToProto converts geoindex.RPKeyExpr to SpanExpressionProto.
func GeoRPKeyExprToProto(rpExpr geoindex.RPKeyExpr) (*SpanExpressionProto, error) {
	if len(rpExpr) == 0 {
		return nil, nil
	}
	spansToRead := make([]SpanExpressionProto_Span, 0, len(rpExpr))
	for _, elem := range rpExpr {
		// The keys in the RPKeyExpr are unique.
		if key, ok := elem.(geoindex.Key); ok {
			spansToRead = append(spansToRead, geoToSpan(geoindex.KeySpan{Start: key, End: key + 1}))
		}
	}
	var stack []*SpanExpressionProto_Node
	for _, elem := range rpExpr {
		switch e := elem.(type) {
		case geoindex.Key:
			stack = append(stack, &SpanExpressionProto_Node{
				FactoredUnionSpans: []SpanExpressionProto_Span{
					geoToSpan(geoindex.KeySpan{Start: e, End: e + 1})},
			})
		case geoindex.RPSetOperator:
			if len(stack) < 2 {
				return nil, errors.Errorf("malformed expression: %s", rpExpr)
			}
			node0, node1 := stack[len(stack)-1], stack[len(stack)-2]
			var node *SpanExpressionProto_Node
			stack = stack[:len(stack)-2]
			switch e {
			case geoindex.RPSetIntersection:
				node = &SpanExpressionProto_Node{
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
					node = &SpanExpressionProto_Node{
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
	return &SpanExpressionProto{
		SpansToRead: spansToRead,
		Node:        *stack[0],
	}, nil
}
