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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/inverted"
	"github.com/cockroachdb/errors"
)

// JSONOrArrayToContainingInvertedExpr converts a JSON or Array datum to a
// SpanExpression that represents the key ranges of datums containing the given
// datum according to the JSON or Array contains (@>) operator. If it is not
// possible to create such a SpanExpression, JSONOrArrayToContainingInvertedExpr
// returns a NonInvertedColExpression. If the provided datum is not a JSON or
// Array, returns an error.
func JSONOrArrayToContainingInvertedExpr(
	evalCtx *tree.EvalContext, d tree.Datum,
) (InvertedExpression, error) {
	var b []byte
	spanExpr, err := rowenc.EncodeContainingInvertedIndexSpans(
		evalCtx, d, b, descpb.EmptyArraysInInvertedIndexesVersion,
	)
	if err != nil {
		return nil, err
	}

	// Convert the spanExpr returned by EncodeContainingInvertedIndexSpans to a
	// format that will be usable by the optimizer and execution engine.
	var convertSpanExpr func(*inverted.SpanExpression) (InvertedExpression, error)
	convertSpanExpr = func(spanExpr *inverted.SpanExpression) (InvertedExpression, error) {
		// First check that the provided spanExpr is valid. Only leaf nodes in a
		// SpanExpression tree are allowed to have UnionSpans set.
		if len(spanExpr.UnionSpans) > 0 && len(spanExpr.Children) > 0 {
			return nil, errors.AssertionFailedf(
				"invalid SpanExpression: cannot be both a leaf and internal node",
			)
		}
		if len(spanExpr.Children) == 0 && len(spanExpr.UnionSpans) == 0 {
			// This can happen if the input is ARRAY[NULL].
			return &SpanExpression{Tight: spanExpr.Tight, Unique: spanExpr.Unique}, nil
		}

		var invExpr InvertedExpression
		for _, span := range spanExpr.UnionSpans {
			invSpan := InvertedSpan{Start: EncInvertedVal(span.Key), End: EncInvertedVal(span.EndKey)}
			newSpanExpr := ExprForInvertedSpan(invSpan, spanExpr.Tight)
			newSpanExpr.Unique = spanExpr.Unique
			if invExpr == nil {
				invExpr = newSpanExpr
			} else {
				invExpr = Or(invExpr, newSpanExpr)
			}
		}

		for _, child := range spanExpr.Children {
			newSpanExpr, err := convertSpanExpr(child)
			if err != nil {
				return nil, err
			}
			if invExpr == nil {
				invExpr = newSpanExpr
			} else {
				switch spanExpr.Operator {
				case inverted.SetIntersection:
					invExpr = And(invExpr, newSpanExpr)
				case inverted.SetUnion:
					invExpr = Or(invExpr, newSpanExpr)
				default:
					return nil, errors.AssertionFailedf("invalid operator %v", spanExpr.Operator)
				}
			}
		}

		if newSpanExpr, ok := invExpr.(*SpanExpression); ok {
			newSpanExpr.Tight = spanExpr.Tight
			newSpanExpr.Unique = spanExpr.Unique
			return newSpanExpr, nil
		}
		return NonInvertedColExpression{}, nil
	}

	return convertSpanExpr(spanExpr)
}
