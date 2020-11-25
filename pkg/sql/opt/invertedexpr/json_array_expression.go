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
)

// JSONOrArrayToContainingSpanExpr converts a JSON or Array datum to a
// SpanExpression that represents the key ranges of datums containing the given
// datum according to the JSON or Array contains (@>) operator. If it is not
// possible to create such a SpanExpression, JSONOrArrayToContainingSpanExpr
// returns nil. If the provided datum is not a JSON or Array, returns an error.
func JSONOrArrayToContainingSpanExpr(
	evalCtx *tree.EvalContext, d tree.Datum,
) (*SpanExpression, error) {
	var b []byte
	spansSlice, tight, unique, err := rowenc.EncodeContainingInvertedIndexSpans(
		evalCtx, d, b, descpb.EmptyArraysInInvertedIndexesVersion,
	)
	if err != nil {
		return nil, err
	}
	if len(spansSlice) == 0 {
		// This can happen if the input is ARRAY[NULL].
		return &SpanExpression{}, nil
	}

	// The spans returned by EncodeContainingInvertedIndexSpans represent the
	// intersection of unions. So the below logic is performing a union on the
	// inner loop and an intersection on the outer loop. See the comment
	// above EncodeContainingInvertedIndexSpans for details.
	var invExpr InvertedExpression
	for _, spans := range spansSlice {
		var invExprLocal InvertedExpression
		for _, span := range spans {
			invSpan := InvertedSpan{Start: EncInvertedVal(span.Key), End: EncInvertedVal(span.EndKey)}
			spanExpr := ExprForInvertedSpan(invSpan, tight)
			if invExprLocal == nil {
				invExprLocal = spanExpr
			} else {
				invExprLocal = Or(invExprLocal, spanExpr)
			}
		}
		if invExpr == nil {
			invExpr = invExprLocal
		} else {
			invExpr = And(invExpr, invExprLocal)
		}
	}

	if spanExpr, ok := invExpr.(*SpanExpression); ok {
		spanExpr.Unique = unique
		return spanExpr, nil
	}
	return nil, nil
}
