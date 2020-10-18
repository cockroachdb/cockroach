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
	"github.com/cockroachdb/errors"
)

// JSONOrArrayToSpanExpr converts a JSON or Array datum to a SpanExpression if
// possible. If not possible, returns nil. If the provided datum is not a JSON
// or Array, returns an error.
func JSONOrArrayToSpanExpr(d tree.Datum) (*SpanExpression, error) {
	var b []byte
	spansSlice, err := rowenc.EncodeInvertedIndexTableSpans(
		d, b, descpb.EmptyArraysInInvertedIndexesVersion,
	)
	if err != nil {
		return nil, err
	}
	if len(spansSlice) == 0 {
		return nil, errors.AssertionFailedf("trying to use null key in index lookup")
	}

	// The spans returned by EncodeInvertedIndexTableSpans represent the
	// intersection of unions. So the below logic is performing a union on the
	// inner loop and an intersection on the outer loop. See the comment
	// above EncodeInvertedIndexTableSpans for details.
	var invExpr InvertedExpression
	for _, spans := range spansSlice {
		var invExprLocal InvertedExpression
		for _, span := range spans {
			invSpan := InvertedSpan{Start: EncInvertedVal(span.Key), End: EncInvertedVal(span.EndKey)}
			if invSpan.End == nil {
				invSpan.End = EncInvertedVal(span.Key.PrefixEnd())
			}
			spanExpr := ExprForInvertedSpan(invSpan, true /* tight */)
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
		return spanExpr, nil
	}
	return nil, nil
}
