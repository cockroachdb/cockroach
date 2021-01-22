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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// DatumsToInvertedExpr is an interface that is used by the
// rowexec.invertedJoiner to extract a SpanExpressionProto given an
// input row. The rowexec.invertedJoiner calls Convert and uses the resulting
// SpanExpressionProto.SpansToRead to determine which spans to read from the
// inverted index. Then it computes a set expression on the scanned rows as
// defined by the SpanExpressionProto.Node.
// For a subset of expressions, it can additionally perform pre-filtering,
// which is filtering a row before evaluating the set expression (but after
// the row is retrieved). This pre-filtering looks at additional state encoded
// in the inverted column.
type DatumsToInvertedExpr interface {
	// CanPreFilter returns true iff this DatumsToInvertedExpr can pre-filter.
	CanPreFilter() bool

	// Convert uses the lookup column to construct an inverted expression. When
	// CanPreFilter() is true, and the returned *SpanExpressionProto is non-nil,
	// the interface{} returned represents an opaque pre-filtering state for
	// this lookup column.
	Convert(context.Context, rowenc.EncDatumRow) (*inverted.SpanExpressionProto, interface{}, error)

	// PreFilter is used for pre-filtering a looked up row whose inverted column is
	// represented as enc. The caller has determined the candidate
	// expressions for whom this row is relevant, and preFilters contains the
	// pre-filtering state for those expressions. The result slice must be the
	// same length as preFilters and will contain true when the row is relevant
	// and false otherwise. It returns true iff there is at least one result
	// index that has a true value. PreFilter must only be called when CanPreFilter()
	// is true. This batching of pre-filter application for a looked up row allows
	// enc to be decoded once.
	//
	// For example, when doing an invertedJoin for a geospatial column, say the
	// left side has a batch of 5 rows. Each of these 5 rows will be used in
	// Convert calls above, which will each return an interface{}. The 5
	// interface{} objects contain state for each of these 5 rows that will be
	// used in pre-filtering. Specifically, for geospatial, this state includes
	// the bounding box of the shape. When an inverted index row is looked up
	// and is known to be relevant to left rows 0, 2, 4 (based on the spans in
	// the SpanExpressionProtos), then PreFilter will be called with
	// len(preFilters) == 3, containing the three interface{} objects returned
	// previously for 0, 2, 4. The results will indicate which span expressions
	// should actually use this inverted index row. More concretely, the span
	// expressions for ST_Intersects work on cell coverings which are different
	// from the bounding box, and the pre-filtering works on the bounding box.
	// The bounding box pre-filtering complements the span expressions, by
	// eliminating some false positives caused by cell coverings.
	PreFilter(enc inverted.EncVal, preFilters []interface{}, result []bool) (bool, error)
}

// PreFiltererStateForInvertedFilterer captures state that needs to be passed
// around in the optimizer for later construction of the
// InvertedFiltererSpec.PreFiltererSpec.
type PreFiltererStateForInvertedFilterer struct {
	// Typ is the type of the original column that is indexed in the inverted
	// index.
	Typ *types.T
	// Expr is an expression with a single variable.
	Expr opt.ScalarExpr
	// Col is the column id of the single variable in the expression.
	Col opt.ColumnID
}
