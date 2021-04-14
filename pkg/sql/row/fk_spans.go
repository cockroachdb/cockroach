// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
)

// FKCheckSpan returns a span that can be scanned to ascertain existence of a
// specific row in a given index.
func FKCheckSpan(
	s *span.Builder, values []tree.Datum, colMap catalog.TableColMap, numCols int,
) (roachpb.Span, error) {
	span, containsNull, err := s.SpanFromDatumRow(values, numCols, colMap)
	if err != nil {
		return roachpb.Span{}, err
	}
	// If it is safe to split this lookup into multiple families, generate a point lookup for
	// family 0. Because we are just checking for existence, we only need family 0.
	if s.CanSplitSpanIntoFamilySpans(1 /* numNeededFamilies */, numCols, containsNull) {
		return s.SpanToPointSpan(span, 0), nil
	}
	return span, nil
}
