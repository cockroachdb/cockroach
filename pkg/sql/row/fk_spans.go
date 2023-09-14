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
	builder *span.Builder,
	splitter span.Splitter,
	values []tree.Datum,
	colMap catalog.TableColMap,
	numCols int,
) (roachpb.Span, error) {
	span, containsNull, err := builder.SpanFromDatumRow(values, numCols, colMap)
	if err != nil {
		return roachpb.Span{}, err
	}
	return splitter.ExistenceCheckSpan(span, numCols, containsNull), nil
}

// UniqueAndFKCheckSpan returns a span that can be scanned to ascertain existence of a
// specific row in a given index.
// TODO(mgartner): This should probably live somewhere else, since it is used
// for unique and FK checks.
func UniqueAndFKCheckSpans(
	builder *span.Builder,
	splitter span.Splitter,
	values []tree.Datum,
	colMap catalog.TableColMap,
	numCols int,
	prefixValues []tree.Datums,
) (roachpb.Spans, error) {
	// If there are prefix values...
	// TODO(mgartner): Explain this.
	if len(prefixValues) > 0 {
		spans := make(roachpb.Spans, len(prefixValues)*len(prefixValues[0]))
		// Create a slice of scratch values that contains the same elements as
		// values but that we can replace prefix values with.
		scratchValues := make([]tree.Datum, len(values))
		copy(scratchValues, values)
		for i := range prefixValues {
			for j := range prefixValues[i] {
				var containsNull bool
				var err error
				spanIdx := i*j + j
				// TODO(mgartner): I think this is broken if prefix values are
				// not the first columns in the table. We need to map them
				// correctly.
				scratchValues[i] = prefixValues[i][j]
				spans[spanIdx], containsNull, err = builder.SpanFromDatumRow(scratchValues, numCols, colMap)
				if err != nil {
					return nil, err
				}
				spans[spanIdx] = splitter.ExistenceCheckSpan(spans[spanIdx], numCols, containsNull)
			}
		}
		return spans, nil
	}

	// Otherwise...
	// TODO(mgartner): Explain this.
	span, containsNull, err := builder.SpanFromDatumRow(values, numCols, colMap)
	if err != nil {
		return nil, err
	}
	return roachpb.Spans{splitter.ExistenceCheckSpan(span, numCols, containsNull)}, nil
}
