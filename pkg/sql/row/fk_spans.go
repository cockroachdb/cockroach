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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
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
	insertCols []exec.TableColumnOrdinal,
) (roachpb.Spans, error) {
	// If there are prefix values...
	// TODO(mgartner): Explain this.
	if len(prefixValues) > 0 {
		numValues := 1
		for i := range prefixValues {
			numValues *= len(prefixValues[i])
		}
		spans := make(roachpb.Spans, numValues)
		// Create a slice of scratch values that contains the same elements as
		// values but that we can replace prefix values with.
		scratchValues := make([]tree.Datum, len(values))
		copy(scratchValues, values)
		if err := generateSpansHelper(builder, splitter, colMap, &scratchValues, &prefixValues, insertCols, numCols, 0, /* level */
			1 /* multiplier */, 0 /* offset */, &spans); err != nil {
			return nil, err
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

func generateSpansHelper(
	builder *span.Builder,
	splitter span.Splitter,
	colMap catalog.TableColMap,
	scratchValues *[]tree.Datum,
	prefixValues *[]tree.Datums,
	insertCols []exec.TableColumnOrdinal,
	numCols, level, multiplier, offset int,
	spans *roachpb.Spans,
) error {
	for j := range (*prefixValues)[level] {
		var containsNull bool
		var err error
		(*scratchValues)[insertCols[level]] = (*prefixValues)[level][j]
		if level == len(*prefixValues)-1 {
			spanIdx := offset + multiplier*j
			(*spans)[spanIdx], containsNull, err = builder.SpanFromDatumRow(*scratchValues, numCols, colMap)
			if err != nil {
				return err
			}
			(*spans)[spanIdx] = splitter.ExistenceCheckSpan((*spans)[spanIdx], numCols, containsNull)
		} else {
			if err = generateSpansHelper(builder, splitter, colMap, scratchValues, prefixValues, insertCols, numCols, level+1,
				multiplier*len((*prefixValues)[level]), /* multiplier */
				offset+multiplier*j,                    /* offset */
				spans,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
