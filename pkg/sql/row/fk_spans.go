// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
