// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Helpers for encoding spans of the inverted index.
//
// TODO(sumeer): this is similar to the code in inverted_joiner.go,
// except that is using invertedexpr.SpanExpressionProto_Span as an
// input. Merge the two.

func generateInvertedSpanKey(
	enc []byte, scratchRow sqlbase.EncDatumRow, sb *span.Builder,
) (roachpb.Key, error) {
	// Pretend that the encoded inverted val is an EncDatum. This isn't always
	// true, since JSON inverted columns use a custom encoding. But since we
	// are providing an already encoded Datum, the following will eventually
	// fall through to EncDatum.Encode() which will reuse the encoded bytes.
	encDatum := sqlbase.EncDatumFromEncoded(sqlbase.DatumEncoding_ASCENDING_KEY, enc)
	scratchRow = append(scratchRow[:0], encDatum)
	span, _, err := sb.SpanFromEncDatums(scratchRow, 1 /* prefixLen */)
	return span.Key, err
}

// GenerateInvertedSpans constructs spans to scan the inverted index.
// REQUIRES: invertedSpans != nil
func GenerateInvertedSpans(
	invertedSpans []invertedexpr.InvertedSpan, sb *span.Builder,
) (roachpb.Spans, error) {
	scratchRow := make(sqlbase.EncDatumRow, 1)
	var spans roachpb.Spans
	for _, span := range invertedSpans {
		var indexSpan roachpb.Span
		var err error
		if indexSpan.Key, err = generateInvertedSpanKey(span.Start, scratchRow, sb); err != nil {
			return nil, err
		}
		if indexSpan.EndKey, err = generateInvertedSpanKey(span.End, scratchRow, sb); err != nil {
			return nil, err
		}
		spans = append(spans, indexSpan)
	}
	sort.Sort(spans)
	return spans, nil
}
