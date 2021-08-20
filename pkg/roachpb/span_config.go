// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"sort"

	"github.com/cockroachdb/errors"
)

// Unnest is a convenience method to return the slice-of-slices form of
// GetSpanConfigsResponse.
//
// NB: This does not make a copy of the underlying []SpanConfigEntry slices.
func (r *GetSpanConfigsResponse) Unnest() [][]SpanConfigEntry {
	ret := make([][]SpanConfigEntry, len(r.Results))
	for i, result := range r.Results {
		ret[i] = result.SpanConfigs
	}
	return ret
}

// Equal compares two span config entries.
func (e SpanConfigEntry) Equal(other SpanConfigEntry) bool {
	return e.Span.Equal(other.Span) && e.Config.Equal(other.Config)
}

// Validate returns an error the request is malformed. Spans included in the
// request are expected to be valid, and have non-empty end keys. Additionally,
// spans in each list (update/delete) are expected to be non-overlapping.
func (r *UpdateSpanConfigsRequest) Validate() error {
	spansToUpdate := func(ents []SpanConfigEntry) []Span {
		var spans []Span
		for _, ent := range ents {
			spans = append(spans, ent.Span)
		}
		return spans
	}(r.ToUpsert)

	for _, list := range [][]Span{r.ToDelete, spansToUpdate} {
		for _, span := range list {
			if !span.Valid() || len(span.EndKey) == 0 {
				return errors.AssertionFailedf("invalid span: %s", span)
			}
		}

		spans := make([]Span, len(list))
		copy(spans, list)
		sort.Sort(Spans(spans))
		for i := range spans {
			if i == 0 {
				continue
			}

			if spans[i].Overlaps(spans[i-1]) {
				return errors.AssertionFailedf("overlapping spans %s and %s in same list",
					spans[i-1], spans[i])
			}
		}
	}

	return nil
}
