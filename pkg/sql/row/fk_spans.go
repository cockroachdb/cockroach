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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// spanForValues produce access spans for a single FK constraint and a
// tuple of columns.
func (f fkExistenceCheckBaseHelper) spanForValues(values tree.Datums) (roachpb.Span, error) {
	if values == nil {
		key := roachpb.Key(f.spanBuilder.keyPrefix)
		return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
	}
	span, err := f.spanBuilder.SpanFromDatumRow(values, f.prefixLen, f.ids)
	if err != nil {
		return roachpb.Span{}, err
	}
	// We can always return the first element in the resulting span list -- if we can't split the span, then it
	// will be the original span. If we split the span, we only requested one family, so the slice has length 1 anyway.
	return f.spanBuilder.MaybeSplitSpanIntoSeparateFamilies(span, f.prefixLen)[0], nil
}
