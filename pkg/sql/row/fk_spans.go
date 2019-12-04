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
	// If it is safe to split this lookup into multiple families, generate a point lookup for
	// family 0. Because we are just checking for existence, we only need family 0.
	if f.spanBuilder.CanSplitSpanIntoSeparateFamilies(1 /* numNeededFamilies */, f.prefixLen) {
		return f.spanBuilder.PointSpanFromDatumRow(values, 0 /* family */, f.ids)
	}
	return f.spanBuilder.SpanFromDatumRow(values, f.prefixLen, f.ids)
}
