// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

// TODO (rohany): I want to put this in sqlbase, but it seems like i can't get the constraints package there.

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// SpanBuilder is a single struct for generating key spans from Constraints, Datums and encDatums.
type SpanBuilder struct {
	table         *sqlbase.TableDescriptor
	index         *sqlbase.IndexDescriptor
	indexColTypes []types.T
	indexColDirs  []sqlbase.IndexDescriptor_Direction

	keyPrefix []byte
	alloc     sqlbase.DatumAlloc

	// Used when converting opt constraints into spans.
	// TODO (rohany): if we rewrite the constraints code to not use interstices then we can
	//  get rid of the somewhat expensive step in MakeSpanBuilder.
	interstices [][]byte

	neededFamilies []sqlbase.FamilyID
}

// MakeSpanBuilder creates a SpanBuilder for a table and index.
func MakeSpanBuilder(table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor) *SpanBuilder {
	s := &SpanBuilder{
		table:          table,
		index:          index,
		keyPrefix:      sqlbase.MakeIndexKeyPrefix(table, index.ID),
		interstices:    make([][]byte, len(index.ColumnDirections)+len(index.ExtraColumnIDs)+1),
		neededFamilies: nil,
	}

	// TODO (rohany): does this need to be with mutations here?
	colIdxMap := table.ColumnIdxMap()
	colTypes := table.ColumnTypes()

	var columnIDs sqlbase.ColumnIDs
	columnIDs, s.indexColDirs = index.FullColumnIDs()
	s.indexColTypes = make([]types.T, len(columnIDs))
	for i, colID := range columnIDs {
		s.indexColTypes[i] = colTypes[colIdxMap[colID]]
	}

	// Set up the interstices for encoding interleaved tables later.
	s.interstices[0] = sqlbase.MakeIndexKeyPrefix(table, index.ID)
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(rohany): too much of this code is copied from EncodePartialIndexKey.
		sharedPrefixLen := 0
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is already encoded in interstices[0].
			if i != 0 {
				s.interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(s.interstices[sharedPrefixLen], uint64(ancestor.TableID))
				s.interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(s.interstices[sharedPrefixLen], uint64(ancestor.IndexID))
			}
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
			s.interstices[sharedPrefixLen] = encoding.EncodeInterleavedSentinel(s.interstices[sharedPrefixLen])
		}
		s.interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(s.interstices[sharedPrefixLen], uint64(table.ID))
		s.interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(s.interstices[sharedPrefixLen], uint64(index.ID))
	}

	return s
}

// SpansFromConstraint generates spans from an optimizer constraint.
// TODO (rohany): see if this can be changed to a more general interface?
// It would be easy if i could just convert constraints into a row of datums...
// maybe just alloc one up front in the spanbuilder?
func (s *SpanBuilder) SpansFromConstraint(
	c *constraint.Constraint, needed util.FastIntSet, forDelete bool,
) (roachpb.Spans, error) {
	var spans roachpb.Spans
	var err error
	if c == nil || c.IsUnconstrained() {
		// Encode a full span.
		spans, err = s.appendSpansFromConstraintSpan(spans, &constraint.UnconstrainedSpan, needed, forDelete)
		if err != nil {
			return nil, err
		}
		return spans, nil
	}

	spans = make(roachpb.Spans, 0, c.Spans.Count())
	for i := 0; i < c.Spans.Count(); i++ {
		spans, err = s.appendSpansFromConstraintSpan(spans, c.Spans.Get(i), needed, forDelete)
		if err != nil {
			return nil, err
		}
	}
	return spans, nil
}

// SetNeededColumns sets the needed columns on the SpanBuilder. This information
// is used by MaybeSplitSpanIntoSeparateFamilies.
func (s *SpanBuilder) SetNeededColumns(neededCols util.FastIntSet) {
	// TODO (rohany): ensure that this is sorted.
	s.neededFamilies = sqlbase.NeededColumnFamilyIDs(s.table.ColumnIdxMap(), s.table.Families, neededCols)
}

// UnsetNeededColumns resets the needed columns for column family specific optimizations
// that the SpanBuilder performs.
func (s *SpanBuilder) UnsetNeededColumns() {
	s.neededFamilies = nil
}

// SpanFromEncDatums encodes a span with prefixLen constraint columns from the index.
// SpanFromEncDatums assumes that the EncDatums in values are in the order of the index columns.
func (s *SpanBuilder) SpanFromEncDatums(
	values sqlbase.EncDatumRow, prefixLen int,
) (roachpb.Span, error) {
	return sqlbase.MakeSpanFromEncDatums(
		s.keyPrefix, values[:prefixLen], s.indexColTypes[:prefixLen], s.indexColDirs[:prefixLen], s.table, s.index, &s.alloc)
}

// SpanFromDatumRow generates an index span with prefixLen constraint columns from the index.
// SpanFromDatumRow assumes that values is a valid table row for the SpanBuilder's table.
func (s *SpanBuilder) SpanFromDatumRow(
	values tree.Datums, prefixLen int, colMap map[sqlbase.ColumnID]int,
) (roachpb.Span, error) {
	span, _, err := sqlbase.EncodePartialIndexSpan(s.table, s.index, prefixLen, colMap, values, s.keyPrefix)
	return span, err
}

// MaybeSplitSpanIntoSeparateFamilies uses the needed columns from SetNeededColumns to maybe split
// the input span into multiple family specific spans. prefixLen is the number of index columns
// encoded in the span.
// TODO (rohany): I don't like this interface, it feels weird to have to pass in the number of columns you created.
//  But, there seem to be cases where we want the encoded key without families on it, which was why i did this.
func (s *SpanBuilder) MaybeSplitSpanIntoSeparateFamilies(
	span roachpb.Span, prefixLen int,
) roachpb.Spans {
	// Right now, we can/should only split a span into separate family point lookups if:
	// * s.neededFamilies is set
	// * the table has more than one family
	// * the index is the primary key
	// * we don't need all of the families
	// * we have all of the columns of the index
	if s.neededFamilies != nil &&
		len(s.table.Families) > 0 &&
		s.index.ID == s.table.PrimaryIndex.ID &&
		prefixLen == len(s.index.ColumnIDs) &&
		len(s.neededFamilies) < len(s.table.Families) {
		return sqlbase.SplitSpanIntoSeparateFamilies(span, s.neededFamilies)
	}
	return roachpb.Spans{span}
}

// MaybeSplitSpanIntoSeparateFamiliesWithNeededSet is the same as MaybeSplitSpanIntoSeparateFamilies
// but doesn't use the needed set from SetNeededColumns.
// TODO (rohany): is it better to just make the user compute needed families
//  and then pass that in here?
// This function is used when it's not safe to call set needed families.
// Its much slower than setting families however.
func (s *SpanBuilder) MaybeSplitSpanIntoSeparateFamiliesWithNeededSet(
	span roachpb.Span, prefixLen int, neededCols util.FastIntSet,
) roachpb.Spans {
	neededFamilies := sqlbase.NeededColumnFamilyIDs(s.table.ColumnIdxMap(), s.table.Families, neededCols)
	if neededFamilies != nil &&
		len(s.table.Families) > 0 &&
		s.index.ID == s.table.PrimaryIndex.ID &&
		prefixLen == len(s.index.ColumnIDs) &&
		len(s.neededFamilies) < len(s.table.Families) {
		return sqlbase.SplitSpanIntoSeparateFamilies(span, s.neededFamilies)
	}
	return roachpb.Spans{span}
}

// UnconstrainedSpans returns the full span corresponding to the SpanBuilder's
// table and index.
func (s *SpanBuilder) UnconstrainedSpans(forDelete bool) (roachpb.Spans, error) {
	return s.SpansFromConstraint(nil, exec.ColumnOrdinalSet{}, forDelete)
}

// appendSpansFromConstraintSpan converts a constraint.Span to one or more
// roachpb.Spans and appends them to the provided spans. It appends multiple
// spans in the case that multiple, non-adjacent column families should be
// scanned. The forDelete parameter indicates whether these spans will be used
// for row deletion.
func (s *SpanBuilder) appendSpansFromConstraintSpan(
	spans roachpb.Spans, cs *constraint.Span, needed util.FastIntSet, forDelete bool,
) (roachpb.Spans, error) {
	var span roachpb.Span
	var err error
	// Encode each logical part of the start key.
	span.Key, err = s.encodeConstraintKey(cs.StartKey())
	if err != nil {
		return nil, err
	}
	if cs.StartBoundary() == constraint.IncludeBoundary {
		span.Key = append(span.Key, s.interstices[cs.StartKey().Length()]...)
	} else {
		// We need to exclude the value this logical part refers to.
		span.Key = span.Key.PrefixEnd()
	}
	// Encode each logical part of the end key.
	span.EndKey, err = s.encodeConstraintKey(cs.EndKey())
	if err != nil {
		return nil, err
	}
	span.EndKey = append(span.EndKey, s.interstices[cs.EndKey().Length()]...)

	// Optimization: for single row lookups on a table with multiple column
	// families, only scan the relevant column families. This is disabled for
	// deletions to ensure that the entire row is deleted.
	if !forDelete && needed.Len() > 0 {
		// TODO (rohany): the call to MaybeSplitSpanIntoSeparateFamilies needs to be prefaced
		//  with setNeededFamilies. However, that would make this not safe to use by multiple threads...
		//  what is the solution here? I don't like this interface.
		return append(spans, s.MaybeSplitSpanIntoSeparateFamiliesWithNeededSet(span, cs.StartKey().Length(), needed)...), nil
	}

	// We tighten the end key to prevent reading interleaved children after the
	// last parent key. If cs.End.Inclusive is true, we also advance the key as
	// necessary.
	endInclusive := cs.EndBoundary() == constraint.IncludeBoundary
	span.EndKey, err = sqlbase.AdjustEndKeyForInterleave(s.table, s.index, span.EndKey, endInclusive)
	if err != nil {
		return nil, err
	}
	return append(spans, span), nil
}

// encodeConstraintKey encodes each logical part of a constraint.Key into a
// roachpb.Key; interstices[i] is inserted before the i-th value.
func (s *SpanBuilder) encodeConstraintKey(ck constraint.Key) (roachpb.Key, error) {
	var key roachpb.Key
	for i := 0; i < ck.Length(); i++ {
		val := ck.Value(i)
		key = append(key, s.interstices[i]...)

		var err error
		// For extra columns (like implicit columns), the direction
		// is ascending.
		dir := encoding.Ascending
		if i < len(s.index.ColumnDirections) {
			dir, err = s.index.ColumnDirections[i].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		}

		if s.index.Type == sqlbase.IndexDescriptor_INVERTED {
			keys, err := sqlbase.EncodeInvertedIndexTableKeys(val, key)
			if err != nil {
				return nil, err
			}
			if len(keys) > 1 {
				err := errors.AssertionFailedf("trying to use multiple keys in index lookup")
				return nil, err
			}
			key = keys[0]
		} else {
			key, err = sqlbase.EncodeTableKey(key, val, dir)
			if err != nil {
				return nil, err
			}
		}
	}
	return key, nil
}
