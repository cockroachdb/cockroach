// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

func unconstrainedSpans(
	tableDesc *sqlbase.ImmutableTableDescriptor, index *sqlbase.IndexDescriptor, forDelete bool,
) (roachpb.Spans, error) {
	return spansFromConstraint(tableDesc, index, nil, exec.ColumnOrdinalSet{}, forDelete)
}

// spansFromConstraint converts the spans in a Constraint to roachpb.Spans.
//
// interstices are pieces of the key that need to be inserted after each column
// (for interleavings).
func spansFromConstraint(
	tableDesc *sqlbase.ImmutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	c *constraint.Constraint,
	needed exec.ColumnOrdinalSet,
	forDelete bool,
) (roachpb.Spans, error) {
	interstices := make([][]byte, len(index.ColumnDirections)+len(index.ExtraColumnIDs)+1)
	interstices[0] = sqlbase.MakeIndexKeyPrefix(tableDesc.TableDesc(), index.ID)
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(eisen): too much of this code is copied from EncodePartialIndexKey.
		sharedPrefixLen := 0
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is already encoded in interstices[0].
			if i != 0 {
				interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(ancestor.TableID))
				interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(ancestor.IndexID))
			}
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
			interstices[sharedPrefixLen] = encoding.EncodeInterleavedSentinel(interstices[sharedPrefixLen])
		}
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(tableDesc.ID))
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(index.ID))
	}

	var spans roachpb.Spans
	var err error
	if c == nil || c.IsUnconstrained() {
		// Encode a full span.
		spans, err = appendSpansFromConstraintSpan(
			spans, tableDesc, index, &constraint.UnconstrainedSpan, interstices, needed, forDelete)
		if err != nil {
			return nil, err
		}
		return spans, nil
	}

	spans = make(roachpb.Spans, 0, c.Spans.Count())
	for i := 0; i < c.Spans.Count(); i++ {
		spans, err = appendSpansFromConstraintSpan(
			spans, tableDesc, index, c.Spans.Get(i), interstices, needed, forDelete)
		if err != nil {
			return nil, err
		}
	}
	return spans, nil
}

// encodeConstraintKey encodes each logical part of a constraint.Key into a
// roachpb.Key; interstices[i] is inserted before the i-th value.
func encodeConstraintKey(
	index *sqlbase.IndexDescriptor, ck constraint.Key, interstices [][]byte,
) (roachpb.Key, error) {
	var key roachpb.Key
	for i := 0; i < ck.Length(); i++ {
		val := ck.Value(i)
		key = append(key, interstices[i]...)

		var err error
		// For extra columns (like implicit columns), the direction
		// is ascending.
		dir := encoding.Ascending
		if i < len(index.ColumnDirections) {
			dir, err = index.ColumnDirections[i].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		}

		if index.Type == sqlbase.IndexDescriptor_INVERTED {
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

// appendSpansFromConstraintSpan converts a constraint.Span to one or more
// roachpb.Spans and appends them to the provided spans. It appends multiple
// spans in the case that multiple, non-adjacent column families should be
// scanned. The forDelete parameter indicates whether these spans will be used
// for row deletion.
func appendSpansFromConstraintSpan(
	spans roachpb.Spans,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	cs *constraint.Span,
	interstices [][]byte,
	needed exec.ColumnOrdinalSet,
	forDelete bool,
) (roachpb.Spans, error) {
	var s roachpb.Span
	var err error
	// Encode each logical part of the start key.
	s.Key, err = encodeConstraintKey(index, cs.StartKey(), interstices)
	if err != nil {
		return nil, err
	}
	if cs.StartBoundary() == constraint.IncludeBoundary {
		s.Key = append(s.Key, interstices[cs.StartKey().Length()]...)
	} else {
		// We need to exclude the value this logical part refers to.
		s.Key = s.Key.PrefixEnd()
	}
	// Encode each logical part of the end key.
	s.EndKey, err = encodeConstraintKey(index, cs.EndKey(), interstices)
	if err != nil {
		return nil, err
	}
	s.EndKey = append(s.EndKey, interstices[cs.EndKey().Length()]...)

	// Optimization: for single row lookups on a table with multiple column
	// families, only scan the relevant column families. This is disabled for
	// deletions to ensure that the entire row is deleted.
	if !forDelete &&
		needed.Len() > 0 &&
		index.ID == tableDesc.PrimaryIndex.ID &&
		len(tableDesc.Families) > 1 &&
		cs.StartKey().Length() == len(tableDesc.PrimaryIndex.ColumnIDs) &&
		s.Key.Equal(s.EndKey) {
		neededFamilyIDs := sqlbase.NeededColumnFamilyIDs(tableDesc.ColumnIdxMap(), tableDesc.Families, needed)
		if len(neededFamilyIDs) < len(tableDesc.Families) {
			return append(spans, sqlbase.SplitSpanIntoSeparateFamilies(s, neededFamilyIDs)...), nil
		}
	}

	// We tighten the end key to prevent reading interleaved children after the
	// last parent key. If cs.End.Inclusive is true, we also advance the key as
	// necessary.
	endInclusive := cs.EndBoundary() == constraint.IncludeBoundary
	s.EndKey, err = sqlbase.AdjustEndKeyForInterleave(tableDesc.TableDesc(), index, s.EndKey, endInclusive)
	if err != nil {
		return nil, err
	}
	return append(spans, s), nil
}
