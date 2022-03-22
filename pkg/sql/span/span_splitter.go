// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package span

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// Splitter is a helper for splitting single-row spans into more specific spans
// that target a subset of column families.
//
// Note that a zero Splitter (NoopSplitter) is a usable instance that never
// splits spans.
type Splitter struct {
	// numKeyColumns is the number of key columns in the index; a span needs to
	// constrain this many columns to be considered for splitting.
	// It is 0 if splitting is not possible.
	numKeyColumns int

	// neededFamilies contains the family IDs into which spans will be split, or
	// nil if splitting is not possible.
	neededFamilies []descpb.FamilyID
}

// NoopSplitter returns a splitter that never splits spans.
func NoopSplitter() Splitter {
	return Splitter{}
}

// MakeSplitter returns a Splitter that splits spans into more specific spans
// for the needed families. If span splitting is not possible/useful, returns
// the NoopSplitter (which never splits).
func MakeSplitter(
	table catalog.TableDescriptor, index catalog.Index, neededColOrdinals util.FastIntSet,
) Splitter {
	// We can only split a span into separate family specific point lookups if:
	//
	// * The table is not a special system table. (System tables claim to have
	//   column families, but actually do not, since they're written to with
	//   raw KV puts in a "legacy" way.)
	if catalog.IsSystemDescriptor(table) {
		return NoopSplitter()
	}
	// * The index is unique.
	if !index.IsUnique() {
		return NoopSplitter()
	}

	// If we're looking at a secondary index...
	if index.GetID() != table.GetPrimaryIndexID() {

		// * The index cannot be inverted.
		if index.GetType() != descpb.IndexDescriptor_FORWARD {
			return NoopSplitter()
		}

		// * The index is a new enough version.
		if index.GetVersion() < descpb.SecondaryIndexFamilyFormatVersion {
			return NoopSplitter()
		}
	}

	neededFamilies := rowenc.NeededColumnFamilyIDs(neededColOrdinals, table, index)

	// Sanity check.
	for i := range neededFamilies[1:] {
		if neededFamilies[i] >= neededFamilies[i+1] {
			panic(errors.AssertionFailedf("family IDs not increasing"))
		}
	}

	// * The index either has just 1 family (so we'll make a GetRequest) or we
	//   need fewer than every column family in the table (otherwise we'd just
	//   make a big ScanRequest).
	// TODO(radu): should we be using IndexKeysPerRow() instead?
	numFamilies := len(table.GetFamilies())
	if numFamilies > 1 && len(neededFamilies) == numFamilies {
		return NoopSplitter()
	}

	return Splitter{
		numKeyColumns:  index.NumKeyColumns(),
		neededFamilies: neededFamilies,
	}
}

// MakeSplitterWithFamilyIDs creates a Splitter that splits spans that constrain
// all key columns along the given family IDs.
//
// Returns NoopSplitter if familyIDs is empty.
//
// This should only be used when the conditions checked by MakeSplitter are
// already known to be satisfied.
func MakeSplitterWithFamilyIDs(numKeyColumns int, familyIDs []descpb.FamilyID) Splitter {
	if len(familyIDs) == 0 {
		return NoopSplitter()
	}

	// Sanity check.
	for i := range familyIDs[1:] {
		if familyIDs[i] >= familyIDs[i+1] {
			panic(errors.AssertionFailedf("family IDs not increasing"))
		}
	}
	return Splitter{
		numKeyColumns:  numKeyColumns,
		neededFamilies: familyIDs,
	}
}

// FamilyIDs returns the family IDs into which spans will be split, or nil if
// splitting is not possible.
func (s *Splitter) FamilyIDs() []descpb.FamilyID {
	return s.neededFamilies
}

// IsNoop returns true if this instance will never split spans.
func (s *Splitter) IsNoop() bool {
	return s.numKeyColumns == 0
}

// MaybeSplitSpanIntoSeparateFamilies uses the needed columns configured by
// MakeSplitter to conditionally split the input span into multiple family
// specific spans.
//
// prefixLen is the number of index columns encoded in the span.
// containsNull indicates if one of the encoded columns was NULL.
//
// The function accepts a slice of spans to append to.
func (s *Splitter) MaybeSplitSpanIntoSeparateFamilies(
	appendTo roachpb.Spans, span roachpb.Span, prefixLen int, containsNull bool,
) roachpb.Spans {
	if s.CanSplitSpanIntoFamilySpans(prefixLen, containsNull) {
		return rowenc.SplitRowKeyIntoFamilySpans(appendTo, span.Key, s.neededFamilies)
	}
	return append(appendTo, span)
}

// CanSplitSpanIntoFamilySpans returns whether a span encoded with prefixLen
// encoded columns can be safely split into 1 or more family specific spans.
func (s *Splitter) CanSplitSpanIntoFamilySpans(prefixLen int, containsNull bool) bool {
	if s.IsNoop() {
		// This is a no-op splitter (the necessary conditions in MakeSplitter are
		// not satisfied).
		return false
	}
	// * The index must be fully constrained.
	if prefixLen != s.numKeyColumns {
		return false
	}
	// * The index constraint must not contain null, since that would cause the
	//   index key to not be completely knowable.
	if containsNull {
		return false
	}

	// We've passed all the conditions, and should be able to safely split this
	// span into multiple column-family-specific spans.
	return true
}

// ExistenceCheckSpan returns a span that can be used to check whether a row
// exists: when there are multiple families, we narrow down the span to family
// 0.
func (s *Splitter) ExistenceCheckSpan(
	span roachpb.Span, prefixLen int, containsNull bool,
) roachpb.Span {
	if s.CanSplitSpanIntoFamilySpans(prefixLen, containsNull) {
		// If it is safe to split this lookup into multiple families, generate a
		// point lookup for family 0. Because we are just checking for existence, we
		// only need family 0.
		key := keys.MakeFamilyKey(span.Key, 0 /* famID */)
		return roachpb.Span{Key: key, EndKey: roachpb.Key(key).PrefixEnd()}
	}
	return span
}
