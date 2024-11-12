// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil

import (
	"context"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/errors"
)

// This file contains a set of helper functions which are useful for turning
// ranges into the SQL related contents which reside within. It includes:
// 1. A utility function which turns a range into a span, and clamps it
//     to its tenant's table space.
// 2. A utility function which takes the above spans and uses the catalog
//     and new descriptor by span utility to turn those spans into a set of
//     table descriptors ordered by id.
// 3. A utility function which transforms those table descriptors into a
//     set of (database, table, index) names which deduplicate and identify
//     each index uniquely.
// 4. A utility function, which merges the ranges and indexes into a map
//     keyed by RangeID whose values are the above index names.
// 5. A primary entrypoint for consumers from which a set of ranges can be
//     passed in and a mapping from those ranges to indexes can be
//     returned.

// rangeLess encapsulate a slice of ranges and returns a comparator function
// so that it can be sorted by go's builtin sort package.
func rangeLess(ranges []roachpb.RangeDescriptor) func(i, j int) bool {
	return func(i, j int) bool {
		return ranges[i].StartKey.Less(ranges[j].StartKey)
	}
}

// GetRangeIndexMappings translates a set of ordered ranges into a
// RangeID -> IndexNamesList mapping. It does this by executing the following steps:
//  1. Sort the incoming ranges by start key
//  2. Convert the set of ranges to a set of spans.
//  3. Get the table descriptors that fall within the given spans.
//  4. Get the database, table and index name for all indexes found in the descriptors.
//  5. Return a mapping of the indexes which appear in each range.
func GetRangeIndexMapping(
	ctx context.Context,
	txn descs.Txn,
	codec keys.SQLCodec,
	databases map[descpb.ID]catalog.DatabaseDescriptor,
	ranges []roachpb.RangeDescriptor,
) (map[roachpb.RangeID]IndexNamesList, error) {
	sort.Slice(ranges, rangeLess(ranges))
	spans, err := RangesToTableSpans(codec, ranges)
	if err != nil {
		return nil, err
	}

	tables, err := SpansToOrderedTableDescriptors(ctx, txn, spans)
	if err != nil {
		return nil, err
	}

	indexes, err := TableDescriptorsToIndexNames(codec, databases, tables)
	if err != nil {
		return nil, err
	}

	return MapRangesToIndexes(ranges, indexes), nil
}

// MapRangesToIndexes is a utility function which iterates over two lists,
// one consisting of ordered ranges, and the other consisting of ordered index names
// and outputs a mapping from range to index.
func MapRangesToIndexes(
	ranges []roachpb.RangeDescriptor, indexes IndexNamesList,
) map[roachpb.RangeID]IndexNamesList {
	results := map[roachpb.RangeID]IndexNamesList{}
	contents := IndexNamesList{}
	flushToResults := func(rangeID roachpb.RangeID) {
		results[rangeID] = contents
		contents = IndexNamesList{}
	}

	// move through the ranges + descriptors
	// using two indexes, i, j.
	// while i and j are valid
	i := 0
	j := 0
	for i < len(ranges) && j < len(indexes) {
		rangeSpan := ranges[i].KeySpan().AsRawSpanWithNoLocals()
		if rangeSpan.Overlaps(indexes[j].Span) {
			contents = append(contents, indexes[j])
		}

		if ranges[i].EndKey.AsRawKey().Less(indexes[j].Span.EndKey) {
			flushToResults(ranges[i].RangeID)
			i++
		} else {
			j++
		}
	}

	if i < len(ranges) {
		flushToResults(ranges[i].RangeID)
	}
	return results
}

// RangeToTableSpans converts a set of ranges to a set of spans bound
// to the codec's SQL table space, and removed if the bound span is
// zero length.
func RangesToTableSpans(
	codec keys.SQLCodec, ranges []roachpb.RangeDescriptor,
) ([]roachpb.Span, error) {
	spans := []roachpb.Span{}

	// cannot use keys.TableDataMin/Max
	// Check the following: keys.TableDataMax.Less(keys.MakeSQLCodec(3).TablePrefix(1)) == true
	bounds := roachpb.Span{
		Key:    codec.TablePrefix(0),
		EndKey: codec.TablePrefix(math.MaxUint32),
	}
	for _, rangeDesc := range ranges {
		span, err := rangeDesc.KeySpan().AsRawSpanWithNoLocals().Clamp(bounds)
		if err != nil {
			return nil, err
		}
		if !span.ZeroLength() {
			spans = append(spans, span)
		}
	}

	return spans, nil
}

// SpansToOrderedTableDescriptors uses the transaction's collection to turn a set of
// spans to a set of descriptors which describe the table space in which those spans lie.
func SpansToOrderedTableDescriptors(
	ctx context.Context, txn descs.Txn, spans []roachpb.Span,
) ([]catalog.TableDescriptor, error) {
	descriptors := []catalog.TableDescriptor{}
	collection := txn.Descriptors()
	nscatalog, err := collection.GetDescriptorsInSpans(ctx, txn.KV(), spans)
	if err != nil {
		return nil, err
	}

	allDescriptors := nscatalog.OrderedDescriptors()
	for _, iDescriptor := range allDescriptors {
		if table, ok := iDescriptor.(catalog.TableDescriptor); ok {
			descriptors = append(descriptors, table)
		}
	}
	return descriptors, nil
}

// TableDescriptorsToIndexNames maps a set of descriptors to the
// database, table, index combinations within. It assumes that every
// table has at least one index, the descriptors input are ordered,
// and that there can be duplicates of the descriptors.
func TableDescriptorsToIndexNames(
	codec keys.SQLCodec,
	databases map[descpb.ID]catalog.DatabaseDescriptor,
	tables []catalog.TableDescriptor,
) (IndexNamesList, error) {
	seen := map[string]struct{}{}
	indexes := IndexNamesList{}

	for _, table := range tables {
		database, ok := databases[table.GetParentID()]
		if !ok {
			return nil, errors.Errorf("could not find database for table %s", table.GetName())
		}
		for _, index := range table.AllIndexes() {
			key := database.GetName() + table.GetName() + index.GetName()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			indexes = append(indexes, IndexNames{
				Database: database.GetName(),
				Table:    table.GetName(),
				Index:    index.GetName(),
				Span:     spanFromIndex(codec, table, index),
			})
		}
	}

	return indexes, nil
}

func spanFromIndex(
	codec keys.SQLCodec, table catalog.TableDescriptor, index catalog.Index,
) roachpb.Span {
	prefix := codec.IndexPrefix(uint32(table.GetID()), uint32(index.GetID()))
	return roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}
}
