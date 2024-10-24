// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil

import (
	"context"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
)

const DescriptorGetBatchSize = 100

type RangeUtilCatalog interface {
}

type RangeElement struct {
	Database string
	Table    string
	Index    string
}

type RangeContents []RangeElement

// SortableRangeDescriptors is a simple utility class for sorting a list
// of range descriptors by their start key.
type SortableRangeDescriptors []roachpb.RangeDescriptor

func (r SortableRangeDescriptors) Len() int {
	return len(r)
}
func (r SortableRangeDescriptors) Less(i, j int) bool {
	return r[i].StartKey.Less(r[j].StartKey)
}

func (r SortableRangeDescriptors) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}

// GetDescriptorNamesInRanges converts the input to sorted batches of 100 range
// descriptors, fetches schema descriptors from those ranges, and returns
// the merged set of those descriptors as its response.
func GetDescriptorNamesInRanges(
	ctx context.Context, txn descs.Txn, codec keys.SQLCodec, ranges []roachpb.RangeDescriptor,
) (map[roachpb.RangeID]RangeContents, error) {

	sort.Stable(SortableRangeDescriptors(ranges))
	descriptors := map[roachpb.RangeID]RangeContents{}
	for i := 0; i < len(ranges); i += DescriptorGetBatchSize {
		fmt.Println(getDescriptorNamesInRangeBatch(ctx, txn, codec, ranges[i:i+DescriptorGetBatchSize]))
	}

	return descriptors, nil
}

// isIndexInRange is a simple utility for determining whether the spans of an
// index and a range overlap.
func isIndexInRange(
	codec keys.SQLCodec,
	rangeDesc roachpb.RangeDescriptor,
	table catalog.TableDescriptor,
	index catalog.Index,
) bool {
	indexStart := codec.IndexPrefix(uint32(table.GetID()), uint32(index.GetID()))
	indexEnd := indexStart.Next()
	// range start must be less than index end
	rStartBeforeEnd := rangeDesc.StartKey.AsRawKey().Compare(indexStart) == -1
	// range end must be more than index start
	rEndAfterStart := rangeDesc.EndKey.AsRawKey().Compare(indexEnd) == 1
	return rStartBeforeEnd && rEndAfterStart
}

// getDatabaseDescriptorsByID uses the collection's GetAllDatabaseDescriptors
// function and remaps the response from an array to a map of DatabaseDescriptors
// where the keys are the descriptor IDs.
func getDatabaseDescriptorsByID(
	ctx context.Context, txn descs.Txn,
) (map[descpb.ID]catalog.DatabaseDescriptor, error) {
	collection := txn.Descriptors()
	descriptors, err := collection.GetAllDatabaseDescriptors(ctx, txn.KV())
	result := map[descpb.ID]catalog.DatabaseDescriptor{}
	if err != nil {
		return nil, err
	}

	for _, descriptor := range descriptors {
		result[descriptor.GetID()] = descriptor
	}

	return result, nil
}

// mapDescriptorsToRanges is a utility function which iterates over two lists,
// one consisting of ordered ranges, and the other consisting of ordered descriptors
// and outputs a mapping from range to table, index values.
func mapDescriptorsToRanges(
	codec keys.SQLCodec,
	databases map[descpb.ID]catalog.DatabaseDescriptor,
	ranges []roachpb.RangeDescriptor,
	descriptors []catalog.Descriptor,
) (map[roachpb.RangeID]RangeContents, error) {
	// move through the ranges + descriptors
	// using two indexes, i, j.
	// while i and j are valid
	i := 0
	j := 0
	results := map[roachpb.RangeID]RangeContents{}
	contents := RangeContents{}
	flushToResults := func(rangeID roachpb.RangeID) {
		results[rangeID] = contents
		contents = RangeContents{}
	}

	for i < len(ranges) && j < len(descriptors) {
		// if descriptor is not a table, skip to next descriptor.
		table, ok := descriptors[j].(catalog.TableDescriptor)
		if !ok {
			j++
			continue
		}

		// check and add indexes from this descriptor to the range.
		for _, idx := range table.AllIndexes() {
			if isIndexInRange(codec, ranges[i], table, idx) {
				elem := RangeElement{
					Database: databases[table.GetParentID()].GetName(),
					Table:    table.GetName(),
					Index:    idx.GetName(),
				}
				contents = append(contents, elem)
			}
		}

		// is the end of a table range simply the Next on the prefix?
		// /Table/110 -> /Table/111
		tableEnd := codec.TablePrefix(uint32(table.GetID())).Next()
		rangeEndFurther := ranges[i].EndKey.AsRawKey().Compare(tableEnd) == 1
		if rangeEndFurther {
			j++
		} else {
			i++
			flushToResults(ranges[i].RangeID)
		}
	}

	if i < len(ranges) {
		flushToResults(ranges[i].RangeID)
	}
	return results, nil
}

// GetDescriptorNamesInRanges translates a set of ordered ranges into a
// rangeid -> RangeContents mapping. It does this by executing the fololowing steps:
//  1. Convert the set of ranges to a set of spans.
//  2. Get the descriptors that fall within the given spans.
//  3. For each table descriptor that corresponds to a range:
//     a. For each index in the table descriptor.
//     * If the index falls within the range, add it to the range's contents
func getDescriptorNamesInRangeBatch(
	ctx context.Context, txn descs.Txn, codec keys.SQLCodec, ranges []roachpb.RangeDescriptor,
) (map[roachpb.RangeID]RangeContents, error) {
	spans := []roachpb.Span{}
	for _, rangeDesc := range ranges {
		spans = append(spans, roachpb.Span{
			Key:    rangeDesc.StartKey.AsRawKey(),
			EndKey: rangeDesc.EndKey.AsRawKey(),
		})
	}

	collection := txn.Descriptors()
	nscatalog, err := collection.GetDescriptorsInSpans(ctx, txn.KV(), spans)
	if err != nil {
		return nil, err
	}
	// Assumption that OrderedDescriptors returns the descriptors in order of
	descriptors := nscatalog.OrderedDescriptors()
	databases, err := getDatabaseDescriptorsByID(ctx, txn)
	if err != nil {
		return nil, err
	}

	return mapDescriptorsToRanges(codec, databases, ranges, descriptors)
}
