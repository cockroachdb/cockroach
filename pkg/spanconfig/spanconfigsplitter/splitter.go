// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package spanconfigsplitter is able to split sql descriptors into its
// constituent spans.
package spanconfigsplitter

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var _ spanconfig.Splitter = &Splitter{}

// Splitter is the concrete implementation of spanconfig.Splitter.
//
// TODO(irfansharif): We could alternatively return only the number of split
// points implied. It's possible to compute it without having to decode any keys
// whatsoever. Consider our table hierarchy again:
//
// 		table -> index -> partition -> partition
//
// Where each partition is either a PARTITION BY LIST kind (where it can then be
// further partitioned), or a PARTITION BY RANGE kind (no further partitioning
// possible). We can classify each parent-child link into two types:
//
// (a) Contiguous      {index, list partition} -> range partition
// (b) Non-contiguous  table -> index, {index, list partition} -> list partition
//
// - Contiguous links are the sort where each child span is contiguous with
//   another, and that the set of all child spans encompass the parent's span.
//   For an index that's partitioned by range:
//
// 		CREATE TABLE db.range(i INT PRIMARY KEY, j INT) PARTITION BY RANGE (i) (
//  		PARTITION less_than_five       VALUES FROM (minvalue) to (5),
//			PARTITION between_five_and_ten VALUES FROM (5) to (10),
//			PARTITION greater_than_ten     VALUES FROM (10) to (maxvalue)
//		);
//
//   With table ID as 106, the parent index span is /Table/106/{1-2}. The child
//   spans are /Table/106/1{-/5}, /Table/106/1/{5-10} and /Table/106/{1/10-2}.
//   They're contiguous and put together, they wholly encompass the parent span.
//
// - Non-contiguous links by contrast are when child spans are neither
//   contiguous with respect to one another, and nor do they start and end at
//   the parent span's boundaries. For a table with a secondary index:
//
//		CREATE TABLE db.t(i INT PRIMARY KEY, j INT);
//		CREATE INDEX idx ON db.t (j);
//		DROP INDEX db.t@idx;
//		CREATE INDEX idx ON db.t (j);
//
//   With table ID as 106, the parent table span is /Table/10{6-7}. The child
//   spans are /Table/106/{1-2} and /Table/106/{3-4}. Compared to the parent
//   span, we're missing /Table/106{-/1}, /Table/106/{2-3}, /Table/10{6/4-7}.
//
// For N children:
// - For a contiguous link, the number of splits equals the number of child
//   elements (i.e. N).
// - For a non-contiguous link, the number of splits equals N + 1 + N. For N
//   children, there are N - 1 gaps. There are also 2 gaps at the start and end
//   of the parent span. Summing that with the N children span themselves, we
//   get to the formula above. This assumes that the N child elements aren't
//   further subdivided, if they are (we can compute it recursively), the
//   formula becomes N + 1 + Σ(grand child spans).
type Splitter struct {
	codec keys.SQLCodec
	knobs *spanconfig.TestingKnobs
}

// New constructs and returns a Splitter.
func New(codec keys.SQLCodec, knobs *spanconfig.TestingKnobs) *Splitter {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Splitter{
		codec: codec,
		knobs: knobs,
	}
}

// Splits is part of the spanconfig.Splitter interface.
func (s *Splitter) Splits(
	ctx context.Context, table catalog.TableDescriptor,
) ([]roachpb.Key, error) {
	if s.knobs.ExcludeDroppedDescriptorsFromLookup && table.Dropped() {
		return nil, nil // we're excluding this descriptor; nothing to do here
	}

	tableStartKey := s.codec.TablePrefix(uint32(table.GetID()))
	tableEndKey := tableStartKey.PrefixEnd()
	tableSpan := roachpb.Span{
		Key:    tableStartKey,
		EndKey: tableEndKey,
	}

	var indexSpans []roachpb.Span
	if err := catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
		currentIndexSpan := table.IndexSpan(s.codec, index.GetID())

		var emptyPrefix []tree.Datum
		indexPartitionSpans, err := s.splitInner(partition{
			Partitioning: index.GetPartitioning(),
			table:        table,
			index:        index,
			prefixSpan:   currentIndexSpan,
			prefixDatums: emptyPrefix,
		})
		if err != nil {
			return err
		}

		{ // TODO(irfansharif): Should we only assert under test builds?
			if len(indexPartitionSpans) == 0 {
				return errors.AssertionFailedf("expected non-zero index partition spans")
			}
			sort.Sort(roachpb.Spans(indexPartitionSpans))
			if firstKey := indexPartitionSpans[0].Key; !firstKey.Equal(currentIndexSpan.Key) {
				return errors.AssertionFailedf("expected first partition key (%s) to match index start (%s)", firstKey, currentIndexSpan.Key)
			}
			if endKey := indexPartitionSpans[len(indexPartitionSpans)-1].EndKey; !endKey.Equal(currentIndexSpan.EndKey) {
				return errors.AssertionFailedf("expected last partition end key (%s) to match index end (%s)", endKey, currentIndexSpan.EndKey)
			}
			for i := range indexPartitionSpans {
				if i == 0 {
					continue
				}
				if prev, cur := indexPartitionSpans[i-1], indexPartitionSpans[i]; !prev.EndKey.Equal(cur.Key) {
					return errors.AssertionFailedf("expected partitions %s and %s to be adjacent", prev, cur)
				}
			}
		}

		indexSpans = append(indexSpans, indexPartitionSpans...) // [start of current index, ...) ... [..., end of current index)
		return nil
	}); err != nil {
		return nil, err
	}

	tableSpans, err := segment(tableSpan, indexSpans)
	if err != nil {
		return nil, err
	}

	splits := make([]roachpb.Key, len(tableSpans))
	for i, sp := range tableSpans {
		splits[i] = sp.Key
	}
	return splits, nil
}

func (s *Splitter) splitInner(part partition) ([]roachpb.Span, error) {
	if part.prefixSpan.Equal(roachpb.Span{}) {
		return nil, errors.AssertionFailedf("expected non-empty prefix span")
	}

	if part.NumColumns() == 0 {
		return []roachpb.Span{part.prefixSpan}, nil // no partitioning
	}

	if part.NumRanges() > 0 {
		var rangePartitionSpans []roachpb.Span
		if err := part.ForEachRange(func(_ string, from, to []byte) error {
			alloc := &tree.DatumAlloc{}
			_, fromKey, err := rowenc.DecodePartitionTuple(
				alloc, s.codec, part.table, part.index, part, from, part.prefixDatums)
			if err != nil {
				return err
			}
			_, toKey, err := rowenc.DecodePartitionTuple(
				alloc, s.codec, part.table, part.index, part, to, part.prefixDatums)
			if err != nil {
				return err
			}

			rangePartitionSpans = append(rangePartitionSpans, roachpb.Span{
				Key:    fromKey,
				EndKey: toKey,
			})
			return nil
		}); err != nil {
			return nil, err
		}
		return rangePartitionSpans, nil
	}

	var listPartitionSpans []roachpb.Span
	if err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
		for _, valueEncBuf := range values {
			alloc := &tree.DatumAlloc{}
			t, keyPrefix, err := rowenc.DecodePartitionTuple(
				alloc, s.codec, part.table, part.index, part, valueEncBuf, part.prefixDatums)
			if err != nil {
				return err
			}

			subPartitioningPrefix := roachpb.Key(keyPrefix)
			subPartitioningPrefixSpan := roachpb.Span{
				Key:    subPartitioningPrefix,
				EndKey: subPartitioningPrefix.PrefixEnd(),
			}
			subPartition := partition{
				Partitioning: subPartitioning,
				table:        part.table,
				index:        part.index,
				prefixSpan:   subPartitioningPrefixSpan,
				prefixDatums: append(part.prefixDatums, t.Datums...),
			}
			subPartitionSpans, err := s.splitInner(subPartition)
			if err != nil {
				return err
			}

			if len(subPartitionSpans) == 1 && subPartitionSpans[0].Equal(part.prefixSpan) {
				if t.SpecialCount == 0 {
					return errors.AssertionFailedf("expected partitioning by DEFAULT")
				}

				// NB: PARTITION BY LISTs can only have DEFAULT partition values (i.e.
				// no {MIN,MAX}VALUE).
				//
				// A. A zero SpecialCount indicates that we're dealing partitions over
				//    actual values; we can simply accumulate the sub-partition spans.
				// B. If we do have a DEFAULT partitioning value and:
				// 	  1. It isn't further sub-partitioned, the sub-partition span we get
				//       is just our parent prefix span. We don't add it here -- below
				//       we'll ensure that we're generating coverings for the entire
				//       prefix.
				//    2. It's further sub-partitioned, and:
				//   		 i.  If all subsequent subpartitions are only PARTITION BY LISTs
				//           over DEFAULT partition values, the span we get is just our
				//           parent prefix span. We don't add it here for the same reason
				//           as B1.
				//       ii. If it's subpartitioned by RANGE or PARTITION BY LISTs over
				//           non-DEFAULT partition values, we'll have meaningful
				//           subpartition spans. We'll add them below.
				continue
			}

			listPartitionSpans = append(listPartitionSpans, subPartitionSpans...)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if len(listPartitionSpans) == 0 {
		return []roachpb.Span{part.prefixSpan}, nil // no sub partitioning
	}

	return segment(part.prefixSpan, listPartitionSpans)
}

// segment returns takes in an outer span [a,z) and a list of inner spans that
// are wholly enclosed by the outer span, and returns a list of segments of the
// form (after sorting inner first):
//
//  [outer-start, inner[0]-start)
//  [inner[0]-start, inner[0]-end)
//  [inner[0]-end, inner[1]-start)
//  ...
//  [inner[len(inner)-1]-end, outer-end)
//
func segment(outer roachpb.Span, inner []roachpb.Span) ([]roachpb.Span, error) {
	sort.Sort(roachpb.Spans(inner))

	var gapSpans []roachpb.Span
	for i := range inner {
		if !outer.Contains(inner[i]) {
			return nil, errors.AssertionFailedf("expected %s to contain %s", outer, inner[i])
		}
		var gapSpan roachpb.Span
		if i == 0 {
			gapSpan = roachpb.Span{ // gap between start of outer span to first inner span
				Key:    outer.Key,
				EndKey: inner[i].Key,
			}
		} else {
			gapSpan = roachpb.Span{ // gap between consecutive partition spans
				Key:    inner[i-1].EndKey,
				EndKey: inner[i].Key,
			}
		}
		gapSpans = append(gapSpans, gapSpan)
	}

	if len(inner) > 0 {
		gapSpans = append(gapSpans, roachpb.Span{ // gap between end of last inner partition span and end of outer span
			Key:    inner[len(inner)-1].EndKey,
			EndKey: outer.EndKey,
		})
	}

	var result []roachpb.Span
	result = append(result, inner...)
	for _, gapSpan := range gapSpans {
		if !gapSpan.Valid() {
			// It's possible for the inner adjacent to one another, so for the "gaps"
			// between them to be non-existent. It's also possible for the "gaps" at
			// the start and end of the outer span to be empty. We filter out these
			// invalid gaps.
			continue
		}
		result = append(result, gapSpan)
	}
	return result, nil
}

// partition groups together everything needed to determine the split points of
// a given partition.
type partition struct {
	catalog.Partitioning // partition itself

	table        catalog.TableDescriptor // table the partition is part of
	index        catalog.Index           // index being partitioned
	prefixSpan   roachpb.Span            // keyspan being partitioned
	prefixDatums []tree.Datum            // datums for partitioning columns in the prefix (non-empty for subpartitions)
}
