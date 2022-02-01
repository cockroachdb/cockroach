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
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var _ spanconfig.Splitter = &Splitter{}

// Splitter is the concrete implementation of spanconfig.Splitter.
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

	var tableSpans []roachpb.Span
	var lastIndexSpan roachpb.Span
	if err := catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
		currentIndexSpan := table.IndexSpan(s.codec, index.GetID())
		if index.Ordinal() == 0 {
			tableSpans = append(tableSpans, roachpb.Span{ // [start of table, start of primary index)
				Key:    tableStartKey,
				EndKey: currentIndexSpan.Key,
			})
		} else {
			gapBetweenIndexesSpan := roachpb.Span{ // [end of last index, start of current index)
				Key:    lastIndexSpan.EndKey,
				EndKey: currentIndexSpan.Key,
			}
			if gapBetweenIndexesSpan.Valid() {
				tableSpans = append(tableSpans, gapBetweenIndexesSpan)
			}
		}

		var emptyPrefix []tree.Datum
		indexPartitionSpans, err := s.splitInner(ctx, partition{
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
				log.Fatal(ctx, "expected non-zero index partition spans")
			}
			sort.Sort(roachpb.Spans(indexPartitionSpans))
			if firstKey := indexPartitionSpans[0].Key; !firstKey.Equal(currentIndexSpan.Key) {
				log.Fatalf(ctx, "expected first partition key (%s) to match index start (%s)", firstKey, currentIndexSpan.Key)
			}
			if endKey := indexPartitionSpans[len(indexPartitionSpans)-1].EndKey; !endKey.Equal(currentIndexSpan.EndKey) {
				log.Fatalf(ctx, "expected last partition end key (%s) to match index end (%s)", endKey, currentIndexSpan.EndKey)
			}
			for i := range indexPartitionSpans {
				if i == 0 {
					continue
				}
				if prev, cur := indexPartitionSpans[i-1], indexPartitionSpans[i]; !prev.EndKey.Equal(cur.Key) {
					log.Fatalf(ctx, "expected partitions %s and %s to be adjacent", prev, cur)
				}
			}
		}

		tableSpans = append(tableSpans, indexPartitionSpans...) // [start of current index, ...) ... [..., end of current index)
		lastIndexSpan = currentIndexSpan
		return nil
	}); err != nil {
		return nil, err
	}
	tableSpans = append(tableSpans, roachpb.Span{ // [end of last index, end of table)
		Key:    lastIndexSpan.EndKey,
		EndKey: tableEndKey,
	})

	splits := make([]roachpb.Key, len(tableSpans))
	for i, sp := range tableSpans {
		splits[i] = sp.Key
	}
	return splits, nil
}

func (s *Splitter) splitInner(ctx context.Context, part partition) ([]roachpb.Span, error) {
	if part.prefixSpan.Equal(roachpb.Span{}) {
		log.Fatalf(ctx, "expected non-empty prefix span")
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
			subPartitionSpans, err := s.splitInner(ctx, subPartition)
			if err != nil {
				return err
			}

			if len(subPartitionSpans) == 1 && subPartitionSpans[0].Equal(part.prefixSpan) {
				if t.SpecialCount == 0 {
					log.Fatalf(ctx, "expected partitioning by DEFAULT")
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

	sort.Sort(roachpb.Spans(listPartitionSpans))

	var gapSpans []roachpb.Span
	for i := range listPartitionSpans {
		var gapSpan roachpb.Span
		if i == 0 {
			gapSpan = roachpb.Span{ // gap between start of partition prefix to first list partition span
				Key:    part.prefixSpan.Key,
				EndKey: listPartitionSpans[i].Key,
			}
		} else {
			gapSpan = roachpb.Span{ // gap between consecutive list partition spans
				Key:    listPartitionSpans[i-1].EndKey,
				EndKey: listPartitionSpans[i].Key,
			}
		}
		gapSpans = append(gapSpans, gapSpan)
	}
	gapSpans = append(gapSpans, roachpb.Span{ // gap between end of last list partition span and end of partition prefix
		Key:    listPartitionSpans[len(listPartitionSpans)-1].EndKey,
		EndKey: part.prefixSpan.EndKey,
	})

	for _, gapSpan := range gapSpans {
		if !gapSpan.Valid() {
			// It's possible for the sub-partitions in PARTITION BY LIST spans to be
			// adjacent to one another, so for the "gaps" between them to be
			// non-existent. It's also possible for the "gaps" at the start and end of
			// the partition prefix to be empty. We filter out these invalid gaps.
			continue
		}
		listPartitionSpans = append(listPartitionSpans, gapSpan)
	}
	return listPartitionSpans, nil
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
