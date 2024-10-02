// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package spanconfigsplitter is able to split sql descriptors into its
// constituent spans.
package spanconfigsplitter

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/dustin/go-humanize"
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

// Splits is part of the spanconfig.Splitter interface, returning the number of
// split points for the given table descriptor. It's able to do this without
// decoding partition keys. Consider our table hierarchy:
//
//	table -> index -> partition -> partition -> (...)
//
// Where each partition is either a PARTITION BY LIST kind (where it can then be
// further partitioned), or a PARTITION BY RANGE kind (no further partitioning
// possible). We can classify each parent-child link into two types:
//
// (a) Contiguous      {index, list partition} -> range partition
// (b) Non-contiguous  table -> index, {index, list partition} -> list partition
//
//   - Contiguous links are the sort where each child span is contiguous with
//     another, and that the set of all child spans encompass the parent's span.
//     For an index that's partitioned by range:
//
//     CREATE TABLE db.range(i INT PRIMARY KEY, j INT) PARTITION BY RANGE (i) (
//     PARTITION less_than_five       VALUES FROM (minvalue) to (5),
//     PARTITION between_five_and_ten VALUES FROM (5) to (10),
//     PARTITION greater_than_ten     VALUES FROM (10) to (maxvalue)
//     );
//
//     With table ID as 106, the parent index span is /Table/106/{1-2}. The child
//     spans are /Table/106/1{-/5}, /Table/106/1/{5-10} and /Table/106/{1/10-2}.
//     They're contiguous; put together they wholly encompass the parent span.
//
//   - Non-contiguous links, by contrast, are when child spans are neither
//     contiguous with respect to one another, nor do they start and end at
//     the parent span's boundaries. For a table with a secondary index:
//
//     CREATE TABLE db.t(i INT PRIMARY KEY, j INT);
//     CREATE INDEX idx ON db.t (j);
//     DROP INDEX db.t@idx;
//     CREATE INDEX idx ON db.t (j);
//
//     With table ID as 106, the parent table span is /Table/10{6-7}. The child
//     spans are /Table/106/{1-2} and /Table/106/{3-4}. Compared to the parent
//     span, we're missing /Table/106{-/1}, /Table/106/{2-3}, /Table/10{6/4-7}.
//
// For N children:
//   - For a contiguous link, the number of splits equals the number of child
//     elements (i.e. N).
//   - For a non-contiguous link, the number of splits equals N + 1 + N. For N
//     children, there are N - 1 gaps. There are also 2 gaps at the start and end
//     of the parent span. Summing that with the N children span themselves, we
//     get to the formula above. This assumes that the N child elements aren't
//     further subdivided, if they are (we can compute it recursively), the
//     formula becomes N + 1 + Σ(grand child spans).
//
// It's possible to compute split points more precisely if we did decode keys.
// We could, for example, recognize that partition-by-list values are adjacent,
// therefore there's no "gap" between them. We do however need to avoid
// key-decoding -- this type is used in places where a fully type-hydrated
// descriptor may not be available[1]. It's also possible to avoid decoding and
// get precise accounting by comparing encoded keys with each other. We just
// didn't because it's more annoying to have to write, and over-counting here is
// more than fine for our current usages.
//
// [1]: Today it's possible to GC type descriptors before GC-ing table
//
//	descriptors that refer to them. This interface is used near by this GC
//	activity, so type information is not always available.
func (s *Splitter) Splits(ctx context.Context, table catalog.TableDescriptor) (int, error) {
	if isNil(table) {
		return 0, nil // nothing to do
	}

	if s.knobs.ExcludeDroppedDescriptorsFromLookup && table.Dropped() {
		return 0, nil // we're excluding this descriptor; nothing to do here
	}

	s.log(0, "+ %-2d between start of table and start of %s index", 1, humanize.Ordinal(1))
	numIndexes, innerSplitCount := 0, 0
	if err := catalog.ForEachIndex(table, catalog.IndexOpts{}, func(index catalog.Index) error {
		splitCount, err := s.splitsInner(ctx, partition{
			Partitioning: index.GetPartitioning(),
			table:        table,
			index:        index,
			level:        1,
		})
		if err != nil {
			return err
		}

		innerSplitCount += splitCount
		numIndexes++

		s.log(0, "+ %-2d for %s index", splitCount, humanize.Ordinal(numIndexes))
		return nil
	}); err != nil {
		return 0, err
	}

	if numIndexes > 1 {
		s.log(0, "+ %-2d gap(s) between %d indexes", numIndexes-1, numIndexes)
	}

	s.log(0, "+ %-2d between end of %s index and end of table", 1, humanize.Ordinal(numIndexes))
	return numIndexes + 1 + innerSplitCount, nil
}

func (s *Splitter) splitsInner(ctx context.Context, part partition) (int, error) {
	if part.NumColumns() == 0 {
		return 1, nil // no partitioning
	}
	if part.NumRanges() > 0 {
		return part.NumRanges(), nil
	}

	s.log(part.level, "+ %-2d between start of index and start of %s partition-by-list value", 1, humanize.Ordinal(1))
	numListValues, innerSplitCount := 0, 0
	if err := part.ForEachList(func(_ string, values [][]byte, subPartitioning catalog.Partitioning) error {
		for i := 0; i < len(values); i++ {
			subPartition := partition{
				Partitioning: subPartitioning,
				table:        part.table,
				index:        part.index,
				level:        part.level + 1,
			}
			splitCount, err := s.splitsInner(ctx, subPartition)
			if err != nil {
				return err
			}

			innerSplitCount += splitCount
			numListValues++

			s.log(part.level, "+ %-2d for %s partition-by-list value", splitCount, humanize.Ordinal(numListValues))
		}
		return nil
	}); err != nil {
		return 0, err
	}

	if numListValues > 1 {
		s.log(part.level, "+ %-2d gap(s) between %d partition-by-list value spans", numListValues-1, numListValues)
	}

	s.log(part.level, "+ %-2d between end of %s partition-by-list value span and end of index", 1, humanize.Ordinal(numListValues))
	return numListValues + 1 + innerSplitCount, nil
}

func (s *Splitter) log(level int, format string, args ...interface{}) {
	padding := strings.Repeat("    ", level)
	if s.knobs.SplitterStepLogger != nil {
		s.knobs.SplitterStepLogger(padding + fmt.Sprintf(format, args...))
	}
}

// partition groups together everything needed to determine the split points of
// a given partition.
type partition struct {
	catalog.Partitioning // partition itself

	table catalog.TableDescriptor // table the partition is part of
	index catalog.Index           // index being partitioned
	level int                     // recursion level, used only for test-logging
}

func isNil(table catalog.TableDescriptor) bool {
	vTable := reflect.ValueOf(table)
	return vTable.Kind() == reflect.Ptr && vTable.IsNil() || table == nil
}
