// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsqlrun

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// We define a group to be a set of rows from a given source with the same
// group key, in this case the set of ordered columns. streamMerger emits
// batches of rows that are the cross-product of matching groups from each
// stream.
type streamMerger struct {
	left         streamGroupAccumulator
	right        streamGroupAccumulator
	datumAlloc   sqlbase.DatumAlloc
	outputBuffer [][2]sqlbase.EncDatumRow
}

// computeBatch adds the cross-product of the next matching set of groups
// from each of streams to the output buffer.
func (sm *streamMerger) computeBatch() error {
	sm.outputBuffer = sm.outputBuffer[:0]

	lrow, err := sm.left.peekAtCurrentGroup()
	if err != nil {
		return err
	}
	rrow, err := sm.right.peekAtCurrentGroup()
	if err != nil {
		return err
	}
	if lrow == nil && rrow == nil {
		return nil
	}

	cmp, err := CompareEncDatumRowForMerge(lrow, rrow, sm.left.ordering, sm.right.ordering, &sm.datumAlloc)
	if err != nil {
		return err
	}
	if cmp != 0 {
		// lrow < rrow or rrow == nil, accumulate set of rows "equal" to lrow
		// and emit (lrow, nil) tuples.
		src := &sm.left
		if cmp > 0 {
			src = &sm.right
		}
		group, err := src.advanceGroup()
		if err != nil {
			return err
		}
		for _, r := range group {
			var outputRow [2]sqlbase.EncDatumRow
			if cmp < 0 {
				outputRow[0] = r
				outputRow[1] = nil
			} else {
				outputRow[0] = nil
				outputRow[1] = r
			}
			sm.outputBuffer = append(sm.outputBuffer, outputRow)
		}
		return nil
	}
	// We found matching groups; we'll output the cross-product.
	leftGroup, err := sm.left.advanceGroup()
	if err != nil {
		return err
	}
	rightGroup, err := sm.right.advanceGroup()
	if err != nil {
		return err
	}
	// TODO(andrei): if groups are large and we have a limit, we might want to
	// stream through the leftGroup instead of accumulating it all.
	for _, l := range leftGroup {
		for _, r := range rightGroup {
			sm.outputBuffer = append(sm.outputBuffer, [2]sqlbase.EncDatumRow{l, r})
		}
	}
	return nil
}

// CompareEncDatumRowForMerge EncDatumRow compares two EncDatumRows for merging.
// When merging two streams and preserving the order (as in a MergeSort or
// a MergeJoin) compare the head of the streams, emitting the one that sorts
// first. It allows for the EncDatumRow to be nil if one of the streams is
// exhausted (and hence nil). CompareEncDatumRowForMerge returns 0 when both
// rows are nil, and a nil row is considered greater than any non-nil row.
// CompareEncDatumRowForMerge assumes that the two rows have the same columns
// in the same orders, but can handle different ordering directions. It takes
// a DatumAlloc which is used for decoding if any underlying EncDatum is not
// yet decoded.
func CompareEncDatumRowForMerge(
	lhs, rhs sqlbase.EncDatumRow,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	da *sqlbase.DatumAlloc,
) (int, error) {
	if lhs == nil && rhs == nil {
		return 0, nil
	}
	if lhs == nil {
		return 1, nil
	}
	if rhs == nil {
		return -1, nil
	}
	if len(leftOrdering) != len(rightOrdering) {
		return 0, errors.Errorf(
			"cannot compare two EncDatumRow types that have different length ColumnOrderings",
		)
	}

	for i, ord := range leftOrdering {
		lIdx := ord.ColIdx
		rIdx := rightOrdering[i].ColIdx
		cmp, err := lhs[lIdx].Compare(da, &rhs[rIdx])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if leftOrdering[i].Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

func (sm *streamMerger) NextBatch() ([][2]sqlbase.EncDatumRow, error) {
	if err := sm.computeBatch(); err != nil {
		return nil, err
	}
	return sm.outputBuffer, nil
}

// makeStreamMerger creates a streamMerger, joining rows from leftSource with
// rows from rightSource.
//
// All metadata from the sources is forwarded to metadataSink.
func makeStreamMerger(
	leftSource RowSource,
	leftOrdering sqlbase.ColumnOrdering,
	rightSource RowSource,
	rightOrdering sqlbase.ColumnOrdering,
	metadataSink RowReceiver,
) (streamMerger, error) {
	if len(leftOrdering) != len(rightOrdering) {
		return streamMerger{}, errors.Errorf(
			"ordering lengths don't match: %d and %d", len(leftOrdering), len(rightOrdering))
	}
	for i, ord := range leftOrdering {
		if ord.Direction != rightOrdering[i].Direction {
			return streamMerger{}, errors.New("Ordering mismatch")
		}
	}

	return streamMerger{
		left: makeStreamGroupAccumulator(
			MakeNoMetadataRowSource(leftSource, metadataSink),
			leftOrdering),
		right: makeStreamGroupAccumulator(
			MakeNoMetadataRowSource(rightSource, metadataSink),
			rightOrdering),
	}, nil
}
