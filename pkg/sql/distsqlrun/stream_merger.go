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
	"github.com/pkg/errors"
)

// We define a group to be a set of rows from a given source with the same
// group key, in this case the set of ordered columns. streamMerger emits
// batches of rows that are the cross-product of matching groups from each
// stream.
type streamMerger struct {
	left         streamCacher
	right        streamCacher
	datumAlloc   sqlbase.DatumAlloc
	outputBuffer [][2]sqlbase.EncDatumRow
	initialized  bool
}

// initialize loads up each stream with the first row received from it's
// corresponding source.
func (sm *streamMerger) initialize() error {
	if _, err := sm.left.nextRow(); err != nil {
		return err
	}
	_, err := sm.right.nextRow()
	return err
}

// computeBatch adds the cross-product of the next matching set of groups
// from each of streams to the output buffer.
func (sm *streamMerger) computeBatch() error {
	sm.outputBuffer = sm.outputBuffer[:0]

	lrow := sm.left.advanceGroup()
	rrow := sm.right.advanceGroup()

	if lrow == nil && rrow == nil {
		return nil
	}

	cmp, err := CompareEncDatumRowForMerge(lrow, rrow, sm.left.ordering, sm.right.ordering, &sm.datumAlloc)
	if err != nil {
		return err
	}

	if cmp < 0 {
		// lrow < rrow or rrow == nil, accumulate set of rows "equal" to lrow
		// and emit (lrow, nil) tuples.
		if err := sm.left.accumulateGroup(); err != nil {
			return err
		}

		for _, l := range sm.left.currentGroup() {
			sm.outputBuffer = append(sm.outputBuffer, [2]sqlbase.EncDatumRow{l, nil})
		}
		return nil
	}

	if cmp > 0 {
		// rrow < lrow or lrow == nil, accumulate set of rows "equal" to rrow
		// and emit (nil, rrow) tuples.
		if err := sm.right.accumulateGroup(); err != nil {
			return err
		}

		for _, r := range sm.right.currentGroup() {
			sm.outputBuffer = append(sm.outputBuffer, [2]sqlbase.EncDatumRow{nil, r})
		}
		return nil
	}

	// lrow == rrow, accumulate set of rows "equal" to lrow and set of rows
	// "equal" to rrow then emit cross product of the two sets ((lrow, rrow)
	// tuples).
	if err := sm.right.accumulateGroup(); err != nil {
		return err
	}
	if err := sm.left.accumulateGroup(); err != nil {
		return err
	}

	// Output cross-product.
	for _, l := range sm.left.currentGroup() {
		for _, r := range sm.right.currentGroup() {
			sm.outputBuffer = append(sm.outputBuffer, [2]sqlbase.EncDatumRow{l, r})
		}
	}

	return nil
}

func (sm *streamMerger) NextBatch() ([][2]sqlbase.EncDatumRow, error) {
	if !sm.initialized {
		if err := sm.initialize(); err != nil {
			return nil, err
		}
		sm.initialized = true
	}
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
		left: streamCacher{
			src:      MakeNoMetadataRowSource(leftSource, metadataSink),
			ordering: leftOrdering,
		},
		right: streamCacher{
			src:      MakeNoMetadataRowSource(rightSource, metadataSink),
			ordering: rightOrdering,
		},
	}, nil
}
