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

package distsql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// streamCacher is a thin wrapper around an ordered RowSource buffering in the most
// recently received rows.
type streamCacher struct {
	src      RowSource
	ordering sqlbase.ColumnOrdering
	// rows contains the last set of rows received from the source.
	rows       []sqlbase.EncDatumRow
	datumAlloc sqlbase.DatumAlloc
}

// nextRow() is a wrapper around RowSource.NextRow(), it simultaneously saves the
// retrieved row within the stream's most recently added rows buffer.
func (s *streamCacher) nextRow() (sqlbase.EncDatumRow, error) {
	row, err := s.src.NextRow()
	// We don't make the explicit check for row == nil and therefore add the nil
	// row to the row buffer, this is because the nil row can be used in
	// group comparisons (end of stream is effectively another group).
	if err != nil {
		return nil, err
	}
	s.rows = append(s.rows, row)
	return row, nil
}

// currentGroup returns the set of rows belonging to the same group as
// s.rows[0], i.e. with the same group key (comprised of the set of ordered
// columns).
func (s *streamCacher) currentGroup() []sqlbase.EncDatumRow {
	// we ignore the last row of the row buffer because this is either a nil row
	// (end of stream) or a row not "equal" to s.rows[0], either case it belongs
	// to the next group.
	return s.rows[:len(s.rows)-1]
}

// advanceGroup moves over the 'current group' window to point to the next group
// discarding the previous.
func (s *streamCacher) advanceGroup() sqlbase.EncDatumRow {
	// for each stream we discard all the rows except the last row (which is
	// either a nil row or a row not "equal" to s.rows[0], either case it
	// belongs to the next group.
	s.rows[0] = s.rows[len(s.rows)-1]
	s.rows = s.rows[:1]
	return s.rows[0]
}

// accumulateGroup collects all the rows that are "equal" to s.rows[0], the
// first occurrence of a row not "equal" to s.rows[0] is stored at the end of
// s.rows.
func (s *streamCacher) accumulateGroup() error {
	for {
		next, err := s.nextRow()
		if err != nil || next == nil {
			return err
		}
		cmp, err := s.rows[0].Compare(&s.datumAlloc, s.ordering, next)
		if err != nil || cmp != 0 {
			return err
		}
	}
}

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

	cmp, err := sm.compare(lrow, rrow)
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

func (sm *streamMerger) compare(lhs, rhs sqlbase.EncDatumRow) (int, error) {
	if lhs == nil && rhs == nil {
		panic("comparing two nil rows")
	}

	if lhs == nil {
		return 1, nil
	}
	if rhs == nil {
		return -1, nil
	}

	for i, ord := range sm.left.ordering {
		lIdx := ord.ColIdx
		rIdx := sm.right.ordering[i].ColIdx
		cmp, err := lhs[lIdx].Compare(&sm.datumAlloc, &rhs[rIdx])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if sm.left.ordering[i].Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
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

func makeStreamMerger(
	orderings []sqlbase.ColumnOrdering, sources []RowSource,
) (streamMerger, error) {
	if len(sources) != 2 {
		return streamMerger{}, errors.Errorf("only 2 sources allowed, %d provided", len(sources))
	}
	if len(sources) != len(orderings) {
		return streamMerger{}, errors.Errorf(
			"orderings count %d doesn't match source count %d", len(orderings), len(sources))
	}
	if len(orderings[0]) != len(orderings[1]) {
		return streamMerger{}, errors.Errorf(
			"ordering lengths don't match: %d and %d", len(orderings[0]), len(orderings[1]))
	}
	for i, ord := range orderings[0] {
		if ord.Direction != orderings[1][i].Direction {
			return streamMerger{}, errors.New("Ordering mismatch")
		}
	}

	return streamMerger{
		left: streamCacher{
			src:      sources[0],
			ordering: orderings[0],
		},
		right: streamCacher{
			src:      sources[1],
			ordering: orderings[1],
		},
	}, nil
}
