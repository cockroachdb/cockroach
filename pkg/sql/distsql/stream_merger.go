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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// stream is a thin wrapper around an ordered RowSource buffering in the most
// recently received rows.
type stream struct {
	noCopy util.NoCopy

	src      RowSource
	ordering sqlbase.ColumnOrdering
	// rows contains the last set of rows received from the source.
	rows []sqlbase.EncDatumRow
}

// advance discards the oldest row seen and returns row following right after.
func (s *stream) advance() (sqlbase.EncDatumRow, error) {
	if len(s.rows) == 0 {
		panic("advancing empty row set")
	}

	// If we have only one row stored, we retrieve the next row from the source.
	if len(s.rows) == 1 {
		if _, err := s.nextRow(); err != nil {
			return nil, err
		}
	}
	// We discard the oldest row seen effectively 'advancing' along the stream.
	s.rows = s.rows[1:]
	return s.rows[0], nil
}

// nextRow() is a wrapper around RowSource.NextRow(), it simultaneously saves the
// retrieved row within the stream's most recently added rows buffer.
func (s *stream) nextRow() (sqlbase.EncDatumRow, error) {
	row, err := s.src.NextRow()
	// We don't make the explicit check for row == nil and therefore add the nil
	// row to the row buffer, this is because the nil row can be used in
	// co-group comparisons (end of stream is effectively another co-group).
	if err != nil {
		return nil, err
	}
	s.rows = append(s.rows, row)
	return row, nil
}

// We define a co-group to be a set of rows from a given source with the same
// group key, in this case the set of ordered columns. streamMerger emits
// batches of rows that are the cross-product of matching co-groups from each
// stream.
type streamMerger struct {
	noCopy util.NoCopy

	left         stream
	right        stream
	datumAlloc   sqlbase.DatumAlloc
	outputBuffer []sqlbase.EncDatumRows
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

// computeBatch adds the cross-product of the next matching set of co-groups
// from each of streams to the output buffer.
func (sm *streamMerger) computeBatch() error {
	lrow := sm.left.rows[0]
	rrow := sm.right.rows[0]

	for lrow != nil && rrow != nil {
		cmp, err := sm.compare(lrow, rrow)
		if err != nil {
			return err
		}
		if cmp == -1 { // lrow < rrow, advance lrow and discard previous.
			next, err := sm.left.advance()
			if err != nil {
				return err
			}
			lrow = next
		} else if cmp == 1 { // rrow < lrow, advance rrow and discard previous.
			next, err := sm.right.advance()
			if err != nil {
				return err
			}
			rrow = next
		} else { // lrow == rrow, output cross product of co-groups.
			// Buffer in all rows from the right stream in the same co-group as
			// rrow.
			for {
				nextRight, err := sm.right.nextRow()
				if err != nil {
					return err
				}
				if nextRight == nil {
					break
				}
				cmp, err := sm.compare(rrow, nextRight)
				if err != nil {
					return err
				}
				if cmp != 0 {
					break
				}
			}

			// Buffer in all rows from the left stream in the same co-group as
			// lrow.
			for {
				nextLeft, err := sm.left.nextRow()
				if err != nil {
					return err
				}
				if nextLeft == nil {
					break
				}
				cmp, err := sm.compare(lrow, nextLeft)
				if err != nil {
					return err
				}
				if cmp != 0 {
					break
				}
			}

			// Output cross-product, we ignore the last row of each stream
			// buffer because this is either a nil row (end of stream) or a row
			// belonging to the next co-group.
			for _, l := range sm.left.rows[:len(sm.left.rows)-1] {
				for _, r := range sm.right.rows[:len(sm.right.rows)-1] {
					sm.outputBuffer = append(sm.outputBuffer, sqlbase.EncDatumRows{l, r})
				}
			}

			// We've finished with the co-groups above, for each stream we
			// discard all the rows save the row from the next co-group.
			sm.left.rows = sm.left.rows[len(sm.left.rows)-1:]
			sm.right.rows = sm.right.rows[len(sm.right.rows)-1:]

			return nil
		}
	}
	return nil
}

func (sm *streamMerger) compare(lhs, rhs sqlbase.EncDatumRow) (int, error) {
	if lhs == nil || rhs == nil {
		return 0, errors.New("empty row")
	}
	for i := 0; i < len(sm.left.ordering); i++ {
		lIdx := sm.left.ordering[i].ColIdx
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

func (sm *streamMerger) NextBatch() ([]sqlbase.EncDatumRows, error) {
	if !sm.initialized {
		if err := sm.initialize(); err != nil {
			return nil, err
		}
		sm.initialized = true
	}
	sm.outputBuffer = sm.outputBuffer[:0]
	if err := sm.computeBatch(); err != nil {
		return nil, err
	}
	return sm.outputBuffer, nil
}

func makeStreamMerger(
	orderings []sqlbase.ColumnOrdering, sources []RowSource,
) (*streamMerger, error) {
	if len(sources) != 2 {
		return nil, errors.Errorf("only 2 sources allowed, %d provided", len(sources))
	}
	if len(sources) != len(orderings) {
		return nil, errors.Errorf(
			"orderings count %d doesn't match source count %d", len(orderings), len(sources))
	}
	if len(orderings[0]) != len(orderings[1]) {
		return nil, errors.Errorf(
			"ordering lengths don't match: %d and %d", len(orderings[0]), len(orderings[1]))
	}
	for i, ord := range orderings[0] {
		if ord.Direction != orderings[1][i].Direction {
			return nil, errors.New("Ordering mismatch")
		}
	}

	s := &streamMerger{
		left: stream{
			src:      sources[0],
			ordering: orderings[0],
		},
		right: stream{
			src:      sources[1],
			ordering: orderings[1],
		},
	}
	return s, nil
}
