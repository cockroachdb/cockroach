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

import "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

// streamCacher is a thin wrapper around an ordered RowSource buffering in the most
// recently received rows.
type streamCacher struct {
	src      RowSource
	ordering sqlbase.ColumnOrdering
	// rows contains the last set of rows received from the source.
	rows       []sqlbase.EncDatumRow
	datumAlloc *sqlbase.DatumAlloc
	done       bool
}

// nextRow() is a wrapper around RowSource.NextRow(), it simultaneously saves the
// retrieved row within the stream's most recently added rows buffer.
func (s *streamCacher) nextRow() (sqlbase.EncDatumRow, error) {
	row, err := s.src.NextRow()
	// We add the nil row to the row buffer, because the nil row can be used in
	// group comparisons (end of stream is effectively another group).
	if row == nil {
		s.done = true
	}
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

func (s *streamCacher) currentGroupRow() sqlbase.EncDatumRow {
	return s.rows[0]
}

func (s *streamCacher) allAccumulatedRows() []sqlbase.EncDatumRow {
	return s.rows
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
		cmp, err := s.rows[0].Compare(s.datumAlloc, s.ordering, next)
		if err != nil || cmp != 0 {
			return err
		}
	}
}
