// Copyright 2017 The Cockroach Authors.
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
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package distsqlrun

import "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

// RepeatableSink is both a RowReceiver and a RowSource used in benchmarks
// to avoid having to reinitialize a new RowSource every time during multiple
// passes of the input. It is intended to be initialized with all rows so
// Push()ing is a noop.
type RepeatableSink struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       sqlbase.EncDatumRows
	// Schema of rows.
	types []sqlbase.ColumnType
}

var _ RowReceiver = &RepeatableSink{}
var _ RowSource = &RepeatableSink{}

func NewRepeatableSink(
	types []sqlbase.ColumnType, rows sqlbase.EncDatumRows,
) *RepeatableSink {
	return &RepeatableSink{rows: rows, types: types}
}

func (r *RepeatableSink) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	return NeedMoreRows
}

func (r *RepeatableSink) ProducerDone() {
	return
}

func (r *RepeatableSink) Types() []sqlbase.ColumnType {
	return r.types
}

func (r *RepeatableSink) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	// If we've emitted all rows, signal that we have reached the end and reset.
	if r.nextRowIdx >= len(r.rows) {
		r.nextRowIdx = 0
		return nil, ProducerMetadata{}
	}
	nextRow := r.rows[r.nextRowIdx]
	r.nextRowIdx++
	return nextRow, ProducerMetadata{}
}

func (r *RepeatableSink) ConsumerDone() {
	return
}

func (r *RepeatableSink) ConsumerClosed() {
	return
}
