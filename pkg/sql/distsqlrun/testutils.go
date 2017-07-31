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

package distsqlrun

import "github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

// RepeatableRowSource is a RowSource used in benchmarks to avoid having to
// reinitialize a new RowSource every time during multiple passes of the input.
// It is intended to be initialized with all rows.
type RepeatableRowSource struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       sqlbase.EncDatumRows
	// Schema of rows.
	types []sqlbase.ColumnType
}

var _ RowSource = &RepeatableRowSource{}

// NewRepeatableRowSource creates a RepeatableRowSource with the given schema
// and rows. types is optional if at least one row is provided.
func NewRepeatableRowSource(
	types []sqlbase.ColumnType, rows sqlbase.EncDatumRows,
) *RepeatableRowSource {
	r := &RepeatableRowSource{rows: rows, types: types}
	if len(r.rows) > 0 && r.types == nil {
		inferredTypes := make([]sqlbase.ColumnType, len(r.rows[0]))
		for i, d := range r.rows[0] {
			inferredTypes[i] = d.Type
		}
		r.types = inferredTypes
	}
	return r
}

// Types is part of the RowSource interface.
func (r *RepeatableRowSource) Types() []sqlbase.ColumnType {
	return r.types
}

// Next is part of the RowSource interface.
func (r *RepeatableRowSource) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	// If we've emitted all rows, signal that we have reached the end.
	if r.nextRowIdx >= len(r.rows) {
		return nil, ProducerMetadata{}
	}
	nextRow := r.rows[r.nextRowIdx]
	r.nextRowIdx++
	return nextRow, ProducerMetadata{}
}

// Reset resets the RepeatableRowSource such that a subsequent call to Next()
// returns the first row.
func (r *RepeatableRowSource) Reset() {
	r.nextRowIdx = 0
}

// ConsumerDone is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerDone() {}

// ConsumerClosed is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerClosed() {}

// RowDisposer is a RowReceiver that discards any rows Push()ed.
type RowDisposer struct{}

var _ RowReceiver = &RowDisposer{}

// Push is part of the RowReceiver interface.
func (r *RowDisposer) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	return NeedMoreRows
}

// ProducerDone is part of the RowReceiver interface.
func (r *RowDisposer) ProducerDone() {}
