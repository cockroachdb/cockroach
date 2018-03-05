// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// batchedPlanNode defines the interface for executing a query
// that operates in batches.
type batchedPlanNode interface {
	planNode

	// The planNode.Next() method is contractually (re-)defined here to
	// advance a full batch. Its first (boolean) return value is
	// false if and only if there is no error and BatchedCount() == 0.
	//
	// Additionally, there is no open KV batch (in particular, no
	// uncommitted in-progress KV write operation), nor unchecked or
	// inconsistent results observable via BatchedCount/BatchedValues,
	// in-between calls to Next().

	// The planNode.Values() method is contractually (re-)defined here
	// to not be callable.

	// BatchedCount() returns the number of rows processed in the last
	// call to Next().
	BatchedCount() int

	// BatchedValues exposes one of the batched rows, in the range 0 to
	// BatchedCount() exclusive.
	BatchedValues(rowIdx int) tree.Datums
}

var _ batchedPlanNode = &deleteNode{}

// serializeNode serializes the results of a batchedPlanNode into a
// plain planNode interface. In other words, it wraps around
// batchedPlanNode's Next() method which advances full batches to
// provide a Next() method that advances row-by-row.
//
// The FastPathResults behavior of the source plan, if any, is also
// preserved.
type serializeNode struct {
	source batchedPlanNode

	// fastPath is set to true during startExec if the source plan
	// was able to use the fast path and provide a row count.
	fastPath bool

	// rowCount is set either to the total row count if fastPath is true,
	// or to the row count of the current batch otherwise.
	rowCount int

	// rowIdx is the index of the current row in the current batch.
	rowIdx int
}

func (s *serializeNode) startExec(params runParams) error {
	if f, ok := s.source.(planNodeFastPath); ok {
		s.rowCount, s.fastPath = f.FastPathResults()
	}
	return nil
}

func (s *serializeNode) Next(params runParams) (bool, error) {
	if s.fastPath {
		return false, nil
	}
	if s.rowIdx+1 >= s.rowCount {
		// First batch, or finished previous batch; advance one.
		if next, err := s.source.Next(params); !next {
			return false, err
		}
		s.rowCount = s.source.BatchedCount()
		s.rowIdx = 0
	} else {
		// Advance one position in the current batch.
		s.rowIdx++
	}
	return s.rowCount > 0, nil
}

func (s *serializeNode) Values() tree.Datums       { return s.source.BatchedValues(s.rowIdx) }
func (s *serializeNode) Close(ctx context.Context) { s.source.Close(ctx) }

// FastPathResults implements the planNodeFastPath interface.
func (s *serializeNode) FastPathResults() (int, bool) {
	return s.rowCount, s.fastPath
}

// rowCountNode serializes the results of a batchedPlanNode into a
// plain planNode interface that has guaranteed FastPathResults
// behavior and no result columns (i.e. just the count of rows
// affected).
// All the batches are consumed in startExec().
//
// This is an optimization upon serializeNode when it is known in
// advance that the result rows will be discarded (e.g.  a
// data-modifying statement with no RETURNING clause or RETURNING
// NOTHING). In that case, we do not need to have individual calls to
// Next() consume the batched rows individually and instead quickly
// accumulate the batch counts themselves.
type rowCountNode struct {
	source   batchedPlanNode
	rowCount int
}

func (r *rowCountNode) startExec(params runParams) error {
	done := false
	if f, ok := r.source.(planNodeFastPath); ok {
		r.rowCount, done = f.FastPathResults()
	}
	if !done {
		for {
			if next, err := r.source.Next(params); !next {
				return err
			}
			r.rowCount += r.source.BatchedCount()
		}
	}
	return nil
}

func (r *rowCountNode) Next(params runParams) (bool, error) { return false, nil }
func (r *rowCountNode) Values() tree.Datums                 { return nil }
func (r *rowCountNode) Close(ctx context.Context)           { r.source.Close(ctx) }

// FastPathResults implements the planNodeFastPath interface.
func (r *rowCountNode) FastPathResults() (int, bool) { return r.rowCount, true }
