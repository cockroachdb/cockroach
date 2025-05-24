// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// mutationOutputHelper is a helper for mutation nodes that need to emit either
// the number of rows affected or the rows themselves.
type mutationOutputHelper struct {
	// rowsNeeded is set to true if the mutation operator needs to return the rows
	// that were affected by the mutation.
	rowsNeeded bool
	// rowsAffectedHelper tracks the number of rows affected by the mutation
	// operator.
	rowsAffectedHelper rowsAffectedOutputHelper
	// rows, if set, contains the affected rows after the mutation operator
	// finishes executing. It must be set before execution if rowsNeeded is true.
	rows *rowcontainer.RowContainer
	// rowIdx and currentRow are used to track the next row to output.
	rowIdx     int
	currentRow tree.Datums
}

// onModifiedRow increments the number of affected rows. It should be called
// whenever the operator mutates a row, regardless of whether affected rows will
// be returned or not.
func (h *mutationOutputHelper) onModifiedRow() {
	h.rowsAffectedHelper.rowCount++
}

// modifiedRowCount returns the number of rows affected by the mutation
// operator.
func (h *mutationOutputHelper) modifiedRowCount() int64 {
	return int64(h.rowsAffectedHelper.rowCount)
}

// addRow adds a mutated row to the row container. It is a no-op if the operator
// is only returning the affected row count.
func (h *mutationOutputHelper) addRow(ctx context.Context, row tree.Datums) error {
	if h.rows == nil {
		return nil
	}
	_, err := h.rows.AddRow(ctx, row)
	return err
}

// next returns true if there is a next row to output. It should only be called
// after the mutation operator has finished executing.
func (h *mutationOutputHelper) next() bool {
	if h.rows == nil {
		return h.rowsAffectedHelper.next()
	}
	if h.rowIdx >= h.rows.Len() {
		return false
	}
	h.currentRow = h.rows.At(h.rowIdx)
	h.rowIdx++
	return true
}

// values returns the current row to output after a call to next().
func (h *mutationOutputHelper) values() tree.Datums {
	if h.rows == nil {
		return h.rowsAffectedHelper.values()
	}
	return h.currentRow
}

func (h *mutationOutputHelper) close(ctx context.Context) {
	if h.rows != nil {
		h.rows.Close(ctx)
		h.rows = nil
	}
}

// rowsAffectedOutputHelper is a helper for nodes that only need to emit the number
// of affected rows.
type rowsAffectedOutputHelper struct {
	// rowCount is the number of rows affected.
	rowCount int
	// done is set to true once the row count has been emitted.
	done bool
}

func (h *rowsAffectedOutputHelper) incAffectedRows() {
	h.rowCount++
}

func (h *rowsAffectedOutputHelper) addAffectedRows(count int) {
	h.rowCount += count
}

func (h *rowsAffectedOutputHelper) next() bool {
	if h.done {
		return false
	}
	h.done = true
	return true
}

func (h *rowsAffectedOutputHelper) values() tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(h.rowCount))}
}
