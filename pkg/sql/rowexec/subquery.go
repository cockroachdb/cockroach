// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

// SubqueryExecMode is an enum to indicate the type of a subquery.
type SubqueryExecMode int

const (
	// SubqueryExecModeExists indicates that the subquery is an argument to
	// EXISTS. Result type is Bool.
	SubqueryExecModeExists SubqueryExecMode = 1 + iota
	// SubqueryExecModeAllRowsNormalized indicates that the subquery is an
	// argument to IN, ANY, SOME, or ALL. Any number of rows are
	// expected. The result type is tuple of rows. As a special case, if
	// there is only one column selected, the result is a tuple of the
	// selected values (instead of a tuple of 1-tuples).
	SubqueryExecModeAllRowsNormalized
	// SubqueryExecModeAllRows indicates that the subquery is an
	// argument to an ARRAY constructor. Any number of rows are expected, and
	// exactly one column is expected. Result type is a tuple
	// of selected values.
	SubqueryExecModeAllRows
	// SubqueryExecModeOneRow indicates that the subquery is an argument to
	// another function. At most 1 row is expected. The result type is a tuple of
	// columns, unless there is exactly 1 column in which case the result type is
	// that column's type. If there are no rows, the result is NULL.
	SubqueryExecModeOneRow
	// SubqueryExecModeDiscardAllRows indicates that the subquery is being
	// executed for its side effects. The subquery should be executed until it
	// produces all rows, but the rows should not be saved, and the result will
	// never be checked.
	SubqueryExecModeDiscardAllRows
)
