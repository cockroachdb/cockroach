// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

// SubqueryExecMode is an enum to indicate the type of a subquery.
type SubqueryExecMode int

const (
	// SubqueryExecModeUnknown is the default value, and is only used to indicate
	// a subquery that was improperly initialized.
	SubqueryExecModeUnknown SubqueryExecMode = iota
	// SubqueryExecModeExists indicates that the subquery is an argument to
	// EXISTS. Result type is Bool.
	SubqueryExecModeExists
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
)

// SubqueryExecModeNames maps SubqueryExecMode values to human readable
// strings for EXPLAIN queries.
var SubqueryExecModeNames = map[SubqueryExecMode]string{
	SubqueryExecModeUnknown:           "<unknown>",
	SubqueryExecModeExists:            "exists",
	SubqueryExecModeAllRowsNormalized: "all rows normalized",
	SubqueryExecModeAllRows:           "all rows",
	SubqueryExecModeOneRow:            "one row",
}
