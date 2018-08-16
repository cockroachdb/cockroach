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

package distsqlrun

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
