// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  *types.T

	// If set, this is an implicit column; used internally.
	Hidden bool

	// If set, a value won't be produced for this column; used internally.
	Omitted bool
}

// ResultColumns is the type used throughout the sql module to
// describe the column types of a table.
type ResultColumns []ResultColumn

// ResultColumnsFromColDescs converts ColumnDescriptors to ResultColumns.
func ResultColumnsFromColDescs(colDescs []ColumnDescriptor) ResultColumns {
	cols := make(ResultColumns, 0, len(colDescs))
	for i := range colDescs {
		// Convert the ColumnDescriptor to ResultColumn.
		colDesc := &colDescs[i]
		typ := &colDesc.Type
		if typ == nil {
			panic(fmt.Sprintf("unsupported column type: %s", colDesc.Type.Family()))
		}

		hidden := colDesc.Hidden
		cols = append(cols, ResultColumn{Name: colDesc.Name, Typ: typ, Hidden: hidden})
	}
	return cols
}

// TypesEqual returns whether the length and types of r matches other. If
// a type in other is NULL, it is considered equal.
func (r ResultColumns) TypesEqual(other ResultColumns) bool {
	if len(r) != len(other) {
		return false
	}
	for i, c := range r {
		// NULLs are considered equal because some types of queries (SELECT CASE,
		// for example) can change their output types between a type and NULL based
		// on input.
		if other[i].Typ.Family() == types.UnknownFamily {
			continue
		}
		if !c.Typ.Equivalent(other[i].Typ) {
			return false
		}
	}
	return true
}

// ExplainPlanColumns are the result columns of an EXPLAIN (PLAN) ...
// statement.
var ExplainPlanColumns = ResultColumns{
	// Tree shows the node type with the tree structure.
	{Name: "tree", Typ: types.String},
	// Field is the part of the node that a row of output pertains to.
	{Name: "field", Typ: types.String},
	// Description contains details about the field.
	{Name: "description", Typ: types.String},
}

// ExplainPlanVerboseColumns are the result columns of an
// EXPLAIN (PLAN, ...) ...
// statement when a flag like VERBOSE or TYPES is passed.
var ExplainPlanVerboseColumns = ResultColumns{
	// Tree shows the node type with the tree structure.
	{Name: "tree", Typ: types.String},
	// Level is the depth of the node in the tree. Hidden by default; can be
	// retrieved using:
	//   SELECT level FROM [ EXPLAIN (VERBOSE) ... ].
	{Name: "level", Typ: types.Int, Hidden: true},
	// Type is the node type. Hidden by default.
	{Name: "node_type", Typ: types.String, Hidden: true},
	// Field is the part of the node that a row of output pertains to.
	{Name: "field", Typ: types.String},
	// Description contains details about the field.
	{Name: "description", Typ: types.String},
	// Columns is the type signature of the data source.
	{Name: "columns", Typ: types.String},
	// Ordering indicates the known ordering of the data from this source.
	{Name: "ordering", Typ: types.String},
}

// ExplainDistSQLColumns are the result columns of an
// EXPLAIN (DISTSQL) statement.
var ExplainDistSQLColumns = ResultColumns{
	{Name: "automatic", Typ: types.Bool},
	{Name: "url", Typ: types.String},
	{Name: "json", Typ: types.String, Hidden: true},
}

// ExplainOptColumns are the result columns of an
// EXPLAIN (OPT) statement.
var ExplainOptColumns = ResultColumns{
	{Name: "text", Typ: types.String},
}

// ShowTraceColumns are the result columns of a SHOW [KV] TRACE statement.
var ShowTraceColumns = ResultColumns{
	{Name: "timestamp", Typ: types.TimestampTZ},
	{Name: "age", Typ: types.Interval}, // Note GetTraceAgeColumnIdx below.
	{Name: "message", Typ: types.String},
	{Name: "tag", Typ: types.String},
	{Name: "location", Typ: types.String},
	{Name: "operation", Typ: types.String},
	{Name: "span", Typ: types.Int},
}

// ShowCompactTraceColumns are the result columns of a
// SHOW COMPACT [KV] TRACE statement.
var ShowCompactTraceColumns = ResultColumns{
	{Name: "age", Typ: types.Interval}, // Note GetTraceAgeColumnIdx below.
	{Name: "message", Typ: types.String},
	{Name: "tag", Typ: types.String},
	{Name: "operation", Typ: types.String},
}

// GetTraceAgeColumnIdx retrieves the index of the age column
// depending on whether the compact format is used.
func GetTraceAgeColumnIdx(compact bool) int {
	if compact {
		return 0
	}
	return 1
}

// ShowReplicaTraceColumns are the result columns of a
// SHOW EXPERIMENTAL_REPLICA TRACE statement.
var ShowReplicaTraceColumns = ResultColumns{
	{Name: "timestamp", Typ: types.TimestampTZ},
	{Name: "node_id", Typ: types.Int},
	{Name: "store_id", Typ: types.Int},
	{Name: "replica_id", Typ: types.Int},
}

// ShowSyntaxColumns are the columns of a SHOW SYNTAX statement.
var ShowSyntaxColumns = ResultColumns{
	{Name: "field", Typ: types.String},
	{Name: "message", Typ: types.String},
}
