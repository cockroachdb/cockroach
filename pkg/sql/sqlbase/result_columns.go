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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  *types.T

	// If set, this is an implicit column; used internally.
	Hidden bool
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

// NodeFormatter returns a tree.NodeFormatter that, when formatted,
// represents the column at the input column index.
func (r ResultColumns) NodeFormatter(colIdx int) tree.NodeFormatter {
	return &varFormatter{ColumnName: tree.Name(r[colIdx].Name)}
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

// ExplainVecColumns are the result columns of an
// EXPLAIN (VEC) statement.
var ExplainVecColumns = ResultColumns{
	{Name: "text", Typ: types.String},
}

// ExplainBundleColumns are the result columns of an EXPLAIN BUNDLE statement.
var ExplainBundleColumns = ResultColumns{
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

// ShowFingerprintsColumns are the result columns of a
// SHOW EXPERIMENTAL_FINGERPRINTS statement.
var ShowFingerprintsColumns = ResultColumns{
	{Name: "index_name", Typ: types.String},
	{Name: "fingerprint", Typ: types.String},
}

// AlterTableSplitColumns are the result columns of an
// ALTER TABLE/INDEX .. SPLIT AT statement.
var AlterTableSplitColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
	{Name: "split_enforced_until", Typ: types.Timestamp},
}

// AlterTableUnsplitColumns are the result columns of an
// ALTER TABLE/INDEX .. UNSPLIT statement.
var AlterTableUnsplitColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
}

// AlterTableRelocateColumns are the result columns of an
// ALTER TABLE/INDEX .. EXPERIMENTAL_RELOCATE statement.
var AlterTableRelocateColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
}

// AlterTableScatterColumns are the result columns of an
// ALTER TABLE/INDEX .. SCATTER statement.
var AlterTableScatterColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
}

// ScrubColumns are the result columns of a SCRUB statement.
var ScrubColumns = ResultColumns{
	{Name: "job_uuid", Typ: types.Uuid},
	{Name: "error_type", Typ: types.String},
	{Name: "database", Typ: types.String},
	{Name: "table", Typ: types.String},
	{Name: "primary_key", Typ: types.String},
	{Name: "timestamp", Typ: types.Timestamp},
	{Name: "repaired", Typ: types.Bool},
	{Name: "details", Typ: types.Jsonb},
}

// SequenceSelectColumns are the result columns of a sequence data source.
var SequenceSelectColumns = ResultColumns{
	{Name: `last_value`, Typ: types.Int},
	{Name: `log_cnt`, Typ: types.Int},
	{Name: `is_called`, Typ: types.Bool},
}

// ExportColumns are the result columns of an EXPORT statement.
var ExportColumns = ResultColumns{
	{Name: "filename", Typ: types.String},
	{Name: "rows", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}
