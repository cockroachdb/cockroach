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

package sqlbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  types.T

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
	for _, colDesc := range colDescs {
		// Convert the ColumnDescriptor to ResultColumn.
		typ := colDesc.Type.ToDatumType()
		if typ == nil {
			panic(fmt.Sprintf("unsupported column type: %s", colDesc.Type.SemanticType))
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
		if other[i].Typ == types.Unknown {
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
	{Name: "Tree", Typ: types.String},
	// Field is the part of the node that a row of output pertains to.
	{Name: "Field", Typ: types.String},
	// Description contains details about the field.
	{Name: "Description", Typ: types.String},
}

// ExplainPlanVerboseColumns are the result columns of an
// EXPLAIN (PLAN, ...) ...
// statement when a flag like VERBOSE or TYPES is passed.
var ExplainPlanVerboseColumns = ResultColumns{
	// Tree shows the node type with the tree structure.
	{Name: "Tree", Typ: types.String},
	// Level is the depth of the node in the tree. Hidden by default; can be
	// retrieved using:
	//   SELECT "Level" FROM [ EXPLAIN (VERBOSE) ... ].
	{Name: "Level", Typ: types.Int, Hidden: true},
	// Type is the node type. Hidden by default.
	{Name: "Type", Typ: types.String, Hidden: true},
	// Field is the part of the node that a row of output pertains to.
	{Name: "Field", Typ: types.String},
	// Description contains details about the field.
	{Name: "Description", Typ: types.String},
	// Columns is the type signature of the data source.
	{Name: "Columns", Typ: types.String},
	// Ordering indicates the known ordering of the data from this source.
	{Name: "Ordering", Typ: types.String},
}
