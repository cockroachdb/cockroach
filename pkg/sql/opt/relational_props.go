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

package opt

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type tableName string
type columnName string
type columnSet = util.FastIntSet
type columnMap = util.FastIntMap
type columnIndex = int

// queryState holds per-query state.
type queryState struct {
	// Map from table name to the column index for the table's columns within the
	// query (they form a contiguous group starting at this index). Note that a
	// table can occur multiple times in a query and each occurrence is given its
	// own column indexes so the map is from table name to slice of base indexes.
	// For example, consider the query:
	//
	//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
	//
	// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not
	// equivalent to `r.y`. In order to achieve this, we need to give these
	// columns different indexes.
	//
	// Therefore, the tables map might look like this:
	//   a -> [0, 2]
	//
	// columnIndex 0 is the first of two columnIndexes corresponding to the
	// columns x and y from table a AS l. Likewise, columnIndex 2 is the first of
	// two columnIndexes for table a AS r.
	tables map[tableName][]columnIndex

	// nextColumnIndex is the next unique column index. This field is incremented
	// each time a new column is added to the query Expr tree.
	nextColumnIndex columnIndex

	// Semantic context used for name resolution and type checking.
	semaCtx tree.SemaContext
}

// columnProps holds properties about each column in a relational expression.
type columnProps struct {
	// name is the column name.
	name columnName

	// table is the name of the table.
	table tableName

	// typ contains the datum type held by the column.
	typ types.T

	// index is the index for this column, which is unique across all the
	// columns in the expression.
	index columnIndex
}

func (c columnProps) String() string {
	if c.name == "" {
		return fmt.Sprintf("@%d", c.index+1)
	}
	if c.table == "" {
		return tree.NameString(string(c.name))
	}
	return fmt.Sprintf("%s.%s",
		tree.NameString(string(c.table)), tree.NameString(string(c.name)))
}

// hasColumn returns true if c references the given column.
func (c columnProps) hasColumn(tblName tableName, colName columnName) bool {
	if colName != c.name {
		return false
	}
	if tblName == "" {
		return true
	}
	return c.table == tblName
}

// relationalProps holds properties of a relational expression.
type relationalProps struct {
	// outputCols is the output column set.
	outputCols columnSet

	// notNullCols is a column set indicating which output columns are known to
	// never be NULL. The NULL-ability of columns flows from the inputs and can
	// also be derived from filters that are NULL-intolerant.
	notNullCols columnSet

	// columns is the set of all columns used in the relational expression.
	columns []columnProps
}

func (p *relationalProps) init() {
	p.outputCols = p.availableOutputCols()
}

func (p *relationalProps) String() string {
	tp := treeprinter.New()
	p.format(tp)
	return tp.String()
}

func (p *relationalProps) format(tp treeprinter.Node) {
	var buf bytes.Buffer
	if len(p.columns) > 0 {
		buf.WriteString("columns:")
		for _, col := range p.columns {
			buf.WriteString(" ")
			buf.WriteString(string(col.table))
			buf.WriteString(".")
			buf.WriteString(string(col.name))
			buf.WriteString(":")
			buf.WriteString(col.typ.String())
			if !p.notNullCols.Contains(col.index) {
				buf.WriteString(",null")
			}
			buf.WriteString(":")
			fmt.Fprintf(&buf, "%d", col.index)

		}
		tp.Child(buf.String())
	}
}

// availableOutputCols returns the set of columns output by the expression.
func (p *relationalProps) availableOutputCols() columnSet {
	var v columnSet
	for _, col := range p.columns {
		v.Add(col.index)
	}
	return v
}
