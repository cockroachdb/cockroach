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
type columnIndex = int

// queryState holds per-query state.
type queryState struct {
	// Map from table name to the column index for the table's columns within the
	// query (they form a contiguous group starting at this index). Note that a
	// table can occur multiple times in a query and each occurrence is given its
	// own column indexes so the map is from table name to slice of base indexes.
	tables map[tableName][]columnIndex

	// The set of all columns used by the query.
	columns []columnProps
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
	if c.table == "" {
		return tree.Name(c.name).String()
	}
	return fmt.Sprintf("%s.%s", tree.Name(c.table), tree.Name(c.name))
}

// relationalProps holds properties of a relational expression.
type relationalProps struct {
	// outputCols is the output column set.
	outputCols columnSet

	// notNullCols is a column set indicating which output columns cannot be NULL.
	// The NULL-ability of columns flows from the inputs and can also be derived
	// from filters that are NULL-intolerant.
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
			buf.WriteString(":")
			fmt.Fprintf(&buf, "%d", col.index)
			if p.notNullCols.Contains(col.index) {
				buf.WriteString("*")
			}
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
