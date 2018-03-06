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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// ColumnIndex uniquely identifies the usage of a column within the scope of a
// query. ColumnIndex 0 is reserved to mean "unknown column". See the comment
// for Metadata for more details.
type ColumnIndex int32

// TableIndex uniquely identifies the usage of a table within the scope of a
// query. The indexes of its columns start at the table index and proceed
// sequentially from there. See the comment for Metadata for more details.
type TableIndex int32

// ColSet efficiently stores an unordered set of column indexes.
type ColSet = util.FastIntSet

// ColList is a list of column indexes.
//
// TODO(radu): perhaps implement a FastIntList with the same "small"
// representation as FastIntMap but with a slice for large cases.
type ColList = []ColumnIndex

// ColMap provides a 1:1 mapping from one column index to another. It is used
// by operators that need to match columns from its inputs.
type ColMap = util.FastIntMap

// mdCol stores information about one of the columns stored in the metadata,
// including its label and type.
type mdCol struct {
	// label is the best-effort name of this column. Since the same column can
	// have multiple labels (using aliasing), one of those is chosen to be used
	// for pretty-printing and debugging. This might be different than what is
	// stored in the physical properties and is presented to end users.
	label string

	// typ is the scalar SQL type of this column.
	typ types.T
}

// Metadata indexes the columns, tables, and other metadata used within the
// scope of a particular query. Because it is specific to one query, the
// indexes tend to be small integers that can be efficiently stored and
// manipulated.
//
// Within a query, every unique column and every projection (that is more than
// just a pass through of a column) is assigned a unique column index.
// Additionally, every separate reference to a table in the query gets a new
// set of output column indexes. Consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not equivalent
// to `r.y`. In order to achieve this, we need to give these columns different
// indexes.
//
// In all cases, the column indexes are global to the query. For example,
// consider the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
type Metadata struct {
	// cols stores information about each metadata column, indexed by
	// ColumnIndex. Skip index 0 so that it is reserved for "unknown column".
	cols []mdCol

	// tables maps from table index to the catalog metadata for the table. The
	// table index is the index of the first column in the table. The remaining
	// columns form a contiguous group following that index.
	tables map[TableIndex]optbase.Table
}

// NewMetadata constructs a new instance of metadata for the optimizer.
func NewMetadata() *Metadata {
	// Skip label index 0 so that it is reserved for "unknown column".
	return &Metadata{cols: make([]mdCol, 1)}
}

// AddColumn indexes a new reference to a column within the query and records
// its label.
func (md *Metadata) AddColumn(label string, typ types.T) ColumnIndex {
	md.cols = append(md.cols, mdCol{label: label, typ: typ})
	return ColumnIndex(len(md.cols) - 1)
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	// Index 0 is skipped.
	return len(md.cols) - 1
}

// ColumnLabel returns the label of the given column. It is used for pretty-
// printing and debugging.
func (md *Metadata) ColumnLabel(index ColumnIndex) string {
	if index == 0 {
		panic("uninitialized column id 0")
	}

	return md.cols[index].label
}

// ColumnType returns the SQL scalar type of the given column.
func (md *Metadata) ColumnType(index ColumnIndex) types.T {
	if index == 0 {
		panic("uninitialized column id 0")
	}

	return md.cols[index].typ
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table indexes (e.g. in
// a self-join query).
func (md *Metadata) AddTable(tbl optbase.Table) TableIndex {
	tblIndex := TableIndex(md.NumColumns() + 1)

	for i := 0; i < tbl.ColumnCount(); i++ {
		col := tbl.Column(i)
		if tbl.TabName() == "" {
			md.AddColumn(string(col.ColName()), col.DatumType())
		} else {
			md.AddColumn(fmt.Sprintf("%s.%s", tbl.TabName(), col.ColName()), col.DatumType())
		}
	}

	if md.tables == nil {
		md.tables = make(map[TableIndex]optbase.Table)
	}

	md.tables[tblIndex] = tbl
	return tblIndex
}

// Table looks up the catalog table associated with the given metadata index.
// The same table can be associated with multiple metadata indexes.
func (md *Metadata) Table(index TableIndex) optbase.Table {
	return md.tables[index]
}

// TableColumn returns the metadata index of the column at the given ordinal
// position in the table.
func (md *Metadata) TableColumn(tblIndex TableIndex, ord int) ColumnIndex {
	return ColumnIndex(int(tblIndex) + ord)
}

// ColListToSet converts a column index list to a column index set.
func ColListToSet(colList ColList) ColSet {
	var r ColSet
	for _, col := range colList {
		r.Add(int(col))
	}
	return r
}
