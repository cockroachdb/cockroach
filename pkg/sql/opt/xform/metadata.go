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

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// ColSet efficiently stores an unordered set of column indexes.
type ColSet = util.FastIntSet

// ColumnIndex uniquely identifies the usage of a column within the scope of a
// query. See the comment for Metadata for more details.
type ColumnIndex int32

// TableIndex uniquely identifies the usage of a table within the scope of a
// query. The indexes of its columns start at the table index and proceed
// sequentially from there. See the comment for Metadata for more details.
type TableIndex int32

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
	// catalog contains system-wide metadata.
	catalog optbase.Catalog

	// labels are the set of column labels, indexed by ColumnIndex. These
	// labels are used for pretty-printing and debugging. Skip index 0 so that
	// it is reserved for "unknown column".
	labels []string

	// nextCol keeps track of the index that will be assigned to the next
	// table or column added to the metadata.
	nextCol ColumnIndex

	// tables maps from table index to the catalog metadata for the table. The
	// table index is the index of the first column in the table. The remaining
	// columns form a contiguous group following that index.
	tables map[TableIndex]optbase.Table
}

func newMetadata(catalog optbase.Catalog) *Metadata {
	// Skip label index 0 so that it is reserved for "unknown column".
	return &Metadata{catalog: catalog, labels: make([]string, 1)}
}

// Catalog returns the system catalog from which query metadata is derived.
func (md *Metadata) Catalog() optbase.Catalog {
	return md.catalog
}

// AddColumn indexes a new reference to a column within the query and records
// its label.
func (md *Metadata) AddColumn(label string) ColumnIndex {
	// Skip index 0 so that it is reserved for "unknown column".
	md.nextCol++
	md.labels = append(md.labels, label)
	return md.nextCol
}

// ColumnLabel returns the label of the given column. It is used for pretty-
// printing and debugging.
func (md *Metadata) ColumnLabel(index ColumnIndex) string {
	if index == 0 {
		panic("uninitialized column id 0")
	}

	return md.labels[index]
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table indexes (e.g. in
// a self-join query).
func (md *Metadata) AddTable(tbl optbase.Table) TableIndex {
	tblIndex := TableIndex(md.nextCol + 1)

	for i := 0; i < tbl.NumColumns(); i++ {
		col := tbl.Column(i)
		if tbl.TabName() == "" {
			md.AddColumn(col.ColName())
		} else {
			md.AddColumn(fmt.Sprintf("%s.%s", tbl.TabName(), col.ColName()))
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
