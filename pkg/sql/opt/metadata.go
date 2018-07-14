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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// TableID uniquely identifies the usage of a table within the scope of a
// query. The ids of its columns start at the table id and proceed sequentially
// from there. See the comment for Metadata for more details.
type TableID int32

// TableAnnID uniquely identifies an annotation on an instance of table
// metadata. A table annotation allows arbitrary values to be cached with table
// metadata, which can be used to avoid recalculating base table properties or
// other information each time it's needed.
//
// To create a TableAnnID, call NewTableAnnID during Go's program initialization
// phase. The returned TableAnnID never clashes with other annotations on the
// same table. Here is a usage example:
//
//   var myAnnID = NewTableAnnID()
//
//   md.SetTableAnnotation(TableID(1), myAnnID, "foo")
//   ann := md.TableAnnotation(TableID(1), myAnnID)
//
// Currently, the following annotations are in use:
//   - WeakKeys: weak keys derived from the base table
//   - Stats: statistics derived from the base table
//
// To add an additional annotation, increase the value of maxTableAnnIDCount and
// add a call to NewTableAnnID.
type TableAnnID int

// tableAnnIDCount counts the number of times NewTableAnnID is called.
var tableAnnIDCount TableAnnID

// maxTableAnnIDCount is the maximum number of times that NewTableAnnID can be
// called. Calling more than this number of times results in a panic. Having
// a maximum enables a static annotation array to be inlined into the metadata
// table struct.
const maxTableAnnIDCount = 2

// Metadata assigns unique ids to the columns, tables, and other metadata used
// within the scope of a particular query. Because it is specific to one query,
// the ids tend to be small integers that can be efficiently stored and
// manipulated.
//
// Within a query, every unique column and every projection (that is more than
// just a pass through of a column) is assigned a unique column id.
// Additionally, every separate reference to a table in the query gets a new
// set of output column ids. Consider the query:
//
//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
//
// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not equivalent
// to `r.y`. In order to achieve this, we need to give these columns different
// ids.
//
// In all cases, the column ids are global to the query. For example, consider
// the query:
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
	// cols stores information about each metadata column, indexed by ColumnID.
	// Skip id 0 so that it is reserved for "unknown column".
	cols []mdColumn

	// tables maps from table id to the catalog metadata for the table. The
	// table id is the id of the first column in the table. The remaining
	// columns form a contiguous group following that id.
	tables map[TableID]*mdTable
}

// mdTable stores information about one of the tables stored in the metadata.
type mdTable struct {
	// tab is a reference to the table in the catalog.
	tab Table

	// anns annotates the table metadata with arbitrary data.
	anns [maxTableAnnIDCount]interface{}
}

// mdColumn stores information about one of the columns stored in the metadata,
// including its label and type.
type mdColumn struct {
	// label is the best-effort name of this column. Since the same column can
	// have multiple labels (using aliasing), one of those is chosen to be used
	// for pretty-printing and debugging. This might be different than what is
	// stored in the physical properties and is presented to end users.
	label string

	// typ is the scalar SQL type of this column.
	typ types.T
}

// NewMetadata constructs a new instance of metadata for the optimizer.
func NewMetadata() *Metadata {
	// Skip mdColumn index 0 so that it is reserved for "unknown column".
	return &Metadata{cols: make([]mdColumn, 1)}
}

// AddColumn assigns a new unique id to a column within the query and records
// its label and type.
func (md *Metadata) AddColumn(label string, typ types.T) ColumnID {
	md.cols = append(md.cols, mdColumn{label: label, typ: typ})
	return ColumnID(len(md.cols) - 1)
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	// Index 0 is skipped.
	return len(md.cols) - 1
}

// IndexColumns returns the set of columns in the given index.
// TODO(justin): cache this value in the table metadata.
func (md *Metadata) IndexColumns(tableID TableID, indexOrdinal int) ColSet {
	tab := md.Table(tableID)
	index := tab.Index(indexOrdinal)

	var indexCols ColSet
	for i := 0; i < index.ColumnCount(); i++ {
		ord := index.Column(i).Ordinal
		indexCols.Add(int(md.TableColumn(tableID, ord)))
	}

	return indexCols
}

// ColumnLabel returns the label of the given column. It is used for pretty-
// printing and debugging.
func (md *Metadata) ColumnLabel(id ColumnID) string {
	if id == 0 {
		panic("uninitialized column id 0")
	}

	return md.cols[id].label
}

// ColumnType returns the SQL scalar type of the given column.
func (md *Metadata) ColumnType(id ColumnID) types.T {
	if id == 0 {
		panic("uninitialized column id 0")
	}

	return md.cols[id].typ
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table ids (e.g. in a
// self-join query).
func (md *Metadata) AddTable(tab Table) TableID {
	return md.AddTableWithName(tab, "")
}

// AddTableWithName indexes a new reference to a table within the query.
// Separate references to the same table are assigned different table ids
// (e.g. in a self-join query). Optionally, include a table name tabName to
// override the name in tab when creating column labels.
func (md *Metadata) AddTableWithName(tab Table, tabName string) TableID {
	tabID := TableID(md.NumColumns() + 1)
	if tabName == "" {
		tabName = string(tab.TabName().TableName)
	}

	for i := 0; i < tab.ColumnCount(); i++ {
		col := tab.Column(i)
		if tabName == "" {
			md.AddColumn(string(col.ColName()), col.DatumType())
		} else {
			md.AddColumn(fmt.Sprintf("%s.%s", tabName, col.ColName()), col.DatumType())
		}
	}

	if md.tables == nil {
		md.tables = make(map[TableID]*mdTable)
	}

	md.tables[tabID] = &mdTable{tab: tab}
	return tabID
}

// Table looks up the catalog table associated with the given metadata id. The
// same table can be associated with multiple metadata ids.
func (md *Metadata) Table(tabID TableID) Table {
	return md.tables[tabID].tab
}

// TableColumn returns the metadata id of the column at the given ordinal
// position in the table.
func (md *Metadata) TableColumn(tabID TableID, ord int) ColumnID {
	return ColumnID(int(tabID) + ord)
}

// TableAnnotation returns the given annotation that is associated with the
// given table. If the table has no such annotation, TableAnnotation returns
// nil.
func (md *Metadata) TableAnnotation(tabID TableID, annID TableAnnID) interface{} {
	return md.tables[tabID].anns[annID]
}

// SetTableAnnotation associates the given annotation with the given table. The
// annotation is associated by the given ID, which was allocated by
// calling NewTableAnnID. If an annotation with the ID already exists on the
// table, then it is overwritten.
//
// See the TableAnnID comment for more details and a usage example.
func (md *Metadata) SetTableAnnotation(tabID TableID, tabAnnID TableAnnID, ann interface{}) {
	md.tables[tabID].anns[tabAnnID] = ann
}

// NewTableAnnID allocates a unique annotation identifier that is used to
// associate arbitrary data with table metadata. Only maxTableAnnIDCount total
// annotation ID's can exist in the system. Attempting to exceed the maximum
// results in a panic.
//
// This method is not thread-safe, and therefore should only be called during
// Go's program initialization phase (which uses a single goroutine to init
// variables).
//
// See the TableAnnID comment for more details and a usage example.
func NewTableAnnID() TableAnnID {
	if tableAnnIDCount == maxTableAnnIDCount {
		panic("can't allocate table annotation id; increase maxTableAnnIDCount to allow")
	}
	cnt := tableAnnIDCount
	tableAnnIDCount++
	return cnt
}
