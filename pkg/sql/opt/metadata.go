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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// TableID uniquely identifies the usage of a table within the scope of a
// query. TableID 0 is reserved to mean "unknown table".
//
// Internally, the TableID consists of an index into the Metadata.tables slice,
// as well as the ColumnID of the first column in the table. Subsequent columns
// have sequential ids, relative to their ordinal position in the table.
//
// See the comment for Metadata for more details on identifiers.
type TableID uint64

const (
	tableIDMask = 0xffffffff
)

// ColumnID returns the metadata id of the column at the given ordinal position
// in the table.
//
// NOTE: This method does not do bounds checking, so it's up to the caller to
//       ensure that a column really does exist at this ordinal position.
func (t TableID) ColumnID(ord int) ColumnID {
	return t.firstColID() + ColumnID(ord)
}

// makeTableID constructs a new TableID from its component parts.
func makeTableID(index int, firstColID ColumnID) TableID {
	// Bias the table index by 1.
	return TableID((uint64(index+1) << 32) | uint64(firstColID))
}

// firstColID returns the ColumnID of the first column in the table.
func (t TableID) firstColID() ColumnID {
	return ColumnID(t & tableIDMask)
}

// index returns the index of the table in Metadata.tables. It's biased by 1, so
// that TableID 0 can be be reserved to mean "unknown table".
func (t TableID) index() int {
	return int((t>>32)&tableIDMask) - 1
}

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
	cols []mdColumn

	// tables stores information about each metadata table, indexed by TableID.
	tables []mdTable

	// deps stores information about all data sources depended on by the query,
	// as well as the privileges required to access those data sources.
	deps []mdDependency
}

type mdDependency struct {
	ds DataSource

	priv privilege.Kind
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
	// tabID is the identifier of the base table to which this column belongs.
	// If the column was synthesized (i.e. no base table), then the value is set
	// to UnknownTableID.
	tabID TableID

	// label is the best-effort name of this column. Since the same column can
	// have multiple labels (using aliasing), one of those is chosen to be used
	// for pretty-printing and debugging. This might be different than what is
	// stored in the physical properties and is presented to end users.
	label string

	// typ is the scalar SQL type of this column.
	typ types.T
}

// Init prepares the metadata for use (or reuse).
func (md *Metadata) Init() {
	// Clear the columns and tables to release memory (this clearing pattern is
	// optimized by Go).
	for i := range md.cols {
		md.cols[i] = mdColumn{}
	}
	for i := range md.tables {
		md.tables[i] = mdTable{}
	}
	md.cols = md.cols[:0]
	md.tables = md.tables[:0]
	md.deps = md.deps[:0]
}

// AddMetadata initializes the metadata with a copy of the provided metadata.
// This metadata can then be modified independent of the copied metadata.
func (md *Metadata) AddMetadata(from *Metadata) {
	if len(md.cols) != 0 || len(md.tables) != 0 || len(md.deps) != 0 {
		panic("AddMetadata not supported when destination metadata is not empty")
	}
	md.cols = append(md.cols, from.cols...)
	md.tables = append(md.tables, from.tables...)
	md.deps = append(md.deps, from.deps...)
}

// AddDependency tracks one of the data sources on which the query depends, as
// well as the privilege required to access that data source. If the Memo using
// this metadata is cached, then a call to CheckDependencies can detect if
// changes to schema or permissions on the data source has invalidated the
// cached metadata.
func (md *Metadata) AddDependency(ds DataSource, priv privilege.Kind) {
	md.deps = append(md.deps, mdDependency{ds: ds, priv: priv})
}

// CheckDependencies resolves each data source on which this metadata depends,
// in order to check that the fully qualified data source names still resolve to
// the same data source (i.e. having the same fingerprint), and that the user
// still has sufficient privileges to access the data source.
func (md *Metadata) CheckDependencies(ctx context.Context, catalog Catalog) bool {
	for _, dep := range md.deps {
		ds, err := catalog.ResolveDataSource(ctx, dep.ds.Name())
		if err != nil {
			return false
		}
		if dep.ds.Fingerprint() != ds.Fingerprint() {
			return false
		}
		if dep.priv != 0 {
			if err = catalog.CheckPrivilege(ctx, ds, dep.priv); err != nil {
				return false
			}
		}
	}
	return true
}

// AddColumn assigns a new unique id to a column within the query and records
// its label and type. If the label is empty, a "column<ID>" label is created.
func (md *Metadata) AddColumn(label string, typ types.T) ColumnID {
	if label == "" {
		label = fmt.Sprintf("column%d", len(md.cols)+1)
	}
	md.cols = append(md.cols, mdColumn{label: label, typ: typ})
	return ColumnID(len(md.cols))
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	return len(md.cols)
}

// IndexColumns returns the set of columns in the given index.
// TODO(justin): cache this value in the table metadata.
func (md *Metadata) IndexColumns(tableID TableID, indexOrdinal int) ColSet {
	tab := md.Table(tableID)
	index := tab.Index(indexOrdinal)

	var indexCols ColSet
	for i, cnt := 0, index.ColumnCount(); i < cnt; i++ {
		ord := index.Column(i).Ordinal
		indexCols.Add(int(tableID.ColumnID(ord)))
	}
	return indexCols
}

// ColumnTableID returns the identifier of the base table to which the given
// column belongs. If the column has no base table because it was synthesized,
// ColumnTableID returns zero.
func (md *Metadata) ColumnTableID(id ColumnID) TableID {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].tabID
}

// ColumnLabel returns the label of the given column. It is used for pretty-
// printing and debugging.
func (md *Metadata) ColumnLabel(id ColumnID) string {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].label
}

// ColumnType returns the SQL scalar type of the given column.
func (md *Metadata) ColumnType(id ColumnID) types.T {
	// ColumnID is biased so that 0 is never used (reserved to indicate the
	// unknown column).
	return md.cols[id-1].typ
}

// ColumnOrdinal returns the ordinal position of the column in its base table.
// It panics if the column has no base table because it was synthesized.
func (md *Metadata) ColumnOrdinal(id ColumnID) int {
	tabID := md.cols[id-1].tabID
	if tabID == 0 {
		panic("column was synthesized and has no ordinal position")
	}
	return int(id - tabID.firstColID())
}

// QualifiedColumnLabel returns the column label, possibly qualified with the
// table name if either of these conditions is true:
//
//   1. fullyQualify is true
//   2. the label might be ambiguous if it's not qualified, because there's
//      another column in the metadata with the same name
//
// If the column label is qualified, the table is prefixed to it and separated
// by a "." character.
func (md *Metadata) QualifiedColumnLabel(id ColumnID, fullyQualify bool) string {
	col := md.cols[id-1]
	if col.tabID == 0 {
		// Column doesn't belong to a table, so no need to qualify it further.
		return col.label
	}

	// If a fully qualified label has not been requested, then only qualify it if
	// it would otherwise be ambiguous.
	ambiguous := fullyQualify
	if !fullyQualify {
		for i := range md.cols {
			if i == int(id-1) {
				continue
			}

			// If there are two columns with same name, then column is ambiguous.
			otherCol := &md.cols[i]
			if otherCol.label == col.label {
				ambiguous = true
				break
			}
		}
	}

	if !ambiguous {
		return col.label
	}

	var sb strings.Builder
	tabName := md.Table(col.tabID).Name()
	if fullyQualify {
		sb.WriteString(tabName.FQString())
	} else {
		sb.WriteString(string(tabName.TableName))
	}
	sb.WriteRune('.')
	sb.WriteString(col.label)
	return sb.String()
}

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table ids (e.g. in a
// self-join query).
func (md *Metadata) AddTable(tab Table) TableID {
	tabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))
	if md.tables == nil {
		md.tables = make([]mdTable, 0, 4)
	}
	md.tables = append(md.tables, mdTable{tab: tab})

	colCount := tab.ColumnCount()
	if md.cols == nil {
		md.cols = make([]mdColumn, 0, colCount)
	}

	for i := 0; i < colCount; i++ {
		col := tab.Column(i)
		md.cols = append(md.cols, mdColumn{
			tabID: tabID,
			label: string(col.ColName()),
			typ:   col.DatumType(),
		})
	}

	return tabID
}

// AddTableWithMutations first calls AddTable to add regular columns to the
// metadata. It then appends any columns that are currently undergoing mutation
// (i.e. being added or dropped from the table), and which need to be
// initialized to their default value by INSERT statements. See this RFC for
// more details:
//
//   cockroachdb/cockroach/docs/RFCS/20151014_online_schema_change.md
//
func (md *Metadata) AddTableWithMutations(tab Table) TableID {
	tabID := md.AddTable(tab)
	for i, n := 0, tab.MutationColumnCount(); i < n; i++ {
		col := tab.MutationColumn(i)
		md.cols = append(md.cols, mdColumn{
			tabID: tabID,
			label: string(col.ColName()),
			typ:   col.DatumType(),
		})
	}
	return tabID
}

// Table looks up the catalog table associated with the given metadata id. The
// same table can be associated with multiple metadata ids.
func (md *Metadata) Table(tabID TableID) Table {
	return md.tables[tabID.index()].tab
}

// TableByDescID looks up the catalog table associated with the given descriptor id.
func (md *Metadata) TableByDescID(tabID uint64) Table {
	for _, mdTab := range md.tables {
		if mdTab.tab.InternalID() == tabID {
			return mdTab.tab
		}
	}
	return nil
}

// TableAnnotation returns the given annotation that is associated with the
// given table. If the table has no such annotation, TableAnnotation returns
// nil.
func (md *Metadata) TableAnnotation(tabID TableID, annID TableAnnID) interface{} {
	return md.tables[tabID.index()].anns[annID]
}

// SetTableAnnotation associates the given annotation with the given table. The
// annotation is associated by the given ID, which was allocated by
// calling NewTableAnnID. If an annotation with the ID already exists on the
// table, then it is overwritten.
//
// See the TableAnnID comment for more details and a usage example.
func (md *Metadata) SetTableAnnotation(tabID TableID, tabAnnID TableAnnID, ann interface{}) {
	md.tables[tabID.index()].anns[tabAnnID] = ann
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
