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

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

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
	cols []ColumnMeta

	// tables stores information about each metadata table, indexed by TableID.
	tables []TableMeta

	// deps stores information about all data sources depended on by the query,
	// as well as the privileges required to access those data sources.
	deps []mdDependency
}

type mdDependency struct {
	ds DataSource

	priv privilege.Kind
}

// Init prepares the metadata for use (or reuse).
func (md *Metadata) Init() {
	// Clear the columns and tables to release memory (this clearing pattern is
	// optimized by Go).
	for i := range md.cols {
		md.cols[i] = ColumnMeta{}
	}
	for i := range md.tables {
		md.tables[i] = TableMeta{}
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
// the same version of the same data source, and that the user still has
// sufficient privileges to access the data source.
func (md *Metadata) CheckDependencies(ctx context.Context, catalog Catalog) bool {
	for _, dep := range md.deps {
		ds, err := catalog.ResolveDataSource(ctx, dep.ds.Name())
		if err != nil {
			return false
		}
		if dep.ds.ID() != ds.ID() {
			return false
		}
		if dep.ds.Version() != ds.Version() {
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

// AddTable indexes a new reference to a table within the query. Separate
// references to the same table are assigned different table ids (e.g. in a
// self-join query). All columns are added to the metadata. If mutation columns
// are present, they are added after active columns.
func (md *Metadata) AddTable(tab Table) TableID {
	tabID := makeTableID(len(md.tables), ColumnID(len(md.cols)+1))
	if md.tables == nil {
		md.tables = make([]TableMeta, 0, 4)
	}
	md.tables = append(md.tables, TableMeta{MetaID: tabID, Table: tab})
	tabMeta := md.TableMeta(tabID)

	colCount := tab.ColumnCount()
	if md.cols == nil {
		md.cols = make([]ColumnMeta, 0, colCount)
	}

	for i := 0; i < colCount; i++ {
		col := tab.Column(i)
		colID := md.AddColumn(string(col.ColName()), col.DatumType())
		md.ColumnMeta(colID).TableMeta = tabMeta
	}

	return tabID
}

// TableMeta looks up the metadata for the table associated with the given table
// id. The same table can be added multiple times to the query metadata and
// associated with multiple table ids.
func (md *Metadata) TableMeta(tabID TableID) *TableMeta {
	return &md.tables[tabID.index()]
}

// Table looks up the catalog table associated with the given metadata id. The
// same table can be associated with multiple metadata ids.
func (md *Metadata) Table(tabID TableID) Table {
	return md.TableMeta(tabID).Table
}

// TableByStableID looks up the catalog table associated with the given
// StableID (unique across all tables and stable across queries).
func (md *Metadata) TableByStableID(id StableID) Table {
	for _, mdTab := range md.tables {
		if mdTab.Table.ID() == id {
			return mdTab.Table
		}
	}
	return nil
}

// AddColumn assigns a new unique id to a column within the query and records
// its alias and type. If the alias is empty, a "column<ID>" alias is created.
func (md *Metadata) AddColumn(alias string, typ types.T) ColumnID {
	if alias == "" {
		alias = fmt.Sprintf("column%d", len(md.cols)+1)
	}
	colID := ColumnID(len(md.cols) + 1)
	md.cols = append(md.cols, ColumnMeta{MetaID: colID, Alias: alias, Type: typ, md: md})
	return colID
}

// NumColumns returns the count of columns tracked by this Metadata instance.
func (md *Metadata) NumColumns() int {
	return len(md.cols)
}

// ColumnMeta looks up the metadata for the column associated with the given
// column id. The same column can be added multiple times to the query metadata
// and associated with multiple column ids.
func (md *Metadata) ColumnMeta(colID ColumnID) *ColumnMeta {
	return &md.cols[colID.index()]
}
