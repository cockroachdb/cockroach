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

package testcat

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Catalog implements the opt.Catalog interface for testing purposes.
type Catalog struct {
	dataSources map[string]opt.DataSource
	fingerprint opt.Fingerprint
}

var _ opt.Catalog = &Catalog{}

// New creates a new empty instance of the test catalog.
func New() *Catalog {
	return &Catalog{dataSources: make(map[string]opt.DataSource)}
}

const (
	// testDB is the default current database for testing purposes.
	testDB = "t"
)

// ResolveDataSource is part of the opt.Catalog interface.
func (tc *Catalog) ResolveDataSource(
	_ context.Context, name *tree.TableName,
) (opt.DataSource, error) {
	// This is a simplified version of tree.TableName.ResolveExisting() from
	// sql/tree/name_resolution.go.
	var ds opt.DataSource
	var err error
	toResolve := *name
	if name.ExplicitSchema && name.ExplicitCatalog {
		// Already 3 parts.
		ds, err = tc.resolveDataSource(&toResolve, name)
		if err == nil {
			return ds, nil
		}
	} else if name.ExplicitSchema {
		// Two parts: Try to use the current database, and be satisfied if it's
		// sufficient to find the object.
		toResolve.CatalogName = testDB
		if tab, err := tc.resolveDataSource(&toResolve, name); err == nil {
			return tab, nil
		}

		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T
		// instead.
		toResolve.CatalogName = name.SchemaName
		toResolve.SchemaName = tree.PublicSchemaName
		toResolve.ExplicitCatalog = true
		ds, err = tc.resolveDataSource(&toResolve, name)
		if err == nil {
			return ds, nil
		}
	} else {
		// This is a naked data source name. Use the current database.
		toResolve.CatalogName = tree.Name(testDB)
		toResolve.SchemaName = tree.PublicSchemaName
		ds, err = tc.resolveDataSource(&toResolve, name)
		if err == nil {
			return ds, nil
		}
	}

	// If we didn't find the table in the catalog, try to lazily resolve it as
	// a virtual table.
	if table, ok := resolveVTable(name); ok {
		// We rely on the check in CreateTable against this table's schema to infer
		// that this is a virtual table.
		return tc.CreateTable(table), nil
	}

	// If this didn't end up being a virtual table, then return the original
	// error returned by resolveDataSource.
	return nil, err
}

// ResolveDataSourceByID is part of the opt.Catalog interface.
func (tc *Catalog) ResolveDataSourceByID(
	ctx context.Context, tableID int64,
) (opt.DataSource, error) {
	for _, ds := range tc.dataSources {
		if tab, ok := ds.(*Table); ok && int64(tab.tableID) == tableID {
			return ds, nil
		}
	}
	return nil, pgerror.NewErrorf(pgerror.CodeUndefinedTableError,
		"relation [%d] does not exist", tableID)
}

// resolveDataSource checks if `toResolve` exists among the data sources in this
// Catalog. If it does, resolveDataSource updates `name` to match `toResolve`,
// and returns the corresponding data source. Otherwise, it returns an error.
func (tc *Catalog) resolveDataSource(toResolve, name *tree.TableName) (opt.DataSource, error) {
	if table, ok := tc.dataSources[toResolve.FQString()]; ok {
		*name = *toResolve
		return table, nil
	}
	return nil, fmt.Errorf("no data source matches prefix: %q", tree.ErrString(toResolve))
}

// Table returns the test table that was previously added with the given name.
func (tc *Catalog) Table(name *tree.TableName) *Table {
	ds, err := tc.ResolveDataSource(context.TODO(), name)
	if err != nil {
		panic(err)
	}
	if tab, ok := ds.(*Table); ok {
		return tab
	}
	panic(fmt.Errorf("\"%q\" is not a table", tree.ErrString(name)))
}

// AddTable adds the given test table to the catalog.
func (tc *Catalog) AddTable(tab *Table) {
	fq := tab.TabName.FQString()
	if _, ok := tc.dataSources[fq]; ok {
		panic(fmt.Errorf("table %q already exists", tree.ErrString(&tab.TabName)))
	}
	tc.dataSources[fq] = tab
}

// View returns the test view that was previously added with the given name.
func (tc *Catalog) View(name *tree.TableName) *View {
	ds, err := tc.ResolveDataSource(context.TODO(), name)
	if err != nil {
		panic(err)
	}
	if vw, ok := ds.(*View); ok {
		return vw
	}
	panic(fmt.Errorf("\"%q\" is not a view", tree.ErrString(name)))
}

// AddView adds the given test view to the catalog.
func (tc *Catalog) AddView(view *View) {
	fq := view.ViewName.FQString()
	if _, ok := tc.dataSources[fq]; ok {
		panic(fmt.Errorf("view %q already exists", tree.ErrString(&view.ViewName)))
	}
	tc.dataSources[fq] = view
}

// ExecuteDDL parses the given DDL SQL statement and creates objects in the test
// catalog. This is used to test without spinning up a cluster.
func (tc *Catalog) ExecuteDDL(sql string) (string, error) {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return "", err
	}

	if stmt.StatementType() != tree.DDL {
		return "", fmt.Errorf("statement type is not DDL: %v", stmt.StatementType())
	}

	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		tab := tc.CreateTable(stmt)
		return tab.String(), nil

	case *tree.CreateView:
		view := tc.CreateView(stmt)
		return view.String(), nil

	case *tree.AlterTable:
		tc.AlterTable(stmt)
		return "", nil

	case *tree.DropTable:
		tc.DropTable(stmt)
		return "", nil

	default:
		return "", fmt.Errorf("unsupported statement: %v", stmt)
	}
}

// nextFingerprint returns a new unique fingerprint for a data source.
func (tc *Catalog) nextFingerprint() opt.Fingerprint {
	tc.fingerprint++
	return tc.fingerprint
}

// qualifyTableName updates the given table name to include catalog and schema
// if not already included.
func (tc *Catalog) qualifyTableName(name *tree.TableName) {
	hadExplicitSchema := name.ExplicitSchema
	hadExplicitCatalog := name.ExplicitCatalog
	name.ExplicitSchema = true
	name.ExplicitCatalog = true

	if hadExplicitSchema {
		if hadExplicitCatalog {
			// Already 3 parts: nothing to do.
			return
		}

		if name.SchemaName == tree.PublicSchemaName {
			// Use the current database.
			name.CatalogName = testDB
			return
		}

		// Compatibility with CockroachDB v1.1: use D.public.T.
		name.CatalogName = name.SchemaName
		name.SchemaName = tree.PublicSchemaName
		return
	}

	// Use the current database.
	name.CatalogName = testDB
	name.SchemaName = tree.PublicSchemaName
}

// View implements the opt.View interface for testing purposes.
type View struct {
	ViewFingerprint opt.Fingerprint
	ViewName        tree.TableName
	QueryText       string
	ColumnNames     tree.NameList

	// If Revoked is true, then the user has had privileges on the view revoked.
	Revoked bool
}

var _ opt.View = &View{}

func (tv *View) String() string {
	tp := treeprinter.New()
	opt.FormatCatalogView(tv, tp)
	return tp.String()
}

// Fingerprint is part of the opt.DataSource interface.
func (tv *View) Fingerprint() opt.Fingerprint {
	return tv.ViewFingerprint
}

// Name is part of the opt.DataSource interface.
func (tv *View) Name() *tree.TableName {
	return &tv.ViewName
}

// CheckPrivilege is part of the opt.DataSource interface.
func (tv *View) CheckPrivilege(ctx context.Context, priv privilege.Kind) error {
	if tv.Revoked {
		return fmt.Errorf("user does not have privilege to access %v", tv.ViewName)
	}
	return nil
}

// Query is part of the opt.View interface.
func (tv *View) Query() string {
	return tv.QueryText
}

// ColumnNameCount is part of the opt.View interface.
func (tv *View) ColumnNameCount() int {
	return len(tv.ColumnNames)
}

// ColumnName is part of the opt.View interface.
func (tv *View) ColumnName(i int) tree.Name {
	return tv.ColumnNames[i]
}

// Table implements the opt.Table interface for testing purposes.
type Table struct {
	TabFingerprint opt.Fingerprint
	TabName        tree.TableName
	Columns        []*Column
	Indexes        []*Index
	Stats          TableStats
	IsVirtual      bool
	Catalog        opt.Catalog

	// If Revoked is true, then the user has had privileges on the table revoked.
	Revoked bool

	tableID sqlbase.ID
}

var _ opt.Table = &Table{}

func (tt *Table) String() string {
	tp := treeprinter.New()
	opt.FormatCatalogTable(tt.Catalog, tt, tp)
	return tp.String()
}

// Fingerprint is part of the opt.DataSource interface.
func (tt *Table) Fingerprint() opt.Fingerprint {
	return tt.TabFingerprint
}

// Name is part of the opt.DataSource interface.
func (tt *Table) Name() *tree.TableName {
	return &tt.TabName
}

// CheckPrivilege is part of the opt.DataSource interface.
func (tt *Table) CheckPrivilege(ctx context.Context, priv privilege.Kind) error {
	if tt.Revoked {
		return fmt.Errorf("user does not have privilege to access %v", tt.TabName)
	}
	return nil
}

// InternalID is part of the opt.Table interface.
func (tt *Table) InternalID() uint64 {
	return uint64(tt.tableID)
}

// IsVirtualTable is part of the opt.Table interface.
func (tt *Table) IsVirtualTable() bool {
	return tt.IsVirtual
}

// ColumnCount is part of the opt.Table interface.
func (tt *Table) ColumnCount() int {
	return len(tt.Columns)
}

// Column is part of the opt.Table interface.
func (tt *Table) Column(i int) opt.Column {
	return tt.Columns[i]
}

// LookupColumnOrdinal is part of the opt.Table interface.
func (tt *Table) LookupColumnOrdinal(colID uint32) (int, error) {
	if int(colID) > len(tt.Columns) || int(colID) < 1 {
		return int(colID), pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
			"column [%d] does not exist", colID)
	}
	return int(colID) - 1, nil
}

// IndexCount is part of the opt.Table interface.
func (tt *Table) IndexCount() int {
	return len(tt.Indexes)
}

// Index is part of the opt.Table interface.
func (tt *Table) Index(i int) opt.Index {
	return tt.Indexes[i]
}

// StatisticCount is part of the opt.Table interface.
func (tt *Table) StatisticCount() int {
	return len(tt.Stats)
}

// Statistic is part of the opt.Table interface.
func (tt *Table) Statistic(i int) opt.TableStatistic {
	return tt.Stats[i]
}

// FindOrdinal returns the ordinal of the column with the given name.
func (tt *Table) FindOrdinal(name string) int {
	for i, col := range tt.Columns {
		if col.Name == name {
			return i
		}
	}
	panic(fmt.Sprintf(
		"cannot find column %q in table %q",
		tree.ErrString((*tree.Name)(&name)),
		tree.ErrString(&tt.TabName),
	))
}

// Index implements the opt.Index interface for testing purposes.
type Index struct {
	Name string
	// Ordinal is the ordinal of this index in the table.
	Ordinal int
	Columns []opt.IndexColumn

	// Table is a back reference to the table this index is on.
	table *Table

	// KeyCount is the number of columns that make up the unique key for the
	// index. See the opt.Index.KeyColumnCount for more details.
	KeyCount int

	// LaxKeyCount is the number of columns that make up a lax key for the
	// index, which allows duplicate rows when at least one of the values is
	// NULL. See the opt.Index.LaxKeyColumnCount for more details.
	LaxKeyCount int

	// Inverted is true when this index is an inverted index.
	Inverted bool

	// foreignKey is a struct representing an outgoing foreign key
	// reference. If fkSet is true, then foreignKey is a valid
	// index reference.
	foreignKey opt.ForeignKeyReference
	fkSet      bool
}

// IdxName is part of the opt.Index interface.
func (ti *Index) IdxName() string {
	return ti.Name
}

// InternalID is part of the opt.Index interface.
func (ti *Index) InternalID() uint64 {
	return 1 + uint64(ti.Ordinal)
}

// Table is part of the opt.Index interface.
func (ti *Index) Table() opt.Table {
	return ti.table
}

// IsInverted is part of the opt.Index interface.
func (ti *Index) IsInverted() bool {
	return ti.Inverted
}

// ColumnCount is part of the opt.Index interface.
func (ti *Index) ColumnCount() int {
	return len(ti.Columns)
}

// KeyColumnCount is part of the opt.Index interface.
func (ti *Index) KeyColumnCount() int {
	return ti.KeyCount
}

// LaxKeyColumnCount is part of the opt.Index interface.
func (ti *Index) LaxKeyColumnCount() int {
	return ti.LaxKeyCount
}

// Column is part of the opt.Index interface.
func (ti *Index) Column(i int) opt.IndexColumn {
	return ti.Columns[i]
}

// ForeignKey is part of the opt.Index interface.
func (ti *Index) ForeignKey() (opt.ForeignKeyReference, bool) {
	return ti.foreignKey, ti.fkSet
}

// Column implements the opt.Column interface for testing purposes.
type Column struct {
	Hidden   bool
	Nullable bool
	Name     string
	Type     types.T
}

var _ opt.Column = &Column{}

// IsNullable is part of the opt.Column interface.
func (tc *Column) IsNullable() bool {
	return tc.Nullable
}

// ColName is part of the opt.Column interface.
func (tc *Column) ColName() tree.Name {
	return tree.Name(tc.Name)
}

// DatumType is part of the opt.Column interface.
func (tc *Column) DatumType() types.T {
	return tc.Type
}

// IsHidden is part of the opt.Column interface.
func (tc *Column) IsHidden() bool {
	return tc.Hidden
}

// TableStat implements the opt.TableStatistic interface for testing purposes.
type TableStat struct {
	js stats.JSONStatistic
	tt *Table
}

var _ opt.TableStatistic = &TableStat{}

// CreatedAt is part of the opt.TableStatistic interface.
func (ts *TableStat) CreatedAt() time.Time {
	d, err := tree.ParseDTimestamp(nil, ts.js.CreatedAt, time.Microsecond)
	if err != nil {
		panic(err)
	}
	return d.Time
}

// ColumnCount is part of the opt.TableStatistic interface.
func (ts *TableStat) ColumnCount() int {
	return len(ts.js.Columns)
}

// ColumnOrdinal is part of the opt.TableStatistic interface.
func (ts *TableStat) ColumnOrdinal(i int) int {
	return ts.tt.FindOrdinal(ts.js.Columns[i])
}

// RowCount is part of the opt.TableStatistic interface.
func (ts *TableStat) RowCount() uint64 {
	return ts.js.RowCount
}

// DistinctCount is part of the opt.TableStatistic interface.
func (ts *TableStat) DistinctCount() uint64 {
	return ts.js.DistinctCount
}

// NullCount is part of the opt.TableStatistic interface.
func (ts *TableStat) NullCount() uint64 {
	return ts.js.NullCount
}

// TableStats is a slice of TableStat pointers.
type TableStats []*TableStat

// Len is part of the Sorter interface.
func (ts TableStats) Len() int { return len(ts) }

// Less is part of the Sorter interface.
func (ts TableStats) Less(i, j int) bool {
	// Sort with most recent first.
	return ts[i].CreatedAt().Unix() > ts[j].CreatedAt().Unix()
}

// Swap is part of the Sorter interface.
func (ts TableStats) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}
