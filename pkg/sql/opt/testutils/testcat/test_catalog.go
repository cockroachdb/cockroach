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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

const (
	// testDB is the default current database for testing purposes.
	testDB = "t"
)

// Catalog implements the cat.Catalog interface for testing purposes.
type Catalog struct {
	testSchema  Schema
	dataSources map[string]cat.DataSource
	counter     int
}

var _ cat.Catalog = &Catalog{}

// New creates a new empty instance of the test catalog.
func New() *Catalog {
	return &Catalog{
		testSchema: Schema{
			SchemaID: 1,
			SchemaName: cat.SchemaName{
				CatalogName:     testDB,
				SchemaName:      tree.PublicSchemaName,
				ExplicitSchema:  true,
				ExplicitCatalog: true,
			},
		},
		dataSources: make(map[string]cat.DataSource),
	}
}

// ResolveSchema is part of the cat.Catalog interface.
func (tc *Catalog) ResolveSchema(_ context.Context, name *cat.SchemaName) (cat.Schema, error) {
	// This is a simplified version of tree.TableName.ResolveTarget() from
	// sql/tree/name_resolution.go.
	toResolve := *name
	if name.ExplicitSchema {
		if name.ExplicitCatalog {
			// Already 2 parts: nothing to do.
			return tc.resolveSchema(&toResolve, name)
		}

		// Only one part specified; assume it's a schema name and determine
		// whether the current database has that schema.
		toResolve.CatalogName = testDB
		if sch, err := tc.resolveSchema(&toResolve, name); err == nil {
			return sch, nil
		}

		// No luck so far. Compatibility with CockroachDB v1.1: use D.public
		// instead.
		toResolve.CatalogName = name.SchemaName
		toResolve.SchemaName = tree.PublicSchemaName
		toResolve.ExplicitCatalog = true
		return tc.resolveSchema(&toResolve, name)
	}

	// Neither schema or catalog was specified, so use t.public.
	toResolve.CatalogName = tree.Name(testDB)
	toResolve.SchemaName = tree.PublicSchemaName
	return tc.resolveSchema(&toResolve, name)
}

// ResolveDataSource is part of the cat.Catalog interface.
func (tc *Catalog) ResolveDataSource(
	_ context.Context, name *cat.DataSourceName,
) (cat.DataSource, error) {
	// This is a simplified version of tree.TableName.ResolveExisting() from
	// sql/tree/name_resolution.go.
	var ds cat.DataSource
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

// ResolveDataSourceByID is part of the cat.Catalog interface.
func (tc *Catalog) ResolveDataSourceByID(
	ctx context.Context, id cat.StableID,
) (cat.DataSource, error) {
	for _, ds := range tc.dataSources {
		if tab, ok := ds.(*Table); ok && tab.TabID == id {
			return ds, nil
		}
	}
	return nil, pgerror.NewErrorf(pgerror.CodeUndefinedTableError,
		"relation [%d] does not exist", id)
}

// CheckPrivilege is part of the cat.Catalog interface.
func (tc *Catalog) CheckPrivilege(ctx context.Context, o cat.Object, priv privilege.Kind) error {
	switch t := o.(type) {
	case *Schema:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.SchemaName)
		}
	case *Table:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.TabName)
		}
	case *View:
		if t.Revoked {
			return fmt.Errorf("user does not have privilege to access %v", t.ViewName)
		}
	default:
		panic("invalid Object")
	}
	return nil
}

func (tc *Catalog) resolveSchema(toResolve, name *cat.SchemaName) (cat.Schema, error) {
	if string(toResolve.CatalogName) != testDB {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidSchemaNameError,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(&toResolve.CatalogName)).
			SetHintf("verify that the current database and search_path are valid and/or the target database exists")
	}

	if string(toResolve.SchemaName) != tree.PublicSchema {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"schema cannot be modified: %q", tree.ErrString(toResolve))
	}

	*name = *toResolve
	return &tc.testSchema, nil
}

// resolveDataSource checks if `toResolve` exists among the data sources in this
// Catalog. If it does, resolveDataSource updates `name` to match `toResolve`,
// and returns the corresponding data source. Otherwise, it returns an error.
func (tc *Catalog) resolveDataSource(toResolve, name *cat.DataSourceName) (cat.DataSource, error) {
	if table, ok := tc.dataSources[toResolve.FQString()]; ok {
		*name = *toResolve
		return table, nil
	}
	return nil, fmt.Errorf("no data source matches prefix: %q", tree.ErrString(toResolve))
}

// Schema returns the singleton test schema.
func (tc *Catalog) Schema() *Schema {
	return &tc.testSchema
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
func (tc *Catalog) View(name *cat.DataSourceName) *View {
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

// nextStableID returns a new unique StableID for a data source.
func (tc *Catalog) nextStableID() cat.StableID {
	tc.counter++

	// 53 is a magic number derived from how CRDB internally stores tables. The
	// first user table is 53. Use this to have the test catalog look more
	// consistent with the real catalog.
	return cat.StableID(53 + tc.counter - 1)
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

// Schema implements the cat.Schema interface for testing purposes.
type Schema struct {
	SchemaID   cat.StableID
	SchemaName cat.SchemaName

	// If Revoked is true, then the user has had privileges on the schema revoked.
	Revoked bool
}

var _ cat.Schema = &Schema{}

// ID is part of the cat.Schema interface.
func (s *Schema) ID() cat.StableID {
	return s.SchemaID
}

// Name is part of the cat.Schema interface.
func (s *Schema) Name() *cat.SchemaName {
	return &s.SchemaName
}

// View implements the cat.View interface for testing purposes.
type View struct {
	ViewID      cat.StableID
	ViewVersion cat.Version
	ViewName    cat.DataSourceName
	QueryText   string
	ColumnNames tree.NameList

	// If Revoked is true, then the user has had privileges on the view revoked.
	Revoked bool
}

var _ cat.View = &View{}

func (tv *View) String() string {
	tp := treeprinter.New()
	cat.FormatCatalogView(tv, tp)
	return tp.String()
}

// ID is part of the cat.DataSource interface.
func (tv *View) ID() cat.StableID {
	return tv.ViewID
}

// Version is part of the cat.DataSource interface.
func (tv *View) Version() cat.Version {
	return tv.ViewVersion
}

// Name is part of the cat.DataSource interface.
func (tv *View) Name() *cat.DataSourceName {
	return &tv.ViewName
}

// Query is part of the cat.View interface.
func (tv *View) Query() string {
	return tv.QueryText
}

// ColumnNameCount is part of the cat.View interface.
func (tv *View) ColumnNameCount() int {
	return len(tv.ColumnNames)
}

// ColumnName is part of the cat.View interface.
func (tv *View) ColumnName(i int) tree.Name {
	return tv.ColumnNames[i]
}

// Table implements the cat.Table interface for testing purposes.
type Table struct {
	TabID      cat.StableID
	TabVersion cat.Version
	TabName    tree.TableName
	Columns    []*Column
	Indexes    []*Index
	Stats      TableStats
	IsVirtual  bool
	Catalog    cat.Catalog
	Mutations  []cat.MutationColumn

	// If Revoked is true, then the user has had privileges on the table revoked.
	Revoked bool
}

var _ cat.Table = &Table{}

func (tt *Table) String() string {
	tp := treeprinter.New()
	cat.FormatCatalogTable(tt.Catalog, tt, tp)
	return tp.String()
}

// ID is part of the cat.DataSource interface.
func (tt *Table) ID() cat.StableID {
	return tt.TabID
}

// Version is part of the cat.DataSource interface.
func (tt *Table) Version() cat.Version {
	return tt.TabVersion
}

// Name is part of the cat.DataSource interface.
func (tt *Table) Name() *cat.DataSourceName {
	return &tt.TabName
}

// IsVirtualTable is part of the cat.Table interface.
func (tt *Table) IsVirtualTable() bool {
	return tt.IsVirtual
}

// ColumnCount is part of the cat.Table interface.
func (tt *Table) ColumnCount() int {
	return len(tt.Columns) + len(tt.Mutations)
}

// Column is part of the cat.Table interface.
func (tt *Table) Column(i int) cat.Column {
	if i < len(tt.Columns) {
		return tt.Columns[i]
	}
	return &tt.Mutations[i-len(tt.Columns)]
}

// IndexCount is part of the cat.Table interface.
func (tt *Table) IndexCount() int {
	return len(tt.Indexes)
}

// Index is part of the cat.Table interface.
func (tt *Table) Index(i int) cat.Index {
	return tt.Indexes[i]
}

// StatisticCount is part of the cat.Table interface.
func (tt *Table) StatisticCount() int {
	return len(tt.Stats)
}

// Statistic is part of the cat.Table interface.
func (tt *Table) Statistic(i int) cat.TableStatistic {
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

// Index implements the v.Index interface for testing purposes.
type Index struct {
	IdxName string

	// Ordinal is the ordinal of this index in the table.
	Ordinal int

	// KeyCount is the number of columns that make up the unique key for the
	// index. See the cat.Index.KeyColumnCount for more details.
	KeyCount int

	// LaxKeyCount is the number of columns that make up a lax key for the
	// index, which allows duplicate rows when at least one of the values is
	// NULL. See the cat.Index.LaxKeyColumnCount for more details.
	LaxKeyCount int

	// Unique is true if this index is declared as UNIQUE in the schema.
	Unique bool

	// Inverted is true when this index is an inverted index.
	Inverted bool

	Columns []cat.IndexColumn

	// table is a back reference to the table this index is on.
	table *Table

	// foreignKey is a struct representing an outgoing foreign key
	// reference. If fkSet is true, then foreignKey is a valid
	// index reference.
	foreignKey cat.ForeignKeyReference
	fkSet      bool
}

// ID is part of the cat.Index interface.
func (ti *Index) ID() cat.StableID {
	return 1 + cat.StableID(ti.Ordinal)
}

// Name is part of the cat.Index interface.
func (ti *Index) Name() tree.Name {
	return tree.Name(ti.IdxName)
}

// Table is part of the cat.Index interface.
func (ti *Index) Table() cat.Table {
	return ti.table
}

// IsUnique is part of the cat.Index interface.
func (ti *Index) IsUnique() bool {
	return ti.Unique
}

// IsInverted is part of the cat.Index interface.
func (ti *Index) IsInverted() bool {
	return ti.Inverted
}

// ColumnCount is part of the cat.Index interface.
func (ti *Index) ColumnCount() int {
	return len(ti.Columns)
}

// KeyColumnCount is part of the cat.Index interface.
func (ti *Index) KeyColumnCount() int {
	return ti.KeyCount
}

// LaxKeyColumnCount is part of the cat.Index interface.
func (ti *Index) LaxKeyColumnCount() int {
	return ti.LaxKeyCount
}

// Column is part of the cat.Index interface.
func (ti *Index) Column(i int) cat.IndexColumn {
	return ti.Columns[i]
}

// ForeignKey is part of the cat.Index interface.
func (ti *Index) ForeignKey() (cat.ForeignKeyReference, bool) {
	return ti.foreignKey, ti.fkSet
}

// Column implements the cat.Column interface for testing purposes.
type Column struct {
	Ordinal      int
	Hidden       bool
	Nullable     bool
	Name         string
	Type         types.T
	DefaultExpr  *string
	ComputedExpr *string
}

var _ cat.Column = &Column{}

// ColID is part of the cat.Index interface.
func (tc *Column) ColID() cat.StableID {
	return 1 + cat.StableID(tc.Ordinal)
}

// IsNullable is part of the cat.Column interface.
func (tc *Column) IsNullable() bool {
	return tc.Nullable
}

// ColName is part of the cat.Column interface.
func (tc *Column) ColName() tree.Name {
	return tree.Name(tc.Name)
}

// DatumType is part of the cat.Column interface.
func (tc *Column) DatumType() types.T {
	return tc.Type
}

// ColTypeStr is part of the cat.Column interface.
func (tc *Column) ColTypeStr() string {
	t, err := coltypes.DatumTypeToColumnType(tc.Type)
	if err != nil {
		panic(err)
	}
	return t.String()
}

// IsHidden is part of the cat.Column interface.
func (tc *Column) IsHidden() bool {
	return tc.Hidden
}

// HasDefault is part of the cat.Column interface.
func (tc *Column) HasDefault() bool {
	return tc.DefaultExpr != nil
}

// IsComputed is part of the cat.Column interface.
func (tc *Column) IsComputed() bool {
	return tc.ComputedExpr != nil
}

// DefaultExprStr is part of the cat.Column interface.
func (tc *Column) DefaultExprStr() string {
	return *tc.DefaultExpr
}

// ComputedExprStr is part of the cat.Column interface.
func (tc *Column) ComputedExprStr() string {
	return *tc.ComputedExpr
}

// TableStat implements the cat.TableStatistic interface for testing purposes.
type TableStat struct {
	js stats.JSONStatistic
	tt *Table
}

var _ cat.TableStatistic = &TableStat{}

// CreatedAt is part of the cat.TableStatistic interface.
func (ts *TableStat) CreatedAt() time.Time {
	d, err := tree.ParseDTimestamp(nil, ts.js.CreatedAt, time.Microsecond)
	if err != nil {
		panic(err)
	}
	return d.Time
}

// ColumnCount is part of the cat.TableStatistic interface.
func (ts *TableStat) ColumnCount() int {
	return len(ts.js.Columns)
}

// ColumnOrdinal is part of the cat.TableStatistic interface.
func (ts *TableStat) ColumnOrdinal(i int) int {
	return ts.tt.FindOrdinal(ts.js.Columns[i])
}

// RowCount is part of the cat.TableStatistic interface.
func (ts *TableStat) RowCount() uint64 {
	return ts.js.RowCount
}

// DistinctCount is part of the cat.TableStatistic interface.
func (ts *TableStat) DistinctCount() uint64 {
	return ts.js.DistinctCount
}

// NullCount is part of the cat.TableStatistic interface.
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
