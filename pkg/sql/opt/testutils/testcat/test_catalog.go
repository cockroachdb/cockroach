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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Catalog implements the opt.Catalog interface for testing purposes.
type Catalog struct {
	tables map[string]*Table
}

var _ opt.Catalog = &Catalog{}

// New creates a new empty instance of the test catalog.
func New() *Catalog {
	return &Catalog{tables: make(map[string]*Table)}
}

const (
	// testDB is the default current database for testing purposes.
	testDB = "t"
)

// FindTable is part of the opt.Catalog interface.
func (tc *Catalog) FindTable(ctx context.Context, name *tree.TableName) (opt.Table, error) {
	// This is a simplified version of tree.TableName.ResolveExisting() from
	// sql/tree/name_resolution.go.
	toFind := *name
	if name.ExplicitSchema {
		if name.ExplicitCatalog {
			// Already 3 parts.
			return tc.findTable(&toFind, name)
		}

		// Two parts: Try to use the current database, and be satisfied if it's
		// sufficient to find the object.
		toFind.CatalogName = testDB
		if tab, err := tc.findTable(&toFind, name); err == nil {
			return tab, nil
		}

		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T
		// instead.
		toFind.CatalogName = name.SchemaName
		toFind.SchemaName = tree.PublicSchemaName
		toFind.ExplicitCatalog = true
		return tc.findTable(&toFind, name)
	}

	// This is a naked table name. Use the current database.
	toFind.CatalogName = tree.Name(testDB)
	toFind.SchemaName = tree.PublicSchemaName
	return tc.findTable(&toFind, name)
}

// findTable checks if the table `toFind` exists among the tables in this
// Catalog. If it does, findTable updates `name` to match `toFind`, and
// returns the corresponding table. Otherwise, it returns an error.
func (tc *Catalog) findTable(toFind, name *tree.TableName) (opt.Table, error) {
	if table, ok := tc.tables[toFind.FQString()]; ok {
		*name = *toFind
		return table, nil
	}
	return nil, fmt.Errorf("table %q not found", tree.ErrString(name))
}

// Table returns the test table that was previously added with the given name.
func (tc *Catalog) Table(name string) *Table {
	tn := tree.MakeUnqualifiedTableName(tree.Name(name))
	tab, err := tc.FindTable(context.TODO(), &tn)
	if err != nil {
		panic(fmt.Errorf("table %q is not in the test catalog", tree.ErrString((*tree.Name)(&name))))
	}
	return tab.(*Table)
}

// AddTable adds the given test table to the catalog.
func (tc *Catalog) AddTable(tab *Table) {
	fq := tab.Name.FQString()
	if _, ok := tc.tables[fq]; ok {
		panic(fmt.Errorf("table %q already exists", tree.ErrString(&tab.Name)))
	}
	tc.tables[fq] = tab
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

	case *tree.AlterTable:
		tc.AlterTable(stmt)
		return "", nil

	case *tree.DropTable:
		tc.DropTable(stmt)
		return "", nil

	default:
		return "", fmt.Errorf("expected CREATE TABLE or ALTER TABLE statement but found: %v", stmt)
	}
}

// qualifyTableName updates the given table name to include catalog and schema
// if not already included.
func (tc *Catalog) qualifyTableName(name *tree.TableName) {
	if name.ExplicitSchema {
		if name.ExplicitCatalog {
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
		name.ExplicitCatalog = true
		return
	}

	// Use the current database.
	name.CatalogName = testDB
	name.SchemaName = tree.PublicSchemaName
}

// Table implements the opt.Table interface for testing purposes.
type Table struct {
	Name      tree.TableName
	Columns   []*Column
	Indexes   []*Index
	Stats     TableStats
	IsVirtual bool
}

var _ opt.Table = &Table{}

func (tt *Table) String() string {
	tp := treeprinter.New()
	opt.FormatCatalogTable(tt, tp)
	return tp.String()
}

// TabName is part of the opt.Table interface.
func (tt *Table) TabName() *tree.TableName {
	return &tt.Name
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
		tree.ErrString(&tt.Name),
	))
}

// Index implements the opt.Index interface for testing purposes.
type Index struct {
	Name    string
	Columns []opt.IndexColumn

	// KeyCount is the number of columns that make up the unique key for the
	// index. See the opt.Index.KeyColumnCount for more details.
	KeyCount int

	// LaxKeyCount is the number of columns that make up a lax key for the
	// index, which allows duplicate rows when at least one of the values is
	// NULL. See the opt.Index.LaxKeyColumnCount for more details.
	LaxKeyCount int

	// Inverted is true when this index is an inverted index.
	Inverted bool
}

// IdxName is part of the opt.Index interface.
func (ti *Index) IdxName() string {
	return ti.Name
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
func (tc *Column) ColName() opt.ColumnName {
	return opt.ColumnName(tc.Name)
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
	d, err := tree.ParseDTimestamp(ts.js.CreatedAt, time.Microsecond)
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
