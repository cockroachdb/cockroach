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

package testutils

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// TestCatalog implements the opt.Catalog interface for testing purposes.
type TestCatalog struct {
	tables map[string]*TestTable
}

var _ opt.Catalog = &TestCatalog{}

// NewTestCatalog creates a new empty instance of the test catalog.
func NewTestCatalog() *TestCatalog {
	return &TestCatalog{tables: make(map[string]*TestTable)}
}

const (
	// testDB is the default current database for testing purposes.
	testDB = "t"
)

// FindTable is part of the opt.Catalog interface.
func (tc *TestCatalog) FindTable(ctx context.Context, name *tree.TableName) (opt.Table, error) {
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
// TestCatalog. If it does, findTable updates `name` to match `toFind`, and
// returns the corresponding table. Otherwise, it returns an error.
func (tc *TestCatalog) findTable(toFind, name *tree.TableName) (opt.Table, error) {
	if table, ok := tc.tables[toFind.FQString()]; ok {
		*name = *toFind
		return table, nil
	}
	return nil, fmt.Errorf("table %q not found", tree.ErrString(name))
}

// Table returns the test table that was previously added with the given name.
func (tc *TestCatalog) Table(name string) *TestTable {
	tn := tree.MakeUnqualifiedTableName(tree.Name(name))
	tab, err := tc.FindTable(context.TODO(), &tn)
	if err != nil {
		panic(fmt.Errorf("table %q is not in the test catalog", tree.ErrString((*tree.Name)(&name))))
	}
	return tab.(*TestTable)
}

// AddTable adds the given test table to the catalog.
func (tc *TestCatalog) AddTable(tab *TestTable) {
	tc.tables[tab.Name.FQString()] = tab
}

// TestTable implements the opt.Table interface for testing purposes.
type TestTable struct {
	Name    tree.TableName
	Columns []*TestColumn
	Indexes []*TestIndex
	Stats   TestTableStats
}

var _ opt.Table = &TestTable{}

func (tt *TestTable) String() string {
	tp := treeprinter.New()
	opt.FormatCatalogTable(tt, tp)
	return tp.String()
}

// TabName is part of the opt.Table interface.
func (tt *TestTable) TabName() opt.TableName {
	return opt.TableName(tt.Name.TableName)
}

// ColumnCount is part of the opt.Table interface.
func (tt *TestTable) ColumnCount() int {
	return len(tt.Columns)
}

// Column is part of the opt.Table interface.
func (tt *TestTable) Column(i int) opt.Column {
	return tt.Columns[i]
}

// IndexCount is part of the opt.Table interface.
func (tt *TestTable) IndexCount() int {
	return len(tt.Indexes)
}

// Index is part of the opt.Table interface.
func (tt *TestTable) Index(i int) opt.Index {
	return tt.Indexes[i]
}

// StatisticCount is part of the opt.Table interface.
func (tt *TestTable) StatisticCount() int {
	return len(tt.Stats)
}

// Statistic is part of the opt.Table interface.
func (tt *TestTable) Statistic(i int) opt.TableStatistic {
	return tt.Stats[i]
}

// FindOrdinal returns the ordinal of the column with the given name.
func (tt *TestTable) FindOrdinal(name string) int {
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

// TestIndex implements the opt.Index interface for testing purposes.
type TestIndex struct {
	Name    string
	Columns []opt.IndexColumn

	// Unique is the number of columns that make up the unique key for the
	// index. The columns are always a non-empty prefix of the Columns
	// collection, so Unique is > 0 and < len(Columns). Note that this is even
	// true for indexes that were not declared as unique, since primary key
	// columns will be implicitly added to the index in order to ensure it is
	// always unique.
	Unique int
}

// IdxName is part of the opt.Index interface.
func (ti *TestIndex) IdxName() string {
	return ti.Name
}

// ColumnCount is part of the opt.Index interface.
func (ti *TestIndex) ColumnCount() int {
	return len(ti.Columns)
}

// UniqueColumnCount is part of the opt.Index interface.
func (ti *TestIndex) UniqueColumnCount() int {
	return ti.Unique
}

// Column is part of the opt.Index interface.
func (ti *TestIndex) Column(i int) opt.IndexColumn {
	return ti.Columns[i]
}

// TestColumn implements the opt.Column interface for testing purposes.
type TestColumn struct {
	Hidden   bool
	Nullable bool
	Name     string
	Type     types.T
}

var _ opt.Column = &TestColumn{}

// IsNullable is part of the opt.Column interface.
func (tc *TestColumn) IsNullable() bool {
	return tc.Nullable
}

// ColName is part of the opt.Column interface.
func (tc *TestColumn) ColName() opt.ColumnName {
	return opt.ColumnName(tc.Name)
}

// DatumType is part of the opt.Column interface.
func (tc *TestColumn) DatumType() types.T {
	return tc.Type
}

// IsHidden is part of the opt.Column interface.
func (tc *TestColumn) IsHidden() bool {
	return tc.Hidden
}

// TestTableStat implements the opt.TableStatistic interface for testing purposes.
type TestTableStat struct {
	js stats.JSONStatistic
	tt *TestTable
}

var _ opt.TableStatistic = &TestTableStat{}

// CreatedAt is part of the opt.TableStatistic interface.
func (ts *TestTableStat) CreatedAt() time.Time {
	d, err := tree.ParseDTimestamp(ts.js.CreatedAt, time.Microsecond)
	if err != nil {
		panic(err)
	}
	return d.Time
}

// ColumnCount is part of the opt.TableStatistic interface.
func (ts *TestTableStat) ColumnCount() int {
	return len(ts.js.Columns)
}

// ColumnOrdinal is part of the opt.TableStatistic interface.
func (ts *TestTableStat) ColumnOrdinal(i int) int {
	return ts.tt.FindOrdinal(ts.js.Columns[i])
}

// RowCount is part of the opt.TableStatistic interface.
func (ts *TestTableStat) RowCount() uint64 {
	return ts.js.RowCount
}

// DistinctCount is part of the opt.TableStatistic interface.
func (ts *TestTableStat) DistinctCount() uint64 {
	return ts.js.DistinctCount
}

// NullCount is part of the opt.TableStatistic interface.
func (ts *TestTableStat) NullCount() uint64 {
	return ts.js.NullCount
}

// TestTableStats is a slice of TestTableStat pointers.
type TestTableStats []*TestTableStat

// Len is part of the Sorter interface.
func (ts TestTableStats) Len() int { return len(ts) }

// Less is part of the Sorter interface.
func (ts TestTableStats) Less(i, j int) bool {
	// Sort with most recent first.
	return ts[i].CreatedAt().Unix() > ts[j].CreatedAt().Unix()
}

// Swap is part of the Sorter interface.
func (ts TestTableStats) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}
