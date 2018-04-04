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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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

// FindTable is part of the opt.Catalog interface.
func (tc *TestCatalog) FindTable(ctx context.Context, name *tree.TableName) (opt.Table, error) {
	if table, ok := tc.tables[name.Table()]; ok {
		return table, nil
	}
	return nil, fmt.Errorf("table %q not found", name.String())
}

// Table returns the test table that was previously added with the given name.
func (tc *TestCatalog) Table(name string) *TestTable {
	return tc.tables[name]
}

// AddTable adds the given test table to the catalog.
func (tc *TestCatalog) AddTable(tab *TestTable) {
	tc.tables[tab.Name] = tab
}

// TestTable implements the opt.Table interface for testing purposes.
type TestTable struct {
	Name    string
	Columns []*TestColumn
	Indexes []*TestIndex
}

var _ opt.Table = &TestTable{}

func (tt *TestTable) String() string {
	tp := treeprinter.New()
	opt.FormatCatalogTable(tt, tp)
	return tp.String()
}

// TabName is part of the opt.Table interface.
func (tt *TestTable) TabName() opt.TableName {
	return opt.TableName(tt.Name)
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

// FindOrdinal returns the ordinal of the column with the given name.
func (tt *TestTable) FindOrdinal(name string) int {
	for i, col := range tt.Columns {
		if col.Name == name {
			return i
		}
	}
	panic(fmt.Sprintf("cannot find column %s in table %s", name, tt.Name))
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
