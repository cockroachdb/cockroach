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

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// TestCatalog implements the optbase.Catalog interface for testing purposes.
type TestCatalog struct {
	tables map[string]*TestTable
}

var _ optbase.Catalog = &TestCatalog{}

// NewTestCatalog creates a new empty instance of the test catalog.
func NewTestCatalog() *TestCatalog {
	return &TestCatalog{tables: make(map[string]*TestTable)}
}

// FindTable is part of the optbase.Catalog interface.
func (tc *TestCatalog) FindTable(
	ctx context.Context, name *tree.TableName,
) (optbase.Table, error) {
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
func (tc *TestCatalog) AddTable(tbl *TestTable) {
	tc.tables[tbl.Name] = tbl
}

// TestTable implements the optbase.Table interface for testing purposes.
type TestTable struct {
	Name             string
	Columns          []*TestColumn
	PrimaryIndex     *TestIndex
	SecondaryIndexes []*TestIndex
}

var _ optbase.Table = &TestTable{}

func (tt *TestTable) String() string {
	tp := treeprinter.New()
	optbase.FormatCatalogTable(tt, tp)
	return tp.String()
}

// TabName is part of the optbase.Table interface.
func (tt *TestTable) TabName() optbase.TableName {
	return optbase.TableName(tt.Name)
}

// ColumnCount is part of the optbase.Table interface.
func (tt *TestTable) ColumnCount() int {
	return len(tt.Columns)
}

// Column is part of the optbase.Table interface.
func (tt *TestTable) Column(i int) optbase.Column {
	return tt.Columns[i]
}

// Primary is part of the optbase.Table interface.
func (tt *TestTable) Primary() optbase.Index {
	return tt.PrimaryIndex
}

// SecondaryCount is part of the optbase.Table interface.
func (tt *TestTable) SecondaryCount() int {
	return len(tt.SecondaryIndexes)
}

// Secondary is part of the optbase.Table interface.
func (tt *TestTable) Secondary(i int) optbase.Index {
	return tt.SecondaryIndexes[i]
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

// TestIndex implements the optbase.Index interface for testing purposes.
type TestIndex struct {
	Name    string
	Columns []optbase.IndexColumn

	// Unique is the number of columns that make up the unique key for the
	// index. The columns are always a non-empty prefix of the Columns
	// collection, so Unique is > 0 and < len(Columns). Note that
	Unique int
}

// IdxName is part of the optbase.Index interface.
func (ti *TestIndex) IdxName() string {
	return ti.Name
}

// ColumnCount is part of the optbase.Index interface.
func (ti *TestIndex) ColumnCount() int {
	return len(ti.Columns)
}

// UniqueColumnCount is part of the optbase.Index interface.
func (ti *TestIndex) UniqueColumnCount() int {
	return ti.Unique
}

// Column is part of the optbase.Index interface.
func (ti *TestIndex) Column(i int) optbase.IndexColumn {
	return ti.Columns[i]
}

// TestColumn implements the optbase.Column interface for testing purposes.
type TestColumn struct {
	Hidden   bool
	Nullable bool
	Name     string
	Type     types.T
}

var _ optbase.Column = &TestColumn{}

// IsNullable is part of the optbase.Column interface.
func (tc *TestColumn) IsNullable() bool {
	return tc.Nullable
}

// ColName is part of the optbase.Column interface.
func (tc *TestColumn) ColName() optbase.ColumnName {
	return optbase.ColumnName(tc.Name)
}

// DatumType is part of the optbase.Column interface.
func (tc *TestColumn) DatumType() types.T {
	return tc.Type
}

// IsHidden is part of the optbase.Column interface.
func (tc *TestColumn) IsHidden() bool {
	return tc.Hidden
}
