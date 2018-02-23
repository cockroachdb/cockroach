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

	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// TestColumn implements the optbase.Column interface for testing purposes.
type TestColumn struct {
	Hidden   bool
	Nullable bool
	Name     string
	Type     types.T
}

var _ optbase.Column = &TestColumn{}

func (c *TestColumn) String() string {
	var buf bytes.Buffer
	c.Format(&buf)
	return buf.String()
}

// Format performs pretty-printing of the TestColumn into a bytes buffer.
func (c *TestColumn) Format(buf *bytes.Buffer) {
	buf.WriteString(c.Name)
	buf.WriteByte(' ')
	buf.WriteString(c.Type.String())

	if c.Nullable {
		buf.WriteString(" NULL")
	} else {
		buf.WriteString(" NOT NULL")
	}

	if c.Hidden {
		buf.WriteString(" (hidden)")
	}

	buf.WriteString("\n")
}

// IsNullable is part of the optbase.Column interface.
func (c *TestColumn) IsNullable() bool {
	return c.Nullable
}

// ColName is part of the optbase.Column interface.
func (c *TestColumn) ColName() optbase.ColumnName {
	return optbase.ColumnName(c.Name)
}

// DatumType is part of the optbase.Column interface.
func (c *TestColumn) DatumType() types.T {
	return c.Type
}

// IsHidden is part of the optbase.Column interface.
func (c *TestColumn) IsHidden() bool {
	return c.Hidden
}

// TestTable implements the optbase.Table interface for testing purposes.
type TestTable struct {
	Name    string
	Columns []*TestColumn
}

var _ optbase.Table = &TestTable{}

func (t *TestTable) String() string {
	var buf bytes.Buffer
	t.Format(&buf)
	return buf.String()
}

// Format performs pretty-printing of the TestTable into a bytes buffer.
func (t *TestTable) Format(buf *bytes.Buffer) {
	buf.WriteString(fmt.Sprintf("table %s\n", t.Name))
	for _, col := range t.Columns {
		buf.WriteString("  ")
		col.Format(buf)
	}

	if len(t.Columns) == 0 {
		buf.WriteString("\n")
	}
}

// TabName is part of the optbase.Table interface.
func (t *TestTable) TabName() optbase.TableName {
	return optbase.TableName(t.Name)
}

// ColumnCount is part of the optbase.Table interface.
func (t *TestTable) ColumnCount() int {
	return len(t.Columns)
}

// Column is part of the optbase.Table interface.
func (t *TestTable) Column(i int) optbase.Column {
	return t.Columns[i]
}

// Primary is part of the optbase.Table interface.
func (t *TestTable) Primary() optbase.Index {
	// TODO(andyk): Implement this.
	return nil
}

// SecondaryCount is part of the optbase.Table interface.
func (t *TestTable) SecondaryCount() int {
	// TODO(andyk): Implement this.
	return 0
}

// Secondary is part of the optbase.Table interface.
func (t *TestTable) Secondary(i int) optbase.Index {
	// TODO(andyk): Implement this.
	panic("no secondary indexes")
}

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
func (c *TestCatalog) FindTable(ctx context.Context, name *tree.TableName) (optbase.Table, error) {
	if table, ok := c.tables[name.Table()]; ok {
		return table, nil
	}
	return nil, fmt.Errorf("table %q not found", name.String())
}

// Table returns the test table that was previously added with the given name.
func (c *TestCatalog) Table(name string) *TestTable {
	return c.tables[name]
}

// AddTable adds the given test table to the catalog.
func (c *TestCatalog) AddTable(tbl *TestTable) {
	c.tables[tbl.Name] = tbl
}
