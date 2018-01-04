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

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Column is an interface to a table column, exposing only the information
// needed by the query optimizer.
type Column interface {
	// IsNullable returns true if the column is nullable.
	IsNullable() bool

	// ColumnName returns the name of the column.
	ColumnName() string

	// DatumType returns the data type of the column.
	DatumType() types.T
}

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
type Table interface {
	// TableName returns the name of the table.
	TableName() string

	// NumColumns returns the number of columns in the table.
	NumColumns() int

	// Column returns a Column interface to the ith column of the table.
	Column(i int) Column
}

// Catalog is an interface to a database catalog, exposing only the information
// needed by the query optimizer.
type Catalog interface {
	// FindTable returns a Table interface for the database table matching the
	// given table name.  Returns an error if the table does not exist.
	FindTable(ctx context.Context, name *tree.TableName) (Table, error)
}

// IsNullable implements the Column interface.
func (desc *ColumnDescriptor) IsNullable() bool {
	return desc.Nullable
}

// ColumnName implements the Column interface.
func (desc *ColumnDescriptor) ColumnName() string {
	return desc.Name
}

// DatumType implements the Column interface.
func (desc *ColumnDescriptor) DatumType() types.T {
	return desc.Type.ToDatumType()
}

// TableName implements the Table interface.
func (desc *TableDescriptor) TableName() string {
	return desc.GetName()
}

// NumColumns implements the Table interface.
func (desc *TableDescriptor) NumColumns() int {
	return len(desc.Columns)
}

// Column implements the Table interface.
func (desc *TableDescriptor) Column(i int) Column {
	return &desc.Columns[i]
}
