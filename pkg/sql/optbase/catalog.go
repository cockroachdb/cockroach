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

package optbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// This file contains interfaces that are used by the query optimizer to avoid
// including specifics of sqlbase structures in the opt code.

// ColumnName is the type of a column name.
type ColumnName string

// TableName is the type of a table name.
type TableName string

// Column is an interface to a table column, exposing only the information
// needed by the query optimizer.
type Column interface {
	// IsNullable returns true if the column is nullable.
	IsNullable() bool

	// ColName returns the name of the column.
	ColName() ColumnName

	// DatumType returns the data type of the column.
	DatumType() types.T

	// IsHidden returns true if the column is hidden (e.g., there is always a
	// hidden column called rowid if there is no primary key on the table).
	IsHidden() bool
}

// Table is an interface to a database table, exposing only the information
// needed by the query optimizer.
type Table interface {
	// TabName returns the name of the table.
	TabName() TableName

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
