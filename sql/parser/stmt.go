// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import "fmt"

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// StatementType is the enumerated type for Statement return styles on
// the wire.
type StatementType int

const (
	// Ack indicates that the statement does not have a meaningful
	// return. Examples include SET, BEGIN, COMMIT.
	Ack StatementType = iota
	// DDL indicates that the statement mutates the database schema.
	DDL
	// RowsAffected indicates that the statement returns the count of
	// affected rows.
	RowsAffected
	// Rows indicates that the statement returns the affected rows after
	// the statement was applied.
	Rows
)

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	// StatementType's returned string is a tag identifying a SQL command (usually a single verb).
	// This is different than the Stringer output, which is the actual statement (including args).
	StatementType() (StatementType, string)
}

// StatementType implements the Statement interface.
func (*AlterTable) StatementType() (StatementType, string) { return DDL, "ALTER TABLE" }

// StatementType implements the Statement interface.
func (*BeginTransaction) StatementType() (StatementType, string) { return Ack, "BEGIN" }

// StatementType implements the Statement interface.
func (*CommitTransaction) StatementType() (StatementType, string) { return Ack, "COMMIT" }

// StatementType implements the Statement interface.
func (*CreateDatabase) StatementType() (StatementType, string) { return DDL, "CREATE DATABASE" }

// StatementType implements the Statement interface.
func (*CreateIndex) StatementType() (StatementType, string) { return DDL, "CREATE INDEX" }

// StatementType implements the Statement interface.
func (*CreateTable) StatementType() (StatementType, string) { return DDL, "CREATE TABLE" }

// StatementType implements the Statement interface.
func (*Delete) StatementType() (StatementType, string) { return RowsAffected, "DELETE" }

// StatementType implements the Statement interface.
func (*DropDatabase) StatementType() (StatementType, string) { return DDL, "DROP DATABASE" }

// StatementType implements the Statement interface.
func (*DropIndex) StatementType() (StatementType, string) { return DDL, "DROP INDEX" }

// StatementType implements the Statement interface.
func (*DropTable) StatementType() (StatementType, string) { return DDL, "DROP TABLE" }

// StatementType implements the Statement interface.
func (*Explain) StatementType() (StatementType, string) { return Rows, "EXPLAIN" }

// StatementType implements the Statement interface.
func (*Grant) StatementType() (StatementType, string) { return DDL, "GRANT" }

// StatementType implements the Statement interface.
func (*Insert) StatementType() (StatementType, string) { return RowsAffected, "INSERT" }

// StatementType implements the Statement interface.
func (*ParenSelect) StatementType() (StatementType, string) { return Rows, "SELECT" }

// StatementType implements the Statement interface.
func (*RenameColumn) StatementType() (StatementType, string) { return DDL, "RENAME COLUMN" }

// StatementType implements the Statement interface.
func (*RenameDatabase) StatementType() (StatementType, string) { return DDL, "RENAME DATABASE" }

// StatementType implements the Statement interface.
func (*RenameIndex) StatementType() (StatementType, string) { return DDL, "RENAME INDEX" }

// StatementType implements the Statement interface.
func (*RenameTable) StatementType() (StatementType, string) { return DDL, "RENAME TABLE" }

// StatementType implements the Statement interface.
func (*Revoke) StatementType() (StatementType, string) { return DDL, "REVOKE" }

// StatementType implements the Statement interface.
func (*RollbackTransaction) StatementType() (StatementType, string) { return Ack, "ROLLBACK" }

// StatementType implements the Statement interface.
func (*Select) StatementType() (StatementType, string) { return Rows, "SELECT" }

// StatementType implements the Statement interface.
func (*Set) StatementType() (StatementType, string) { return Ack, "SET" }

// StatementType implements the Statement interface.
func (*SetTransaction) StatementType() (StatementType, string) { return Ack, "SET TRANSACTION" }

// StatementType implements the Statement interface.
func (*SetTimeZone) StatementType() (StatementType, string) { return Ack, "SET TIMEZONE" }

// StatementType implements the Statement interface.
func (*Show) StatementType() (StatementType, string) { return Rows, "SHOW" }

// StatementType implements the Statement interface.
func (*ShowColumns) StatementType() (StatementType, string) { return Rows, "SHOW COLUMNS" }

// StatementType implements the Statement interface.
func (*ShowDatabases) StatementType() (StatementType, string) { return Rows, "SHOW DATABASES" }

// StatementType implements the Statement interface.
func (*ShowGrants) StatementType() (StatementType, string) { return Rows, "SHOW GRANTS" }

// StatementType implements the Statement interface.
func (*ShowIndex) StatementType() (StatementType, string) { return Rows, "SHOW INDEX" }

// StatementType implements the Statement interface.
func (*ShowTables) StatementType() (StatementType, string) { return Rows, "SHOW TABLES" }

// StatementType implements the Statement interface.
func (*Truncate) StatementType() (StatementType, string) { return Ack, "TRUNCATE" }

// StatementType implements the Statement interface.
func (*Update) StatementType() (StatementType, string) { return RowsAffected, "UPDATE" }

// StatementType implements the Statement interface.
func (*Union) StatementType() (StatementType, string) { return Rows, "UNION" }

// StatementType implements the Statement interface.
func (Values) StatementType() (StatementType, string) { return Rows, "VALUES" }
