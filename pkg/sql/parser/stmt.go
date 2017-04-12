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
	// CopyIn indicates a COPY FROM statement.
	CopyIn
	// Unknown indicates that the statement does not have a known
	// return style at the time of parsing. This is not first in the
	// enumeration because it is more convenient to have Ack as a zero
	// value, and because the use of Unknown should be an explicit choice.
	// The primary example of this statement type is EXECUTE, where the
	// statement type depends on the statement type of the prepared statement
	// being executed.
	Unknown
)

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	NodeFormatter
	StatementType() StatementType
	// StatementTag is a short string identifying the type of statement
	// (usually a single verb). This is different than the Stringer output,
	// which is the actual statement (including args).
	// TODO(dt): Currently tags are always pg-compatible in the future it
	// might make sense to pass a tag format specifier.
	StatementTag() string
}

// HiddenFromStats is a pseudo-interface to be implemented
// by statements that should not show up in per-app statistics.
type HiddenFromStats interface {
	hiddenFromStats()
}

// IndependentFromParallelizedPriors is a pseudo-interface to be implemented
// by statements which do not force parallel statement execution synchronization
// when they run.
type IndependentFromParallelizedPriors interface {
	independentFromParallelizedPriors()
}

// StatementType implements the Statement interface.
func (*AlterTable) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTable) StatementTag() string { return "ALTER TABLE" }

// StatementType implements the Statement interface.
func (*Backup) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Backup) StatementTag() string { return "BACKUP" }

// StatementType implements the Statement interface.
func (*BeginTransaction) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*BeginTransaction) StatementTag() string { return "BEGIN" }

func (*BeginTransaction) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*CommitTransaction) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*CommitTransaction) StatementTag() string { return "COMMIT" }

func (*CommitTransaction) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*CopyFrom) StatementType() StatementType { return CopyIn }

// StatementTag returns a short string identifying the type of statement.
func (*CopyFrom) StatementTag() string { return "COPY" }

// StatementType implements the Statement interface.
func (*CreateDatabase) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateDatabase) StatementTag() string { return "CREATE DATABASE" }

// StatementType implements the Statement interface.
func (*CreateIndex) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateIndex) StatementTag() string { return "CREATE INDEX" }

// StatementType implements the Statement interface.
func (*CreateTable) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateTable) StatementTag() string {
	if n.As() {
		return "SELECT"
	}
	return "CREATE TABLE"
}

// StatementType implements the Statement interface.
func (*CreateUser) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*CreateUser) StatementTag() string { return "CREATE USER" }

// StatementType implements the Statement interface.
func (*CreateView) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateView) StatementTag() string { return "CREATE VIEW" }

// StatementType implements the Statement interface.
func (*Deallocate) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (n *Deallocate) StatementTag() string {
	// Postgres distinguishes the command tags for these two cases of Deallocate statements.
	if n.Name == "" {
		return "DEALLOCATE ALL"
	}
	return "DEALLOCATE"
}

func (*Deallocate) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (n *Delete) StatementType() StatementType { return n.Returning.statementType() }

// StatementTag returns a short string identifying the type of statement.
func (*Delete) StatementTag() string { return "DELETE" }

// StatementType implements the Statement interface.
func (*DropDatabase) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropDatabase) StatementTag() string { return "DROP DATABASE" }

// StatementType implements the Statement interface.
func (*DropIndex) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropIndex) StatementTag() string { return "DROP INDEX" }

// StatementType implements the Statement interface.
func (*DropTable) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropTable) StatementTag() string { return "DROP TABLE" }

// StatementType implements the Statement interface.
func (*DropView) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropView) StatementTag() string { return "DROP VIEW" }

// StatementType implements the Statement interface.
func (*Execute) StatementType() StatementType { return Unknown }

// StatementTag returns a short string identifying the type of statement.
func (*Execute) StatementTag() string { return "EXECUTE" }

// StatementType implements the Statement interface.
func (*Explain) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Explain) StatementTag() string { return "EXPLAIN" }

func (*Explain) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*Grant) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*Grant) StatementTag() string { return "GRANT" }

func (*Grant) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (n *Insert) StatementType() StatementType { return n.Returning.statementType() }

// StatementTag returns a short string identifying the type of statement.
func (*Insert) StatementTag() string { return "INSERT" }

// StatementType implements the Statement interface.
func (*ParenSelect) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ParenSelect) StatementTag() string { return "SELECT" }

// StatementType implements the Statement interface.
func (*Prepare) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*Prepare) StatementTag() string { return "PREPARE" }

func (*Prepare) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*ReleaseSavepoint) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*ReleaseSavepoint) StatementTag() string { return "RELEASE" }

func (*ReleaseSavepoint) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*RenameColumn) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameColumn) StatementTag() string { return "RENAME COLUMN" }

// StatementType implements the Statement interface.
func (*RenameDatabase) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameDatabase) StatementTag() string { return "RENAME DATABASE" }

// StatementType implements the Statement interface.
func (*RenameIndex) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameIndex) StatementTag() string { return "RENAME INDEX" }

// StatementType implements the Statement interface.
func (*RenameTable) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (n *RenameTable) StatementTag() string {
	if n.IsView {
		return "RENAME VIEW"
	}
	return "RENAME TABLE"
}

// StatementType implements the Statement interface.
func (*Relocate) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Relocate) StatementTag() string { return "TESTING_RELOCATE" }

// StatementType implements the Statement interface.
func (*Restore) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Restore) StatementTag() string { return "RESTORE" }

// StatementType implements the Statement interface.
func (*Revoke) StatementType() StatementType { return DDL }

// StatementTag returns a short string identifying the type of statement.
func (*Revoke) StatementTag() string { return "REVOKE" }

func (*Revoke) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*RollbackToSavepoint) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackToSavepoint) StatementTag() string { return "ROLLBACK" }

func (*RollbackToSavepoint) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*RollbackTransaction) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackTransaction) StatementTag() string { return "ROLLBACK" }

func (*RollbackTransaction) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*Savepoint) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*Savepoint) StatementTag() string { return "SAVEPOINT" }

// StatementType implements the Statement interface.
func (*Scatter) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Scatter) StatementTag() string { return "SCATTER" }

// StatementType implements the Statement interface.
func (*Select) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Select) StatementTag() string { return "SELECT" }

// StatementType implements the Statement interface.
func (*SelectClause) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*SelectClause) StatementTag() string { return "SELECT" }

// StatementType implements the Statement interface.
func (*Set) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*Set) StatementTag() string { return "SET" }

func (*Set) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*SetTransaction) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*SetTransaction) StatementTag() string { return "SET TRANSACTION" }

func (*SetTransaction) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*SetTimeZone) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*SetTimeZone) StatementTag() string { return "SET TIME ZONE" }

func (*SetTimeZone) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*SetDefaultIsolation) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*SetDefaultIsolation) StatementTag() string { return "SET" }

func (*SetDefaultIsolation) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*Show) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Show) StatementTag() string { return "SHOW" }

func (*Show) hiddenFromStats()                   {}
func (*Show) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowColumns) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowColumns) StatementTag() string { return "SHOW COLUMNS" }

func (*ShowColumns) hiddenFromStats()                   {}
func (*ShowColumns) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowCreateTable) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateTable) StatementTag() string { return "SHOW CREATE TABLE" }

func (*ShowCreateTable) hiddenFromStats()                   {}
func (*ShowCreateTable) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowCreateView) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateView) StatementTag() string { return "SHOW CREATE VIEW" }

func (*ShowCreateView) hiddenFromStats()                   {}
func (*ShowCreateView) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowDatabases) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDatabases) StatementTag() string { return "SHOW DATABASES" }

func (*ShowDatabases) hiddenFromStats()                   {}
func (*ShowDatabases) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowGrants) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowGrants) StatementTag() string { return "SHOW GRANTS" }

func (*ShowGrants) hiddenFromStats()                   {}
func (*ShowGrants) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowIndex) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowIndex) StatementTag() string { return "SHOW INDEX" }

func (*ShowIndex) hiddenFromStats()                   {}
func (*ShowIndex) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowTransactionStatus) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTransactionStatus) StatementTag() string { return "SHOW TRANSACTION STATUS" }

func (*ShowTransactionStatus) hiddenFromStats()                   {}
func (*ShowTransactionStatus) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowUsers) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowUsers) StatementTag() string { return "SHOW USERS" }

func (*ShowUsers) hiddenFromStats()                   {}
func (*ShowUsers) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowRanges) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRanges) StatementTag() string { return "SHOW TESTING_RANGES" }

func (*ShowRanges) hiddenFromStats() {}

// StatementType implements the Statement interface.
func (*Help) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Help) StatementTag() string { return "HELP" }

func (*Help) hiddenFromStats()                   {}
func (*Help) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowConstraints) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowConstraints) StatementTag() string { return "SHOW CONSTRAINTS" }

func (*ShowConstraints) hiddenFromStats()                   {}
func (*ShowConstraints) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*ShowTables) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTables) StatementTag() string { return "SHOW TABLES" }

func (*ShowTables) hiddenFromStats()                   {}
func (*ShowTables) independentFromParallelizedPriors() {}

// StatementType implements the Statement interface.
func (*Split) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*Split) StatementTag() string { return "SPLIT" }

// StatementType implements the Statement interface.
func (*Truncate) StatementType() StatementType { return Ack }

// StatementTag returns a short string identifying the type of statement.
func (*Truncate) StatementTag() string { return "TRUNCATE" }

// StatementType implements the Statement interface.
func (n *Update) StatementType() StatementType { return n.Returning.statementType() }

// StatementTag returns a short string identifying the type of statement.
func (*Update) StatementTag() string { return "UPDATE" }

// StatementType implements the Statement interface.
func (*UnionClause) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (*UnionClause) StatementTag() string { return "UNION" }

// StatementType implements the Statement interface.
func (ValuesClause) StatementType() StatementType { return Rows }

// StatementTag returns a short string identifying the type of statement.
func (ValuesClause) StatementTag() string { return "VALUES" }

func (n *AlterTable) String() string               { return AsString(n) }
func (n AlterTableCmds) String() string            { return AsString(n) }
func (n *AlterTableAddColumn) String() string      { return AsString(n) }
func (n *AlterTableAddConstraint) String() string  { return AsString(n) }
func (n *AlterTableDropColumn) String() string     { return AsString(n) }
func (n *AlterTableDropConstraint) String() string { return AsString(n) }
func (n *AlterTableDropNotNull) String() string    { return AsString(n) }
func (n *AlterTableSetDefault) String() string     { return AsString(n) }
func (n *Backup) String() string                   { return AsString(n) }
func (n *BeginTransaction) String() string         { return AsString(n) }
func (n *CommitTransaction) String() string        { return AsString(n) }
func (n *CopyFrom) String() string                 { return AsString(n) }
func (n *CreateDatabase) String() string           { return AsString(n) }
func (n *CreateIndex) String() string              { return AsString(n) }
func (n *CreateTable) String() string              { return AsString(n) }
func (n *CreateUser) String() string               { return AsString(n) }
func (n *CreateView) String() string               { return AsString(n) }
func (n *Deallocate) String() string               { return AsString(n) }
func (n *Delete) String() string                   { return AsString(n) }
func (n *DropDatabase) String() string             { return AsString(n) }
func (n *DropIndex) String() string                { return AsString(n) }
func (n *DropTable) String() string                { return AsString(n) }
func (n *DropView) String() string                 { return AsString(n) }
func (n *Execute) String() string                  { return AsString(n) }
func (n *Explain) String() string                  { return AsString(n) }
func (n *Grant) String() string                    { return AsString(n) }
func (n *Help) String() string                     { return AsString(n) }
func (n *Insert) String() string                   { return AsString(n) }
func (n *ParenSelect) String() string              { return AsString(n) }
func (n *Prepare) String() string                  { return AsString(n) }
func (n *ReleaseSavepoint) String() string         { return AsString(n) }
func (n *Relocate) String() string                 { return AsString(n) }
func (n *RenameColumn) String() string             { return AsString(n) }
func (n *RenameDatabase) String() string           { return AsString(n) }
func (n *RenameIndex) String() string              { return AsString(n) }
func (n *RenameTable) String() string              { return AsString(n) }
func (n *Restore) String() string                  { return AsString(n) }
func (n *Revoke) String() string                   { return AsString(n) }
func (n *RollbackToSavepoint) String() string      { return AsString(n) }
func (n *RollbackTransaction) String() string      { return AsString(n) }
func (n *Savepoint) String() string                { return AsString(n) }
func (n *Scatter) String() string                  { return AsString(n) }
func (n *Select) String() string                   { return AsString(n) }
func (n *SelectClause) String() string             { return AsString(n) }
func (n *Set) String() string                      { return AsString(n) }
func (n *SetDefaultIsolation) String() string      { return AsString(n) }
func (n *SetTimeZone) String() string              { return AsString(n) }
func (n *SetTransaction) String() string           { return AsString(n) }
func (n *Show) String() string                     { return AsString(n) }
func (n *ShowColumns) String() string              { return AsString(n) }
func (n *ShowCreateTable) String() string          { return AsString(n) }
func (n *ShowCreateView) String() string           { return AsString(n) }
func (n *ShowDatabases) String() string            { return AsString(n) }
func (n *ShowGrants) String() string               { return AsString(n) }
func (n *ShowIndex) String() string                { return AsString(n) }
func (n *ShowConstraints) String() string          { return AsString(n) }
func (n *ShowTables) String() string               { return AsString(n) }
func (n *ShowTransactionStatus) String() string    { return AsString(n) }
func (n *ShowUsers) String() string                { return AsString(n) }
func (n *ShowRanges) String() string               { return AsString(n) }
func (n *Split) String() string                    { return AsString(n) }
func (l StatementList) String() string             { return AsString(l) }
func (n *Truncate) String() string                 { return AsString(n) }
func (n *UnionClause) String() string              { return AsString(n) }
func (n *Update) String() string                   { return AsString(n) }
func (n *ValuesClause) String() string             { return AsString(n) }
