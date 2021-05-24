// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package tree

import "fmt"

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// StatementReturnType is the enumerated type for Statement return styles on
// the wire.
type StatementReturnType int

// StatementType is the enumerated type for Statement type return
type StatementType int

//go:generate stringer -type=StatementReturnType
const (
	// Ack indicates that the statement does not have a meaningful
	// return. Examples include SET, BEGIN, COMMIT.
	Ack StatementReturnType = iota
	// DDL indicates that the statement mutates the database schema.
	//
	// Note: this is the type indicated back to the client; it is not a
	// sufficient test for schema mutation for planning purposes. There
	// are schema-modifying statements (e.g. CREATE TABLE AS) which
	// report RowsAffected to the client, not DDL.
	// Use CanModifySchema() below instead.
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

//go:generate stringer -type=StatementType
const (
	// DDL (Data Definition Language) deals with database schemas and descriptions.
	TypeDDL StatementType = iota
	// DML (Data Manipulation Language) deals with data manipulation and it is used to
	// store, modify, retrieve, delete and update data in a database.
	TypeDML
	// DCL (Data Control Language) deals with commands such as GRANT and mostly
	// concerned with rights, permissions and other controls of the database system.
	TypeDCL
	// TCL (Transaction Control Language) deals with a transaction within a database.
	TypeTCL
)

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	NodeFormatter

	// StatementReturnType is the return styles on the wire
	// (Ack, DDL, RowsAffected, Rows, CopyIn or Unknown)
	StatementReturnType() StatementReturnType
	// StatementType identifies whether the statement is a DDL, DML, DCL, or TCL.
	StatementType() StatementType
	// StatementTag is a short string identifying the type of statement
	// (usually a single verb). This is different than the Stringer output,
	// which is the actual statement (including args).
	// TODO(dt): Currently tags are always pg-compatible in the future it
	// might make sense to pass a tag format specifier.
	StatementTag() string
}

// canModifySchema is to be implemented by statements that can modify
// the database schema but may have StatementReturnType() != DDL.
// See CanModifySchema() below.
type canModifySchema interface {
	modifiesSchema() bool
}

// CanModifySchema returns true if the statement can modify
// the database schema.
func CanModifySchema(stmt Statement) bool {
	if stmt.StatementReturnType() == DDL {
		return true
	}
	scm, ok := stmt.(canModifySchema)
	return ok && scm.modifiesSchema()
}

// CanWriteData returns true if the statement can modify data.
func CanWriteData(stmt Statement) bool {
	switch stmt.(type) {
	// Normal write operations.
	case *Insert, *Delete, *Update, *Truncate:
		return true
	// Import operations.
	case *CopyFrom, *Import, *Restore:
		return true
	// CockroachDB extensions.
	case *Split, *Unsplit, *Relocate, *Scatter:
		return true
	}
	return false
}

// HiddenFromShowQueries is a pseudo-interface to be implemented
// by statements that should not show up in SHOW QUERIES (and are hence
// not cancellable using CANCEL QUERIES either). Usually implemented by
// statements that spawn jobs.
type HiddenFromShowQueries interface {
	hiddenFromShowQueries()
}

// ObserverStatement is a marker interface for statements which are allowed to
// run regardless of the current transaction state: statements other than
// rollback are generally rejected if the session is in a failed transaction
// state, but it's convenient to allow some statements (e.g. "show syntax; set
// tracing").
// Such statements are not expected to modify the database, the transaction or
// session state (other than special cases such as enabling/disabling tracing).
//
// These statements short-circuit the regular execution - they don't get planned
// (there are no corresponding planNodes). The connExecutor recognizes them and
// handles them.
type ObserverStatement interface {
	observerStatement()
}

// CCLOnlyStatement is a marker interface for statements that require
// a CCL binary for successful planning or execution.
// It is used to enhance error messages when attempting to use these
// statements in non-CCL binaries.
type CCLOnlyStatement interface {
	cclOnlyStatement()
}

var _ CCLOnlyStatement = &Backup{}
var _ CCLOnlyStatement = &ShowBackup{}
var _ CCLOnlyStatement = &Restore{}
var _ CCLOnlyStatement = &CreateChangefeed{}
var _ CCLOnlyStatement = &Import{}
var _ CCLOnlyStatement = &Export{}
var _ CCLOnlyStatement = &ScheduledBackup{}
var _ CCLOnlyStatement = &StreamIngestion{}
var _ CCLOnlyStatement = &ReplicationStream{}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseOwner) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseOwner) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseOwner) StatementTag() string { return "ALTER DATABASE OWNER" }

func (*AlterDatabaseOwner) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseAddRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseAddRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseAddRegion) StatementTag() string { return "ALTER DATABASE ADD REGION" }

func (*AlterDatabaseAddRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseDropRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseDropRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseDropRegion) StatementTag() string { return "ALTER DATABASE DROP REGION" }

func (*AlterDatabaseDropRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabasePrimaryRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabasePrimaryRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabasePrimaryRegion) StatementTag() string { return "ALTER DATABASE PRIMARY REGION" }

func (*AlterDatabasePrimaryRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseSurvivalGoal) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseSurvivalGoal) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseSurvivalGoal) StatementTag() string { return "ALTER DATABASE SURVIVE" }

func (*AlterDatabaseSurvivalGoal) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterIndex) StatementTag() string { return "ALTER INDEX" }

func (*AlterIndex) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTable) StatementTag() string { return "ALTER TABLE" }

func (*AlterTable) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTableLocality) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTableLocality) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTableLocality) StatementTag() string { return "ALTER TABLE SET LOCALITY" }

func (*AlterTableLocality) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTableOwner) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTableOwner) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTableOwner) StatementTag() string { return "ALTER TABLE OWNER" }

func (*AlterTableOwner) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTableSetSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTableSetSchema) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTableSetSchema) StatementTag() string { return "ALTER TABLE SET SCHEMA" }

func (*AlterTableSetSchema) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterSchema) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*AlterSchema) StatementTag() string { return "ALTER SCHEMA" }

func (*AlterSchema) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterType) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterType) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*AlterType) StatementTag() string { return "ALTER TYPE" }

func (*AlterType) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterSequence) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterSequence) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterSequence) StatementTag() string { return "ALTER SEQUENCE" }

// StatementReturnType implements the Statement interface.
func (*AlterRole) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*AlterRole) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterRole) StatementTag() string { return "ALTER ROLE" }

func (*AlterRole) cclOnlyStatement() {}

func (*AlterRole) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*Analyze) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*Analyze) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*Analyze) StatementTag() string { return "ANALYZE" }

// StatementReturnType implements the Statement interface.
func (*Backup) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Backup) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Backup) StatementTag() string { return "BACKUP" }

func (*Backup) cclOnlyStatement() {}

func (*Backup) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*ScheduledBackup) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ScheduledBackup) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ScheduledBackup) StatementTag() string { return "SCHEDULED BACKUP" }

func (*ScheduledBackup) cclOnlyStatement() {}

func (*ScheduledBackup) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*BeginTransaction) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*BeginTransaction) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*BeginTransaction) StatementTag() string { return "BEGIN" }

// StatementReturnType implements the Statement interface.
func (*ControlJobs) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*ControlJobs) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (n *ControlJobs) StatementTag() string {
	return fmt.Sprintf("%s JOBS", JobCommandToStatement[n.Command])
}

// StatementReturnType implements the Statement interface.
func (*ControlSchedules) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*ControlSchedules) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (n *ControlSchedules) StatementTag() string {
	return fmt.Sprintf("%s SCHEDULES", n.Command)
}

// StatementReturnType implements the Statement interface.
func (*ControlJobsForSchedules) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*ControlJobsForSchedules) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (n *ControlJobsForSchedules) StatementTag() string {
	return fmt.Sprintf("%s JOBS FOR SCHEDULES", JobCommandToStatement[n.Command])
}

// StatementReturnType implements the Statement interface.
func (*CancelQueries) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*CancelQueries) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CancelQueries) StatementTag() string { return "CANCEL QUERIES" }

// StatementReturnType implements the Statement interface.
func (*CancelSessions) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*CancelSessions) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CancelSessions) StatementTag() string { return "CANCEL SESSIONS" }

// StatementReturnType implements the Statement interface.
func (*CannedOptPlan) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*CannedOptPlan) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CannedOptPlan) StatementTag() string { return "PREPARE AS OPT PLAN" }

// StatementReturnType implements the Statement interface.
func (*CommentOnColumn) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnColumn) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnColumn) StatementTag() string { return "COMMENT ON COLUMN" }

// StatementReturnType implements the Statement interface.
func (*CommentOnDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnDatabase) StatementTag() string { return "COMMENT ON DATABASE" }

// StatementReturnType implements the Statement interface.
func (*CommentOnIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnIndex) StatementTag() string { return "COMMENT ON INDEX" }

// StatementReturnType implements the Statement interface.
func (*CommentOnTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnTable) StatementTag() string { return "COMMENT ON TABLE" }

// StatementReturnType implements the Statement interface.
func (*CommitTransaction) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*CommitTransaction) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*CommitTransaction) StatementTag() string { return "COMMIT" }

// StatementReturnType implements the Statement interface.
func (*CopyFrom) StatementReturnType() StatementReturnType { return CopyIn }

// StatementType implements the Statement interface.
func (*CopyFrom) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CopyFrom) StatementTag() string { return "COPY" }

// StatementReturnType implements the Statement interface.
func (*CreateChangefeed) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*CreateChangefeed) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateChangefeed) StatementTag() string {
	if n.SinkURI == nil {
		return "EXPERIMENTAL CHANGEFEED"
	}
	return "CREATE CHANGEFEED"
}

func (*CreateChangefeed) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*CreateDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateDatabase) StatementTag() string { return "CREATE DATABASE" }

// StatementReturnType implements the Statement interface.
func (*CreateExtension) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*CreateExtension) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateExtension) StatementTag() string { return "CREATE EXTENSION" }

// StatementReturnType implements the Statement interface.
func (*CreateIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateIndex) StatementTag() string { return "CREATE INDEX" }

// StatementReturnType implements the Statement interface.
func (n *CreateSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateSchema) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateSchema) StatementTag() string {
	return "CREATE SCHEMA"
}

// modifiesSchema implements the canModifySchema interface.
func (*CreateSchema) modifiesSchema() bool { return true }

// StatementReturnType implements the Statement interface.
func (n *CreateTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateTable) StatementTag() string {
	if n.As() {
		return "CREATE TABLE AS"
	}
	return "CREATE TABLE"
}

// modifiesSchema implements the canModifySchema interface.
func (*CreateTable) modifiesSchema() bool { return true }

// StatementReturnType implements the Statement interface.
func (*CreateType) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateType) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*CreateType) StatementTag() string { return "CREATE TYPE" }

func (*CreateType) modifiesSchema() bool { return true }

// StatementReturnType implements the Statement interface.
func (*CreateRole) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*CreateRole) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateRole) StatementTag() string { return "CREATE ROLE" }

func (*CreateRole) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*CreateView) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateView) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateView) StatementTag() string { return "CREATE VIEW" }

// StatementReturnType implements the Statement interface.
func (*CreateSequence) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateSequence) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateSequence) StatementTag() string { return "CREATE SEQUENCE" }

// StatementReturnType implements the Statement interface.
func (*CreateStats) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateStats) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateStats) StatementTag() string { return "CREATE STATISTICS" }

// StatementReturnType implements the Statement interface.
func (*Deallocate) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*Deallocate) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (n *Deallocate) StatementTag() string {
	// Postgres distinguishes the command tags for these two cases of Deallocate statements.
	if n.Name == "" {
		return "DEALLOCATE ALL"
	}
	return "DEALLOCATE"
}

// StatementReturnType implements the Statement interface.
func (*Discard) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*Discard) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*Discard) StatementTag() string { return "DISCARD" }

// StatementReturnType implements the Statement interface.
func (n *Delete) StatementReturnType() StatementReturnType { return n.Returning.statementReturnType() }

// StatementType implements the Statement interface.
func (*Delete) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Delete) StatementTag() string { return "DELETE" }

// StatementReturnType implements the Statement interface.
func (*DropDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropDatabase) StatementTag() string { return "DROP DATABASE" }

// StatementReturnType implements the Statement interface.
func (*DropIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropIndex) StatementTag() string { return "DROP INDEX" }

// StatementReturnType implements the Statement interface.
func (*DropTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropTable) StatementTag() string { return "DROP TABLE" }

// StatementReturnType implements the Statement interface.
func (*DropView) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropView) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropView) StatementTag() string { return "DROP VIEW" }

// StatementReturnType implements the Statement interface.
func (*DropSequence) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropSequence) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropSequence) StatementTag() string { return "DROP SEQUENCE" }

// StatementReturnType implements the Statement interface.
func (*DropRole) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*DropRole) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropRole) StatementTag() string { return "DROP ROLE" }

func (*DropRole) cclOnlyStatement() {}

func (*DropRole) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*DropType) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropType) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropType) StatementTag() string { return "DROP TYPE" }

// StatementReturnType implements the Statement interface.
func (*DropSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropSchema) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*DropSchema) StatementTag() string { return "DROP SCHEMA" }

// StatementReturnType implements the Statement interface.
func (*Execute) StatementReturnType() StatementReturnType { return Unknown }

// StatementType implements the Statement interface.
func (*Execute) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*Execute) StatementTag() string { return "EXECUTE" }

// StatementReturnType implements the Statement interface.
func (*Explain) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Explain) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Explain) StatementTag() string { return "EXPLAIN" }

// StatementReturnType implements the Statement interface.
func (*ExplainAnalyze) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ExplainAnalyze) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ExplainAnalyze) StatementTag() string { return "EXPLAIN ANALYZE" }

// StatementReturnType implements the Statement interface.
func (*Export) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Export) StatementType() StatementType { return TypeDML }

func (*Export) cclOnlyStatement() {}

// StatementTag returns a short string identifying the type of statement.
func (*Export) StatementTag() string { return "EXPORT" }

// StatementReturnType implements the Statement interface.
func (*Grant) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*Grant) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*Grant) StatementTag() string { return "GRANT" }

// StatementReturnType implements the Statement interface.
func (*GrantRole) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*GrantRole) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*GrantRole) StatementTag() string { return "GRANT" }

// StatementReturnType implements the Statement interface.
func (n *Insert) StatementReturnType() StatementReturnType { return n.Returning.statementReturnType() }

// StatementType implements the Statement interface.
func (*Insert) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Insert) StatementTag() string { return "INSERT" }

// StatementReturnType implements the Statement interface.
func (*Import) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Import) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Import) StatementTag() string { return "IMPORT" }

func (*Import) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*ParenSelect) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ParenSelect) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ParenSelect) StatementTag() string { return "SELECT" }

// StatementReturnType implements the Statement interface.
func (*Prepare) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*Prepare) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*Prepare) StatementTag() string { return "PREPARE" }

// StatementReturnType implements the Statement interface.
func (*ReassignOwnedBy) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*ReassignOwnedBy) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*ReassignOwnedBy) StatementTag() string { return "REASSIGN OWNED BY" }

// StatementReturnType implements the Statement interface.
func (*DropOwnedBy) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropOwnedBy) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*DropOwnedBy) StatementTag() string { return "DROP OWNED BY" }

// StatementReturnType implements the Statement interface.
func (*RefreshMaterializedView) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RefreshMaterializedView) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*RefreshMaterializedView) StatementTag() string { return "REFRESH MATERIALIZED VIEW" }

// StatementReturnType implements the Statement interface.
func (*ReleaseSavepoint) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*ReleaseSavepoint) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*ReleaseSavepoint) StatementTag() string { return "RELEASE" }

// StatementReturnType implements the Statement interface.
func (*RenameColumn) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameColumn) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameColumn) StatementTag() string { return "RENAME COLUMN" }

// StatementReturnType implements the Statement interface.
func (*RenameDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameDatabase) StatementTag() string { return "RENAME DATABASE" }

// StatementReturnType implements the Statement interface.
func (*ReparentDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*ReparentDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*ReparentDatabase) StatementTag() string { return "CONVERT TO SCHEMA" }

// StatementReturnType implements the Statement interface.
func (*RenameIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameIndex) StatementTag() string { return "RENAME INDEX" }

// StatementReturnType implements the Statement interface.
func (*RenameTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *RenameTable) StatementTag() string {
	if n.IsView {
		return "RENAME VIEW"
	} else if n.IsSequence {
		return "RENAME SEQUENCE"
	}
	return "RENAME TABLE"
}

// StatementReturnType implements the Statement interface.
func (*Relocate) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Relocate) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *Relocate) StatementTag() string {
	if n.RelocateLease {
		return "EXPERIMENTAL_RELOCATE LEASE"
	} else if n.RelocateNonVoters {
		return "EXPERIMENTAL_RELOCATE NON_VOTERS"
	}
	return "EXPERIMENTAL_RELOCATE VOTERS"
}

// StatementReturnType implements the Statement interface.
func (*ReplicationStream) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ReplicationStream) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ReplicationStream) StatementTag() string { return "CREATE REPLICATION STREAM" }

func (*ReplicationStream) cclOnlyStatement() {}

func (*ReplicationStream) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*Restore) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Restore) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Restore) StatementTag() string { return "RESTORE" }

func (*Restore) cclOnlyStatement() {}

func (*Restore) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*Revoke) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*Revoke) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*Revoke) StatementTag() string { return "REVOKE" }

// StatementReturnType implements the Statement interface.
func (*RevokeRole) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RevokeRole) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*RevokeRole) StatementTag() string { return "REVOKE" }

// StatementReturnType implements the Statement interface.
func (*RollbackToSavepoint) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*RollbackToSavepoint) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackToSavepoint) StatementTag() string { return "ROLLBACK" }

// StatementReturnType implements the Statement interface.
func (*RollbackTransaction) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*RollbackTransaction) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*RollbackTransaction) StatementTag() string { return "ROLLBACK" }

// StatementReturnType implements the Statement interface.
func (*Savepoint) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*Savepoint) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*Savepoint) StatementTag() string { return "SAVEPOINT" }

// StatementReturnType implements the Statement interface.
func (*Scatter) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Scatter) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Scatter) StatementTag() string { return "SCATTER" }

// StatementReturnType implements the Statement interface.
func (*Scrub) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Scrub) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *Scrub) StatementTag() string { return "SCRUB" }

// StatementReturnType implements the Statement interface.
func (*Select) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Select) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Select) StatementTag() string { return "SELECT" }

// StatementReturnType implements the Statement interface.
func (*SelectClause) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*SelectClause) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*SelectClause) StatementTag() string { return "SELECT" }

// StatementReturnType implements the Statement interface.
func (*SetVar) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetVar) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetVar) StatementTag() string { return "SET" }

// StatementReturnType implements the Statement interface.
func (*SetClusterSetting) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetClusterSetting) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetClusterSetting) StatementTag() string { return "SET CLUSTER SETTING" }

// StatementReturnType implements the Statement interface.
func (*SetTransaction) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetTransaction) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetTransaction) StatementTag() string { return "SET TRANSACTION" }

// StatementReturnType implements the Statement interface.
func (*SetTracing) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetTracing) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetTracing) StatementTag() string { return "SET TRACING" }

// observerStatement implements the ObserverStatement interface.
func (*SetTracing) observerStatement() {}

// StatementReturnType implements the Statement interface.
func (*SetZoneConfig) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*SetZoneConfig) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetZoneConfig) StatementTag() string { return "CONFIGURE ZONE" }

// StatementReturnType implements the Statement interface.
func (*SetSessionAuthorizationDefault) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetSessionAuthorizationDefault) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetSessionAuthorizationDefault) StatementTag() string { return "SET" }

// StatementReturnType implements the Statement interface.
func (*SetSessionCharacteristics) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetSessionCharacteristics) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetSessionCharacteristics) StatementTag() string { return "SET" }

// StatementReturnType implements the Statement interface.
func (*ShowVar) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowVar) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowVar) StatementTag() string { return "SHOW" }

// StatementReturnType implements the Statement interface.
func (*ShowClusterSetting) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowClusterSetting) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowClusterSetting) StatementTag() string { return "SHOW" }

// StatementReturnType implements the Statement interface.
func (*ShowClusterSettingList) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowClusterSettingList) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowClusterSettingList) StatementTag() string { return "SHOW" }

// StatementReturnType implements the Statement interface.
func (*ShowColumns) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowColumns) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowColumns) StatementTag() string { return "SHOW COLUMNS" }

// StatementReturnType implements the Statement interface.
func (*ShowCreate) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreate) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreate) StatementTag() string { return "SHOW CREATE" }

// StatementReturnType implements the Statement interface.
func (*ShowCreateAllTables) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateAllTables) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateAllTables) StatementTag() string { return "SHOW CREATE ALL TABLES" }

// StatementReturnType implements the Statement interface.
func (*ShowBackup) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowBackup) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowBackup) StatementTag() string { return "SHOW BACKUP" }

func (*ShowBackup) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*ShowDatabases) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowDatabases) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDatabases) StatementTag() string { return "SHOW DATABASES" }

// StatementReturnType implements the Statement interface.
func (*ShowEnums) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowEnums) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowEnums) StatementTag() string { return "SHOW ENUMS" }

// StatementReturnType implements the Statement interface.
func (*ShowTypes) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTypes) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTypes) StatementTag() string { return "SHOW TYPES" }

// StatementReturnType implements the Statement interface.
func (*ShowTraceForSession) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTraceForSession) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTraceForSession) StatementTag() string { return "SHOW TRACE FOR SESSION" }

// StatementReturnType implements the Statement interface.
func (*ShowGrants) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowGrants) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowGrants) StatementTag() string { return "SHOW GRANTS" }

// StatementReturnType implements the Statement interface.
func (*ShowDatabaseIndexes) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowDatabaseIndexes) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDatabaseIndexes) StatementTag() string { return "SHOW INDEXES FROM DATABASE" }

// StatementReturnType implements the Statement interface.
func (*ShowIndexes) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowIndexes) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowIndexes) StatementTag() string { return "SHOW INDEXES FROM TABLE" }

// StatementReturnType implements the Statement interface.
func (*ShowPartitions) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowPartitions) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of the statement.
func (*ShowPartitions) StatementTag() string { return "SHOW PARTITIONS" }

// StatementReturnType implements the Statement interface.
func (*ShowQueries) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowQueries) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowQueries) StatementTag() string { return "SHOW STATEMENTS" }

// StatementReturnType implements the Statement interface.
func (*ShowJobs) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowJobs) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowJobs) StatementTag() string { return "SHOW JOBS" }

// StatementReturnType implements the Statement interface.
func (*ShowChangefeedJobs) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowChangefeedJobs) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowChangefeedJobs) StatementTag() string { return "SHOW CHANGEFEED JOBS" }

// StatementReturnType implements the Statement interface.
func (*ShowRoleGrants) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowRoleGrants) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRoleGrants) StatementTag() string { return "SHOW GRANTS ON ROLE" }

// StatementReturnType implements the Statement interface.
func (*ShowSessions) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSessions) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSessions) StatementTag() string { return "SHOW SESSIONS" }

// StatementReturnType implements the Statement interface.
func (*ShowTableStats) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTableStats) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTableStats) StatementTag() string { return "SHOW STATISTICS" }

// StatementReturnType implements the Statement interface.
func (*ShowHistogram) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowHistogram) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowHistogram) StatementTag() string { return "SHOW HISTOGRAM" }

// StatementReturnType implements the Statement interface.
func (*ShowSchedules) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSchedules) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSchedules) StatementTag() string { return "SHOW SCHEDULES" }

// StatementReturnType implements the Statement interface.
func (*ShowSyntax) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSyntax) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSyntax) StatementTag() string { return "SHOW SYNTAX" }

func (*ShowSyntax) observerStatement() {}

// StatementReturnType implements the Statement interface.
func (*ShowTransactionStatus) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTransactionStatus) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTransactionStatus) StatementTag() string { return "SHOW TRANSACTION STATUS" }

func (*ShowTransactionStatus) observerStatement() {}

// StatementReturnType implements the Statement interface.
func (*ShowSavepointStatus) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSavepointStatus) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSavepointStatus) StatementTag() string { return "SHOW SAVEPOINT STATUS" }

func (*ShowSavepointStatus) observerStatement() {}

// StatementReturnType implements the Statement interface.
func (*ShowLastQueryStatistics) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowLastQueryStatistics) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowLastQueryStatistics) StatementTag() string { return "SHOW LAST QUERY STATISTICS" }

func (*ShowLastQueryStatistics) observerStatement() {}

// StatementReturnType implements the Statement interface.
func (*ShowUsers) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowUsers) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowUsers) StatementTag() string { return "SHOW USERS" }

// StatementReturnType implements the Statement interface.
func (*ShowFullTableScans) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowFullTableScans) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowFullTableScans) StatementTag() string { return "SHOW FULL TABLE SCANS" }

// StatementReturnType implements the Statement interface.
func (*ShowRoles) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowRoles) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRoles) StatementTag() string { return "SHOW ROLES" }

// StatementReturnType implements the Statement interface.
func (*ShowZoneConfig) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowZoneConfig) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowZoneConfig) StatementTag() string { return "SHOW ZONE CONFIGURATION" }

// StatementReturnType implements the Statement interface.
func (*ShowRanges) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowRanges) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRanges) StatementTag() string { return "SHOW RANGES" }

// StatementReturnType implements the Statement interface.
func (*ShowRangeForRow) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowRangeForRow) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRangeForRow) StatementTag() string { return "SHOW RANGE FOR ROW" }

// StatementReturnType implements the Statement interface.
func (*ShowSurvivalGoal) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSurvivalGoal) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSurvivalGoal) StatementTag() string { return "SHOW SURVIVAL GOAL" }

// StatementReturnType implements the Statement interface.
func (*ShowRegions) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowRegions) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowRegions) StatementTag() string { return "SHOW REGIONS" }

// StatementReturnType implements the Statement interface.
func (*ShowFingerprints) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowFingerprints) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowFingerprints) StatementTag() string { return "SHOW EXPERIMENTAL_FINGERPRINTS" }

// StatementReturnType implements the Statement interface.
func (*ShowConstraints) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowConstraints) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowConstraints) StatementTag() string { return "SHOW CONSTRAINTS" }

// StatementReturnType implements the Statement interface.
func (*ShowTables) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTables) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTables) StatementTag() string { return "SHOW TABLES" }

// StatementReturnType implements the Statement interface
func (*ShowTransactions) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTransactions) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTransactions) StatementTag() string { return "SHOW TRANSACTIONS" }

// StatementReturnType implements the Statement interface.
func (*ShowSchemas) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSchemas) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSchemas) StatementTag() string { return "SHOW SCHEMAS" }

// StatementReturnType implements the Statement interface.
func (*ShowSequences) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowSequences) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowSequences) StatementTag() string { return "SHOW SCHEMAS" }

// StatementReturnType implements the Statement interface.
func (*Split) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Split) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Split) StatementTag() string { return "SPLIT" }

// StatementReturnType implements the Statement interface.
func (*StreamIngestion) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*StreamIngestion) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*StreamIngestion) StatementTag() string { return "RESTORE FROM REPLICATION STREAM" }

func (*StreamIngestion) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*Unsplit) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Unsplit) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Unsplit) StatementTag() string { return "UNSPLIT" }

// StatementReturnType implements the Statement interface.
func (*Truncate) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*Truncate) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*Truncate) StatementTag() string { return "TRUNCATE" }

// modifiesSchema implements the canModifySchema interface.
func (*Truncate) modifiesSchema() bool { return true }

// StatementReturnType implements the Statement interface.
func (n *Update) StatementReturnType() StatementReturnType { return n.Returning.statementReturnType() }

// StatementType implements the Statement interface.
func (*Update) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Update) StatementTag() string { return "UPDATE" }

// StatementReturnType implements the Statement interface.
func (*UnionClause) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*UnionClause) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*UnionClause) StatementTag() string { return "UNION" }

// StatementReturnType implements the Statement interface.
func (*ValuesClause) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ValuesClause) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ValuesClause) StatementTag() string { return "VALUES" }

func (n *AlterIndex) String() string                     { return AsString(n) }
func (n *AlterDatabaseOwner) String() string             { return AsString(n) }
func (n *AlterDatabaseAddRegion) String() string         { return AsString(n) }
func (n *AlterDatabaseDropRegion) String() string        { return AsString(n) }
func (n *AlterDatabaseSurvivalGoal) String() string      { return AsString(n) }
func (n *AlterDatabasePrimaryRegion) String() string     { return AsString(n) }
func (n *AlterSchema) String() string                    { return AsString(n) }
func (n *AlterTable) String() string                     { return AsString(n) }
func (n *AlterTableCmds) String() string                 { return AsString(n) }
func (n *AlterTableAddColumn) String() string            { return AsString(n) }
func (n *AlterTableAddConstraint) String() string        { return AsString(n) }
func (n *AlterTableAlterColumnType) String() string      { return AsString(n) }
func (n *AlterTableDropColumn) String() string           { return AsString(n) }
func (n *AlterTableDropConstraint) String() string       { return AsString(n) }
func (n *AlterTableDropNotNull) String() string          { return AsString(n) }
func (n *AlterTableDropStored) String() string           { return AsString(n) }
func (n *AlterTableLocality) String() string             { return AsString(n) }
func (n *AlterTableSetDefault) String() string           { return AsString(n) }
func (n *AlterTableSetVisible) String() string           { return AsString(n) }
func (n *AlterTableSetNotNull) String() string           { return AsString(n) }
func (n *AlterTableOwner) String() string                { return AsString(n) }
func (n *AlterTableSetSchema) String() string            { return AsString(n) }
func (n *AlterType) String() string                      { return AsString(n) }
func (n *AlterRole) String() string                      { return AsString(n) }
func (n *AlterSequence) String() string                  { return AsString(n) }
func (n *Analyze) String() string                        { return AsString(n) }
func (n *Backup) String() string                         { return AsString(n) }
func (n *BeginTransaction) String() string               { return AsString(n) }
func (n *ControlJobs) String() string                    { return AsString(n) }
func (n *ControlSchedules) String() string               { return AsString(n) }
func (n *ControlJobsForSchedules) String() string        { return AsString(n) }
func (n *CancelQueries) String() string                  { return AsString(n) }
func (n *CancelSessions) String() string                 { return AsString(n) }
func (n *CannedOptPlan) String() string                  { return AsString(n) }
func (n *CommentOnColumn) String() string                { return AsString(n) }
func (n *CommentOnDatabase) String() string              { return AsString(n) }
func (n *CommentOnIndex) String() string                 { return AsString(n) }
func (n *CommentOnTable) String() string                 { return AsString(n) }
func (n *CommitTransaction) String() string              { return AsString(n) }
func (n *CopyFrom) String() string                       { return AsString(n) }
func (n *CreateChangefeed) String() string               { return AsString(n) }
func (n *CreateDatabase) String() string                 { return AsString(n) }
func (n *CreateExtension) String() string                { return AsString(n) }
func (n *CreateIndex) String() string                    { return AsString(n) }
func (n *CreateRole) String() string                     { return AsString(n) }
func (n *CreateTable) String() string                    { return AsString(n) }
func (n *CreateSchema) String() string                   { return AsString(n) }
func (n *CreateSequence) String() string                 { return AsString(n) }
func (n *CreateStats) String() string                    { return AsString(n) }
func (n *CreateView) String() string                     { return AsString(n) }
func (n *Deallocate) String() string                     { return AsString(n) }
func (n *Delete) String() string                         { return AsString(n) }
func (n *DropDatabase) String() string                   { return AsString(n) }
func (n *DropIndex) String() string                      { return AsString(n) }
func (n *DropOwnedBy) String() string                    { return AsString(n) }
func (n *DropSchema) String() string                     { return AsString(n) }
func (n *DropSequence) String() string                   { return AsString(n) }
func (n *DropTable) String() string                      { return AsString(n) }
func (n *DropType) String() string                       { return AsString(n) }
func (n *DropView) String() string                       { return AsString(n) }
func (n *DropRole) String() string                       { return AsString(n) }
func (n *Execute) String() string                        { return AsString(n) }
func (n *Explain) String() string                        { return AsString(n) }
func (n *ExplainAnalyze) String() string                 { return AsString(n) }
func (n *Export) String() string                         { return AsString(n) }
func (n *Grant) String() string                          { return AsString(n) }
func (n *GrantRole) String() string                      { return AsString(n) }
func (n *Insert) String() string                         { return AsString(n) }
func (n *Import) String() string                         { return AsString(n) }
func (n *ParenSelect) String() string                    { return AsString(n) }
func (n *Prepare) String() string                        { return AsString(n) }
func (n *ReassignOwnedBy) String() string                { return AsString(n) }
func (n *ReleaseSavepoint) String() string               { return AsString(n) }
func (n *Relocate) String() string                       { return AsString(n) }
func (n *RefreshMaterializedView) String() string        { return AsString(n) }
func (n *RenameColumn) String() string                   { return AsString(n) }
func (n *RenameDatabase) String() string                 { return AsString(n) }
func (n *ReparentDatabase) String() string               { return AsString(n) }
func (n *ReplicationStream) String() string              { return AsString(n) }
func (n *RenameIndex) String() string                    { return AsString(n) }
func (n *RenameTable) String() string                    { return AsString(n) }
func (n *Restore) String() string                        { return AsString(n) }
func (n *Revoke) String() string                         { return AsString(n) }
func (n *RevokeRole) String() string                     { return AsString(n) }
func (n *RollbackToSavepoint) String() string            { return AsString(n) }
func (n *RollbackTransaction) String() string            { return AsString(n) }
func (n *Savepoint) String() string                      { return AsString(n) }
func (n *Scatter) String() string                        { return AsString(n) }
func (n *ScheduledBackup) String() string                { return AsString(n) }
func (n *Scrub) String() string                          { return AsString(n) }
func (n *Select) String() string                         { return AsString(n) }
func (n *SelectClause) String() string                   { return AsString(n) }
func (n *SetClusterSetting) String() string              { return AsString(n) }
func (n *SetZoneConfig) String() string                  { return AsString(n) }
func (n *SetSessionAuthorizationDefault) String() string { return AsString(n) }
func (n *SetSessionCharacteristics) String() string      { return AsString(n) }
func (n *SetTransaction) String() string                 { return AsString(n) }
func (n *SetTracing) String() string                     { return AsString(n) }
func (n *SetVar) String() string                         { return AsString(n) }
func (n *ShowBackup) String() string                     { return AsString(n) }
func (n *ShowClusterSetting) String() string             { return AsString(n) }
func (n *ShowClusterSettingList) String() string         { return AsString(n) }
func (n *ShowColumns) String() string                    { return AsString(n) }
func (n *ShowConstraints) String() string                { return AsString(n) }
func (n *ShowCreate) String() string                     { return AsString(n) }
func (n *ShowCreateAllTables) String() string            { return AsString(n) }
func (n *ShowDatabases) String() string                  { return AsString(n) }
func (n *ShowDatabaseIndexes) String() string            { return AsString(n) }
func (n *ShowEnums) String() string                      { return AsString(n) }
func (n *ShowFullTableScans) String() string             { return AsString(n) }
func (n *ShowGrants) String() string                     { return AsString(n) }
func (n *ShowHistogram) String() string                  { return AsString(n) }
func (n *ShowSchedules) String() string                  { return AsString(n) }
func (n *ShowIndexes) String() string                    { return AsString(n) }
func (n *ShowJobs) String() string                       { return AsString(n) }
func (n *ShowChangefeedJobs) String() string             { return AsString(n) }
func (n *ShowLastQueryStatistics) String() string        { return AsString(n) }
func (n *ShowPartitions) String() string                 { return AsString(n) }
func (n *ShowQueries) String() string                    { return AsString(n) }
func (n *ShowRanges) String() string                     { return AsString(n) }
func (n *ShowRangeForRow) String() string                { return AsString(n) }
func (n *ShowRegions) String() string                    { return AsString(n) }
func (n *ShowRoleGrants) String() string                 { return AsString(n) }
func (n *ShowRoles) String() string                      { return AsString(n) }
func (n *ShowSavepointStatus) String() string            { return AsString(n) }
func (n *ShowSchemas) String() string                    { return AsString(n) }
func (n *ShowSequences) String() string                  { return AsString(n) }
func (n *ShowSessions) String() string                   { return AsString(n) }
func (n *ShowSurvivalGoal) String() string               { return AsString(n) }
func (n *ShowSyntax) String() string                     { return AsString(n) }
func (n *ShowTableStats) String() string                 { return AsString(n) }
func (n *ShowTables) String() string                     { return AsString(n) }
func (n *ShowTypes) String() string                      { return AsString(n) }
func (n *ShowTraceForSession) String() string            { return AsString(n) }
func (n *ShowTransactionStatus) String() string          { return AsString(n) }
func (n *ShowTransactions) String() string               { return AsString(n) }
func (n *ShowUsers) String() string                      { return AsString(n) }
func (n *ShowVar) String() string                        { return AsString(n) }
func (n *ShowZoneConfig) String() string                 { return AsString(n) }
func (n *ShowFingerprints) String() string               { return AsString(n) }
func (n *Split) String() string                          { return AsString(n) }
func (n *StreamIngestion) String() string                { return AsString(n) }
func (n *Unsplit) String() string                        { return AsString(n) }
func (n *Truncate) String() string                       { return AsString(n) }
func (n *UnionClause) String() string                    { return AsString(n) }
func (n *Update) String() string                         { return AsString(n) }
func (n *ValuesClause) String() string                   { return AsString(n) }
