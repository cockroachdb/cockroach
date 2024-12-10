// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"fmt"
	"strings"
)

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
	// are schema-modifying statements (e.g. DISCARD ALL) which
	// report Ack to the client, not DDL.
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
	// CopyOut indicates a COPY TO statement.
	CopyOut
	// Replication indicates a replication protocol statement.
	Replication
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

const (
	AlterTableTag          = "ALTER TABLE"
	AlterPolicyTag         = "ALTER POLICY"
	BackupTag              = "BACKUP"
	CreateIndexTag         = "CREATE INDEX"
	CreateFunctionTag      = "CREATE FUNCTION"
	CreateProcedureTag     = "CREATE PROCEDURE"
	CreateTriggerTag       = "CREATE TRIGGER"
	CreateSchemaTag        = "CREATE SCHEMA"
	CreateSequenceTag      = "CREATE SEQUENCE"
	CreateDatabaseTag      = "CREATE DATABASE"
	CreatePolicyTag        = "CREATE POLICY"
	CommentOnColumnTag     = "COMMENT ON COLUMN"
	CommentOnConstraintTag = "COMMENT ON CONSTRAINT"
	CommentOnDatabaseTag   = "COMMENT ON DATABASE"
	CommentOnIndexTag      = "COMMENT ON INDEX"
	CommentOnSchemaTag     = "COMMENT ON SCHEMA"
	CommentOnTableTag      = "COMMENT ON TABLE"
	CommentOnTypeTag       = "COMMENT ON TYPE"
	DropDatabaseTag        = "DROP DATABASE"
	DropFunctionTag        = "DROP FUNCTION"
	DropPolicyTag          = "DROP POLICY"
	DropProcedureTag       = "DROP PROCEDURE"
	DropTriggerTag         = "DROP TRIGGER"
	DropIndexTag           = "DROP INDEX"
	DropOwnedByTag         = "DROP OWNED BY"
	DropSchemaTag          = "DROP SCHEMA"
	DropSequenceTag        = "DROP SEQUENCE"
	DropTableTag           = "DROP TABLE"
	DropTypeTag            = "DROP TYPE"
	DropViewTag            = "DROP VIEW"
	ImportTag              = "IMPORT"
	RestoreTag             = "RESTORE"
	ConfigureZoneTag       = "CONFIGURE ZONE"
)

// Statements represent a list of statements.
type Statements []Statement

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
	if stmt == nil {
		// Some drivers send empty queries to test the connection.
		return false
	}
	if stmt.StatementReturnType() == DDL || stmt.StatementType() == TypeDDL {
		return true
	}
	scm, ok := stmt.(canModifySchema)
	return ok && scm.modifiesSchema()
}

// CanWriteData returns true if the statement can modify data.
func CanWriteData(stmt Statement) bool {
	if stmt.StatementType() == TypeDCL {
		// Commands like GRANT and REVOKE modify system tables.
		return true
	}
	switch stmt.(type) {
	// Normal write operations.
	case *Insert, *Delete, *Update, *Truncate:
		return true
	// Import operations.
	case *CopyFrom, *Import, *Restore:
		return true
	// Backup creates a job and allows you to write into userfiles.
	case *Backup:
		return true
	// CockroachDB extensions.
	case *Split, *Unsplit, *Relocate, *RelocateRange, *Scatter:
		return true
	// Replication operations.
	case *CreateTenantFromReplication, *AlterTenantReplication, *CreateLogicalReplicationStream:
		return true
	}
	return false
}

// ReturnsAtMostOneRow returns true if the statement returns either no rows or
// a single row.
func ReturnsAtMostOneRow(stmt Statement) bool {
	switch stmt.(type) {
	// Import operations.
	case *CopyFrom, *Import, *Restore:
		return true
	// Backup creates a job and allows you to write into userfiles.
	case *Backup:
		return true
	// CockroachDB extensions.
	case *Scatter:
		return true
	// Replication operations.
	case *CreateTenantFromReplication, *AlterTenantReplication:
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

var _ CCLOnlyStatement = &AlterBackup{}
var _ CCLOnlyStatement = &AlterBackupSchedule{}
var _ CCLOnlyStatement = &Backup{}
var _ CCLOnlyStatement = &ShowBackup{}
var _ CCLOnlyStatement = &Restore{}
var _ CCLOnlyStatement = &CreateChangefeed{}
var _ CCLOnlyStatement = &AlterChangefeed{}
var _ CCLOnlyStatement = &Import{}
var _ CCLOnlyStatement = &Export{}
var _ CCLOnlyStatement = &ScheduledBackup{}
var _ CCLOnlyStatement = &CreateTenantFromReplication{}
var _ CCLOnlyStatement = &CreateLogicalReplicationStream{}

// StatementReturnType implements the Statement interface.
func (*AlterChangefeed) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*AlterChangefeed) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*AlterChangefeed) StatementTag() string { return `ALTER CHANGEFEED` }

func (*AlterChangefeed) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*AlterBackup) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*AlterBackup) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*AlterBackup) StatementTag() string { return "ALTER BACKUP" }

func (*AlterBackup) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseOwner) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseOwner) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseOwner) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabaseOwner) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseAddRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseAddRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseAddRegion) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabaseAddRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseDropRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseDropRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseDropRegion) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabaseDropRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabasePrimaryRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabasePrimaryRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabasePrimaryRegion) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabasePrimaryRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseSurvivalGoal) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseSurvivalGoal) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseSurvivalGoal) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabaseSurvivalGoal) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabasePlacement) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabasePlacement) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabasePlacement) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabasePlacement) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseAddSuperRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseAddSuperRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseAddSuperRegion) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabaseAddSuperRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseDropSuperRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseDropSuperRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseDropSuperRegion) StatementTag() string { return "ALTER DATABASE" }

func (*AlterDatabaseDropSuperRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseAlterSuperRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseAlterSuperRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseAlterSuperRegion) StatementTag() string {
	return "ALTER DATABASE"
}

func (*AlterDatabaseAlterSuperRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseSecondaryRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseSecondaryRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseSecondaryRegion) StatementTag() string {
	return "ALTER DATABASE"
}

func (*AlterDatabaseSecondaryRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseDropSecondaryRegion) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseDropSecondaryRegion) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseDropSecondaryRegion) StatementTag() string {
	return "ALTER DATABASE"
}

func (*AlterDatabaseDropSecondaryRegion) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDatabaseSetZoneConfigExtension) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDatabaseSetZoneConfigExtension) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDatabaseSetZoneConfigExtension) StatementTag() string {
	return "ALTER DATABASE"
}

func (*AlterDatabaseSetZoneConfigExtension) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterDefaultPrivileges) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterDefaultPrivileges) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterDefaultPrivileges) StatementTag() string { return "ALTER DEFAULT PRIVILEGES" }

// StatementReturnType implements the Statement interface.
func (*AlterIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterIndex) StatementTag() string { return "ALTER INDEX" }

func (*AlterIndex) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterIndexVisible) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterIndexVisible) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterIndexVisible) StatementTag() string { return "ALTER INDEX" }

func (*AlterIndexVisible) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterPolicy) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterPolicy) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterPolicy) StatementTag() string { return AlterPolicyTag }

func (*AlterPolicy) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTable) StatementTag() string { return AlterTableTag }

func (*AlterTable) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTableLocality) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTableLocality) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTableLocality) StatementTag() string { return "ALTER TABLE" }

func (*AlterTableLocality) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTableOwner) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTableOwner) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTableOwner) StatementTag() string { return "ALTER TABLE" }

func (*AlterTableOwner) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTableSetSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterTableSetSchema) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *AlterTableSetSchema) StatementTag() string {
	if n.IsView {
		if n.IsMaterialized {
			return "ALTER MATERIALIZED VIEW"
		}
		return "ALTER VIEW"
	} else if n.IsSequence {
		return "ALTER SEQUENCE"
	}
	return "ALTER TABLE"
}

func (*AlterTableSetSchema) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterSchema) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*AlterSchema) StatementTag() string { return "ALTER SCHEMA" }

func (*AlterSchema) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterTenantCapability) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*AlterTenantCapability) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTenantCapability) StatementTag() string { return "ALTER VIRTUAL CLUSTER CAPABILITY" }

// StatementReturnType implements the Statement interface.
func (*AlterTenantSetClusterSetting) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*AlterTenantSetClusterSetting) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTenantSetClusterSetting) StatementTag() string {
	return "ALTER VIRTUAL CLUSTER SET CLUSTER SETTING"
}

// StatementReturnType implements the Statement interface.
func (*AlterTenantReplication) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*AlterTenantReplication) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTenantReplication) StatementTag() string { return "ALTER VIRTUAL CLUSTER REPLICATION" }

func (*AlterTenantReplication) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*AlterTenantRename) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*AlterTenantRename) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTenantRename) StatementTag() string { return "ALTER VIRTUAL CLUSTER RENAME" }

// StatementReturnType implements the Statement interface.
func (*AlterTenantReset) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*AlterTenantReset) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTenantReset) StatementTag() string { return "ALTER VIRTUAL CLUSTER RESET" }

func (*AlterTenantReset) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*AlterTenantService) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*AlterTenantService) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterTenantService) StatementTag() string { return "ALTER VIRTUAL CLUSTER SERVICE" }

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
func (*AlterRole) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterRole) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterRole) StatementTag() string { return "ALTER ROLE" }

func (*AlterRole) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*AlterRoleSet) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterRoleSet) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterRoleSet) StatementTag() string { return "ALTER ROLE" }

func (*AlterRoleSet) hiddenFromShowQueries() {}

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
func (*Backup) StatementTag() string { return BackupTag }

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
func (*AlterBackupSchedule) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*AlterBackupSchedule) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*AlterBackupSchedule) StatementTag() string { return "SCHEDULED BACKUP" }

func (*AlterBackupSchedule) cclOnlyStatement() {}

func (*AlterBackupSchedule) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*BeginTransaction) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*BeginTransaction) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*BeginTransaction) StatementTag() string { return "BEGIN" }

// StatementReturnType implements the Statement interface.
func (*Call) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Call) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*Call) StatementTag() string { return "CALL" }

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
func (*ControlJobsOfType) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*ControlJobsOfType) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (n *ControlJobsOfType) StatementTag() string {
	return fmt.Sprintf("%s ALL %s JOBS", JobCommandToStatement[n.Command], strings.ToUpper(n.Type))
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
func (*CloseCursor) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*CloseCursor) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*CloseCursor) StatementTag() string { return "CLOSE" }

// StatementReturnType implements the Statement interface.
func (*CommentOnColumn) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnColumn) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnColumn) StatementTag() string { return CommentOnColumnTag }

// StatementReturnType implements the Statement interface.
func (*CommentOnConstraint) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnConstraint) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnConstraint) StatementTag() string { return CommentOnConstraintTag }

// StatementReturnType implements the Statement interface.
func (*CommentOnDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnDatabase) StatementTag() string { return CommentOnDatabaseTag }

// StatementReturnType implements the Statement interface.
func (*CommentOnSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnSchema) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnSchema) StatementTag() string { return CommentOnSchemaTag }

// StatementReturnType implements the Statement interface.
func (*CommentOnIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnIndex) StatementTag() string { return CommentOnIndexTag }

// StatementReturnType implements the Statement interface.
func (*CommentOnTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnTable) StatementTag() string { return CommentOnTableTag }

// StatementReturnType implements the Statement interface.
func (*CommentOnType) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CommentOnType) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CommentOnType) StatementTag() string { return CommentOnTypeTag }

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
func (*CopyTo) StatementReturnType() StatementReturnType { return CopyOut }

// StatementType implements the Statement interface.
func (*CopyTo) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CopyTo) StatementTag() string { return "COPY" }

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
func (*ScheduledChangefeed) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ScheduledChangefeed) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *ScheduledChangefeed) StatementTag() string {
	return "SCHEDULED CHANGEFEED"
}

func (*ScheduledChangefeed) cclOnlyStatement() {}

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
func (*CreateExternalConnection) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*CreateExternalConnection) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateExternalConnection) StatementTag() string { return "CREATE EXTERNAL CONNECTION" }

// StatementReturnType implements the Statement interface.
func (*CreateTenant) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*CreateTenant) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateTenant) StatementTag() string { return "CREATE VIRTUAL CLUSTER" }

// StatementReturnType implements the Statement interface.
func (*CreateTenantFromReplication) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*CreateTenantFromReplication) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CreateTenantFromReplication) StatementTag() string {
	return "CREATE VIRTUAL CLUSTER FROM REPLICATION"
}

func (*CreateTenantFromReplication) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*CreateLogicalReplicationStream) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*CreateLogicalReplicationStream) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*CreateLogicalReplicationStream) StatementTag() string {
	return "CREATE LOGICAL REPLICATION STREAM"
}

func (*CreateLogicalReplicationStream) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*DropExternalConnection) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*DropExternalConnection) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropExternalConnection) StatementTag() string { return "DROP EXTERNAL CONNECTION" }

// StatementReturnType implements the Statement interface.
func (*CreateIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreateIndex) StatementTag() string { return CreateIndexTag }

// StatementReturnType implements the Statement interface.
func (*CreatePolicy) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreatePolicy) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*CreatePolicy) StatementTag() string { return CreatePolicyTag }

func (*CreatePolicy) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (n *CreateSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateSchema) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateSchema) StatementTag() string {
	return CreateSchemaTag
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
func (*CreateRole) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateRole) StatementType() StatementType { return TypeDCL }

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
func (*CreateSequence) StatementTag() string { return CreateSequenceTag }

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
func (d *Discard) StatementTag() string {
	switch d.Mode {
	case DiscardModeAll:
		return "DISCARD ALL"
	}
	return "DISCARD"
}

// modifiesSchema implements the canModifySchema interface.
func (*Discard) modifiesSchema() bool { return true }

// StatementReturnType implements the Statement interface.
func (n *DeclareCursor) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*DeclareCursor) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*DeclareCursor) StatementTag() string { return "DECLARE CURSOR" }

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
func (*DropDatabase) StatementTag() string { return DropDatabaseTag }

// StatementReturnType implements the Statement interface.
func (*DropIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropIndex) StatementTag() string { return DropIndexTag }

// StatementReturnType implements the Statement interface.
func (*DropPolicy) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropPolicy) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropPolicy) StatementTag() string { return DropPolicyTag }

func (*DropPolicy) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*DropTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropTable) StatementTag() string { return DropTableTag }

// StatementReturnType implements the Statement interface.
func (*DropView) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropView) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropView) StatementTag() string { return DropViewTag }

// StatementReturnType implements the Statement interface.
func (*DropSequence) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropSequence) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropSequence) StatementTag() string { return DropSequenceTag }

// StatementReturnType implements the Statement interface.
func (*DropRole) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropRole) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*DropRole) StatementTag() string { return "DROP ROLE" }

func (*DropRole) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*DropType) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropType) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*DropType) StatementTag() string { return DropTypeTag }

// StatementReturnType implements the Statement interface.
func (*DropSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropSchema) StatementType() StatementType { return TypeDDL }

// StatementTag implements the Statement interface.
func (*DropSchema) StatementTag() string { return DropSchemaTag }

// StatementReturnType implements the Statement interface.
func (*DropTenant) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*DropTenant) StatementType() StatementType { return TypeDCL }

// StatementTag implements the Statement interface.
func (*DropTenant) StatementTag() string { return "DROP VIRTUAL CLUSTER" }

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
func (*ExplainAnalyze) StatementTag() string { return "EXPLAIN" }

// StatementReturnType implements the Statement interface.
func (*Export) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Export) StatementType() StatementType { return TypeDML }

func (*Export) cclOnlyStatement() {}

// StatementTag returns a short string identifying the type of statement.
func (*Export) StatementTag() string { return "EXPORT" }

// StatementReturnType implements the Statement interface.
func (*Grant) StatementReturnType() StatementReturnType { return DDL }

// StatementReturnType implements the Statement interface.
func (n *FetchCursor) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*FetchCursor) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*FetchCursor) StatementTag() string { return "FETCH" }

// StatementReturnType implements the Statement interface.
func (n *MoveCursor) StatementReturnType() StatementReturnType { return RowsAffected }

// StatementType implements the Statement interface.
func (*MoveCursor) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*MoveCursor) StatementTag() string { return "MOVE" }

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
func (*Import) StatementTag() string { return ImportTag }

func (*Import) cclOnlyStatement() {}

// StatementReturnType implements the Statement interface.
func (*LiteralValuesClause) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*LiteralValuesClause) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*LiteralValuesClause) StatementTag() string { return "VALUES" }

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
func (*DropOwnedBy) StatementTag() string { return DropOwnedByTag }

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
func (*RenameColumn) StatementTag() string { return "ALTER TABLE" }

// StatementReturnType implements the Statement interface.
func (*RenameDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameDatabase) StatementTag() string { return "ALTER DATABASE" }

// StatementReturnType implements the Statement interface.
func (*ReparentDatabase) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*ReparentDatabase) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*ReparentDatabase) StatementTag() string { return "ALTER DATABASE" }

// StatementReturnType implements the Statement interface.
func (*RenameIndex) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameIndex) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*RenameIndex) StatementTag() string { return "ALTER INDEX" }

// StatementReturnType implements the Statement interface.
func (*RenameTable) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*RenameTable) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *RenameTable) StatementTag() string {
	if n.IsView {
		if n.IsMaterialized {
			return "ALTER MATERIALIZED VIEW"
		}
		return "ALTER VIEW"
	} else if n.IsSequence {
		return "ALTER SEQUENCE"
	}
	return "ALTER TABLE"
}

// StatementReturnType implements the Statement interface.
func (*Relocate) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Relocate) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *Relocate) StatementTag() string {
	name := "RELOCATE TABLE "
	if n.TableOrIndex.Index != "" {
		name = "RELOCATE INDEX "
	}
	return name + n.SubjectReplicas.String()
}

// StatementReturnType implements the Statement interface.
func (*RelocateRange) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*RelocateRange) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *RelocateRange) StatementTag() string {
	return "RELOCATE RANGE " + n.SubjectReplicas.String()
}

// StatementReturnType implements the Statement interface.
func (*Restore) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Restore) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Restore) StatementTag() string { return RestoreTag }

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
func (*SetVar) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *SetVar) StatementTag() string {
	if n.Reset || n.ResetAll {
		return "RESET"
	}
	return "SET"
}

// StatementReturnType implements the Statement interface.
func (*SetClusterSetting) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetClusterSetting) StatementType() StatementType { return TypeDCL }

// StatementTag returns a short string identifying the type of statement.
func (*SetClusterSetting) StatementTag() string { return "SET CLUSTER SETTING" }

// StatementReturnType implements the Statement interface.
func (*SetTransaction) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetTransaction) StatementType() StatementType { return TypeTCL }

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
func (*SetSessionAuthorizationDefault) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*SetSessionAuthorizationDefault) StatementTag() string { return "SET" }

// StatementReturnType implements the Statement interface.
func (*SetSessionCharacteristics) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*SetSessionCharacteristics) StatementType() StatementType { return TypeDML }

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
func (*ShowTenantClusterSetting) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTenantClusterSetting) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTenantClusterSetting) StatementTag() string { return "SHOW" }

// StatementReturnType implements the Statement interface.
func (*ShowTenantClusterSettingList) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTenantClusterSettingList) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTenantClusterSettingList) StatementTag() string { return "SHOW" }

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
func (*ShowCreateAllSchemas) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateAllSchemas) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateAllSchemas) StatementTag() string { return "SHOW CREATE ALL SCHEMAS" }

// StatementReturnType implements the Statement interface.
func (*ShowCreateAllTables) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateAllTables) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateAllTables) StatementTag() string { return "SHOW CREATE ALL TABLES" }

// StatementReturnType implements the Statement interface.
func (*ShowCreateAllTypes) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateAllTypes) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateAllTypes) StatementTag() string { return "SHOW CREATE ALL TYPES" }

// StatementReturnType implements the Statement interface.
func (*ShowCreateSchedules) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateSchedules) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateSchedules) StatementTag() string { return "SHOW CREATE SCHEDULES" }

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
func (*ShowPolicies) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowPolicies) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of the statement.
func (*ShowPolicies) StatementTag() string { return "SHOW POLICIES" }

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
func (*ShowTransferState) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTransferState) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTransferState) StatementTag() string { return "SHOW TRANSFER STATE" }

func (*ShowTransferState) observerStatement() {}

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
func (*ShowDefaultSessionVariablesForRole) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowDefaultSessionVariablesForRole) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDefaultSessionVariablesForRole) StatementTag() string {
	return "SHOW DEFAULT SESSION VARIABLES FOR ROLE"
}

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

func (*ShowLogicalReplicationJobs) StatementReturnType() StatementReturnType { return Rows }

func (*ShowLogicalReplicationJobs) StatementType() StatementType { return TypeDML }

func (*ShowLogicalReplicationJobs) StatementTag() string {
	return "SHOW LOGICAL REPLICATION JOBS"
}

// StatementReturnType implements the Statement interface.
func (*ShowTables) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTables) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTables) StatementTag() string { return "SHOW TABLES" }

// StatementReturnType implements the Statement interface.
func (*ShowTenant) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTenant) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTenant) StatementTag() string { return "SHOW VIRTUAL CLUSTER" }

// StatementReturnType implements the Statement interface.
func (*ShowRoutines) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowRoutines) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *ShowRoutines) StatementTag() string {
	if n.Procedure {
		return "SHOW PROCEDURES"
	}
	return "SHOW FUNCTIONS"
}

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
func (*ShowSequences) StatementTag() string { return "SHOW SEQUENCES" }

// StatementReturnType implements the Statement interface.
func (*ShowDefaultPrivileges) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowDefaultPrivileges) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowDefaultPrivileges) StatementTag() string { return "SHOW DEFAULT PRIVILEGES" }

// StatementReturnType implements the Statement interface.
func (*ShowCompletions) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCompletions) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCompletions) StatementTag() string { return "SHOW COMPLETIONS" }

// observerStatement implements the ObserverStatement interface.
func (*ShowCompletions) observerStatement() {}

func (*ShowCompletions) hiddenFromShowQueries() {}

// StatementReturnType implements the Statement interface.
func (*ShowCreateRoutine) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateRoutine) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *ShowCreateRoutine) StatementTag() string {
	if n.Procedure {
		return "SHOW CREATE PROCEDURE"
	}
	return "SHOW CREATE FUNCTION"
}

// StatementReturnType implements the Statement interface.
func (*ShowCreateExternalConnections) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateExternalConnections) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCreateExternalConnections) StatementTag() string {
	return "SHOW CREATE EXTERNAL CONNECTIONS"
}

// StatementReturnType implements the Statement interface.
func (*ShowExternalConnections) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowExternalConnections) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowExternalConnections) StatementTag() string {
	return "SHOW EXTERNAL CONNECTIONS"
}

// StatementReturnType implements the Statement interface.
func (*ShowCommitTimestamp) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCommitTimestamp) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*ShowCommitTimestamp) StatementTag() string {
	return "SHOW COMMIT TIMESTAMP"
}

// StatementReturnType implements the Statement interface.
func (*ShowTriggers) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowTriggers) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ShowTriggers) StatementTag() string { return "SHOW TRIGGERS" }

// StatementReturnType implements the Statement interface.
func (*ShowCreateTrigger) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ShowCreateTrigger) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (n *ShowCreateTrigger) StatementTag() string {
	return "SHOW CREATE TRIGGER"
}

// StatementReturnType implements the Statement interface.
func (*Split) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*Split) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*Split) StatementTag() string { return "SPLIT" }

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
func (*Unlisten) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*Unlisten) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*Unlisten) StatementTag() string { return "UNLISTEN" }

// StatementReturnType implements the Statement interface.
func (*ValuesClause) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*ValuesClause) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*ValuesClause) StatementTag() string { return "VALUES" }

// StatementReturnType implements the Statement interface.
func (*CreateRoutine) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateRoutine) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateRoutine) StatementTag() string {
	if n.IsProcedure {
		return CreateProcedureTag
	}
	return CreateFunctionTag
}

// StatementReturnType implements the Statement interface.
func (*RoutineReturn) StatementReturnType() StatementReturnType { return Rows }

// StatementType implements the Statement interface.
func (*RoutineReturn) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*RoutineReturn) StatementTag() string { return "RETURN" }

// StatementReturnType implements the Statement interface.
func (*DropRoutine) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropRoutine) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *DropRoutine) StatementTag() string {
	if n.Procedure {
		return DropProcedureTag
	}
	return DropFunctionTag
}

// StatementReturnType implements the Statement interface.
func (*CreateTrigger) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*CreateTrigger) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *CreateTrigger) StatementTag() string {
	return CreateTriggerTag
}

// StatementReturnType implements the Statement interface.
func (*DropTrigger) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*DropTrigger) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *DropTrigger) StatementTag() string {
	return DropTriggerTag
}

// StatementReturnType implements the Statement interface.
func (*AlterFunctionOptions) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterFunctionOptions) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterFunctionOptions) StatementTag() string { return "ALTER FUNCTION" }

// StatementReturnType implements the Statement interface.
func (*AlterRoutineRename) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterRoutineRename) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *AlterRoutineRename) StatementTag() string {
	if n.Procedure {
		return "ALTER PROCEDURE"
	} else {
		return "ALTER FUNCTION"
	}
}

// StatementReturnType implements the Statement interface.
func (*AlterRoutineSetSchema) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterRoutineSetSchema) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *AlterRoutineSetSchema) StatementTag() string {
	if n.Procedure {
		return "ALTER PROCEDURE"
	} else {
		return "ALTER FUNCTION"
	}
}

// StatementReturnType implements the Statement interface.
func (*AlterRoutineSetOwner) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterRoutineSetOwner) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (n *AlterRoutineSetOwner) StatementTag() string {
	if n.Procedure {
		return "ALTER PROCEDURE"
	} else {
		return "ALTER FUNCTION"
	}
}

// StatementReturnType implements the Statement interface.
func (*AlterFunctionDepExtension) StatementReturnType() StatementReturnType { return DDL }

// StatementType implements the Statement interface.
func (*AlterFunctionDepExtension) StatementType() StatementType { return TypeDDL }

// StatementTag returns a short string identifying the type of statement.
func (*AlterFunctionDepExtension) StatementTag() string { return "ALTER FUNCTION" }

func (n *AlterChangefeed) String() string                     { return AsString(n) }
func (n *AlterChangefeedCmds) String() string                 { return AsString(n) }
func (n *AlterBackup) String() string                         { return AsString(n) }
func (n *AlterBackupSchedule) String() string                 { return AsString(n) }
func (n *AlterBackupScheduleCmds) String() string             { return AsString(n) }
func (n *AlterIndex) String() string                          { return AsString(n) }
func (n *AlterIndexVisible) String() string                   { return AsString(n) }
func (n *AlterDatabaseOwner) String() string                  { return AsString(n) }
func (n *AlterDatabaseAddRegion) String() string              { return AsString(n) }
func (n *AlterDatabaseDropRegion) String() string             { return AsString(n) }
func (n *AlterDatabaseSurvivalGoal) String() string           { return AsString(n) }
func (n *AlterDatabasePlacement) String() string              { return AsString(n) }
func (n *AlterDatabasePrimaryRegion) String() string          { return AsString(n) }
func (n *AlterDatabaseAddSuperRegion) String() string         { return AsString(n) }
func (n *AlterDatabaseDropSuperRegion) String() string        { return AsString(n) }
func (n *AlterDatabaseAlterSuperRegion) String() string       { return AsString(n) }
func (n *AlterDatabaseSecondaryRegion) String() string        { return AsString(n) }
func (n *AlterDatabaseDropSecondaryRegion) String() string    { return AsString(n) }
func (n *AlterDatabaseSetZoneConfigExtension) String() string { return AsString(n) }
func (n *AlterDefaultPrivileges) String() string              { return AsString(n) }
func (n *AlterFunctionOptions) String() string                { return AsString(n) }
func (n *AlterPolicy) String() string                         { return AsString(n) }
func (n *AlterRoutineRename) String() string                  { return AsString(n) }
func (n *AlterRoutineSetSchema) String() string               { return AsString(n) }
func (n *AlterRoutineSetOwner) String() string                { return AsString(n) }
func (n *AlterFunctionDepExtension) String() string           { return AsString(n) }
func (n *AlterSchema) String() string                         { return AsString(n) }
func (n *AlterTable) String() string                          { return AsString(n) }
func (n *AlterTableCmds) String() string                      { return AsString(n) }
func (n *AlterTableAddColumn) String() string                 { return AsString(n) }
func (n *AlterTableAddConstraint) String() string             { return AsString(n) }
func (n *AlterTableAlterColumnType) String() string           { return AsString(n) }
func (n *AlterTableDropColumn) String() string                { return AsString(n) }
func (n *AlterTableDropConstraint) String() string            { return AsString(n) }
func (n *AlterTableDropNotNull) String() string               { return AsString(n) }
func (n *AlterTableDropStored) String() string                { return AsString(n) }
func (n *AlterTableAddIdentity) String() string               { return AsString(n) }
func (n *AlterTableIdentity) String() string                  { return AsString(n) }
func (n *AlterTableLocality) String() string                  { return AsString(n) }
func (n *AlterTableSetDefault) String() string                { return AsString(n) }
func (n *AlterTableSetVisible) String() string                { return AsString(n) }
func (n *AlterTableSetNotNull) String() string                { return AsString(n) }
func (n *AlterTableOwner) String() string                     { return AsString(n) }
func (n *AlterTableSetSchema) String() string                 { return AsString(n) }
func (n *AlterTenantCapability) String() string               { return AsString(n) }
func (n *AlterTenantSetClusterSetting) String() string        { return AsString(n) }
func (n *AlterTenantReset) String() string                    { return AsString(n) }
func (n *AlterTenantRename) String() string                   { return AsString(n) }
func (n *AlterTenantReplication) String() string              { return AsString(n) }
func (n *AlterTenantService) String() string                  { return AsString(n) }
func (n *AlterType) String() string                           { return AsString(n) }
func (n *AlterRole) String() string                           { return AsString(n) }
func (n *AlterRoleSet) String() string                        { return AsString(n) }
func (n *AlterSequence) String() string                       { return AsString(n) }
func (n *Analyze) String() string                             { return AsString(n) }
func (n *Backup) String() string                              { return AsString(n) }
func (n *BeginTransaction) String() string                    { return AsString(n) }
func (n *Call) String() string                                { return AsString(n) }
func (n *ControlJobs) String() string                         { return AsString(n) }
func (n *ControlSchedules) String() string                    { return AsString(n) }
func (n *ControlJobsForSchedules) String() string             { return AsString(n) }
func (n *ControlJobsOfType) String() string                   { return AsString(n) }
func (n *CancelQueries) String() string                       { return AsString(n) }
func (n *CancelSessions) String() string                      { return AsString(n) }
func (n *CannedOptPlan) String() string                       { return AsString(n) }
func (n *CloseCursor) String() string                         { return AsString(n) }
func (n *CommentOnColumn) String() string                     { return AsString(n) }
func (n *CommentOnConstraint) String() string                 { return AsString(n) }
func (n *CommentOnDatabase) String() string                   { return AsString(n) }
func (n *CommentOnSchema) String() string                     { return AsString(n) }
func (n *CommentOnIndex) String() string                      { return AsString(n) }
func (n *CommentOnTable) String() string                      { return AsString(n) }
func (n *CommentOnType) String() string                       { return AsString(n) }
func (n *CommitTransaction) String() string                   { return AsString(n) }
func (n *CopyFrom) String() string                            { return AsString(n) }
func (n *CopyTo) String() string                              { return AsString(n) }
func (n *CreateChangefeed) String() string                    { return AsString(n) }
func (n *CreateDatabase) String() string                      { return AsString(n) }
func (n *CreateExtension) String() string                     { return AsString(n) }
func (n *CreateRoutine) String() string                       { return AsString(n) }
func (n *CreateTrigger) String() string                       { return AsString(n) }
func (n *CreateIndex) String() string                         { return AsString(n) }
func (n *CreateLogicalReplicationStream) String() string      { return AsString(n) }
func (n *CreatePolicy) String() string                        { return AsString(n) }
func (n *CreateRole) String() string                          { return AsString(n) }
func (n *CreateTable) String() string                         { return AsString(n) }
func (n *CreateTenant) String() string                        { return AsString(n) }
func (n *CreateTenantFromReplication) String() string         { return AsString(n) }
func (n *CreateSchema) String() string                        { return AsString(n) }
func (n *CreateSequence) String() string                      { return AsString(n) }
func (n *CreateStats) String() string                         { return AsString(n) }
func (n *CreateView) String() string                          { return AsString(n) }
func (n *Deallocate) String() string                          { return AsString(n) }
func (n *Delete) String() string                              { return AsString(n) }
func (n *DeclareCursor) String() string                       { return AsString(n) }
func (n *DropDatabase) String() string                        { return AsString(n) }
func (n *DropPolicy) String() string                          { return AsString(n) }
func (n *DropRoutine) String() string                         { return AsString(n) }
func (n *DropTrigger) String() string                         { return AsString(n) }
func (n *DropIndex) String() string                           { return AsString(n) }
func (n *DropOwnedBy) String() string                         { return AsString(n) }
func (n *DropSchema) String() string                          { return AsString(n) }
func (n *DropSequence) String() string                        { return AsString(n) }
func (n *DropTable) String() string                           { return AsString(n) }
func (n *DropType) String() string                            { return AsString(n) }
func (n *DropView) String() string                            { return AsString(n) }
func (n *DropRole) String() string                            { return AsString(n) }
func (n *DropTenant) String() string                          { return AsString(n) }
func (n *Execute) String() string                             { return AsString(n) }
func (n *Explain) String() string                             { return AsString(n) }
func (n *ExplainAnalyze) String() string                      { return AsString(n) }
func (n *Export) String() string                              { return AsString(n) }
func (n *CreateExternalConnection) String() string            { return AsString(n) }
func (n *DropExternalConnection) String() string              { return AsString(n) }
func (n *FetchCursor) String() string                         { return AsString(n) }
func (n *Grant) String() string                               { return AsString(n) }
func (n *GrantRole) String() string                           { return AsString(n) }
func (n *MoveCursor) String() string                          { return AsString(n) }
func (n *Insert) String() string                              { return AsString(n) }
func (n *Import) String() string                              { return AsString(n) }
func (n *LiteralValuesClause) String() string                 { return AsString(n) }
func (n *ParenSelect) String() string                         { return AsString(n) }
func (n *Prepare) String() string                             { return AsString(n) }
func (n *ReassignOwnedBy) String() string                     { return AsString(n) }
func (n *ReleaseSavepoint) String() string                    { return AsString(n) }
func (n *Relocate) String() string                            { return AsString(n) }
func (n *RelocateRange) String() string                       { return AsString(n) }
func (n *RefreshMaterializedView) String() string             { return AsString(n) }
func (n *RenameColumn) String() string                        { return AsString(n) }
func (n *RenameDatabase) String() string                      { return AsString(n) }
func (n *ReparentDatabase) String() string                    { return AsString(n) }
func (n *RenameIndex) String() string                         { return AsString(n) }
func (n *RenameTable) String() string                         { return AsString(n) }
func (n *Restore) String() string                             { return AsString(n) }
func (n *RoutineReturn) String() string                       { return AsString(n) }
func (n *Revoke) String() string                              { return AsString(n) }
func (n *RevokeRole) String() string                          { return AsString(n) }
func (n *RollbackToSavepoint) String() string                 { return AsString(n) }
func (n *RollbackTransaction) String() string                 { return AsString(n) }
func (n *Savepoint) String() string                           { return AsString(n) }
func (n *Scatter) String() string                             { return AsString(n) }
func (n *ScheduledBackup) String() string                     { return AsString(n) }
func (n *Scrub) String() string                               { return AsString(n) }
func (n *Select) String() string                              { return AsString(n) }
func (n *SelectClause) String() string                        { return AsString(n) }
func (n *SetClusterSetting) String() string                   { return AsString(n) }
func (n *SetZoneConfig) String() string                       { return AsString(n) }
func (n *SetSessionAuthorizationDefault) String() string      { return AsString(n) }
func (n *SetSessionCharacteristics) String() string           { return AsString(n) }
func (n *SetTransaction) String() string                      { return AsString(n) }
func (n *SetTracing) String() string                          { return AsString(n) }
func (n *SetVar) String() string                              { return AsString(n) }
func (n *ShowBackup) String() string                          { return AsString(n) }
func (n *ShowClusterSetting) String() string                  { return AsString(n) }
func (n *ShowClusterSettingList) String() string              { return AsString(n) }
func (n *ShowTenantClusterSetting) String() string            { return AsString(n) }
func (n *ShowTenantClusterSettingList) String() string        { return AsString(n) }
func (n *ShowColumns) String() string                         { return AsString(n) }
func (n *ShowConstraints) String() string                     { return AsString(n) }
func (n *ShowCreate) String() string                          { return AsString(n) }
func (n *ShowCreateAllSchemas) String() string                { return AsString(n) }
func (n *ShowCreateAllTables) String() string                 { return AsString(n) }
func (n *ShowCreateAllTypes) String() string                  { return AsString(n) }
func (n *ShowCreateSchedules) String() string                 { return AsString(n) }
func (n *ShowDatabases) String() string                       { return AsString(n) }
func (n *ShowDatabaseIndexes) String() string                 { return AsString(n) }
func (n *ShowEnums) String() string                           { return AsString(n) }
func (n *ShowFullTableScans) String() string                  { return AsString(n) }
func (n *ShowCreateRoutine) String() string                   { return AsString(n) }
func (n *ShowCreateExternalConnections) String() string       { return AsString(n) }
func (n *ShowExternalConnections) String() string             { return AsString(n) }
func (n *ShowRoutines) String() string                        { return AsString(n) }
func (n *ShowGrants) String() string                          { return AsString(n) }
func (n *ShowHistogram) String() string                       { return AsString(n) }
func (n *ShowSchedules) String() string                       { return AsString(n) }
func (n *ShowIndexes) String() string                         { return AsString(n) }
func (n *ShowJobs) String() string                            { return AsString(n) }
func (n *ShowChangefeedJobs) String() string                  { return AsString(n) }
func (n *ShowLastQueryStatistics) String() string             { return AsString(n) }
func (n *ShowPartitions) String() string                      { return AsString(n) }
func (n *ShowPolicies) String() string                        { return AsString(n) }
func (n *ShowQueries) String() string                         { return AsString(n) }
func (n *ShowRanges) String() string                          { return AsString(n) }
func (n *ShowRangeForRow) String() string                     { return AsString(n) }
func (n *ShowRegions) String() string                         { return AsString(n) }
func (n *ShowRoleGrants) String() string                      { return AsString(n) }
func (n *ShowRoles) String() string                           { return AsString(n) }
func (n *ShowSavepointStatus) String() string                 { return AsString(n) }
func (n *ShowSchemas) String() string                         { return AsString(n) }
func (n *ShowSequences) String() string                       { return AsString(n) }
func (n *ShowSessions) String() string                        { return AsString(n) }
func (n *ShowSurvivalGoal) String() string                    { return AsString(n) }
func (n *ShowSyntax) String() string                          { return AsString(n) }
func (n *ShowTableStats) String() string                      { return AsString(n) }
func (n *ShowTables) String() string                          { return AsString(n) }
func (n *ShowTenant) String() string                          { return AsString(n) }
func (n *ShowTypes) String() string                           { return AsString(n) }
func (n *ShowTraceForSession) String() string                 { return AsString(n) }
func (n *ShowTransactionStatus) String() string               { return AsString(n) }
func (n *ShowTransactions) String() string                    { return AsString(n) }
func (n *ShowTransferState) String() string                   { return AsString(n) }
func (n *ShowUsers) String() string                           { return AsString(n) }
func (n *ShowDefaultSessionVariablesForRole) String() string  { return AsString(n) }
func (n *ShowVar) String() string                             { return AsString(n) }
func (n *ShowZoneConfig) String() string                      { return AsString(n) }
func (n *ShowFingerprints) String() string                    { return AsString(n) }
func (n *ShowDefaultPrivileges) String() string               { return AsString(n) }
func (n *ShowCompletions) String() string                     { return AsString(n) }
func (n *ShowCommitTimestamp) String() string                 { return AsString(n) }
func (n *ShowLogicalReplicationJobs) String() string          { return AsString(n) }
func (n *ShowTriggers) String() string                        { return AsString(n) }
func (n *ShowCreateTrigger) String() string                   { return AsString(n) }
func (n *Split) String() string                               { return AsString(n) }
func (n *Truncate) String() string                            { return AsString(n) }
func (n *TenantSpec) String() string                          { return AsString(n) }
func (n *UnionClause) String() string                         { return AsString(n) }
func (n *Unsplit) String() string                             { return AsString(n) }
func (n *Update) String() string                              { return AsString(n) }
func (n *ValuesClause) String() string                        { return AsString(n) }
