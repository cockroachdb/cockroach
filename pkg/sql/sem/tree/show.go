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

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
)

// ShowVar represents a SHOW statement.
type ShowVar struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *ShowVar) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	// Session var names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
		ctx.FormatNameP(&node.Name)
	})
}

// ShowClusterSetting represents a SHOW CLUSTER SETTING statement.
type ShowClusterSetting struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *ShowClusterSetting) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CLUSTER SETTING ")
	// Cluster setting names never contain PII and should be distinguished
	// for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
		ctx.FormatNameP(&node.Name)
	})
}

// ShowClusterSettingList represents a SHOW [ALL|PUBLIC] CLUSTER SETTINGS statement.
type ShowClusterSettingList struct {
	// All indicates whether to include non-public settings in the output.
	All bool
}

// Format implements the NodeFormatter interface.
func (node *ShowClusterSettingList) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	qual := "PUBLIC"
	if node.All {
		qual = "ALL"
	}
	ctx.WriteString(qual)
	ctx.WriteString(" CLUSTER SETTINGS")
}

// ShowBackupDetails represents the type of details to display for a SHOW BACKUP
// statement.
type ShowBackupDetails int

const (
	// BackupDefaultDetails identifies a bare SHOW BACKUP statement.
	BackupDefaultDetails ShowBackupDetails = iota
	// BackupRangeDetails identifies a SHOW BACKUP RANGES statement.
	BackupRangeDetails
	// BackupFileDetails identifies a SHOW BACKUP FILES statement.
	BackupFileDetails
	// BackupSchemaDetails identifies a SHOW BACKUP SCHEMAS statement.
	BackupSchemaDetails
	// BackupValidateDetails identifies a SHOW BACKUP VALIDATION
	// statement.
	BackupValidateDetails
	// BackupConnectionTest identifies a SHOW BACKUP CONNECTION statement
	BackupConnectionTest
)

// TODO (msbutler): 22.2 after removing old style show backup syntax, rename
// Path to Subdir and InCollection to Dest.

// ShowBackup represents a SHOW BACKUP statement.
//
// TODO(msbutler): implement a walkableStmt for ShowBackup.
type ShowBackup struct {
	Path         Expr
	InCollection StringOrPlaceholderOptList
	From         bool
	Details      ShowBackupDetails
	Options      ShowBackupOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowBackup) Format(ctx *FmtCtx) {
	if node.InCollection != nil && node.Path == nil {
		ctx.WriteString("SHOW BACKUPS IN ")
		ctx.FormatURIs(node.InCollection)
		return
	}
	ctx.WriteString("SHOW BACKUP ")

	switch node.Details {
	case BackupRangeDetails:
		ctx.WriteString("RANGES ")
	case BackupFileDetails:
		ctx.WriteString("FILES ")
	case BackupSchemaDetails:
		ctx.WriteString("SCHEMAS ")
	case BackupConnectionTest:
		ctx.WriteString("CONNECTION ")
	}

	if node.From {
		ctx.WriteString("FROM ")
	}

	if node.InCollection != nil {
		ctx.FormatNode(node.Path)
		ctx.WriteString(" IN ")
		ctx.FormatURIs(node.InCollection)
	} else {
		ctx.FormatURI(node.Path)
	}
	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}

type ShowBackupOptions struct {
	AsJson               bool
	CheckFiles           bool
	DebugIDs             bool
	IncrementalStorage   StringOrPlaceholderOptList
	DecryptionKMSURI     StringOrPlaceholderOptList
	EncryptionPassphrase Expr
	Privileges           bool
	SkipSize             bool

	// EncryptionInfoDir is a hidden option used when the user wants to run the deprecated
	//
	// SHOW BACKUP <incremental_dir>
	//
	// on an encrypted incremental backup will need to pass their full backup's
	// directory to the encryption_info_dir parameter because the
	// `ENCRYPTION-INFO` file necessary to decode the incremental backup lives in
	// the full backup dir.
	EncryptionInfoDir Expr
	DebugMetadataSST  bool

	CheckConnectionTransferSize Expr
	CheckConnectionDuration     Expr
	CheckConnectionConcurrency  Expr
}

var _ NodeFormatter = &ShowBackupOptions{}

func (o *ShowBackupOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	if o.AsJson {
		ctx.WriteString("as_json")
		addSep = true
	}
	if o.CheckFiles {
		maybeAddSep()
		ctx.WriteString("check_files")
	}
	if o.DebugIDs {
		maybeAddSep()
		ctx.WriteString("debug_ids")
	}
	if o.EncryptionPassphrase != nil {
		maybeAddSep()
		ctx.WriteString("encryption_passphrase = ")
		if ctx.flags.HasFlags(FmtShowPasswords) {
			ctx.FormatNode(o.EncryptionPassphrase)
		} else {
			ctx.WriteString(PasswordSubstitution)
		}
	}
	if o.IncrementalStorage != nil {
		maybeAddSep()
		ctx.WriteString("incremental_location = ")
		ctx.FormatURIs(o.IncrementalStorage)
	}

	if o.Privileges {
		maybeAddSep()
		ctx.WriteString("privileges")
	}

	if o.EncryptionInfoDir != nil {
		maybeAddSep()
		ctx.WriteString("encryption_info_dir = ")
		ctx.FormatNode(o.EncryptionInfoDir)
	}
	if o.DecryptionKMSURI != nil {
		maybeAddSep()
		ctx.WriteString("kms = ")
		ctx.FormatURIs(o.DecryptionKMSURI)
	}
	if o.SkipSize {
		maybeAddSep()
		ctx.WriteString("skip size")
	}
	if o.DebugMetadataSST {
		maybeAddSep()
		ctx.WriteString("debug_dump_metadata_sst")
	}

	// The following are only used in connection-check SHOW.
	if o.CheckConnectionConcurrency != nil {
		maybeAddSep()
		ctx.WriteString("CONCURRENTLY = ")
		ctx.FormatNode(o.CheckConnectionConcurrency)
	}
	if o.CheckConnectionTransferSize != nil {
		maybeAddSep()
		ctx.WriteString("TRANSFER = ")
		ctx.FormatNode(o.CheckConnectionTransferSize)
	}
	if o.CheckConnectionDuration != nil {
		maybeAddSep()
		ctx.WriteString("TIME = ")
		ctx.FormatNode(o.CheckConnectionDuration)
	}
}

func (o ShowBackupOptions) IsDefault() bool {
	options := ShowBackupOptions{}
	return o.AsJson == options.AsJson &&
		o.CheckFiles == options.CheckFiles &&
		o.DebugIDs == options.DebugIDs &&
		cmp.Equal(o.IncrementalStorage, options.IncrementalStorage) &&
		cmp.Equal(o.DecryptionKMSURI, options.DecryptionKMSURI) &&
		o.EncryptionPassphrase == options.EncryptionPassphrase &&
		o.Privileges == options.Privileges &&
		o.SkipSize == options.SkipSize &&
		o.DebugMetadataSST == options.DebugMetadataSST &&
		o.EncryptionInfoDir == options.EncryptionInfoDir &&
		o.CheckConnectionTransferSize == options.CheckConnectionTransferSize &&
		o.CheckConnectionDuration == options.CheckConnectionDuration &&
		o.CheckConnectionConcurrency == options.CheckConnectionConcurrency
}

func combineBools(v1 bool, v2 bool, label string) (bool, error) {
	if v2 && v1 {
		return false, errors.Newf("% option specified multiple times", label)
	}
	return v2 || v1, nil
}
func combineExpr(v1 Expr, v2 Expr, label string) (Expr, error) {
	if v1 != nil {
		if v2 != nil {
			return v1, errors.Newf("% option specified multiple times", label)
		}
		return v1, nil
	}
	return v2, nil
}
func combineStringOrPlaceholderOptList(
	v1 StringOrPlaceholderOptList, v2 StringOrPlaceholderOptList, label string,
) (StringOrPlaceholderOptList, error) {
	if v1 != nil {
		if v2 != nil {
			return v1, errors.Newf("% option specified multiple times", label)
		}
		return v1, nil
	}
	return v2, nil
}

// CombineWith merges other backup options into this backup options struct.
// An error is returned if the same option merged multiple times.
func (o *ShowBackupOptions) CombineWith(other *ShowBackupOptions) error {
	var err error
	o.AsJson, err = combineBools(o.AsJson, other.AsJson, "as_json")
	if err != nil {
		return err
	}
	o.CheckFiles, err = combineBools(o.CheckFiles, other.CheckFiles, "check_files")
	if err != nil {
		return err
	}
	o.DebugIDs, err = combineBools(o.DebugIDs, other.DebugIDs, "debug_ids")
	if err != nil {
		return err
	}
	o.EncryptionPassphrase, err = combineExpr(o.EncryptionPassphrase, other.EncryptionPassphrase,
		"encryption_passphrase")
	if err != nil {
		return err
	}
	o.IncrementalStorage, err = combineStringOrPlaceholderOptList(o.IncrementalStorage,
		other.IncrementalStorage, "incremental_location")
	if err != nil {
		return err
	}
	o.DecryptionKMSURI, err = combineStringOrPlaceholderOptList(o.DecryptionKMSURI,
		other.DecryptionKMSURI, "kms")
	if err != nil {
		return err
	}
	o.Privileges, err = combineBools(o.Privileges, other.Privileges, "privileges")
	if err != nil {
		return err
	}
	o.SkipSize, err = combineBools(o.SkipSize, other.SkipSize, "skip size")
	if err != nil {
		return err
	}
	o.DebugMetadataSST, err = combineBools(o.DebugMetadataSST, other.DebugMetadataSST,
		"debug_dump_metadata_sst")
	if err != nil {
		return err
	}
	o.EncryptionInfoDir, err = combineExpr(o.EncryptionInfoDir, other.EncryptionInfoDir,
		"encryption_info_dir")
	if err != nil {
		return err
	}

	o.CheckConnectionTransferSize, err = combineExpr(o.CheckConnectionTransferSize, other.CheckConnectionTransferSize,
		"transfer")
	if err != nil {
		return err
	}

	o.CheckConnectionDuration, err = combineExpr(o.CheckConnectionDuration, other.CheckConnectionDuration,
		"time")
	if err != nil {
		return err
	}

	o.CheckConnectionConcurrency, err = combineExpr(o.CheckConnectionConcurrency, other.CheckConnectionConcurrency,
		"concurrently")
	if err != nil {
		return err
	}

	return nil
}

// ShowColumns represents a SHOW COLUMNS statement.
type ShowColumns struct {
	Table       *UnresolvedObjectName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowColumns) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW COLUMNS FROM ")
	ctx.FormatNode(node.Table)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowDatabases represents a SHOW DATABASES statement.
type ShowDatabases struct {
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowDatabases) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DATABASES")

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowEnums represents a SHOW ENUMS statement.
type ShowEnums struct {
	ObjectNamePrefix
}

// Format implements the NodeFormatter interface.
func (node *ShowEnums) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ENUMS")
}

// ShowTypes represents a SHOW TYPES statement.
type ShowTypes struct {
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTypes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TYPES")

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowTraceType is an enum of SHOW TRACE variants.
type ShowTraceType string

// A list of the SHOW TRACE variants.
const (
	ShowTraceRaw     ShowTraceType = "TRACE"
	ShowTraceKV      ShowTraceType = "KV TRACE"
	ShowTraceReplica ShowTraceType = "EXPERIMENTAL_REPLICA TRACE"
)

// ShowTraceForSession represents a SHOW TRACE FOR SESSION statement.
type ShowTraceForSession struct {
	TraceType ShowTraceType
	Compact   bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTraceForSession) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Compact {
		ctx.WriteString("COMPACT ")
	}
	ctx.WriteString(string(node.TraceType))
	ctx.WriteString(" FOR SESSION")
}

// ShowIndexes represents a SHOW INDEX statement.
type ShowIndexes struct {
	Table       *UnresolvedObjectName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowIndexes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW INDEXES FROM ")
	ctx.FormatNode(node.Table)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowDatabaseIndexes represents a SHOW INDEXES FROM DATABASE statement.
type ShowDatabaseIndexes struct {
	Database    Name
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowDatabaseIndexes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW INDEXES FROM DATABASE ")
	ctx.FormatNode(&node.Database)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowQueries represents a SHOW STATEMENTS statement.
type ShowQueries struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowQueries) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.All {
		ctx.WriteString("ALL ")
	}
	if node.Cluster {
		ctx.WriteString("CLUSTER STATEMENTS")
	} else {
		ctx.WriteString("LOCAL STATEMENTS")
	}
}

// ShowJobs represents a SHOW JOBS statement
type ShowJobs struct {
	// If non-nil, a select statement that provides the job ids to be shown.
	Jobs *Select

	// If Automatic is true, show only automatically-generated jobs such
	// as automatic CREATE STATISTICS jobs. If Automatic is false, show
	// only non-automatically-generated jobs.
	Automatic bool

	// Whether to block and wait for completion of all running jobs to be displayed.
	Block bool

	// If non-nil, only display jobs started by the specified
	// schedules.
	Schedules *Select

	// Options contain any options that were specified in the `SHOW JOB` query.
	Options *ShowJobOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowJobs) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Automatic {
		ctx.WriteString("AUTOMATIC ")
	}
	ctx.WriteString("JOBS")
	if node.Block {
		ctx.WriteString(" WHEN COMPLETE")
	}
	if node.Jobs != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.Jobs)
	}
	if node.Schedules != nil {
		ctx.WriteString(" FOR SCHEDULES ")
		ctx.FormatNode(node.Schedules)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH")
		ctx.FormatNode(node.Options)
	}
}

// ShowJobOptions describes options for the SHOW JOB execution.
type ShowJobOptions struct {
	// ExecutionDetails, if true, will render job specific details about the job's
	// execution. These details will provide improved observability into the
	// execution of the job.
	ExecutionDetails bool
}

func (s *ShowJobOptions) Format(ctx *FmtCtx) {
	if s.ExecutionDetails {
		ctx.WriteString(" EXECUTION DETAILS")
	}
}

func (s *ShowJobOptions) CombineWith(other *ShowJobOptions) error {
	s.ExecutionDetails = other.ExecutionDetails
	return nil
}

var _ NodeFormatter = &ShowJobOptions{}

// ShowChangefeedJobs represents a SHOW CHANGEFEED JOBS statement
type ShowChangefeedJobs struct {
	// If non-nil, a select statement that provides the job ids to be shown.
	Jobs *Select
}

// Format implements the NodeFormatter interface.
func (node *ShowChangefeedJobs) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CHANGEFEED JOBS")
	if node.Jobs != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.Jobs)
	}
}

// ShowSurvivalGoal represents a SHOW SURVIVAL GOAL statement
type ShowSurvivalGoal struct {
	DatabaseName Name
}

// Format implements the NodeFormatter interface.
func (node *ShowSurvivalGoal) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SURVIVAL GOAL FROM DATABASE")
	if node.DatabaseName != "" {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.DatabaseName)
	}
}

// ShowRegionsFrom denotes what kind of SHOW REGIONS command is being used.
type ShowRegionsFrom int

const (
	// ShowRegionsFromCluster represents SHOW REGIONS FROM CLUSTER.
	ShowRegionsFromCluster ShowRegionsFrom = iota
	// ShowRegionsFromDatabase represents SHOW REGIONS FROM DATABASE.
	ShowRegionsFromDatabase
	// ShowRegionsFromAllDatabases represents SHOW REGIONS FROM ALL DATABASES.
	ShowRegionsFromAllDatabases
	// ShowRegionsFromDefault represents SHOW REGIONS.
	ShowRegionsFromDefault
	// ShowSuperRegionsFromDatabase represents SHOW SUPER REGIONS FROM DATABASE.
	ShowSuperRegionsFromDatabase
)

// ShowRegions represents a SHOW REGIONS statement
type ShowRegions struct {
	ShowRegionsFrom ShowRegionsFrom
	DatabaseName    Name
}

// Format implements the NodeFormatter interface.
func (node *ShowRegions) Format(ctx *FmtCtx) {
	if node.ShowRegionsFrom == ShowSuperRegionsFromDatabase {
		ctx.WriteString("SHOW SUPER REGIONS")
	} else {
		ctx.WriteString("SHOW REGIONS")
	}
	switch node.ShowRegionsFrom {
	case ShowRegionsFromDefault:
	case ShowRegionsFromAllDatabases:
		ctx.WriteString(" FROM ALL DATABASES")
	case ShowRegionsFromDatabase, ShowSuperRegionsFromDatabase:
		ctx.WriteString(" FROM DATABASE")
		if node.DatabaseName != "" {
			ctx.WriteString(" ")
			ctx.FormatNode(&node.DatabaseName)
		}
	case ShowRegionsFromCluster:
		ctx.WriteString(" FROM CLUSTER")
	default:
		panic(fmt.Sprintf("unknown ShowRegionsFrom: %v", node.ShowRegionsFrom))
	}
}

// ShowSessions represents a SHOW SESSIONS statement
type ShowSessions struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowSessions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.All {
		ctx.WriteString("ALL ")
	}
	if node.Cluster {
		ctx.WriteString("CLUSTER SESSIONS")
	} else {
		ctx.WriteString("LOCAL SESSIONS")
	}
}

// ShowSchemas represents a SHOW SCHEMAS statement.
type ShowSchemas struct {
	Database    Name
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowSchemas) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SCHEMAS")
	if node.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.Database)
	}
	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowSequences represents a SHOW SEQUENCES statement.
type ShowSequences struct {
	Database Name
}

// Format implements the NodeFormatter interface.
func (node *ShowSequences) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SEQUENCES")
	if node.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.Database)
	}
}

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	ObjectNamePrefix
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTables) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TABLES")
	if node.ExplicitSchema {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.ObjectNamePrefix)
	}

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowRoutines represents a SHOW FUNCTIONS or SHOW PROCEDURES statement.
type ShowRoutines struct {
	ObjectNamePrefix
	Procedure bool
}

// Format implements the NodeFormatter interface.
func (node *ShowRoutines) Format(ctx *FmtCtx) {
	if node.Procedure {
		ctx.WriteString("SHOW PROCEDURES")
	} else {
		ctx.WriteString("SHOW FUNCTIONS")
	}
	if node.ExplicitSchema {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.ObjectNamePrefix)
	}
}

// ShowTransactions represents a SHOW TRANSACTIONS statement
type ShowTransactions struct {
	All     bool
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTransactions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.All {
		ctx.WriteString("ALL ")
	}
	if node.Cluster {
		ctx.WriteString("CLUSTER TRANSACTIONS")
	} else {
		ctx.WriteString("LOCAL TRANSACTIONS")
	}
}

// ShowConstraints represents a SHOW CONSTRAINTS statement.
type ShowConstraints struct {
	Table       *UnresolvedObjectName
	WithComment bool
}

// Format implements the NodeFormatter interface.
func (node *ShowConstraints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CONSTRAINTS FROM ")
	ctx.FormatNode(node.Table)

	if node.WithComment {
		ctx.WriteString(" WITH COMMENT")
	}
}

// ShowGrants represents a SHOW GRANTS statement.
// GrantTargetList is defined in grant.go.
type ShowGrants struct {
	Targets  *GrantTargetList
	Grantees RoleSpecList
}

// Format implements the NodeFormatter interface.
func (node *ShowGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Targets != nil && node.Targets.System {
		ctx.WriteString("SYSTEM ")
	}
	ctx.WriteString("GRANTS")
	if node.Targets != nil {
		if !node.Targets.System {
			ctx.WriteString(" ON ")
			ctx.FormatNode(node.Targets)
		}
	}
	if node.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&node.Grantees)
	}
}

// ShowRoleGrants represents a SHOW GRANTS ON ROLE statement.
type ShowRoleGrants struct {
	Roles    RoleSpecList
	Grantees RoleSpecList
}

// Format implements the NodeFormatter interface.
func (node *ShowRoleGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW GRANTS ON ROLE")
	if node.Roles != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.Roles)
	}
	if node.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&node.Grantees)
	}
}

// ShowCreateMode denotes what kind of SHOW CREATE should be used
type ShowCreateMode int

const (
	// ShowCreateModeTable represents SHOW CREATE TABLE
	ShowCreateModeTable ShowCreateMode = iota
	// ShowCreateModeView represents SHOW CREATE VIEW
	ShowCreateModeView
	// ShowCreateModeSequence represents SHOW CREATE SEQUENCE
	ShowCreateModeSequence
	// ShowCreateModeDatabase represents SHOW CREATE DATABASE
	ShowCreateModeDatabase
	// ShowCreateModeIndexes represents SHOW CREATE INDEXES FROM
	ShowCreateModeIndexes
	// ShowCreateModeSecondaryIndexes represents SHOW CREATE SECONDARY INDEXES FROM
	ShowCreateModeSecondaryIndexes
)

type ShowCreateFormatOption int

const (
	ShowCreateFormatOptionNone ShowCreateFormatOption = iota
	ShowCreateFormatOptionRedactedValues
)

// ShowCreate represents a SHOW CREATE statement.
type ShowCreate struct {
	Mode   ShowCreateMode
	Name   *UnresolvedObjectName
	FmtOpt ShowCreateFormatOption
}

// Format implements the NodeFormatter interface.
func (node *ShowCreate) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ")

	switch node.Mode {
	case ShowCreateModeDatabase:
		ctx.WriteString("DATABASE ")
	case ShowCreateModeIndexes:
		ctx.WriteString("INDEXES FROM ")
	case ShowCreateModeSecondaryIndexes:
		ctx.WriteString("SECONDARY INDEXES FROM ")
	}
	ctx.FormatNode(node.Name)

	switch node.FmtOpt {
	case ShowCreateFormatOptionRedactedValues:
		ctx.WriteString(" WITH REDACT")
	}
}

// ShowCreateAllSchemas represents a SHOW CREATE ALL SCHEMAS statement.
type ShowCreateAllSchemas struct{}

// Format implements the NodeFormatter interface.
func (node *ShowCreateAllSchemas) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ALL SCHEMAS")
}

// ShowCreateAllTables represents a SHOW CREATE ALL TABLES statement.
type ShowCreateAllTables struct{}

// Format implements the NodeFormatter interface.
func (node *ShowCreateAllTables) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ALL TABLES")
}

// ShowCreateAllTypes represents a SHOW CREATE ALL TYPES statement.
type ShowCreateAllTypes struct{}

// Format implements the NodeFormatter interface.
func (node *ShowCreateAllTypes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ALL TYPES")
}

// ShowCreateSchedules represents a SHOW CREATE SCHEDULE statement.
type ShowCreateSchedules struct {
	ScheduleID Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateSchedules) Format(ctx *FmtCtx) {
	if node.ScheduleID != nil {
		ctx.WriteString("SHOW CREATE SCHEDULE ")
		ctx.FormatNode(node.ScheduleID)
		return
	}
	ctx.Printf("SHOW CREATE ALL SCHEDULES")
}

// ShowSyntax represents a SHOW SYNTAX statement.
// This the most lightweight thing that can be done on a statement
// server-side: just report the statement that was entered without
// any processing. Meant for use for syntax checking on clients,
// when the client version might differ from the server.
type ShowSyntax struct {
	Statement string
}

// Format implements the NodeFormatter interface.
func (node *ShowSyntax) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SYNTAX ")
	if ctx.flags.HasFlags(FmtAnonymize) || ctx.flags.HasFlags(FmtHideConstants) {
		ctx.WriteString("'_'")
	} else {
		ctx.WriteString(lexbase.EscapeSQLString(node.Statement))
	}
}

// ShowTransactionStatus represents a SHOW TRANSACTION STATUS statement.
type ShowTransactionStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowTransactionStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRANSACTION STATUS")
}

// ShowLastQueryStatistics represents a SHOW LAST QUERY STATS statement.
type ShowLastQueryStatistics struct {
	Columns NameList
}

// ShowLastQueryStatisticsDefaultColumns is the default list of columns
// when the USING clause is not specified.
// Note: the form that does not specify the USING clause is deprecated.
// Remove it when there are no more clients using it (22.1 or later).
var ShowLastQueryStatisticsDefaultColumns = NameList([]Name{
	"parse_latency",
	"plan_latency",
	"exec_latency",
	"service_latency",
	"post_commit_jobs_latency",
})

// Format implements the NodeFormatter interface.
func (node *ShowLastQueryStatistics) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW LAST QUERY STATISTICS RETURNING ")
	// The column names for this statement never contain PII and should
	// be distinguished for feature tracking purposes.
	ctx.WithFlags(ctx.flags & ^FmtAnonymize & ^FmtMarkRedactionNode, func() {
		ctx.FormatNode(&node.Columns)
	})
}

// ShowFullTableScans represents a SHOW FULL TABLE SCANS statement.
type ShowFullTableScans struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowFullTableScans) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW FULL TABLE SCANS")
}

// ShowSavepointStatus represents a SHOW SAVEPOINT STATUS statement.
type ShowSavepointStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowSavepointStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SAVEPOINT STATUS")
}

// ShowUsers represents a SHOW USERS statement.
type ShowUsers struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowUsers) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW USERS")
}

// ShowDefaultSessionVariablesForRole represents a SHOW DEFAULT SESSION VARIABLES FOR ROLE <name> statement.
type ShowDefaultSessionVariablesForRole struct {
	Name   RoleSpec
	IsRole bool
	All    bool
}

// Format implements the NodeFormatter interface.
func (node *ShowDefaultSessionVariablesForRole) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DEFAULT SESSION VARIABLES FOR")
	if node.IsRole {
		ctx.WriteString(" ROLE ")
	} else {
		ctx.WriteString(" USER ")
	}
	if node.All {
		ctx.WriteString("ALL")
	} else {
		ctx.FormatNode(&node.Name)
	}
}

// ShowRoles represents a SHOW ROLES statement.
type ShowRoles struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowRoles) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ROLES")
}

// ShowRanges represents a SHOW RANGES statement.
type ShowRanges struct {
	DatabaseName Name
	TableOrIndex TableIndexName
	Options      *ShowRangesOptions
	Source       ShowRangesSource
}

// ShowRangesSource represents the source of a SHOW RANGES statement.
type ShowRangesSource int8

const (
	// SHOW RANGES FROM CURRENT_CATALOG
	ShowRangesCurrentDatabase ShowRangesSource = iota
	// SHOW RANGES FROM DATABASE
	ShowRangesDatabase
	// SHOW RANGES FROM TABLE
	ShowRangesTable
	// SHOW RANGES FROM INDEX
	ShowRangesIndex
	// SHOW CLUSTER RANGES
	ShowRangesCluster
)

// ShowRangesOptions represents the WITH clause in SHOW RANGES.
type ShowRangesOptions struct {
	Details bool
	Explain bool
	Keys    bool
	Mode    ShowRangesMode
}

// ShowRangesMode represents the WITH clause in SHOW RANGES.
type ShowRangesMode int8

const (
	// UniqueRanges tells to use just 1 row per range in the output.
	//
	// Note: The UniqueRanges constant must have value 0; otherwise,
	// the parsing logic would become incorrect.
	UniqueRanges ShowRangesMode = iota
	// ExpandTables requests one row per table in the output.
	ExpandTables
	// ExpandIndexes requests one row per index in the output.
	ExpandIndexes
)

// Format implements the NodeFormatter interface.
func (node *ShowRanges) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Source == ShowRangesCluster {
		ctx.WriteString("CLUSTER ")
	}
	ctx.WriteString("RANGES")
	if node.Source != ShowRangesCluster {
		ctx.WriteString(" FROM ")
		switch node.Source {
		case ShowRangesCurrentDatabase:
			ctx.WriteString("CURRENT_CATALOG")
		case ShowRangesDatabase:
			ctx.WriteString("DATABASE ")
			ctx.FormatNode(&node.DatabaseName)
		case ShowRangesIndex:
			ctx.WriteString("INDEX ")
			ctx.FormatNode(&node.TableOrIndex)
		case ShowRangesTable:
			ctx.WriteString("TABLE ")
			ctx.FormatNode(&node.TableOrIndex)
		}
	}
	ctx.FormatNode(node.Options)
}

// Format implements the NodeFormatter interface.
func (node *ShowRangesOptions) Format(ctx *FmtCtx) {
	noOpts := ShowRangesOptions{}
	if *node == noOpts {
		return
	}
	ctx.WriteString(" WITH ")
	comma := ""
	if node.Details {
		ctx.WriteString("DETAILS")
		comma = ", "
	}
	if node.Keys {
		ctx.WriteString(comma)
		ctx.WriteString("KEYS")
		comma = ", "
	}
	if node.Explain {
		ctx.WriteString(comma)
		ctx.WriteString("EXPLAIN")
		comma = ", "
	}
	if node.Mode != UniqueRanges {
		ctx.WriteString(comma)
		switch node.Mode {
		case ExpandTables:
			ctx.WriteString("TABLES")
		case ExpandIndexes:
			ctx.WriteString("INDEXES")
		}
	}
}

// ShowRangeForRow represents a SHOW RANGE FOR ROW statement.
type ShowRangeForRow struct {
	TableOrIndex TableIndexName
	Row          Exprs
}

// Format implements the NodeFormatter interface.
func (node *ShowRangeForRow) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RANGE FROM ")
	if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
	} else {
		ctx.WriteString("TABLE ")
	}
	ctx.FormatNode(&node.TableOrIndex)
	ctx.WriteString(" FOR ROW (")
	ctx.FormatNode(&node.Row)
	ctx.WriteString(")")
}

// ShowFingerprints represents a SHOW EXPERIMENTAL_FINGERPRINTS statement.
type ShowFingerprints struct {
	TenantSpec *TenantSpec
	Table      *UnresolvedObjectName

	Options ShowFingerprintOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowFingerprints) Format(ctx *FmtCtx) {
	if node.Table != nil {
		ctx.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ")
		ctx.FormatNode(node.Table)
	} else {
		ctx.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER ")
		ctx.FormatNode(node.TenantSpec)
	}

	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}

// ShowFingerprintOptions describes options for the SHOW EXPERIMENTAL_FINGERPINT
// execution.
type ShowFingerprintOptions struct {
	StartTimestamp      Expr
	ExcludedUserColumns StringOrPlaceholderOptList
}

func (s *ShowFingerprintOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}

	if s.StartTimestamp != nil {
		maybeAddSep()
		ctx.WriteString("START TIMESTAMP = ")
		_, canOmitParentheses := s.StartTimestamp.(alreadyDelimitedAsSyntacticDExpr)
		if !canOmitParentheses {
			ctx.WriteByte('(')
		}
		ctx.FormatNode(s.StartTimestamp)
		if !canOmitParentheses {
			ctx.WriteByte(')')
		}
	}
	if s.ExcludedUserColumns != nil {
		maybeAddSep()
		ctx.WriteString("EXCLUDE COLUMNS = ")
		s.ExcludedUserColumns.Format(ctx)
	}
}

// CombineWith merges other TenantReplicationOptions into this struct.
// An error is returned if the same option merged multiple times.
func (s *ShowFingerprintOptions) CombineWith(other *ShowFingerprintOptions) error {
	if s.StartTimestamp != nil {
		if other.StartTimestamp != nil {
			return errors.New("START TIMESTAMP option specified multiple times")
		}
	} else {
		s.StartTimestamp = other.StartTimestamp
	}

	var err error
	s.ExcludedUserColumns, err = combineStringOrPlaceholderOptList(s.ExcludedUserColumns, other.ExcludedUserColumns, "excluded_user_columns")
	if err != nil {
		return err
	}
	return nil
}

// IsDefault returns true if this backup options struct has default value.
func (s ShowFingerprintOptions) IsDefault() bool {
	options := ShowFingerprintOptions{}
	return s.StartTimestamp == options.StartTimestamp && cmp.Equal(s.ExcludedUserColumns, options.ExcludedUserColumns)
}

var _ NodeFormatter = &ShowFingerprintOptions{}

// ShowTableStats represents a SHOW STATISTICS FOR TABLE statement.
type ShowTableStats struct {
	Table     *UnresolvedObjectName
	UsingJSON bool
	Options   KVOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowTableStats) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW STATISTICS ")
	if node.UsingJSON {
		ctx.WriteString("USING JSON ")
	}
	ctx.WriteString("FOR TABLE ")
	ctx.FormatNode(node.Table)
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}

// ShowTenantOptions represents the WITH clause in SHOW VIRTUAL CLUSTER.
type ShowTenantOptions struct {
	WithReplication      bool
	WithPriorReplication bool
	WithCapabilities     bool
}

// ShowTenant represents a SHOW VIRTUAL CLUSTER statement.
type ShowTenant struct {
	TenantSpec *TenantSpec
	ShowTenantOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowTenant) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW VIRTUAL CLUSTER ")
	ctx.FormatNode(node.TenantSpec)

	withs := []string{}
	if node.WithReplication {
		withs = append(withs, "REPLICATION STATUS")
	}
	if node.WithPriorReplication {
		withs = append(withs, "PRIOR REPLICATION DETAILS")
	}
	if node.WithCapabilities {
		withs = append(withs, "CAPABILITIES")
	}
	if len(withs) > 0 {
		ctx.WriteString(" WITH ")
		ctx.WriteString(strings.Join(withs, ", "))
	}
}

// ShowLogicalReplicationJobsOptions represents the WITH clause in SHOW LOGICAL REPLICATION JOBS.
type ShowLogicalReplicationJobsOptions struct {
	WithDetails bool
}

// ShowLogicalReplicationJobs represents a SHOW LOGICAL REPLICATION JOBS statement.
type ShowLogicalReplicationJobs struct {
	ShowLogicalReplicationJobsOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowLogicalReplicationJobs) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW LOGICAL REPLICATION JOBS")

	if node.WithDetails {
		ctx.WriteString(" WITH DETAILS")
	}
}

// ShowHistogram represents a SHOW HISTOGRAM statement.
type ShowHistogram struct {
	HistogramID int64
}

// Format implements the NodeFormatter interface.
func (node *ShowHistogram) Format(ctx *FmtCtx) {
	ctx.Printf("SHOW HISTOGRAM %d", node.HistogramID)
}

// ShowPartitions represents a SHOW PARTITIONS statement.
type ShowPartitions struct {
	IsDB     bool
	Database Name

	IsIndex bool
	Index   TableIndexName

	IsTable bool
	Table   *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowPartitions) Format(ctx *FmtCtx) {
	if node.IsDB {
		ctx.Printf("SHOW PARTITIONS FROM DATABASE ")
		ctx.FormatNode(&node.Database)
	} else if node.IsIndex {
		ctx.Printf("SHOW PARTITIONS FROM INDEX ")
		ctx.FormatNode(&node.Index)
	} else {
		ctx.Printf("SHOW PARTITIONS FROM TABLE ")
		ctx.FormatNode(node.Table)
	}
}

// ShowPolicies represents a SHOW POLICIES statement.
type ShowPolicies struct {
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowPolicies) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW POLICIES FOR ")
	ctx.FormatNode(node.Table)
}

var _ Statement = &ShowPolicies{}

// ScheduledJobExecutorType is a type identifying the names of
// the supported scheduled job executors.
type ScheduledJobExecutorType int

const (
	// InvalidExecutor is a placeholder for an invalid executor type.
	InvalidExecutor ScheduledJobExecutorType = iota

	// ScheduledBackupExecutor is an executor responsible for
	// the execution of the scheduled backups.
	ScheduledBackupExecutor

	// ScheduledSQLStatsCompactionExecutor is an executor responsible for the
	// execution of the scheduled SQL Stats compaction.
	ScheduledSQLStatsCompactionExecutor

	// ScheduledRowLevelTTLExecutor is an executor responsible for the cleanup
	// of rows on row level TTL tables.
	ScheduledRowLevelTTLExecutor

	// ScheduledSchemaTelemetryExecutor is an executor responsible for the logging
	// of schema telemetry.
	ScheduledSchemaTelemetryExecutor

	// ScheduledChangefeedExecutor is an executor responsible for
	// the execution of the scheduled changefeeds.
	ScheduledChangefeedExecutor
)

var scheduleExecutorInternalNames = map[ScheduledJobExecutorType]string{
	InvalidExecutor:                     "unknown-executor",
	ScheduledBackupExecutor:             "scheduled-backup-executor",
	ScheduledSQLStatsCompactionExecutor: "scheduled-sql-stats-compaction-executor",
	ScheduledRowLevelTTLExecutor:        "scheduled-row-level-ttl-executor",
	ScheduledSchemaTelemetryExecutor:    "scheduled-schema-telemetry-executor",
	ScheduledChangefeedExecutor:         "scheduled-changefeed-executor",
}

// InternalName returns an internal executor name.
// This name can be used to filter matching schedules.
func (t ScheduledJobExecutorType) InternalName() string {
	return scheduleExecutorInternalNames[t]
}

// UserName returns a user friendly executor name.
func (t ScheduledJobExecutorType) UserName() string {
	switch t {
	case ScheduledBackupExecutor:
		return "BACKUP"
	case ScheduledSQLStatsCompactionExecutor:
		return "SQL STATISTICS"
	case ScheduledRowLevelTTLExecutor:
		return "ROW LEVEL TTL"
	case ScheduledSchemaTelemetryExecutor:
		return "SCHEMA TELEMETRY"
	case ScheduledChangefeedExecutor:
		return "CHANGEFEED"
	}
	return "unsupported-executor"
}

// ScheduleState describes what kind of schedules to display
type ScheduleState int

const (
	// SpecifiedSchedules indicates that show schedules should
	// only show subset of schedules.
	SpecifiedSchedules ScheduleState = iota

	// ActiveSchedules indicates that show schedules should
	// only show those schedules that are currently active.
	ActiveSchedules

	// PausedSchedules indicates that show schedules should
	// only show those schedules that are currently paused.
	PausedSchedules
)

// Format implements the NodeFormatter interface.
func (s ScheduleState) Format(ctx *FmtCtx) {
	switch s {
	case ActiveSchedules:
		ctx.WriteString("RUNNING")
	case PausedSchedules:
		ctx.WriteString("PAUSED")
	default:
		// Nothing
	}
}

// ShowSchedules represents a SHOW SCHEDULES statement.
type ShowSchedules struct {
	WhichSchedules ScheduleState
	ExecutorType   ScheduledJobExecutorType
	ScheduleID     Expr
}

var _ Statement = &ShowSchedules{}

// Format implements the NodeFormatter interface.
func (n *ShowSchedules) Format(ctx *FmtCtx) {
	if n.ScheduleID != nil {
		ctx.WriteString("SHOW SCHEDULE ")
		ctx.FormatNode(n.ScheduleID)
		return
	}
	ctx.Printf("SHOW")

	if n.WhichSchedules != SpecifiedSchedules {
		ctx.WriteString(" ")
		ctx.FormatNode(&n.WhichSchedules)
	}

	ctx.Printf(" SCHEDULES")

	if n.ExecutorType != InvalidExecutor {
		// TODO(knz): beware of using ctx.FormatNode here if
		// FOR changes to support expressions.
		ctx.Printf(" FOR %s", n.ExecutorType.UserName())
	}
}

// ShowDefaultPrivileges represents a SHOW DEFAULT PRIVILEGES statement.
type ShowDefaultPrivileges struct {
	Roles       RoleSpecList
	ForAllRoles bool
	ForGrantee  bool
	// If Schema is not specified, SHOW DEFAULT PRIVILEGES is being
	// run on the current database.
	Schema Name
}

var _ Statement = &ShowDefaultPrivileges{}

// Format implements the NodeFormatter interface.
func (n *ShowDefaultPrivileges) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DEFAULT PRIVILEGES ")
	if len(n.Roles) > 0 {
		if n.ForGrantee {
			ctx.WriteString("FOR GRANTEE ")
		} else {
			ctx.WriteString("FOR ROLE ")
		}

		for i := range n.Roles {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(&n.Roles[i])
		}
		ctx.WriteString(" ")
	} else if n.ForAllRoles {
		ctx.WriteString("FOR ALL ROLES ")
	}
	if n.Schema != Name("") {
		ctx.WriteString("IN SCHEMA ")
		ctx.FormatNode(&n.Schema)
	}
}

// ShowTransferState represents a SHOW TRANSFER STATE statement.
type ShowTransferState struct {
	TransferKey *StrVal
}

// Format implements the NodeFormatter interface.
func (node *ShowTransferState) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRANSFER STATE")
	if node.TransferKey != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(node.TransferKey)
	}
}

// ShowCompletions represents a SHOW COMPLETIONS statement.
type ShowCompletions struct {
	Statement *StrVal
	Offset    *NumVal
}

// Format implements the NodeFormatter interface.
func (s ShowCompletions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW COMPLETIONS AT OFFSET ")
	s.Offset.Format(ctx)
	ctx.WriteString(" FOR ")
	ctx.FormatNode(s.Statement)
}

var _ Statement = &ShowCompletions{}

// ShowCreateRoutine represents a SHOW CREATE FUNCTION or SHOW CREATE PROCEDURE
// statement.
type ShowCreateRoutine struct {
	Name      ResolvableFunctionReference
	Procedure bool
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateRoutine) Format(ctx *FmtCtx) {
	if node.Procedure {
		ctx.WriteString("SHOW CREATE PROCEDURE ")
	} else {
		ctx.WriteString("SHOW CREATE FUNCTION ")
	}
	ctx.FormatNode(&node.Name)
}

var _ Statement = &ShowCreateRoutine{}

// ShowCreateExternalConnections represents a SHOW CREATE EXTERNAL CONNECTION
// statement.
type ShowCreateExternalConnections struct {
	ConnectionLabel Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateExternalConnections) Format(ctx *FmtCtx) {
	if node.ConnectionLabel != nil {
		ctx.WriteString("SHOW CREATE EXTERNAL CONNECTION ")
		ctx.FormatNode(node.ConnectionLabel)
		return
	}
	ctx.Printf("SHOW CREATE ALL EXTERNAL CONNECTIONS")
}

var _ Statement = &ShowCreateExternalConnections{}

// ShowCommitTimestamp represents a SHOW COMMIT TIMESTAMP statement.
//
// If the current session is in an open transaction state, this statement will
// implicitly commit the underlying kv transaction and return the HLC timestamp
// at which it committed. The transaction state machine will be left in a state
// such that only COMMIT or RELEASE cockroach_savepoint; COMMIT are acceptable.
// The statement may also be sent after RELEASE cockroach_savepoint; and before
// COMMIT.
//
// If the current session is not in an open transaction state, this statement
// will return the commit timestamp of the previous transaction, assuming there
// was one.
type ShowCommitTimestamp struct{}

func (s ShowCommitTimestamp) Format(ctx *FmtCtx) {
	ctx.Printf("SHOW COMMIT TIMESTAMP")
}

var _ Statement = (*ShowCommitTimestamp)(nil)

// ShowExternalConnections represents a SHOW EXTERNAL CONNECTIONS statement.
type ShowExternalConnections struct {
	ConnectionLabel Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowExternalConnections) Format(ctx *FmtCtx) {
	if node.ConnectionLabel != nil {
		ctx.WriteString("SHOW EXTERNAL CONNECTION ")
		ctx.FormatNode(node.ConnectionLabel)
		return
	}
	ctx.Printf("SHOW EXTERNAL CONNECTIONS")
}

var _ Statement = &ShowExternalConnections{}

// ShowTriggers represents a SHOW TRIGGERS statement.
type ShowTriggers struct {
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowTriggers) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRIGGERS FROM ")
	ctx.FormatNode(node.Table)
}

var _ Statement = &ShowTriggers{}

// ShowCreateTrigger represents a SHOW CREATE TRIGGER statement.
type ShowCreateTrigger struct {
	Name      Name
	TableName *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateTrigger) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE TRIGGER ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.TableName)
}

var _ Statement = &ShowCreateTrigger{}
