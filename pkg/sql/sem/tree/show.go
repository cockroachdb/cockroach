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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
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
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
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
	ctx.WithFlags(ctx.flags & ^FmtAnonymize, func() {
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

// BackupDetails represents the type of details to display for a SHOW BACKUP
// statement.
type BackupDetails int

const (
	// BackupDefaultDetails identifies a bare SHOW BACKUP statement.
	BackupDefaultDetails BackupDetails = iota
	// BackupRangeDetails identifies a SHOW BACKUP RANGES statement.
	BackupRangeDetails
	// BackupFileDetails identifies a SHOW BACKUP FILES statement.
	BackupFileDetails
	// BackupManifestAsJSON displays full backup manifest as json
	BackupManifestAsJSON
)

// ShowBackup represents a SHOW BACKUP statement.
type ShowBackup struct {
	Path                 Expr
	InCollection         Expr
	Details              BackupDetails
	ShouldIncludeSchemas bool
	Options              KVOptions
}

// Format implements the NodeFormatter interface.
func (node *ShowBackup) Format(ctx *FmtCtx) {
	if node.InCollection != nil && node.Path == nil {
		ctx.WriteString("SHOW BACKUPS IN ")
		ctx.FormatNode(node.InCollection)
		return
	}
	ctx.WriteString("SHOW BACKUP ")
	if node.Details == BackupRangeDetails {
		ctx.WriteString("RANGES ")
	} else if node.Details == BackupFileDetails {
		ctx.WriteString("FILES ")
	}
	if node.ShouldIncludeSchemas {
		ctx.WriteString("SCHEMAS ")
	}
	ctx.FormatNode(node.Path)
	if node.InCollection != nil {
		ctx.WriteString(" IN ")
		ctx.FormatNode(node.InCollection)
	}
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
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
type ShowTypes struct{}

// Format implements the NodeFormatter interface.
func (node *ShowTypes) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TYPES")
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
}

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

// ShowSurvivalGoal represents a SHOW REGIONS statement
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
)

// ShowRegions represents a SHOW REGIONS statement
type ShowRegions struct {
	ShowRegionsFrom ShowRegionsFrom
	DatabaseName    Name
}

// Format implements the NodeFormatter interface.
func (node *ShowRegions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW REGIONS")
	switch node.ShowRegionsFrom {
	case ShowRegionsFromDefault:
	case ShowRegionsFromAllDatabases:
		ctx.WriteString(" FROM ALL DATABASES")
	case ShowRegionsFromDatabase:
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
	Database Name
}

// Format implements the NodeFormatter interface.
func (node *ShowSchemas) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW SCHEMAS")
	if node.Database != "" {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.Database)
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
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowConstraints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CONSTRAINTS FROM ")
	ctx.FormatNode(node.Table)
}

// ShowGrants represents a SHOW GRANTS statement.
// TargetList is defined in grant.go.
type ShowGrants struct {
	Targets  *TargetList
	Grantees NameList
}

// Format implements the NodeFormatter interface.
func (node *ShowGrants) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW GRANTS")
	if node.Targets != nil {
		ctx.WriteString(" ON ")
		ctx.FormatNode(node.Targets)
	}
	if node.Grantees != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(&node.Grantees)
	}
}

// ShowRoleGrants represents a SHOW GRANTS ON ROLE statement.
type ShowRoleGrants struct {
	Roles    NameList
	Grantees NameList
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
)

// ShowCreate represents a SHOW CREATE statement.
type ShowCreate struct {
	Mode ShowCreateMode
	Name *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreate) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ")

	switch node.Mode {
	case ShowCreateModeDatabase:
		ctx.WriteString("DATABASE ")
	}
	ctx.FormatNode(node.Name)
}

// ShowCreateAllTables represents a SHOW CREATE ALL TABLES statement.
type ShowCreateAllTables struct{}

// Format implements the NodeFormatter interface.
func (node *ShowCreateAllTables) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ALL TABLES")
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
		ctx.WriteByte('_')
	} else {
		ctx.WriteString(lex.EscapeSQLString(node.Statement))
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
type ShowLastQueryStatistics struct{}

// Format implements the NodeFormatter interface.
func (node *ShowLastQueryStatistics) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW LAST QUERY STATISTICS")
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

// ShowRoles represents a SHOW ROLES statement.
type ShowRoles struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowRoles) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ROLES")
}

// ShowRanges represents a SHOW RANGES statement.
type ShowRanges struct {
	TableOrIndex TableIndexName
	DatabaseName Name
}

// Format implements the NodeFormatter interface.
func (node *ShowRanges) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RANGES FROM ")
	if node.DatabaseName != "" {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.DatabaseName)
	} else if node.TableOrIndex.Index != "" {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(&node.TableOrIndex)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(&node.TableOrIndex)
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
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowFingerprints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ")
	ctx.FormatNode(node.Table)
}

// ShowTableStats represents a SHOW STATISTICS FOR TABLE statement.
type ShowTableStats struct {
	Table     *UnresolvedObjectName
	UsingJSON bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTableStats) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW STATISTICS ")
	if node.UsingJSON {
		ctx.WriteString("USING JSON ")
	}
	ctx.WriteString("FOR TABLE ")
	ctx.FormatNode(node.Table)
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

// ScheduledJobExecutorType is a type identifying the names of
// the supported scheduled job executors.
type ScheduledJobExecutorType int

const (
	// InvalidExecutor is a placeholder for an invalid executor type.
	InvalidExecutor ScheduledJobExecutorType = iota

	// ScheduledBackupExecutor is an executor responsible for
	// the execution of the scheduled backups.
	ScheduledBackupExecutor
)

var scheduleExecutorInternalNames = map[ScheduledJobExecutorType]string{
	InvalidExecutor:         "unknown-executor",
	ScheduledBackupExecutor: "scheduled-backup-executor",
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
