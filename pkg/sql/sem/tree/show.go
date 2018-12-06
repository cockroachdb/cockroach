// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/lex"

// ShowVar represents a SHOW statement.
type ShowVar struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *ShowVar) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	// Session var names never contain PII and should be distinguished
	// for feature tracking purposes.
	deAnonCtx := *ctx
	deAnonCtx.flags &= ^FmtAnonymize
	deAnonCtx.FormatNameP(&node.Name)
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
	deAnonCtx := *ctx
	deAnonCtx.flags &= ^FmtAnonymize
	deAnonCtx.FormatNameP(&node.Name)
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
)

// ShowBackup represents a SHOW BACKUP statement.
type ShowBackup struct {
	Path    Expr
	Details BackupDetails
}

// Format implements the NodeFormatter interface.
func (node *ShowBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW BACKUP ")
	if node.Details == BackupRangeDetails {
		ctx.WriteString("RANGES ")
	} else if node.Details == BackupFileDetails {
		ctx.WriteString("FILES ")
	}
	ctx.FormatNode(node.Path)
}

// ShowColumns represents a SHOW COLUMNS statement.
type ShowColumns struct {
	Table TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowColumns) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW COLUMNS FROM ")
	ctx.FormatNode(&node.Table)
}

// ShowDatabases represents a SHOW DATABASES statement.
type ShowDatabases struct{}

// Format implements the NodeFormatter interface.
func (node *ShowDatabases) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW DATABASES")
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

// ShowIndex represents a SHOW INDEX statement.
type ShowIndex struct {
	Table TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW INDEXES FROM ")
	ctx.FormatNode(&node.Table)
}

// ShowQueries represents a SHOW QUERIES statement
type ShowQueries struct {
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowQueries) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
	if node.Cluster {
		ctx.WriteString("CLUSTER QUERIES")
	} else {
		ctx.WriteString("LOCAL QUERIES")
	}
}

// ShowJobs represents a SHOW JOBS statement
type ShowJobs struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowJobs) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW JOBS")
}

// ShowSessions represents a SHOW SESSIONS statement
type ShowSessions struct {
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowSessions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW ")
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

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	TableNamePrefix
}

// Format implements the NodeFormatter interface.
func (node *ShowTables) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TABLES")
	if node.ExplicitSchema {
		ctx.WriteString(" FROM ")
		ctx.FormatNode(&node.TableNamePrefix)
	}
}

// ShowConstraints represents a SHOW CONSTRAINTS statement.
type ShowConstraints struct {
	Table TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowConstraints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CONSTRAINTS FROM ")
	ctx.FormatNode(&node.Table)
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

// ShowCreate represents a SHOW CREATE statement.
type ShowCreate struct {
	Name TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreate) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW CREATE ")
	ctx.FormatNode(&node.Name)
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
	ctx.WriteString(lex.EscapeSQLString(node.Statement))
}

// ShowTransactionStatus represents a SHOW TRANSACTION STATUS statement.
type ShowTransactionStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowTransactionStatus) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TRANSACTION STATUS")
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

// ShowRanges represents a SHOW EXPERIMENTAL_RANGES statement.
// Only one of Table and Index can be set.
type ShowRanges struct {
	Table *TableName
	Index *TableNameWithIndex
}

// Format implements the NodeFormatter interface.
func (node *ShowRanges) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW EXPERIMENTAL_RANGES FROM ")
	if node.Index != nil {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(node.Index)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(node.Table)
	}
}

// ShowFingerprints represents a SHOW EXPERIMENTAL_FINGERPRINTS statement.
type ShowFingerprints struct {
	Table TableName
}

// Format implements the NodeFormatter interface.
func (node *ShowFingerprints) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ")
	ctx.FormatNode(&node.Table)
}

// ShowTableStats represents a SHOW STATISTICS FOR TABLE statement.
type ShowTableStats struct {
	Table     TableName
	UsingJSON bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTableStats) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW STATISTICS ")
	if node.UsingJSON {
		ctx.WriteString("USING JSON ")
	}
	ctx.WriteString("FOR TABLE ")
	ctx.FormatNode(&node.Table)
}

// ShowHistogram represents a SHOW HISTOGRAM statement.
type ShowHistogram struct {
	HistogramID int64
}

// Format implements the NodeFormatter interface.
func (node *ShowHistogram) Format(ctx *FmtCtx) {
	ctx.Printf("SHOW HISTOGRAM %d", node.HistogramID)
}
