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

import (
	"bytes"
	"fmt"
)

// Show represents a SHOW statement.
type Show struct {
	Name           string
	ClusterSetting bool
}

// Format implements the NodeFormatter interface.
func (node *Show) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW ")
	if node.ClusterSetting {
		buf.WriteString("CLUSTER SETTING ")
	}
	buf.WriteString(node.Name)
}

// ShowBackup represents a SHOW BACKUP statement.
type ShowBackup struct {
	Path Expr
}

// Format implements the NodeFormatter interface.
func (node *ShowBackup) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW BACKUP ")
	FormatNode(buf, f, node.Path)
}

// ShowColumns represents a SHOW COLUMNS statement.
type ShowColumns struct {
	Table NormalizableTableName
}

// Format implements the NodeFormatter interface.
func (node *ShowColumns) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW COLUMNS FROM ")
	FormatNode(buf, f, node.Table)
}

// ShowDatabases represents a SHOW DATABASES statement.
type ShowDatabases struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowDatabases) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW DATABASES")
}

// ShowTrace represents a SHOW SESSION TRACE statement.
type ShowTrace struct {
	Statement   Statement
	OnlyKVTrace bool
}

// Format implements the NodeFormatter interface.
func (node *ShowTrace) Format(buf *bytes.Buffer, f FmtFlags) {
	onlyKV := ""
	if node.OnlyKVTrace {
		onlyKV = " KV"
	}
	if node.Statement == nil {
		fmt.Fprintf(buf, "SHOW SESSION%s TRACE", onlyKV)
	} else {
		fmt.Fprintf(buf, "SHOW%s TRACE FOR ", onlyKV)
		FormatNode(buf, f, node.Statement)
	}
}

// ShowIndex represents a SHOW INDEX statement.
type ShowIndex struct {
	Table NormalizableTableName
}

// Format implements the NodeFormatter interface.
func (node *ShowIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW INDEXES FROM ")
	FormatNode(buf, f, node.Table)
}

// ShowQueries represents a SHOW QUERIES statement
type ShowQueries struct {
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowQueries) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW ")
	if node.Cluster {
		buf.WriteString("CLUSTER QUERIES")
	} else {
		buf.WriteString("LOCAL QUERIES")
	}
}

// ShowJobs represents a SHOW JOBS statement
type ShowJobs struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowJobs) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW JOBS")
}

// ShowSessions represents a SHOW SESSIONS statement
type ShowSessions struct {
	Cluster bool
}

// Format implements the NodeFormatter interface.
func (node *ShowSessions) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW ")
	if node.Cluster {
		buf.WriteString("CLUSTER SESSIONS")
	} else {
		buf.WriteString("LOCAL SESSIONS")
	}
}

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	Database Name
}

// ShowConstraints represents a SHOW CONSTRAINTS statement.
type ShowConstraints struct {
	Table NormalizableTableName
}

// Format implements the NodeFormatter interface.
func (node *ShowConstraints) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW CONSTRAINTS")
	if node.Table.TableNameReference != nil {
		buf.WriteString(" FROM ")
		FormatNode(buf, f, node.Table)
	}
}

// Format implements the NodeFormatter interface.
func (node *ShowTables) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW TABLES")
	if node.Database != "" {
		buf.WriteString(" FROM ")
		FormatNode(buf, f, node.Database)
	}
}

// ShowGrants represents a SHOW GRANTS statement.
// TargetList is defined in grant.go.
type ShowGrants struct {
	Targets  *TargetList
	Grantees NameList
}

// Format implements the NodeFormatter interface.
func (node *ShowGrants) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW GRANTS")
	if node.Targets != nil {
		buf.WriteString(" ON ")
		FormatNode(buf, f, node.Targets)
	}
	if node.Grantees != nil {
		buf.WriteString(" FOR ")
		FormatNode(buf, f, node.Grantees)
	}
}

// ShowCreateTable represents a SHOW CREATE TABLE statement.
type ShowCreateTable struct {
	Table NormalizableTableName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateTable) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW CREATE TABLE ")
	FormatNode(buf, f, node.Table)
}

// ShowCreateView represents a SHOW CREATE VIEW statement.
type ShowCreateView struct {
	View NormalizableTableName
}

// Format implements the NodeFormatter interface.
func (node *ShowCreateView) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW CREATE VIEW ")
	FormatNode(buf, f, node.View)
}

// ShowTransactionStatus represents a SHOW TRANSACTION STATUS statement.
type ShowTransactionStatus struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowTransactionStatus) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW TRANSACTION STATUS")
}

// ShowUsers represents a SHOW USERS statement.
type ShowUsers struct {
}

// Format implements the NodeFormatter interface.
func (node *ShowUsers) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW USERS")
}

// Help represents a HELP statement.
type Help struct {
	Name Name
}

// Format implements the NodeFormatter interface.
func (node *Help) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("HELP ")
	FormatNode(buf, f, node.Name)
}

// ShowRanges represents a SHOW TESTING_RANGES statement.
// Only one of Table and Index can be set.
type ShowRanges struct {
	Table *NormalizableTableName
	Index *TableNameWithIndex
}

// Format implements the NodeFormatter interface.
func (node *ShowRanges) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW TESTING_RANGES FROM ")
	if node.Index != nil {
		buf.WriteString("INDEX ")
		FormatNode(buf, f, node.Index)
	} else {
		buf.WriteString("TABLE ")
		FormatNode(buf, f, node.Table)
	}
}

// ShowFingerprints represents a SHOW EXPERIMENTAL_FINGERPRINTS statement.
type ShowFingerprints struct {
	Table *NormalizableTableName
	AsOf  AsOfClause
}

// Format implements the NodeFormatter interface.
func (node *ShowFingerprints) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ")
	FormatNode(buf, f, node.Table)
	if node.AsOf.Expr != nil {
		buf.WriteString(" ")
		FormatNode(buf, f, node.AsOf)
	}
}
