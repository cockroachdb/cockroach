// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package auditlogging

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/olekukonko/tablewriter"
)

// Auditor is an interface used to check and build different audit events.
type Auditor interface {
	GetQualifiedTableNameByID(ctx context.Context, id int64, requiredType tree.RequiredTableKind) (*tree.TableName, error)
	Txn() *kv.Txn
	AuditConfig() *AuditConfigLock
}

// AuditEventBuilder is the interface used to build different audit events.
type AuditEventBuilder interface {
	BuildAuditEvent(
		context.Context,
		Auditor,
		eventpb.CommonSQLEventDetails,
		eventpb.CommonSQLExecDetails,
	) logpb.EventPayload
}

// Define special statement types "ALL" and "NONE"
const (
	AuditAllStatementsConst tree.StatementType = -1
	AuditNoneStatementConst tree.StatementType = -2
)

// allUserRole is a special role value for an audit setting, it designates that
// the audit setting applies to all users.
const allUserRole = "all"

type AuditConfigLock struct {
	syncutil.RWMutex
	Config *AuditConfig
}

// AuditConfig is a parsed configuration.
type AuditConfig struct {
	// Settings are the collection of AuditSettings that make up the AuditConfig.
	Settings []AuditSetting
	// AllRoleAuditSettingIdx is an index corresponding to an AuditSetting in Settings that applies to all
	// users, if it exists. Default value -1 (DefaultAllAuditSettingIdx).
	AllRoleAuditSettingIdx int
}

const DefaultAllAuditSettingIdx = -1

func EmptyAuditConfig() *AuditConfig {
	return &AuditConfig{
		AllRoleAuditSettingIdx: DefaultAllAuditSettingIdx,
	}
}

// GetMatchingAuditSetting checks if any user's roles match any roles configured in the audit config.
// Returns the first matching AuditSetting.
func (c AuditConfig) GetMatchingAuditSetting(
	userRoles map[username.SQLUsername]bool,
) *AuditSetting {
	// If the user matches any Setting, return the corresponding filter.
	for idx, filter := range c.Settings {
		// If we have matched an audit setting by role, return the audit setting.
		_, exists := userRoles[filter.Role]
		if exists {
			return &filter
		}
		// If we have reached an audit setting that applies to all roles, return the audit setting.
		if idx == c.AllRoleAuditSettingIdx {
			return &filter
		}
	}
	// No filter found.
	return nil
}

func (c AuditConfig) String() string {
	if len(c.Settings) == 0 {
		return "# (empty configuration)\n"
	}

	var sb strings.Builder
	sb.WriteString("# Original configuration:\n")
	for _, setting := range c.Settings {
		fmt.Fprintf(&sb, "# %s\n", setting.Input)
	}
	sb.WriteString("#\n# Interpreted configuration:\n")

	table := tablewriter.NewWriter(&sb)
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(false)
	table.SetNoWhiteSpace(true)
	table.SetTrimWhiteSpaceAtEOL(true)
	table.SetTablePadding(" ")

	row := []string{"# ROLE", "STATEMENT_TYPE"}
	table.Append(row)
	for _, setting := range c.Settings {
		row[0] = setting.Role.Normalized()
		row[1] = strings.Join(writeStatementTypes(setting.StatementTypes), ",")
		table.Append(row)
	}
	table.Render()
	return sb.String()
}

func writeStatementTypes(vals []tree.StatementType) []string {
	var stmtTypes []string
	for _, stmtType := range vals {
		switch stmtType {
		case AuditAllStatementsConst:
			stmtTypes = append(stmtTypes,
				tree.TypeDDL.String(),
				tree.TypeDML.String(),
				tree.TypeDCL.String(),
			)
		case AuditNoneStatementConst:
			stmtTypes = append(stmtTypes, "NONE")
		default:
			stmtTypes = append(stmtTypes, stmtType.String())
		}
	}
	return stmtTypes
}

// AuditSetting is a single rule in the audit logging configuration.
type AuditSetting struct {
	// Input is the original configuration line in the audit logging configuration string.
	Input string

	Role           username.SQLUsername
	StatementTypes []tree.StatementType
}

func (s AuditSetting) String() string {
	return AuditConfig{Settings: []AuditSetting{s}}.String()
}

func (s AuditSetting) CheckMatchingStatementType(currStmtType tree.StatementType) bool {
	// If we are auditing all statement types, return true.
	if s.HasAllStatementType() {
		return true
	}
	// If we are auditing no statement types, return false.
	if s.HasNoneStatementType() {
		return false
	}
	// Check if the given statement matches the audit setting's statement type(s).
	for _, stmtType := range s.StatementTypes {
		if currStmtType == stmtType {
			return true
		}
	}
	return false
}

func (s AuditSetting) HasAllStatementType() bool {
	// If we are auditing all statement types, return true.
	if len(s.StatementTypes) == 1 && s.StatementTypes[0] == AuditAllStatementsConst {
		return true
	}
	return false
}

func (s AuditSetting) HasNoneStatementType() bool {
	// If we are auditing all statement types, return true.
	if len(s.StatementTypes) == 1 && s.StatementTypes[0] == AuditNoneStatementConst {
		return true
	}
	return false
}
