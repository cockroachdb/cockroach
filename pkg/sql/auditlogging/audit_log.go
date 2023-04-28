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

// allUserRole is a special role value for an audit setting, it designates that
// the audit setting applies to all users.
const allUserRole = "all"

// AuditConfigLock is a mutex wrapper around AuditConfig, to provide safety
// with concurrent usage.
type AuditConfigLock struct {
	syncutil.RWMutex
	Config *AuditConfig
}

// AuditConfig is a parsed configuration.
type AuditConfig struct {
	// settings are the collection of AuditSettings that make up the AuditConfig.
	settings []AuditSetting
	// allRoleAuditSettingIdx is an index corresponding to an AuditSetting in settings that applies to all
	// users, if it exists. Default value -1 (defaultAllAuditSettingIdx).
	allRoleAuditSettingIdx int
}

const defaultAllAuditSettingIdx = -1

func EmptyAuditConfig() *AuditConfig {
	return &AuditConfig{
		allRoleAuditSettingIdx: defaultAllAuditSettingIdx,
	}
}

// GetMatchingAuditSetting checks if any user's roles match any roles configured in the audit config.
// Returns the first matching AuditSetting.
func (c AuditConfig) GetMatchingAuditSetting(
	userRoles map[username.SQLUsername]bool,
) *AuditSetting {
	// If the user matches any Setting, return the corresponding filter.
	for idx, filter := range c.settings {
		// If we have matched an audit setting by role, return the audit setting.
		_, exists := userRoles[filter.Role]
		if exists {
			return &filter
		}
		// If we have reached an audit setting that applies to all roles, return the audit setting.
		if idx == c.allRoleAuditSettingIdx {
			return &filter
		}
	}
	// No filter found.
	return nil
}

func (c AuditConfig) String() string {
	if len(c.settings) == 0 {
		return "# (empty configuration)\n"
	}

	var sb strings.Builder
	sb.WriteString("# Original configuration:\n")
	for _, setting := range c.settings {
		fmt.Fprintf(&sb, "# %s\n", setting.input)
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
	for _, setting := range c.settings {
		row[0] = setting.Role.Normalized()
		row[1] = strings.Join(writeStatementTypes(setting.StatementTypes), ",")
		table.Append(row)
	}
	table.Render()
	return sb.String()
}

func writeStatementTypes(vals map[tree.StatementType]int) []string {
	if len(vals) == 0 {
		return []string{"NONE"}
	}
	stmtTypes := make([]string, len(vals))
	// Assign statement types in the order they were input.
	for stmtType := range vals {
		stmtTypes[vals[stmtType]] = stmtType.String()
	}
	return stmtTypes
}

// AuditSetting is a single rule in the audit logging configuration.
type AuditSetting struct {
	// input is the original configuration line in the audit logging configuration string.
	input string
	// Role is user/role this audit setting applies for.
	Role username.SQLUsername
	// StatementTypes is a mapping of statement type to the index/order it was input in the config.
	// The order is used so we can print the setting's statement types in the same order it was
	StatementTypes map[tree.StatementType]int
}

func (s AuditSetting) String() string {
	return AuditConfig{settings: []AuditSetting{s}}.String()
}
