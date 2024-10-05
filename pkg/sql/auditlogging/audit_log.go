// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auditlogging

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/olekukonko/tablewriter"
)

// ConfigureRoleBasedAuditClusterSettings is a noop global var injected by CCL hook.
// See corresponding ConfigureRoleBasedAuditClusterSettings in auditloggingccl.
var ConfigureRoleBasedAuditClusterSettings = func(ctx context.Context, acl *AuditConfigLock, st *cluster.Settings, sv *settings.Values) {
}

// UserAuditEnabled is a noop global var injected by CCL hook.
var UserAuditEnabled = func(st *cluster.Settings) bool {
	return false
}

// UserAuditReducedConfigEnabled is a noop global var injected by CCL hook.
var UserAuditReducedConfigEnabled = func(sv *settings.Values) bool {
	return false
}

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

// ReducedAuditConfig is a computed audit configuration initialized at the first SQL event emit by the user.
type ReducedAuditConfig struct {
	syncutil.RWMutex
	Initialized  bool
	AuditSetting *AuditSetting
}

// GetMatchingAuditSetting returns the first audit setting in the cluster setting
// configuration that matches the user's username/roles. If no such audit setting
// exists, returns nil.
func (cl *AuditConfigLock) GetMatchingAuditSetting(
	userRoles map[username.SQLUsername]bool, name username.SQLUsername,
) *AuditSetting {
	cl.RLock()
	defer cl.RUnlock()

	// Get matching audit setting.
	return cl.Config.getMatchingAuditSetting(userRoles, name)
}

// AuditConfig is a parsed configuration.
type AuditConfig struct {
	// Settings are the collection of AuditSettings that make up the AuditConfig.
	Settings []AuditSetting
	// allRoleAuditSettingIdx is an index corresponding to an AuditSetting in Settings that applies to all
	// users, if it exists. Default value -1 (defaultAllAuditSettingIdx).
	allRoleAuditSettingIdx int
}

const defaultAllAuditSettingIdx = -1

// EmptyAuditConfig returns an audit configuration with no audit Settings.
func EmptyAuditConfig() *AuditConfig {
	return &AuditConfig{
		allRoleAuditSettingIdx: defaultAllAuditSettingIdx,
	}
}

// getMatchingAuditSetting checks if any user's roles match any roles configured in the audit config.
// Returns the first matching AuditSetting.
func (c AuditConfig) getMatchingAuditSetting(
	userRoles map[username.SQLUsername]bool, name username.SQLUsername,
) *AuditSetting {
	// If the user matches any Setting, return the corresponding filter.
	for idx, filter := range c.Settings {
		// If we have matched an audit setting by role, return the audit setting.
		_, exists := userRoles[filter.Role]
		if exists {
			return &filter
		}
		// If we have matched an audit setting by the user's name, return the audit setting.
		if filter.Role == name {
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
	if len(c.Settings) == 0 {
		return "# (empty configuration)\n"
	}

	var sb strings.Builder
	sb.WriteString("# Original configuration:\n")
	for _, setting := range c.Settings {
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

	row := []string{"# ROLE", "STATEMENT_FILTER"}
	table.Append(row)
	for _, setting := range c.Settings {
		row[0] = setting.Role.Normalized()
		row[1] = writeStatementFilter(setting.IncludeStatements)
		table.Append(row)
	}
	table.Render()
	return sb.String()
}

func writeStatementFilter(includeStmts bool) string {
	if includeStmts {
		return "ALL"
	}
	return "NONE"
}

// AuditSetting is a single rule in the audit logging configuration.
type AuditSetting struct {
	// input is the original configuration line in the audit logging configuration string.
	input string
	// Role is user/role this audit setting applies for.
	Role username.SQLUsername
	// IncludeStatements designates whether we audit all statements for this audit setting.
	// If false, this audit setting will *exclude* statements for this audit setting from emitting
	// an audit event.
	IncludeStatements bool
}

func (s AuditSetting) String() string {
	return AuditConfig{Settings: []AuditSetting{s}}.String()
}
