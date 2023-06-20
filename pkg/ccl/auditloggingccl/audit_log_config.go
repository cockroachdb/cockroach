// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package auditloggingccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const auditConfigDefaultValue = ""

// UserAuditLogConfig is a cluster setting that takes a user/role-based audit configuration.
var UserAuditLogConfig = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.log.user_audit",
	"user/role-based audit logging configuration",
	auditConfigDefaultValue,
	validateAuditLogConfig,
).WithPublic()

// UserAuditEnableReducedConfig is a cluster setting that enables/disables a computed
// reduced configuration. This allows us to compute the audit configuration once at
// session start, instead of computing at each SQL event. The tradeoff is that changes to
// the audit configuration (user role memberships or cluster setting configuration) are not
// reflected within session. Users will need to start a new session to see these changes in their
// auditing behaviour.
var UserAuditEnableReducedConfig = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.log.user_audit.reduced_config.enabled",
	"enables logic to compute a reduced audit configuration, computing the audit "+
		"configuration only once at session start instead of at each SQL event. The tradeoff "+
		"with the increase in performance (~5%), is that changes to the audit configuration "+
		"(user role memberships/cluster setting) are not reflected within session. "+
		"Users will need to start a new session to see these changes in their auditing behaviour.",
	false,
).WithPublic()

func validateAuditLogConfig(_ *settings.Values, input string) error {
	if input == auditConfigDefaultValue {
		// Empty config
		return nil
	}
	_, enterpriseCheckErr := auditlogging.ReadEnterpriseParamsHook()
	if enterpriseCheckErr != nil {
		return pgerror.Wrap(enterpriseCheckErr,
			pgcode.InsufficientPrivilege, "role-based audit logging requires enterprise license")
	}
	// Ensure it can be parsed.
	conf, err := auditlogging.Parse(input)
	if err != nil {
		return err
	}
	if len(conf.Settings) == 0 {
		// The string was not empty, but we were unable to parse any settings.
		return errors.WithHint(errors.New("no entries"),
			"To use the default configuration, assign the empty string ('').")
	}
	return nil
}

// UpdateAuditConfigOnChange initializes the local
// node's audit configuration each time the cluster setting
// is updated.
func UpdateAuditConfigOnChange(
	ctx context.Context, acl *auditlogging.AuditConfigLock, st *cluster.Settings,
) {
	val := UserAuditLogConfig.Get(&st.SV)
	config, err := auditlogging.Parse(val)
	if err != nil {
		// We encounter an error parsing (i.e. invalid config), fallback
		// to an empty config.
		log.Ops.Warningf(ctx, "invalid audit log config (sql.log.user_audit): %v\n"+
			"falling back to empty audit config", err)
		config = auditlogging.EmptyAuditConfig()
	}
	acl.Lock()
	acl.Config = config
	acl.Unlock()
}

var ConfigureRoleBasedAuditClusterSettings = func(ctx context.Context, acl *auditlogging.AuditConfigLock, st *cluster.Settings, sv *settings.Values) {
	UserAuditLogConfig.SetOnChange(
		sv, func(ctx context.Context) {
			UpdateAuditConfigOnChange(ctx, acl, st)
		})
	UpdateAuditConfigOnChange(ctx, acl, st)
}

var UserAuditLogConfigEmpty = func(sv *settings.Values) bool {
	return UserAuditLogConfig.Get(sv) == ""
}

var UserAuditReducedConfigEnabled = func(sv *settings.Values) bool {
	return UserAuditEnableReducedConfig.Get(sv)
}

func init() {
	auditlogging.ConfigureRoleBasedAuditClusterSettings = ConfigureRoleBasedAuditClusterSettings
	auditlogging.UserAuditLogConfigEmpty = UserAuditLogConfigEmpty
	auditlogging.UserAuditReducedConfigEnabled = UserAuditReducedConfigEnabled
}
