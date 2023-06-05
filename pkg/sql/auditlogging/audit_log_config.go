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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// UserAuditLogConfig is a cluster setting that takes a user/role-based audit configuration.
var UserAuditLogConfig = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
	"sql.log.user_audit",
	"user/role-based audit logging configuration",
	"",
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
	if input == "" {
		// Empty config
		return nil
	}
	// Ensure it can be parsed.
	conf, err := parse(input)
	if err != nil {
		return err
	}
	if len(conf.settings) == 0 {
		// The string was not empty, but we were unable to parse any settings.
		return errors.WithHint(errors.New("no entries"),
			"To use the default configuration, assign the empty string ('').")
	}
	return nil
}

// UpdateAuditConfigOnChange initializes the local
// node's audit configuration each time the cluster setting
// is updated.
func UpdateAuditConfigOnChange(ctx context.Context, acl *AuditConfigLock, st *cluster.Settings) {
	val := UserAuditLogConfig.Get(&st.SV)
	config, err := parse(val)
	if err != nil {
		// We encounter an error parsing (i.e. invalid config), fallback
		// to an empty config.
		log.Ops.Warningf(ctx, "invalid audit log config (sql.log.user_audit): %v\n"+
			"falling back to empty audit config", err)
		config = EmptyAuditConfig()
	}
	acl.Lock()
	acl.Config = config
	acl.Unlock()
}
