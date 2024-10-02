// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging/auditevents"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func (p *planner) maybeAuditSensitiveTableAccessEvent(
	privilegeObject privilege.Object, priv privilege.Kind,
) {
	// Check if we can add this event.
	tableDesc, ok := privilegeObject.(catalog.TableDescriptor)
	if !ok || tableDesc.GetAuditMode() == descpb.TableDescriptor_DISABLED {
		return
	}
	writing := false
	switch priv {
	case privilege.INSERT, privilege.DELETE, privilege.UPDATE:
		writing = true
	}
	p.curPlan.auditEventBuilders = append(p.curPlan.auditEventBuilders,
		&auditevents.SensitiveTableAccessEvent{TableDesc: tableDesc, Writing: writing},
	)
}

func (p *planner) maybeAuditRoleBasedAuditEvent(ctx context.Context, execType executorType) {
	// Avoid doing audit work if not necessary.
	if p.shouldNotRoleBasedAudit(execType) {
		return
	}

	// Use reduced audit config is enabled.
	if auditlogging.UserAuditReducedConfigEnabled(&p.execCfg.Settings.SV) {
		p.logReducedAuditConfig(ctx)
		return
	}

	user := p.User()
	userRoles, err := func() (map[username.SQLUsername]bool, error) {
		if p.Txn() != nil {
			return p.MemberOfWithAdminOption(ctx, user)
		} else {
			// If there is no open transaction, then we need to create an internal
			// one. This is the case for logging BEGIN statements, since the
			// transaction is not opened until after BEGIN is logged.
			var userRoles map[username.SQLUsername]bool
			innerErr := p.ExecCfg().InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
				var err error
				userRoles, err = MemberOfWithAdminOption(ctx, p.ExecCfg(), txn, user)
				return err
			})
			return userRoles, innerErr
		}
	}()
	if err != nil {
		log.Errorf(ctx, "RoleBasedAuditEvent: error getting user role memberships: %v", err)
		return
	}

	// Get matching audit setting.
	auditSetting := p.AuditConfig().GetMatchingAuditSetting(userRoles, user)
	// No matching setting, return early.
	if auditSetting == nil {
		return
	}

	if auditSetting.IncludeStatements {
		p.curPlan.auditEventBuilders = append(p.curPlan.auditEventBuilders,
			&auditevents.RoleBasedAuditEvent{
				Role: auditSetting.Role.Normalized(),
			},
		)
	}
}

func (p *planner) logReducedAuditConfig(ctx context.Context) {
	if !p.reducedAuditConfig.Initialized {
		p.initializeReducedAuditConfig(ctx)
	}

	// Return early if no matching audit setting was found.
	if p.reducedAuditConfig.AuditSetting == nil {
		return
	}

	if p.reducedAuditConfig.AuditSetting.IncludeStatements {
		p.curPlan.auditEventBuilders = append(p.curPlan.auditEventBuilders,
			&auditevents.RoleBasedAuditEvent{
				Role: p.reducedAuditConfig.AuditSetting.Role.Normalized(),
			},
		)
	}
}

func (p *planner) initializeReducedAuditConfig(ctx context.Context) {
	p.reducedAuditConfig.Lock()
	defer p.reducedAuditConfig.Unlock()
	if p.reducedAuditConfig.Initialized {
		return
	}
	// Set the initialized flag to true, even for an attempt that errors.
	// We do this to avoid the potential overhead of continuously retrying
	// to fetch user role memberships.
	p.reducedAuditConfig.Initialized = true

	user := p.User()
	userRoles, err := func() (map[username.SQLUsername]bool, error) {
		if p.Txn() != nil {
			return p.MemberOfWithAdminOption(ctx, user)
		} else {
			// If there is no open transaction, then we need to create an internal
			// one. This is the case for logging BEGIN statements, since the
			// transaction is not opened until after BEGIN is logged.
			var userRoles map[username.SQLUsername]bool
			innerErr := p.ExecCfg().InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
				var err error
				userRoles, err = MemberOfWithAdminOption(ctx, p.ExecCfg(), txn, user)
				return err
			})
			return userRoles, innerErr
		}
	}()
	if err != nil {
		log.Errorf(ctx, "initialize reduced audit config: error getting user role memberships: %v", err)
		return
	}
	// Get matching audit setting.
	p.reducedAuditConfig.AuditSetting = p.AuditConfig().GetMatchingAuditSetting(userRoles, user)
}

// shouldNotRoleBasedAudit checks if we should do any auditing work for
// RoleBasedAuditEvents.
func (p *planner) shouldNotRoleBasedAudit(execType executorType) bool {
	// Do not emit audit events for internal executors.
	// Do not do audit work if role-based auditing is not enabled.
	// Do not emit audit events for reserved users/roles. This does not omit the
	// root user.
	return execType == executorTypeInternal ||
		!auditlogging.UserAuditEnabled(p.execCfg.Settings) ||
		p.User().IsReserved()
}
