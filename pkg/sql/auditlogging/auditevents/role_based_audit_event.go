// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auditevents

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// RoleBasedAuditEvent identifies query executions on roles that have been configured
// for auditing. See the 'sql.log.user_audit' cluster setting.
type RoleBasedAuditEvent struct {
	Role string
}

// BuildAuditEvent implements the auditlogging.AuditEventBuilder interface
func (f *RoleBasedAuditEvent) BuildAuditEvent(
	_ context.Context,
	_ auditlogging.Auditor,
	details eventpb.CommonSQLEventDetails,
	exec eventpb.CommonSQLExecDetails,
) logpb.EventPayload {
	return &eventpb.RoleBasedAuditEvent{
		CommonSQLEventDetails: details,
		CommonSQLExecDetails:  exec,
		Role:                  f.Role,
	}
}
