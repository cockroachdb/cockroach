// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package auditevents

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

type RoleBasedAuditEvent struct {
	Setting        *auditlogging.AuditSetting
	StatementType  string
	DatabaseName   string
	ServerAddress  string
	RemoteAddress  string
	ConnectionType string
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
		Role:                  f.Setting.Role.Normalized(),
		StatementType:         f.StatementType,
		DatabaseName:          f.DatabaseName,
		ServerAddress:         f.ServerAddress,
		RemoteAddress:         f.RemoteAddress,
		ConnectionType:        f.ConnectionType,
	}
}
