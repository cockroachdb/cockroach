// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

const (
	// Role is used when the syntax used is the ROLE version (ie. CREATE ROLE).
	Role = "role"
	// User is used when the syntax used is the USER version (ie. CREATE USER).
	User = "user"

	// AlterRole is used when an ALTER ROLE / USER is the operation.
	AlterRole = "alter"
	// CreateRole is used when an CREATE ROLE / USER is the operation.
	CreateRole = "create"
	// OnDatabase is used when a GRANT/REVOKE is happening on a database.
	OnDatabase = "on_database"
	// OnSchema is used when a GRANT/REVOKE is happening on a schema.
	OnSchema = "on_schema"
	// OnTable is used when a GRANT/REVOKE is happening on a table.
	OnTable = "on_table"
	// OnType is used when a GRANT/REVOKE is happening on a type.
	OnType = "on_type"

	iamRoles = "iam.roles"
)

// IncIAMOptionCounter is to be incremented every time a CREATE/ALTER role
// with an OPTION (ie. NOLOGIN) happens.
func IncIAMOptionCounter(opName string, option string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, opName, option)))
}

// IncIAMCreateCounter is to be incremented every time a CREATE ROLE happens.
func IncIAMCreateCounter(typ string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "create", typ)))
}

// IncIAMAlterCounter is to be incremented every time an ALTER ROLE happens.
func IncIAMAlterCounter(typ string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "alter", typ)))
}

// IncIAMDropCounter is to be incremented every time a DROP ROLE happens.
func IncIAMDropCounter(typ string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "drop", typ)))
}

// IncIAMGrantCounter is to be incremented every time a GRANT ROLE happens.
func IncIAMGrantCounter(withAdmin bool) {
	var s string
	if withAdmin {
		s = fmt.Sprintf("%s.%s.with_admin", iamRoles, "grant")
	} else {
		s = fmt.Sprintf("%s.%s", iamRoles, "grant")
	}
	telemetry.Inc(telemetry.GetCounter(s))
}

// IncIAMRevokeCounter is to be incremented every time a REVOKE ROLE happens.
func IncIAMRevokeCounter(withAdmin bool) {
	var s string
	if withAdmin {
		s = fmt.Sprintf("%s.%s.with_admin", iamRoles, "revoke")
	} else {
		s = fmt.Sprintf("%s.%s", iamRoles, "revoke")
	}
	telemetry.Inc(telemetry.GetCounter(s))
}

// IncIAMGrantPrivilegesCounter is to be incremented every time a GRANT <privileges> happens.
func IncIAMGrantPrivilegesCounter(on string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s.%s", iamRoles, "grant", "privileges", on)))
}

// IncIAMRevokePrivilegesCounter is to be incremented every time a REVOKE <privileges> happens.
func IncIAMRevokePrivilegesCounter(on string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s.%s", iamRoles, "revoke", "privileges", on)))
}

// TurnConnAuditingOnUseCounter counts how many time connection audit logs were enabled.
var TurnConnAuditingOnUseCounter = telemetry.GetCounterOnce("auditing.connection.enabled")

// TurnConnAuditingOffUseCounter counts how many time connection audit logs were disabled.
var TurnConnAuditingOffUseCounter = telemetry.GetCounterOnce("auditing.connection.disabled")

// TurnAuthAuditingOnUseCounter counts how many time connection audit logs were enabled.
var TurnAuthAuditingOnUseCounter = telemetry.GetCounterOnce("auditing.authentication.enabled")

// TurnAuthAuditingOffUseCounter counts how many time connection audit logs were disabled.
var TurnAuthAuditingOffUseCounter = telemetry.GetCounterOnce("auditing.authentication.disabled")
