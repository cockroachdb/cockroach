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

// Role is used when the syntax used is the ROLE version (ie. CREATE ROLE).
const Role = "role"

// User is used when the syntax used is the USER version (ie. CREATE USER).
const User = "user"

// AlterRole is used when an ALTER ROLE / USER is the operation.
const AlterRole = "alter"

// CreateRole is used when an CREATE ROLE / USER is the operation.
const CreateRole = "create"

// OnDatabase is used when a GRANT/REVOKE is happening on a database.
const OnDatabase = "on_database"

// OnTable is used when a GRANT/REVOKE is happening on a table.
const OnTable = "on_table"

const iamRoles = "iam.roles"

// IncIAMOption is to be incremented every time a CREATE/ALTER role
// with an OPTION (ie. NOLOGIN) happens.
func IncIAMOption(opName string, option string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, opName, option)))
}

// IncIAMCreate is to be incremented every time a CREATE ROLE happens.
func IncIAMCreate(typ string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "create", typ)))
}

// IAMAlter is to be incremented every time an ALTER ROLE happens.
func IAMAlter(typ string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "alter", typ)))
}

// IncIAMDrop is to be incremented every time a DROP ROLE happens.
func IncIAMDrop(typ string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "drop", typ)))
}

// IncIAMGrant is to be incremented every time a GRANT ROLE happens.
func IncIAMGrant(withAdmin bool) {
	var s string
	if withAdmin {
		s = fmt.Sprintf("%s.%s.with_admin", iamRoles, "grant")
	} else {
		s = fmt.Sprintf("%s.%s", iamRoles, "grant")
	}
	telemetry.Inc(telemetry.GetCounter(s))
}

// IncIAMRevoke is to be incremented every time a REVOKE ROLE happens.
func IncIAMRevoke(withAdmin bool) {
	var s string
	if withAdmin {
		s = fmt.Sprintf("%s.%s.with_admin", iamRoles, "revoke")
	} else {
		s = fmt.Sprintf("%s.%s", iamRoles, "revoke")
	}
	telemetry.Inc(telemetry.GetCounter(s))
}

// IncIAMGrantPrivileges is to be incremented every time a GRANT <privileges> happens.
func IncIAMGrantPrivileges(on string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s.%s", iamRoles, "grant", "privileges", on)))
}

// IncIAMRevokePrivileges is to be incremented every time a REVOKE <privileges> happens.
func IncIAMRevokePrivileges(on string) {
	telemetry.Inc(telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s.%s", iamRoles, "revoke", "privileges", on)))
}
