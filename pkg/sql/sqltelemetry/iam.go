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

const iamRoles = "iam.roles"

// IAMOption is to be incremented every time a CREATE/ALTER role
// with an OPTION (ie. NOLOGIN) happens.
func IAMOption(opName string, option string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, opName, option))
}

// IAMCreate is to be incremented every time a CREATE ROLE happens.
func IAMCreate(typ string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "create", typ))
}

// IAMAlter is to be incremented every time an ALTER ROLE happens.
func IAMAlter(typ string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "alter", typ))
}

// IAMDrop is to be incremented every time a DROP ROLE happens.
func IAMDrop(typ string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "drop", typ))
}

// IAMGrant is to be incremented every time a GRANT ROLE happens.
func IAMGrant(withAdmin string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s%s", iamRoles, "grant", withAdmin))
}

// IAMRevoke is to be incremented every time a REVOKE ROLE happens.
func IAMRevoke(withAdmin string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s%s", iamRoles, "revoke", withAdmin))
}

// IAMGrantPrivileges is to be incremented every time a GRANT <privileges> happens.
func IAMGrantPrivileges(on string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s.%s", iamRoles, "grant", "privileges", on))
}

// IAMRevokePrivileges is to be incremented every time a REVOKE <privileges> happens.
func IAMRevokePrivileges(on string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s.%s", iamRoles, "revoke", "privileges", on))
}

// IAMShow is to be incremented every time a SHOW (ROLES/USERS/GRANTS) happens.
func IAMShow(typ string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("%s.%s.%s", iamRoles, "show", typ))
}
