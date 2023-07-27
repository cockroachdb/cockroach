// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestValidRoles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	ctx := context.Background()
	fooUser := username.MakeSQLUsernameFromPreNormalizedString("foo")
	_, err := sqlDB.Exec(fmt.Sprintf("CREATE USER %s", fooUser))
	require.NoError(t, err)

	for name := range roleoption.ByName {
		// Test user without the role.
		hasRole, err := s.(*TestServer).status.baseStatusServer.privilegeChecker.hasRoleOption(ctx, fooUser, roleoption.ByName[name])
		require.NoError(t, err)
		require.Equal(t, false, hasRole)

		// Skip PASSWORD and DEFAULTSETTINGS options.
		// Since PASSWORD still resides in system.users and
		// DEFAULTSETTINGS is stored in system.database_role_settings.
		if name == "PASSWORD" || name == "DEFAULTSETTINGS" {
			continue
		}
		// Add the role and check if the role was added (or in the cases of roles starting
		// with NO, that the value is not there.
		extraInfo := ""
		if name == "VALID UNTIL" {
			extraInfo = " '3000-01-01'"
		}
		_, err = sqlDB.Exec(fmt.Sprintf("ALTER USER %s %s%s", fooUser, name, extraInfo))
		require.NoError(t, err)

		hasRole, err = s.(*TestServer).status.baseStatusServer.privilegeChecker.hasRoleOption(ctx, fooUser, roleoption.ByName[name])
		require.NoError(t, err)

		expectedHasRole := true
		if strings.HasPrefix(name, "NO") || name == "LOGIN" || name == "SQLLOGIN" {
			expectedHasRole = false
		}
		if name == "NOLOGIN" || name == "NOSQLLOGIN" {
			expectedHasRole = true
		}
		require.Equal(t, expectedHasRole, hasRole)
	}
}

func TestSQLRolesAPI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	db := sqlutils.MakeSQLRunner(sqlDB)
	var res serverpb.UserSQLRolesResponse

	// Admin user.
	expRoles := []string{"ADMIN"}
	err := getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, true)
	require.NoError(t, err)
	require.ElementsMatch(t, expRoles, res.Roles)

	// No roles added to a non-admin user.
	expRoles = []string{}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	require.NoError(t, err)
	require.ElementsMatch(t, expRoles, res.Roles)

	// Role option and global privilege added to the non-admin user.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	db.Exec(t, fmt.Sprintf("GRANT SYSTEM MODIFYCLUSTERSETTING TO %s", authenticatedUserNameNoAdmin().Normalized()))
	expRoles = []string{"MODIFYCLUSTERSETTING", "VIEWACTIVITY"}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	require.NoError(t, err)
	require.ElementsMatch(t, expRoles, res.Roles)

	// Two role options and two global privileges added to the non-admin user.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", authenticatedUserNameNoAdmin().Normalized()))
	db.Exec(t, fmt.Sprintf("GRANT SYSTEM CANCELQUERY TO %s", authenticatedUserNameNoAdmin().Normalized()))
	expRoles = []string{"CANCELQUERY", "MODIFYCLUSTERSETTING", "VIEWACTIVITY", "VIEWACTIVITYREDACTED"}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	sort.Strings(res.Roles)
	require.NoError(t, err)
	require.ElementsMatch(t, expRoles, res.Roles)

	// Remove one role option and one global privilege from non-admin user.
	db.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	db.Exec(t, fmt.Sprintf("REVOKE SYSTEM MODIFYCLUSTERSETTING FROM %s", authenticatedUserNameNoAdmin().Normalized()))
	expRoles = []string{"CANCELQUERY", "VIEWACTIVITYREDACTED"}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	require.NoError(t, err)
	require.ElementsMatch(t, expRoles, res.Roles)
}
