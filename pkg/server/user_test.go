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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	_, err := sqlDB.Exec(fmt.Sprintf("CREATE USER %s", fooUser))
	require.NoError(t, err)

	for name := range roleoption.ByName {
		hasRole, err := s.(*TestServer).status.baseStatusServer.privilegeChecker.hasRoleOption(ctx, fooUser, roleoption.ByName[name])
		require.NoError(t, err)
		require.Equal(t, false, hasRole)
	}

	for name := range roleoption.ByName {
		// Skip PASSWORD and DEFAULTSETTINGS options.
		// Since PASSWORD still resides in system.users and DEFAULTSETTINGS is stored in system.database_role_settings.
		if name == "PASSWORD" || name == "DEFAULTSETTINGS" {
			continue
		}

		extraInfo := ""
		if name == "VALID UNTIL" {
			extraInfo = " '3000-01-01'"
		}
		_, err := sqlDB.Exec(fmt.Sprintf("ALTER USER %s %s%s", fooUser, name, extraInfo))
		require.NoError(t, err)

		hasRole, err := s.(*TestServer).status.baseStatusServer.privilegeChecker.hasRoleOption(ctx, fooUser, roleoption.ByName[name])
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

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	testServer, sqlDB, _ := serverutils.StartServer(t, params)
	defer testServer.Stopper().Stop(ctx)

	conn, err := testServer.RPCContext().GRPCDialNode(
		testServer.RPCAddr(), testServer.NodeID(), rpc.DefaultClass,
	).Connect(ctx)
	require.NoError(t, err)
	client := serverpb.NewStatusClient(conn)
	userName, err := userFromContext(ctx)
	require.NoError(t, err)

	// No roles added to the user.
	res, err := client.UserSQLRoles(ctx, &serverpb.UserSQLRolesRequest{})
	require.NoError(t, err)
	if len(res.Roles) != 0 {
		t.Errorf("Expected 0 roles, but got %v", res.Roles)
	}

	// One role added to the user.
	_, err = sqlDB.Exec(fmt.Sprintf("ALTER USER %s VIEWACTIVITY", userName))
	require.NoError(t, err)
	res, err = client.UserSQLRoles(ctx, &serverpb.UserSQLRolesRequest{})
	require.NoError(t, err)
	expRoles := []string{"VIEWACTIVITY"}
	require.Equal(t, expRoles, res.Roles)

	// Two roles added to the user.
	_, err = sqlDB.Exec(fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", userName))
	require.NoError(t, err)
	res, err = client.UserSQLRoles(ctx, &serverpb.UserSQLRolesRequest{})
	require.NoError(t, err)
	expRoles = []string{"VIEWACTIVITY", "VIEWACTIVITYREDACTED"}
	require.Equal(t, expRoles, res.Roles)

	// Remove one role.
	_, err = sqlDB.Exec(fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", userName))
	require.NoError(t, err)
	res, err = client.UserSQLRoles(ctx, &serverpb.UserSQLRolesRequest{})
	require.NoError(t, err)
	expRoles = []string{"VIEWACTIVITYREDACTED"}
	require.Equal(t, expRoles, res.Roles)
}
