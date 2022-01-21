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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, expRoles, res.Roles)

	// No roles added to a non-admin user.
	expRoles = []string{}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	require.NoError(t, err)
	require.Equal(t, expRoles, res.Roles)

	// One role added to the non-admin user.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	expRoles = []string{"VIEWACTIVITY"}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	require.NoError(t, err)
	require.Equal(t, expRoles, res.Roles)

	// Two roles added to the non-admin user.
	db.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", authenticatedUserNameNoAdmin().Normalized()))
	expRoles = []string{"VIEWACTIVITY", "VIEWACTIVITYREDACTED"}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	sort.Strings(res.Roles)
	require.NoError(t, err)
	require.Equal(t, expRoles, res.Roles)

	// Remove one role from non-admin user.
	db.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", authenticatedUserNameNoAdmin().Normalized()))
	expRoles = []string{"VIEWACTIVITYREDACTED"}
	err = getStatusJSONProtoWithAdminOption(s, "sqlroles", &res, false)
	require.NoError(t, err)
	require.Equal(t, expRoles, res.Roles)
}
