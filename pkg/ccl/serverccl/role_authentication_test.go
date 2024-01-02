// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestVerifyPassword(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ts := s.ApplicationLayer()

	if util.RaceEnabled {
		// The default bcrypt cost makes this test approximately 30s slower when the
		// race detector is on.
		security.BcryptCost.Override(ctx, &ts.ClusterSettings().SV, int64(bcrypt.MinCost))
	}

	//location is used for timezone testing.
	shanghaiLoc, err := timeutil.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}

	for _, user := range []struct {
		username         string
		password         string
		loginFlag        string
		validUntilClause string
		qargs            []interface{}
	}{
		{"azure_diamond", "hunter2", "LOGIN", "", nil},
		{"druidia", "12345", "LOGIN", "", nil},

		{"richardc", "12345", "NOLOGIN", "", nil},
		{"richardc2", "12345", "NOSQLLOGIN", "", nil},
		{"has_global_nosqlogin", "12345", "", "", nil},
		{"inherits_global_nosqlogin", "12345", "", "", nil},
		{"before_epoch", "12345", "", "VALID UNTIL '1969-01-01'", nil},
		{"epoch", "12345", "", "VALID UNTIL '1970-01-01'", nil},
		{"cockroach", "12345", "", "VALID UNTIL '2100-01-01'", nil},
		{"cthon98", "12345", "", "VALID UNTIL NULL", nil},

		{"toolate", "12345", "", "VALID UNTIL $1",
			[]interface{}{timeutil.Now().Add(-10 * time.Minute)}},
		{"timelord", "12345", "", "VALID UNTIL $1",
			[]interface{}{timeutil.Now().Add(59 * time.Minute).In(shanghaiLoc)}},

		{"replication_user", "12345", "LOGIN REPLICATION", "", nil},

		{"some_admin", "12345", "LOGIN", "", nil},
	} {
		cmd := fmt.Sprintf(
			"CREATE ROLE %s WITH PASSWORD '%s' %s %s",
			user.username, user.password, user.loginFlag, user.validUntilClause)

		if _, err := db.Exec(cmd, user.qargs...); err != nil {
			t.Fatalf("failed to create user: %s", err)
		}
	}

	if _, err := db.Exec("GRANT admin TO some_admin"); err != nil {
		t.Fatalf("failed to grant admin: %s", err)
	}

	_, err = db.Exec("ALTER USER replication_user REPLICATION")
	require.NoError(t, err)

	// Set up NOSQLLOGIN global privilege.
	_, err = db.Exec("GRANT SYSTEM NOSQLLOGIN TO has_global_nosqlogin")
	require.NoError(t, err)
	_, err = db.Exec("GRANT has_global_nosqlogin TO inherits_global_nosqlogin")
	require.NoError(t, err)

	for _, tc := range []struct {
		testName                       string
		username                       string
		password                       string
		isSuperuser                    bool
		canUseReplicationMode          bool
		shouldAuthenticateSQL          bool
		shouldAuthenticateDBConsole    bool
		shouldHaveReplicateModeEnabled bool
	}{
		{
			testName:                       "valid login",
			username:                       "azure_diamond",
			password:                       "hunter2",
			shouldAuthenticateSQL:          true,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "wrong password",
			username:                       "azure_diamond",
			password:                       "hunter",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "empty password",
			username:                       "azure_diamond",
			password:                       "",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "wrong emoji password",
			username:                       "azure_diamond",
			password:                       "🍦",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},

		{
			testName:                       "correct password with suffix should fail",
			username:                       "azure_diamond",
			password:                       "hunter2345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "correct password with prefix should fail",
			username:                       "azure_diamond",
			password:                       "shunter2",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},

		{
			testName:                       "wrong password all numeric",
			username:                       "azure_diamond",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "wrong password all stars",
			username:                       "azure_diamond",
			password:                       "*******",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "valid login numeric password",
			username:                       "druidia",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          true,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "wrong password matching other user",
			username:                       "druidia",
			password:                       "hunter2",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "root with empty password should fail",
			username:                       "root",
			password:                       "",
			isSuperuser:                    true,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: true,
		},
		{
			testName:                       "some_admin with empty password should fail",
			username:                       "some_admin",
			password:                       "",
			isSuperuser:                    true,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: true,
		},
		{
			testName:                       "empty username and password should fail",
			username:                       "",
			password:                       "",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "username does not exist should fail",
			username:                       "doesntexist",
			password:                       "zxcvbn",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with NOLOGIN role option should fail",
			username:                       "richardc",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		// The NOSQLLOGIN cases are the only cases where SQL and DB Console login outcomes differ.
		{
			testName:                       "user with NOSQLLOGIN role option should fail SQL but succeed on DB Console",
			username:                       "richardc2",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with NOSQLLOGIN global privilege should fail SQL but succeed on DB Console",
			username:                       "has_global_nosqlogin",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user who inherits NOSQLLOGIN global privilege should fail SQL but succeed on DB Console",
			username:                       "inherits_global_nosqlogin",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "replication user should succeed",
			username:                       "replication_user",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          true,
			shouldAuthenticateSQL:          true,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: true,
		},
		{
			testName:                       "user with VALID UNTIL before the Unix epoch should fail",
			username:                       "before_epoch",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with VALID UNTIL at Unix epoch should fail",
			username:                       "epoch",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with VALID UNTIL future date should succeed",
			username:                       "cockroach",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          true,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with VALID UNTIL 10 minutes ago should fail",
			username:                       "toolate",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          false,
			shouldAuthenticateDBConsole:    false,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with VALID UNTIL future time in Shanghai time zone should succeed",
			username:                       "timelord",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          true,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
		{
			testName:                       "user with VALID UNTIL NULL should succeed",
			username:                       "cthon98",
			password:                       "12345",
			isSuperuser:                    false,
			canUseReplicationMode:          false,
			shouldAuthenticateSQL:          true,
			shouldAuthenticateDBConsole:    true,
			shouldHaveReplicateModeEnabled: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
			username := username.MakeSQLUsernameFromPreNormalizedString(tc.username)
			exists, canLoginSQL, canLoginDBConsole, canUseReplicationMode, isSuperuser, _, pwRetrieveFn, err := sql.GetUserSessionInitInfo(
				context.Background(), &execCfg, username, "", /* databaseName */
			)

			if err != nil {
				t.Errorf(
					"credentials %s/%s failed with error %s, wanted no error",
					tc.username,
					tc.password,
					err,
				)
			}

			valid := true
			validDBConsole := true
			expired := false

			if !exists || !canLoginSQL {
				valid = false
			}

			if !exists || !canLoginDBConsole {
				validDBConsole = false
			}
			if exists && (canLoginSQL || canLoginDBConsole) {
				var hashedPassword password.PasswordHash
				expired, hashedPassword, err = pwRetrieveFn(ctx)
				if err != nil {
					t.Errorf(
						"credentials %s/%s failed with error %s, wanted no error",
						tc.username,
						tc.password,
						err,
					)
				}

				pwCompare, err := password.CompareHashAndCleartextPassword(
					ctx,
					hashedPassword,
					tc.password,
					security.GetExpensiveHashComputeSem(ctx),
				)
				if err != nil {
					t.Error(err)
					valid = false
					validDBConsole = false
				}
				if !pwCompare {
					valid = false
					validDBConsole = false
				}
			}

			if valid && !expired != tc.shouldAuthenticateSQL {
				t.Errorf(
					"sql credentials %s/%s valid = %t, wanted %t",
					tc.username,
					tc.password,
					valid,
					tc.shouldAuthenticateSQL,
				)
			}
			if validDBConsole && !expired != tc.shouldAuthenticateDBConsole {
				t.Errorf(
					"db console credentials %s/%s valid = %t, wanted %t",
					tc.username,
					tc.password,
					validDBConsole,
					tc.shouldAuthenticateDBConsole,
				)
			}
			require.Equal(t, tc.isSuperuser, isSuperuser)
			require.Equal(t, tc.shouldHaveReplicateModeEnabled, canUseReplicationMode)
		})
	}
}
