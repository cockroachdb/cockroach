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
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/crypto/bcrypt"
)

func TestVerifyPassword(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ie := sql.MakeInternalExecutor(
		context.Background(),
		s.(*server.TestServer).Server.PGServer().SQLServer,
		sql.MemoryMetrics{},
		s.ExecutorConfig().(sql.ExecutorConfig).Settings,
	)

	if util.RaceEnabled {
		// The default bcrypt cost makes this test approximately 30s slower when the
		// race detector is on.
		defer func(prev int) { security.BcryptCost = prev }(security.BcryptCost)
		security.BcryptCost = bcrypt.MinCost
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
		{"before_epoch", "12345", "", "VALID UNTIL '1969-01-01'", nil},
		{"epoch", "12345", "", "VALID UNTIL '1970-01-01'", nil},
		{"cockroach", "12345", "", "VALID UNTIL '2100-01-01'", nil},
		{"cthon98", "12345", "", "VALID UNTIL NULL", nil},

		{"toolate", "12345", "", "VALID UNTIL $1",
			[]interface{}{timeutil.Now().Add(-10 * time.Minute)}},
		{"timelord", "12345", "", "VALID UNTIL $1",
			[]interface{}{timeutil.Now().Add(59 * time.Minute).In(shanghaiLoc)}},
	} {
		cmd := fmt.Sprintf(
			"CREATE ROLE %s WITH PASSWORD '%s' %s %s",
			user.username, user.password, user.loginFlag, user.validUntilClause)

		if _, err := db.Exec(cmd, user.qargs...); err != nil {
			t.Fatalf("failed to create user: %s", err)
		}
	}

	for _, tc := range []struct {
		username           string
		password           string
		shouldAuthenticate bool
		expectedErrString  string
	}{
		{"azure_diamond", "hunter2", true, ""},
		{"Azure_Diamond", "hunter2", false, ""},
		{"azure_diamond", "hunter", false, "crypto/bcrypt"},
		{"azure_diamond", "", false, "crypto/bcrypt"},
		{"azure_diamond", "🍦", false, "crypto/bcrypt"},
		{"azure_diamond", "hunter2345", false, "crypto/bcrypt"},
		{"azure_diamond", "shunter2", false, "crypto/bcrypt"},
		{"azure_diamond", "12345", false, "crypto/bcrypt"},
		{"azure_diamond", "*******", false, "crypto/bcrypt"},
		{"druidia", "12345", true, ""},
		{"druidia", "hunter2", false, "crypto/bcrypt"},
		{"root", "", false, "crypto/bcrypt"},
		{"", "", false, "does not exist"},
		{"doesntexist", "zxcvbn", false, "does not exist"},

		{"richardc", "12345", false,
			"richardc does not have login privilege"},
		{"before_epoch", "12345", false, ""},
		{"epoch", "12345", false, ""},
		{"cockroach", "12345", true, ""},
		{"toolate", "12345", false, ""},
		{"timelord", "12345", true, ""},
		{"cthon98", "12345", true, ""},
	} {
		t.Run("", func(t *testing.T) {
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			username := security.MakeSQLUsernameFromPreNormalizedString(tc.username)
			exists, canLogin, pwRetrieveFn, validUntilFn, err := sql.GetUserHashedPassword(context.Background(), &execCfg, &ie, username)

			if err != nil {
				t.Errorf(
					"credentials %s/%s failed with error %s, wanted no error",
					tc.username,
					tc.password,
					err,
				)
			}

			valid := true
			expired := false

			if !exists || !canLogin {
				valid = false
			}

			hashedPassword, err := pwRetrieveFn(ctx)
			if err != nil {
				t.Errorf(
					"credentials %s/%s failed with error %s, wanted no error",
					tc.username,
					tc.password,
					err,
				)
			}

			err = security.CompareHashAndPassword(ctx, hashedPassword, tc.password)
			if err != nil {
				valid = false
			}

			validUntil, err := validUntilFn(ctx)
			if err != nil {
				t.Errorf(
					"credentials %s/%s failed with error %s, wanted no error",
					tc.username,
					tc.password,
					err,
				)
			}

			if validUntil != nil {
				if validUntil.Time.Sub(timeutil.Now()) < 0 {
					expired = true
				}
			}

			if valid && !expired != tc.shouldAuthenticate {
				t.Errorf(
					"credentials %s/%s valid = %t, wanted %t",
					tc.username,
					tc.password,
					valid,
					tc.shouldAuthenticate,
				)
			}
		})
	}
}
