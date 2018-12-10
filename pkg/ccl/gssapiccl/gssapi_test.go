// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package gssapiccl

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestGSS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	db := sqlutils.MakeSQLRunner(conn)

	os.Setenv("KRB5_KTNAME", "/etc/postgres.keytab")

	const (
		user = "tester@MY.EX"
	)

	db.Exec(t, fmt.Sprintf(`CREATE USER '%s'`, user))

	tests := []struct {
		// The hba.conf file/setting.
		conf string
		// Error message of gss login.
		gssErr string
	}{
		{
			conf: `
				host all all all gss
			`,
		},
		{
			conf: `
				host all tester@MY.EX all gss
			`,
		},
		{
			conf: `
				host all nope@MY.EX all gss
			`,
			gssErr: "no server.host_based_authentication.configuration entry",
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Log(tc.conf)
			db.Exec(t, `SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, tc.conf)

			testUserPgURL, cleanupFn := sqlutils.PGUrl(
				t, s.ServingAddr(), t.Name(), url.User(server.TestUser))
			defer cleanupFn()
			_, port, err := net.SplitHostPort(testUserPgURL.Host)
			if err != nil {
				t.Fatal(err)
			}
			testUserPgURL.Host = fmt.Sprintf("localhost:%s", port)
			testUserPgURL.User = url.User(user)
			fmt.Println(testUserPgURL.String())
			//os.Setenv("KRB5_CONFIG", "./krb5.conf")
			out, err := exec.Command("psql", "-c", "SELECT 1", testUserPgURL.String()).CombinedOutput()
			err = errors.Wrap(err, strings.TrimSpace(string(out)))
			if !testutils.IsError(err, tc.gssErr) {
				t.Errorf("expected err %v, got %v", tc.gssErr, err)
			}
		})
	}
}
