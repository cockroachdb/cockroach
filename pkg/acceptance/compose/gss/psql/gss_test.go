// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// "make test" would normally test this file, but it should only be tested
// within docker compose. We also can't use just "gss" here because that
// tag is reserved for the toplevel Makefile's linux-gnu build.

// +build gss_compose

package gss

import (
	gosql "database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/lib/pq/auth/kerberos"
)

func init() {
	pq.RegisterGSSProvider(func() (pq.GSS, error) { return kerberos.NewGSS() })
}

func TestGSS(t *testing.T) {
	connector, err := pq.NewConnector("user=root sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	db := gosql.OpenDB(connector)
	defer db.Close()

	tests := []struct {
		// The hba.conf file/setting.
		conf string
		user string
		// Error message of hba conf
		hbaErr string
		// Error message of gss login.
		gssErr string
	}{
		{
			conf:   `host all all all gss include_realm=0 nope=1`,
			hbaErr: `unsupported option`,
		},
		{
			conf:   `host all all all gss include_realm=1`,
			hbaErr: `include_realm must be set to 0`,
		},
		{
			conf:   `host all all all gss`,
			hbaErr: `missing "include_realm=0"`,
		},
		{
			conf:   `host all all all gss include_realm=0`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
		{
			conf:   `host all tester all gss include_realm=0`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
		{
			conf:   `host all nope all gss include_realm=0`,
			user:   "tester",
			gssErr: "no server.host_based_authentication.configuration entry",
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=MY.EX`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=NOPE.EX`,
			user:   "tester",
			gssErr: `GSSAPI realm \(MY.EX\) didn't match any configured realm`,
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=NOPE.EX krb_realm=MY.EX`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if _, err := db.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, tc.conf); !IsError(err, tc.hbaErr) {
				t.Fatalf("expected err %v, got %v", tc.hbaErr, err)
			}
			if tc.hbaErr != "" {
				return
			}
			if _, err := db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'`, tc.user)); err != nil {
				t.Fatal(err)
			}
			t.Run("libpq", func(t *testing.T) {
				userConnector, err := pq.NewConnector(fmt.Sprintf("user=%s sslmode=require krbspn=postgres/gss_cockroach_1.gss_default", tc.user))
				if err != nil {
					t.Fatal(err)
				}
				userDB := gosql.OpenDB(userConnector)
				defer userDB.Close()
				_, err = userDB.Exec("SELECT 1")
				if !IsError(err, tc.gssErr) {
					t.Errorf("expected err %v, got %v", tc.gssErr, err)
				}
			})
			t.Run("psql", func(t *testing.T) {
				out, err := exec.Command("psql", "-c", "SELECT 1", "-U", tc.user).CombinedOutput()
				err = errors.Wrap(err, strings.TrimSpace(string(out)))
				if !IsError(err, tc.gssErr) {
					t.Errorf("expected err %v, got %v", tc.gssErr, err)
				}
			})
			t.Run("cockroach", func(t *testing.T) {
				out, err := exec.Command("/cockroach/cockroach", "sql",
					"-e", "SELECT 1",
					"--certs-dir", "/certs",
					// TODO(mjibson): Teach the CLI to not ask for passwords during kerberos.
					// See #51588.
					"--url", fmt.Sprintf("postgresql://%s:nopassword@cockroach:26257/?sslmode=require&krbspn=postgres/gss_cockroach_1.gss_default", tc.user),
				).CombinedOutput()
				err = errors.Wrap(err, strings.TrimSpace(string(out)))
				if !IsError(err, tc.gssErr) {
					t.Errorf("expected err %v, got %v", tc.gssErr, err)
				}
			})
		})
	}
}

func TestGSSFileDescriptorCount(t *testing.T) {
	// When the docker-compose.yml added a ulimit for the cockroach
	// container the open file count would just stop there, it wouldn't
	// cause cockroach to panic or error like I had hoped since it would
	// allow a test to assert that multiple gss connections didn't leak
	// file descriptors. Another possibility would be to have something
	// track the open file count in the cockroach container, but that seems
	// brittle and probably not worth the effort. However this test is
	// useful when doing manual tracking of file descriptor count.
	t.Skip("#51791")

	rootConnector, err := pq.NewConnector("user=root sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	rootDB := gosql.OpenDB(rootConnector)
	defer rootDB.Close()

	if _, err := rootDB.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, "host all all all gss include_realm=0"); err != nil {
		t.Fatal(err)
	}
	const user = "tester"
	if _, err := rootDB.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'`, user)); err != nil {
		t.Fatal(err)
	}

	start := timeutil.Now()
	for i := 0; i < 1000; i++ {
		fmt.Println(i, timeutil.Since(start))
		out, err := exec.Command("psql", "-c", "SELECT 1", "-U", user).CombinedOutput()
		if IsError(err, "GSS authentication requires an enterprise license") {
			t.Log(string(out))
			t.Fatal(err)
		}
	}
}

func IsError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}
