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

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

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
			out, err := exec.Command("psql", "-c", "SELECT 1", "-U", tc.user).CombinedOutput()
			err = errors.Wrap(err, strings.TrimSpace(string(out)))
			if !IsError(err, tc.gssErr) {
				t.Errorf("expected err %v, got %v", tc.gssErr, err)
			}
		})
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
