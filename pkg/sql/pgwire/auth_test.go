// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire_test

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors/stdstrings"
	"github.com/lib/pq"
)

// TestAuthenticationAndHBARules exercises the authentication code
// using datadriven testing.
//
// It supports the following DSL:
//
// config [secure] [insecure]
//       Only run the test file if the server is in the specified
//       security mode. (The default is `config secure insecure` i.e.
//       the test file is applicable to both.)
//
// set_hba
// <hba config>
//       Load the provided HBA configuration via the cluster setting
//       server.host_based_authentication.configuration.
//       The expected output is the configuration after parsing
//       and reloading in the server.
//
// sql
// <sql input>
//       Execute the specified SQL statement using the default root
//       connection provided by StartServer().
//
// connect [key=value ...]
//       Attempt a SQL connection using the provided connection
//       parameters using the pg "DSN notation": k/v pairs separated
//       by spaces.
//       The following standard pg keys are recognized:
//            user - the username
//            password - the password
//            host - the server name/address
//            port - the server port
//            sslmode, sslrootcert, sslcert, sslkey - SSL parameters.
//
//       The order of k/v pairs matters: if the same key is specified
//       multiple times, the first occurrence takes priority.
//
//       Additionally, the test runner will always _append_ a default
//       value for user (root), host/port/sslrootcert from the
//       initialized test server. This default configuration is placed
//       at the end so that each test can override the values.
//
//       The test runner also adds a default value for sslcert and
//       sslkey based on the value of "user" — either when provided by
//       the test, or root by default.
//
//       When the user is either "root" or "testuser" (those are the
//       users for which the test server generates certificates),
//       sslmode also gets a default of "verify-full". For other
//       users, sslmode is initialized by default to "verify-ca".
//
// For the directives "sql" and "connect", the expected output can be
// either "ok" (no error) or "ERRROR:" followed by the expected error
// string.
//
func TestAuthenticationAndHBARules(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		hbaRunTest(t, insecure)
	})
}

func hbaRunTest(t *testing.T, insecure bool) {
	httpScheme := "http://"
	if !insecure {
		httpScheme = "https://"
	}
	datadriven.Walk(t, "testdata/auth", func(t *testing.T, path string) {
		s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: insecure})
		defer s.Stopper().Stop(context.TODO())

		pgServer := s.(*server.TestServer).PGServer()

		httpClient, err := s.GetHTTPClient()
		if err != nil {
			t.Fatal(err)
		}
		httpHBAUrl := httpScheme + s.HTTPAddr() + "/debug/hba_conf"

		if _, err := conn.ExecContext(context.Background(), `CREATE USER $1`, server.TestUser); err != nil {
			t.Fatal(err)
		}

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "config":
				allowed := false
				for _, a := range td.CmdArgs {
					switch a.Key {
					case "secure":
						allowed = allowed || !insecure
					case "insecure":
						allowed = allowed || insecure
					default:
						t.Fatalf("unknown configuration: %s", a.Key)
					}
				}
				if !allowed {
					t.Skip("Test file not applicable at this security level.")
				}

			case "set_hba":
				_, err := conn.ExecContext(context.Background(),
					`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, td.Input)
				if err != nil {
					return fmtErr(err)
				}

				// Wait until the configuration has propagated back to the
				// test client. We need to wait because the cluster setting
				// change propagates asynchronously.
				var expConf *hba.Conf
				if td.Input != "" {
					expConf, err = hba.Parse(td.Input)
					if err != nil {
						// The SET above succeeded so we don't expect a problem here.
						t.Fatal(err)
					}
					pgwire.NormalizeHBAEntries(expConf)
				}
				testutils.SucceedsSoon(t, func() error {
					curConf := pgServer.TestingGetHBAConf()
					if !reflect.DeepEqual(expConf, curConf) {
						return errors.New("HBA config not yet loaded")
					}
					return nil
				})

				// Verify the HBA configuration was processed properly by
				// reporting the resulting cached configuration.
				resp, err := httpClient.Get(httpHBAUrl)
				if err != nil {
					return fmtErr(err)
				}
				defer resp.Body.Close()
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return fmtErr(err)
				}
				return string(body)

			case "sql":
				_, err := conn.ExecContext(context.Background(), td.Input)
				return fmtErr(err)

			case "connect":
				// Prepare a connection string using the server's default.
				// What is the user requested by the test?
				user := security.RootUser
				if td.HasArg("user") {
					td.ScanArgs(t, "user", &user)
				}

				// We want the certs to be present in the filesystem for this test.
				// However, certs are only generated for users "root" and "testuser" specifically.
				sqlURL, cleanupFn := sqlutils.PGUrlWithOptionalClientCerts(
					t, s.ServingSQLAddr(), t.Name(), url.User(user),
					user == security.RootUser || user == server.TestUser /* withClientCerts */)
				defer cleanupFn()

				host, port, err := net.SplitHostPort(s.ServingSQLAddr())
				if err != nil {
					t.Fatal(err)
				}
				options, err := url.ParseQuery(sqlURL.RawQuery)
				if err != nil {
					t.Fatal(err)
				}

				// Here we make use of the fact that pq accepts connection
				// strings using the alternate postgres configuration format,
				// consisting of k=v pairs separated by spaces.
				// For example this is a valid connection string:
				//  "host=localhost port=5432 user=root"
				// We also make use of the datadriven K/V parsing facility,
				// which always prioritizes the first K instance in the test's
				// argument list. We append the server's config parameters
				// at the end, letting the test override by introducing its
				// own values at the beginning.
				args := append(td.CmdArgs,
					datadriven.CmdArg{Key: "user", Vals: []string{sqlURL.User.Username()}},
					datadriven.CmdArg{Key: "host", Vals: []string{host}},
					datadriven.CmdArg{Key: "port", Vals: []string{port}},
				)
				for key := range options {
					args = append(args,
						datadriven.CmdArg{Key: key, Vals: []string{options.Get(key)}})
				}
				// Now turn the cmdargs into a dsn.
				var dsnBuf strings.Builder
				sp := ""
				seenKeys := map[string]struct{}{}
				for _, a := range args {
					if _, ok := seenKeys[a.Key]; ok {
						continue
					}
					seenKeys[a.Key] = struct{}{}
					val := ""
					if len(a.Vals) > 0 {
						val = a.Vals[0]
					}
					fmt.Fprintf(&dsnBuf, "%s%s=%s", sp, a.Key, val)
					sp = " "
				}
				dsn := dsnBuf.String()

				// Finally, connect and test the connection.
				dbSQL, err := gosql.Open("postgres", dsn)
				if err != nil {
					return fmtErr(err)
				}
				defer dbSQL.Close()
				row := dbSQL.QueryRow("SELECT current_catalog")
				var dbName string
				if err := row.Scan(&dbName); err != nil {
					return fmtErr(err)
				}
				return "ok " + dbName

			default:
				td.Fatalf(t, "unknown command: %s", td.Cmd)
			}
			return ""
		})
	})
}

func fmtErr(err error) string {
	if err != nil {
		errStr := ""
		if pqErr, ok := err.(*pq.Error); ok {
			errStr = pqErr.Message
			if pqErr.Code != pgcode.Uncategorized {
				errStr += fmt.Sprintf(" (SQLSTATE %s)", pqErr.Code)
			}
			if pqErr.Hint != "" {
				hint := strings.Replace(pqErr.Hint, stdstrings.IssueReferral, "<STANDARD REFERRAL>", 1)
				errStr += "\nHINT: " + hint
			}
			if pqErr.Detail != "" {
				errStr += "\nDETAIL: " + pqErr.Detail
			}
		} else {
			errStr = err.Error()
		}
		return "ERROR: " + errStr
	}
	return "ok"
}
