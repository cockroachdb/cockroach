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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/stdstrings"
	"github.com/cockroachdb/redact"
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
// accept_sql_without_tls
//       Enable TCP connections without TLS in secure mode.
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
// authlog N
// <regexp>
//       Expect <regexp> at the end of the auth log then report the
//       N entries before that.
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
// either "ok" (no error) or "ERROR:" followed by the expected error
// string.
// The auth and connection log entries, if any, are also produced
// alongside the "ok" or "ERROR" message.
//
func TestAuthenticationAndHBARules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "takes >1min under race")

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		hbaRunTest(t, insecure)
	})
}

const socketConnVirtualPort = "6"

func makeSocketFile(t *testing.T) (socketDir, socketFile string, cleanupFn func()) {
	if runtime.GOOS == "windows" {
		// Unix sockets not supported on windows.
		return "", "", func() {}
	}
	// We need a temp directory in which we'll create the unix socket.
	//
	// On BSD, binding to a socket is limited to a path length of 104 characters
	// (including the NUL terminator). In glibc, this limit is 108 characters.
	//
	// macOS has a tendency to produce very long temporary directory names, so
	// we are careful to keep all the constants involved short.
	tempDir, err := ioutil.TempDir("", "TestAuth")
	if err != nil {
		t.Fatal(err)
	}
	// ".s.PGSQL.NNNN" is the standard unix socket name supported by pg clients.
	return tempDir,
		filepath.Join(tempDir, ".s.PGSQL."+socketConnVirtualPort),
		func() { _ = os.RemoveAll(tempDir) }
}

func hbaRunTest(t *testing.T, insecure bool) {
	httpScheme := "http://"
	if !insecure {
		httpScheme = "https://"
	}

	datadriven.Walk(t, "testdata/auth", func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()

		maybeSocketDir, maybeSocketFile, cleanup := makeSocketFile(t)
		defer cleanup()

		// We really need to have the logs go to files, so that -show-logs
		// does not break the "authlog" directives.
		sc := log.ScopeWithoutShowLogs(t)
		defer sc.Close(t)

		// Enable logging channels.
		log.TestingResetActive()
		cfg := logconfig.DefaultConfig()
		// Make a sink for just the session log.
		bt := true
		cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
			"auth": {
				FileDefaults: logconfig.FileDefaults{
					CommonSinkConfig: logconfig.CommonSinkConfig{Auditable: &bt},
				},
				Channels: logconfig.ChannelList{Channels: []log.Channel{channel.SESSIONS}},
			}}
		dir := sc.GetDirectory()
		if err := cfg.Validate(&dir); err != nil {
			t.Fatal(err)
		}
		cleanup, err := log.ApplyConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()

		s, conn, _ := serverutils.StartServer(t,
			base.TestServerArgs{Insecure: insecure, SocketFile: maybeSocketFile})
		defer s.Stopper().Stop(context.Background())

		// Enable conn/auth logging.
		// We can't use the cluster settings to do this, because
		// cluster settings propagate asynchronously.
		testServer := s.(*server.TestServer)
		pgServer := s.(*server.TestServer).PGServer()
		pgServer.TestingEnableConnLogging()
		pgServer.TestingEnableAuthLogging()

		httpClient, err := s.GetAdminAuthenticatedHTTPClient()
		if err != nil {
			t.Fatal(err)
		}
		httpHBAUrl := httpScheme + s.HTTPAddr() + "/debug/hba_conf"

		if _, err := conn.ExecContext(context.Background(), `CREATE USER $1`, security.TestUser); err != nil {
			t.Fatal(err)
		}

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			resultString, err := func() (string, error) {
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
						skip.IgnoreLint(t, "Test file not applicable at this security level.")
					}

				case "accept_sql_without_tls":
					testServer.Cfg.AcceptSQLWithoutTLS = true

				case "set_hba":
					_, err := conn.ExecContext(context.Background(),
						`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, td.Input)
					if err != nil {
						return "", err
					}

					// Wait until the configuration has propagated back to the
					// test client. We need to wait because the cluster setting
					// change propagates asynchronously.
					expConf := pgwire.DefaultHBAConfig
					if td.Input != "" {
						expConf, err = pgwire.ParseAndNormalize(td.Input)
						if err != nil {
							// The SET above succeeded so we don't expect a problem here.
							t.Fatal(err)
						}
					}
					testutils.SucceedsSoon(t, func() error {
						curConf := pgServer.GetAuthenticationConfiguration()
						if expConf.String() != curConf.String() {
							return errors.Newf(
								"HBA config not yet loaded\ngot:\n%s\nexpected:\n%s",
								curConf, expConf)
						}
						return nil
					})

					// Verify the HBA configuration was processed properly by
					// reporting the resulting cached configuration.
					resp, err := httpClient.Get(httpHBAUrl)
					if err != nil {
						return "", err
					}
					defer resp.Body.Close()
					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						return "", err
					}
					return string(body), nil

				case "sql":
					_, err := conn.ExecContext(context.Background(), td.Input)
					return "ok", err

				case "authlog":
					if len(td.CmdArgs) < 0 {
						t.Fatal("not enough arguments")
					}
					numEntries, err := strconv.Atoi(td.CmdArgs[0].Key)
					if err != nil {
						t.Fatal(err)
					}
					re, err := regexp.Compile(td.Input)
					if err != nil {
						t.Fatal(err)
					}

					var buf strings.Builder
					if err := testutils.SucceedsSoonError(func() error {
						buf.Reset()
						// t.Logf("attempting to scan logs...")

						// Note: even though FetchEntriesFromFiles advertises a mechanism
						// to filter entries by timestamp or just retrieve the last N entries,
						// this is currently broken for secondary loggers.
						// See: https://github.com/cockroachdb/cockroach/issues/45745
						// So instead we need to do the filtering ourselves.
						entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 10000, authLogFileRe,
							log.WithMarkedSensitiveData)
						if err != nil {
							t.Fatal(err)
						}
						if len(entries) == 0 {
							return errors.New("no log entries")
						} else {
							// Note: entries are delivered by Fetch in reverse order.
							i := numEntries - 1
							if i < 0 || i >= len(entries) {
								i = len(entries) - 1
							}
							for ; i >= 0; i-- {
								entry := &entries[i]
								// t.Logf("found log entry: %+v", *entry)

								if !strings.HasPrefix(entry.Message, "={") {
									// TODO(knz): Enhance this when the log file
									// contains proper markers for structured entries.
									t.Errorf("malformed structured message: %q", entry.Message)
								}

								jsonPayload := []byte(entry.Message[1:])
								if entry.Redactable {
									jsonPayload = redact.RedactableBytes(jsonPayload).StripMarkers()
								}
								var info map[string]interface{}
								if err := json.Unmarshal(jsonPayload, &info); err != nil {
									return errors.Wrapf(err, "unable to decode json: %q", jsonPayload)
								}
								// Erase non-deterministic fields.
								info["Timestamp"] = "XXX"
								info["RemoteAddress"] = "XXX"
								if _, ok := info["Duration"]; ok {
									info["Duration"] = "NNN"
								}
								msg, err := json.Marshal(info)
								if err != nil {
									t.Fatal(err)
								}
								fmt.Fprintf(&buf, "%d %s\n", entry.Counter, msg)
							}
							lastLogMsg := entries[0].Message
							if !re.MatchString(lastLogMsg) {
								return errors.Newf("last entry does not match: %q", lastLogMsg)
							}
						}
						return nil
					}); err != nil {
						buf.WriteString("ERROR: unable to find log line matching regexp")
					}
					return buf.String(), nil

				case "connect", "connect_unix":
					if td.Cmd == "connect_unix" && runtime.GOOS == "windows" {
						// Unix sockets not supported; assume the test succeeded.
						return td.Expected, nil
					}

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
						user == security.RootUser || user == security.TestUser /* withClientCerts */)
					defer cleanupFn()

					var host, port string
					if td.Cmd == "connect" {
						host, port, err = net.SplitHostPort(s.ServingSQLAddr())
						if err != nil {
							t.Fatal(err)
						}
					} else /* unix */ {
						host = maybeSocketDir
						port = socketConnVirtualPort
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
						datadriven.CmdArg{Key: "user", Vals: []string{user}},
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
					if dbSQL != nil {
						// Note: gosql.Open may return a valid db (with an open
						// TCP connection) even if there is an error. We want to
						// ensure this gets closed so that we catch the conn close
						// message in the log.
						defer dbSQL.Close()
					}
					if err != nil {
						return "", err
					}
					row := dbSQL.QueryRow("SELECT current_catalog")
					var dbName string
					if err := row.Scan(&dbName); err != nil {
						return "", err
					}
					return "ok " + dbName, nil

				default:
					td.Fatalf(t, "unknown command: %s", td.Cmd)
				}
				return "", nil
			}()
			if err != nil {
				return fmtErr(err)
			}
			return resultString
		})
	})
}

var authLogFileRe = regexp.MustCompile(`"EventType":"client_`)

// fmtErr formats an error into an expected output.
func fmtErr(err error) string {
	if err != nil {
		errStr := ""
		if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
			errStr = pqErr.Message
			if pgcode.MakeCode(string(pqErr.Code)) != pgcode.Uncategorized {
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

// TestClientAddrOverride checks that the crdb:remote_addr parameter
// can override the client address.
func TestClientAddrOverride(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	// Start a server.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(), "testClientAddrOverride" /* prefix */, url.User(security.TestUser),
	)
	defer cleanupFunc()

	// Ensure the test user exists.
	if _, err := db.Exec(`CREATE USER $1`, security.TestUser); err != nil {
		t.Fatal(err)
	}

	// Enable conn/auth logging.
	// We can't use the cluster settings to do this, because
	// cluster settings for booleans propagate asynchronously.
	testServer := s.(*server.TestServer)
	pgServer := testServer.PGServer()
	pgServer.TestingEnableAuthLogging()

	testCases := []struct {
		specialAddr string
		specialPort string
	}{
		{"11.22.33.44", "5566"},    // IPv4
		{"[11:22:33::44]", "5566"}, // IPv6
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s:%s", tc.specialAddr, tc.specialPort), func(t *testing.T) {
			// Create a custom HBA rule to refuse connections by the testuser
			// when coming from the special address.
			addr := tc.specialAddr
			mask := "32"
			if addr[0] == '[' {
				// An IPv6 address. The CIDR format in HBA rules does not
				// require the square brackets.
				addr = addr[1 : len(addr)-1]
				mask = "128"
			}
			hbaConf := "host all " + security.TestUser + " " + addr + "/" + mask + " reject\n" +
				"host all all all cert-password\n"
			if _, err := db.Exec(
				`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`,
				hbaConf,
			); err != nil {
				t.Fatal(err)
			}

			// Wait until the configuration has propagated back to the
			// test client. We need to wait because the cluster setting
			// change propagates asynchronously.
			expConf, err := pgwire.ParseAndNormalize(hbaConf)
			if err != nil {
				// The SET above succeeded so we don't expect a problem here.
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				curConf := pgServer.GetAuthenticationConfiguration()
				if expConf.String() != curConf.String() {
					return errors.Newf(
						"HBA config not yet loaded\ngot:\n%s\nexpected:\n%s",
						curConf, expConf)
				}
				return nil
			})

			// Inject the custom client address.
			options, _ := url.ParseQuery(pgURL.RawQuery)
			options["crdb:remote_addr"] = []string{tc.specialAddr + ":" + tc.specialPort}
			pgURL.RawQuery = options.Encode()

			t.Run("check-server-reject-override", func(t *testing.T) {
				// Connect a first time, with trust override disabled. In that case,
				// the server will complain that the remote override is not supported.
				_ = pgServer.TestingSetTrustClientProvidedRemoteAddr(false)

				testDB, err := gosql.Open("postgres", pgURL.String())
				if err != nil {
					t.Fatal(err)
				}
				defer testDB.Close()
				if err := testDB.Ping(); !testutils.IsError(err, "server not configured to accept remote address override") {
					t.Error(err)
				}
			})

			// Wait two full microseconds: we're parsing the log output below, and
			// the logging format has a microsecond precision on timestamps. We need to ensure that this check will not pick up log entries
			// from a previous test.
			time.Sleep(2 * time.Microsecond)
			testStartTime := timeutil.Now()

			t.Run("check-server-hba-uses-override", func(t *testing.T) {
				// Now recognize the override. Now we're expecting the connection
				// to hit the HBA rule and fail with an authentication error.
				_ = pgServer.TestingSetTrustClientProvidedRemoteAddr(true)

				testDB, err := gosql.Open("postgres", pgURL.String())
				if err != nil {
					t.Fatal(err)
				}
				defer testDB.Close()
				if err := testDB.Ping(); !testutils.IsError(err, "authentication rejected") {
					t.Error(err)
				}
			})

			t.Run("check-server-log-uses-override", func(t *testing.T) {
				// Wait for the disconnection event in logs.
				testutils.SucceedsSoon(t, func() error {
					log.Flush()
					entries, err := log.FetchEntriesFromFiles(testStartTime.UnixNano(), math.MaxInt64, 10000, sessionTerminatedRe,
						log.WithMarkedSensitiveData)
					if err != nil {
						t.Fatal(err)
					}
					if len(entries) == 0 {
						return errors.New("entry not found")
					}
					return nil
				})

				// Now we want to check that the logging tags are also updated.
				log.Flush()
				entries, err := log.FetchEntriesFromFiles(testStartTime.UnixNano(), math.MaxInt64, 10000, authLogFileRe,
					log.WithMarkedSensitiveData)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) == 0 {
					t.Fatal("no entries")
				}
				seenClient := false
				for _, e := range entries {
					t.Log(e.Tags)
					if strings.Contains(e.Tags, "client=") {
						seenClient = true
						if !strings.Contains(e.Tags, "client="+string(redact.StartMarker())+tc.specialAddr+":"+tc.specialPort+string(redact.EndMarker())) {
							t.Fatalf("expected override addr in log tags, got %+v", e)
						}
					}
				}
				if !seenClient {
					t.Fatal("no log entry found with the 'client' tag set")
				}
			})
		})
	}
}

var sessionTerminatedRe = regexp.MustCompile("client_session_end")
