// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package authccl

import (
	"context"
	gosql "database/sql"
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/ccl/jwtauthccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
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
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// The code in this file takes inspiration from pgwire/auth_test.go

// TestAuthenticationAndHBARules exercises the authentication code
// using datadriven testing.
//
// It supports the following DSL:
//
// jwt_cluster_setting [key]=[value]
//
//	Overrides JWT related cluster settings.
//
// config [secure] [insecure]
//
//	Only run the test file if the server is in the specified
//	security mode. (The default is `config secure insecure` i.e.
//	the test file is applicable to both.)
//
// sql
// <sql input>
//
//	Execute the specified SQL statement using the default root
//	connection provided by StartServer().
//
// connect [key=value ...]
//
//	Attempt a SQL connection using the provided connection
//	parameters using the pg "DSN notation": k/v pairs separated
//	by spaces.
//	The following standard pg keys are recognized:
//	     user - the username
//	     password - the password
//	     host - the server name/address
//	     port - the server port
//	     options - the options to include in the connect request
//	     force_certs - force the use of baked-in certificates
//	     sslmode, sslrootcert, sslcert, sslkey - SSL parameters.
//
//	The order of k/v pairs matters: if the same key is specified
//	multiple times, the first occurrence takes priority.
//
//	Additionally, the test runner will always _append_ a default
//	value for user (root), host/port/sslrootcert from the
//	initialized test server. This default configuration is placed
//	at the end so that each test can override the values.
//
//	The test runner also adds a default value for sslcert and
//	sslkey based on the value of "user" — either when provided by
//	the test, or root by default.
//
//	When the user is either "root" or "testuser" (those are the
//	users for which the test server generates certificates),
//	sslmode also gets a default of "verify-full". For other
//	users, sslmode is initialized by default to "verify-ca".
//
// For the directives "sql" and "connect", the expected output can be
// either "ok" (no error) or "ERROR:" followed by the expected error
// string.
// The auth and connection log entries, if any, are also produced
// alongside the "ok" or "ERROR" message.
func TestAuthenticationAndHBARules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "takes >1min under race")

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		jwtRunTest(t, insecure)
	})
}

const socketConnVirtualPort = "6"

func makeSocketFile(t *testing.T) (socketDir, socketFile string, cleanupFn func()) {
	if runtime.GOOS == "windows" {
		// Unix sockets not supported on windows.
		return "", "", func() {}
	}
	socketName := ".s.PGSQL." + socketConnVirtualPort
	// We need a temp directory in which we'll create the unix socket.
	//
	// On BSD, binding to a socket is limited to a path length of 104 characters
	// (including the NUL terminator). In glibc, this limit is 108 characters.
	//
	// macOS has a tendency to produce very long temporary directory names, so
	// we are careful to keep all the constants involved short.
	baseTmpDir := os.TempDir()
	if len(baseTmpDir) >= 104-1-len(socketName)-1-len("TestAuth")-10 {
		t.Logf("temp dir name too long: %s", baseTmpDir)
		t.Logf("using /tmp instead.")
		// Note: /tmp might fail in some systems, that's why we still prefer
		// os.TempDir() if available.
		baseTmpDir = "/tmp"
	}
	tempDir, err := os.MkdirTemp(baseTmpDir, "TestAuth")
	require.NoError(t, err)
	// ".s.PGSQL.NNNN" is the standard unix socket name supported by pg clients.
	return tempDir,
		filepath.Join(tempDir, socketName),
		func() { _ = os.RemoveAll(tempDir) }
}

func jwtRunTest(t *testing.T, insecure bool) {

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
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
				Channels: logconfig.SelectChannels(channel.SESSIONS),
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
			base.TestServerArgs{
				Insecure:   insecure,
				SocketFile: maybeSocketFile,
			})
		defer s.Stopper().Stop(context.Background())

		sv := &s.ClusterSettings().SV
		if len(s.TestTenants()) > 0 {
			sv = &s.TestTenants()[0].ClusterSettings().SV
		}

		if _, err := conn.ExecContext(context.Background(), fmt.Sprintf(`CREATE USER %s`, username.TestUser)); err != nil {
			t.Fatal(err)
		}

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			resultString, err := func() (string, error) {
				switch td.Cmd {
				case "jwt_cluster_setting":
					for _, a := range td.CmdArgs {
						switch a.Key {
						case "enabled":
							if len(a.Vals) != 1 {
								t.Fatalf("wrong number of argumenets to jwt_cluster_setting enabled: %d", len(a.Vals))
							}
							v, err := strconv.ParseBool(a.Vals[0])
							if err != nil {
								t.Fatalf("unknown value for jwt_cluster_setting enabled: %s", a.Vals[0])
							}
							jwtauthccl.JWTAuthEnabled.Override(context.Background(), sv, v)
						case "audience":
							if len(a.Vals) != 1 {
								t.Fatalf("wrong number of argumenets to jwt_cluster_setting audience: %d", len(a.Vals))
							}
							jwtauthccl.JWTAuthAudience.Override(context.Background(), sv, a.Vals[0])
						case "issuers":
							if len(a.Vals) != 1 {
								t.Fatalf("wrong number of argumenets to jwt_cluster_setting issuers: %d", len(a.Vals))
							}
							jwtauthccl.JWTAuthIssuers.Override(context.Background(), sv, a.Vals[0])
						case "jwks":
							if len(a.Vals) != 1 {
								t.Fatalf("wrong number of argumenets to jwt_cluster_setting jwks: %d", len(a.Vals))
							}
							jwtauthccl.JWTAuthJWKS.Override(context.Background(), sv, a.Vals[0])
						case "claim":
							if len(a.Vals) != 1 {
								t.Fatalf("wrong number of argumenets to jwt_cluster_setting claim: %d", len(a.Vals))
							}
							jwtauthccl.JWTAuthClaim.Override(context.Background(), sv, a.Vals[0])
						case "ident_map":
							if len(a.Vals) != 1 {
								t.Fatalf("wrong number of argumenets to jwt_cluster_setting ident_map: %d", len(a.Vals))
							}
							args := strings.Split(a.Vals[0], ",")
							if len(args) != 3 {
								t.Fatalf("wrong number of comma separated argumenets to jwt_cluster_setting ident_map: %d", len(a.Vals))
							}
							pgwire.ConnIdentityMapConf.Override(context.Background(), sv, strings.Join(args, "    "))
						default:
							t.Fatalf("unknown jwt_cluster_setting: %s", a.Key)
						}
					}
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

				case "sql":
					_, err := conn.ExecContext(context.Background(), td.Input)
					return "ok", err

				case "connect", "connect_unix":
					if td.Cmd == "connect_unix" && runtime.GOOS == "windows" {
						// Unix sockets not supported; assume the test succeeded.
						return td.Expected, nil
					}

					// Prepare a connection string using the server's default.
					// What is the user requested by the test?
					user := username.RootUser
					if td.HasArg("user") {
						td.ScanArgs(t, "user", &user)
					}

					// Allow connections for non-root, non-testuser to force the
					// use of client certificates.
					forceCerts := false
					if td.HasArg("force_certs") {
						forceCerts = true
					}

					// We want the certs to be present in the filesystem for this test.
					// However, certs are only generated for users "root" and "testuser" specifically.
					sqlURL, cleanupFn := sqlutils.PGUrlWithOptionalClientCerts(
						t, s.ServingSQLAddr(), t.Name(), url.User(user),
						forceCerts || user == username.RootUser || user == username.TestUser /* withClientCerts */)
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
					if td.HasArg("options") {
						var additionalOptions string
						td.ScanArgs(t, "options", &additionalOptions)
						args = append(args, datadriven.CmdArg{Key: "options", Vals: []string{additionalOptions}})
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
		t, s.ServingSQLAddr(), "testClientAddrOverride" /* prefix */, url.User(username.TestUser),
	)
	defer cleanupFunc()

	// Ensure the test user exists.
	if _, err := db.Exec(fmt.Sprintf(`CREATE USER %s`, username.TestUser)); err != nil {
		t.Fatal(err)
	}

	// Enable conn/auth logging.
	// We can't use the cluster settings to do this, because
	// cluster settings for booleans propagate asynchronously.
	var pgServer *pgwire.Server
	var pgPreServer *pgwire.PreServeConnHandler
	if len(s.TestTenants()) > 0 {
		pgServer = s.TestTenants()[0].PGServer().(*pgwire.Server)
		pgPreServer = s.TestTenants()[0].(*server.TestTenant).PGPreServer()
	} else {
		pgServer = s.PGServer().(*pgwire.Server)
		pgPreServer = s.(*server.TestServer).PGPreServer()
	}
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
			hbaConf := "host all " + username.TestUser + " " + addr + "/" + mask + " reject\n" +
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
				curConf, _ := pgServer.GetAuthenticationConfiguration()
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
				_ = pgPreServer.TestingSetTrustClientProvidedRemoteAddr(false)

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
				_ = pgPreServer.TestingSetTrustClientProvidedRemoteAddr(true)

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
					log.FlushFileSinks()
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
				log.FlushFileSinks()
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
						if !strings.Contains(e.Tags, "client="+tc.specialAddr+":"+tc.specialPort) {
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
