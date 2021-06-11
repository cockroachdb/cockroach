// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestStdFlagToPflag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "test.") {
			return
		}
		if pf := cf.Lookup(f.Name); pf == nil {
			t.Errorf("unable to find \"%s\"", f.Name)
		}
	})
}

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Verify that the cockroach binary doesn't depend on certain packages.
	buildutil.VerifyNoImports(t,
		"github.com/cockroachdb/cockroach/pkg/cmd/cockroach", true,
		[]string{
			"testing",  // defines flags
			"go/build", // probably not something we want in the main binary
			"github.com/cockroachdb/cockroach/pkg/security/securitytest", // contains certificates
		},
		[]string{
			"github.com/cockroachdb/cockroach/pkg/testutils", // meant for testing code only
		},
		// Sentry and the errors library use go/build to determine
		// the list of source directories (used to strip the source prefix
		// in stack trace reports).
		"github.com/cockroachdb/cockroach/vendor/github.com/cockroachdb/sentry-go",
		"github.com/cockroachdb/cockroach/vendor/github.com/cockroachdb/errors/withstack",
	)
}

func TestCacheFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	f := startCmd.Flags()
	args := []string{"--cache", "100MB"}
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}

	const expectedCacheSize = 100 * 1000 * 1000
	if expectedCacheSize != serverCfg.CacheSize {
		t.Errorf("expected %d, but got %d", expectedCacheSize, serverCfg.CacheSize)
	}
}

func TestClusterNameFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	testCases := []struct {
		value       string
		expectedErr string
	}{
		{"abc", ""},
		{"a-b", ""},
		{"a123", ""},
		{"", "cluster name cannot be empty"},
		{fmt.Sprintf("%*s", 1000, "a"), "cluster name can contain at most 256 characters"},
		{"a-b.c", errClusterNameInvalidFormat.Error()},
		{"a123.456", errClusterNameInvalidFormat.Error()},
		{"...", errClusterNameInvalidFormat.Error()},
		{"-abc", errClusterNameInvalidFormat.Error()},
		{"123a", errClusterNameInvalidFormat.Error()},
		{"abc.", errClusterNameInvalidFormat.Error()},
		{"_abc", errClusterNameInvalidFormat.Error()},
		{"a.b_c._.", errClusterNameInvalidFormat.Error()},
	}

	for _, c := range testCases {
		baseCfg.ClusterName = ""
		f := startCmd.Flags()
		args := []string{"--cluster-name", c.value}
		err := f.Parse(args)
		if !testutils.IsError(err, c.expectedErr) {
			t.Fatal(err)
		}
		if err == nil {
			if baseCfg.ClusterName != c.value {
				t.Errorf("expected %q, got %q", c.value, baseCfg.ClusterName)
			}
		}
	}
}

func TestSQLMemoryPoolFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	f := startCmd.Flags()

	// Check absolute values.
	testCases := []struct {
		value    string
		expected int64
	}{
		{"100MB", 100 * 1000 * 1000},
		{".5GiB", 512 * 1024 * 1024},
		{"1.3", 1},
	}
	for _, c := range testCases {
		args := []string{"--max-sql-memory", c.value}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		if c.expected != serverCfg.MemoryPoolSize {
			t.Errorf("expected %d, but got %d", c.expected, serverCfg.MemoryPoolSize)
		}
	}

	for _, c := range []string{".30", "0.3"} {
		args := []string{"--max-sql-memory", c}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}

		// Check fractional values.
		maxMem, err := status.GetTotalMemory(context.Background())
		if err != nil {
			t.Logf("total memory unknown: %v", err)
			return
		}
		expectedLow := (maxMem * 28) / 100
		expectedHigh := (maxMem * 32) / 100
		if serverCfg.MemoryPoolSize < expectedLow || serverCfg.MemoryPoolSize > expectedHigh {
			t.Errorf("expected %d-%d, but got %d", expectedLow, expectedHigh, serverCfg.MemoryPoolSize)
		}
	}
}

func TestClockOffsetFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args     []string
		expected time.Duration
	}{
		{nil, base.DefaultMaxClockOffset},
		{[]string{"--max-offset", "200ms"}, 200 * time.Millisecond},
	}

	for i, td := range testData {
		initCLIDefaults()

		if err := f.Parse(td.args); err != nil {
			t.Fatal(err)
		}
		if td.expected != time.Duration(serverCfg.MaxOffset) {
			t.Errorf("%d. MaxOffset expected %v, but got %v", i, td.expected, serverCfg.MaxOffset)
		}
	}
}

func TestClientURLFlagEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	// Prepare a dummy default certificate directory.
	defCertsDirPath, err := ioutil.TempDir("", "defCerts")
	if err != nil {
		t.Fatal(err)
	}
	defCertsDirPath, _ = filepath.Abs(defCertsDirPath)
	cleanup := createTestCerts(defCertsDirPath)
	defer func() { _ = cleanup() }()

	// Prepare a custom certificate directory.
	testCertsDirPath, err := ioutil.TempDir("", "customCerts")
	if err != nil {
		t.Fatal(err)
	}
	testCertsDirPath, _ = filepath.Abs(testCertsDirPath)
	cleanup2 := createTestCerts(testCertsDirPath)
	defer func() { _ = cleanup2() }()

	anyCmd := []string{"sql", "quit"}
	anyNonSQL := []string{"quit", "init"}
	anySQL := []string{"sql"}
	sqlShell := []string{"sql"}
	anyNonSQLShell := []string{"quit"}
	anyImportCmd := []string{"import db pgdump", "import table pgdump"}

	testData := []struct {
		cmds       []string
		flags      []string
		refargs    []string
		expErr     string
		reparseErr string
	}{
		// Check individual URL components.
		{anyCmd, []string{"--url=http://foo"}, nil, `unrecognized URL scheme: http`, ""},
		{anyCmd, []string{"--url=postgresql:foo/bar"}, nil, `unknown URL format`, ""},

		{anyCmd, []string{"--url=postgresql://foo"}, []string{"--host=foo"}, "", ""},
		{anyCmd, []string{"--url=postgresql://:foo"}, []string{"--port=foo"}, "invalid port \":foo\" after host", ""},
		{anyCmd, []string{"--url=postgresql://:12345"}, []string{"--port=12345"}, "", ""},

		{sqlShell, []string{"--url=postgresql:///foo"}, []string{"--database=foo"}, "", ""},
		{anyImportCmd, []string{"--url=postgresql:///foo"}, []string{"--database=foo"}, "", ""},
		{anyNonSQLShell, []string{"--url=postgresql://foo/bar"}, []string{"--host=foo" /*db ignored*/}, "", ""},

		{anySQL, []string{"--url=postgresql://foo@"}, []string{"--user=foo"}, "", ""},
		{anyNonSQL, []string{"--url=postgresql://foo@bar"}, []string{"--host=bar" /*user ignored*/}, "", ""},

		{sqlShell, []string{"--url=postgresql://a@b:12345/d"}, []string{"--user=a", "--host=b", "--port=12345", "--database=d"}, "", ""},
		{sqlShell, []string{"--url=postgresql://a@b:c/d"}, nil, `invalid port ":c" after host`, ""},
		{anySQL, []string{"--url=postgresql://a@b:12345"}, []string{"--user=a", "--host=b", "--port=12345"}, "", ""},
		{anySQL, []string{"--url=postgresql://a@b:c"}, nil, `invalid port ":c" after host`, ""},
		{anyNonSQL, []string{"--url=postgresql://b:12345"}, []string{"--host=b", "--port=12345"}, "", ""},
		{anyNonSQL, []string{"--url=postgresql://b:c"}, nil, `invalid port ":c" after host`, ""},

		{anyNonSQL, []string{"--url=postgresql://foo?application_name=abc"}, []string{"--host=foo"}, "", ""},
		{anySQL, []string{"--url=postgresql://foo?application_name=abc"}, []string{"--host=foo"}, "", ""},

		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=disable"}, []string{"--host=foo", "--insecure"}, "", ""},
		{anySQL, []string{"--url=postgresql://foo?sslmode=disable"}, []string{"--host=foo"}, "", ""},

		{anySQL, []string{"--url=postgresql://foo?sslmode=require"}, []string{"--host=foo", "--insecure=false"}, "", ""},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=require"}, nil, "command .* only supports sslmode=disable or sslmode=verify-full", ""},
		{anyCmd, []string{"--url=postgresql://foo?sslmode=verify-full"}, []string{"--host=foo", "--insecure=false"}, "", ""},

		// URL picks up previous flags if component not specified.
		{anyCmd, []string{"--host=baz", "--url=postgresql://:12345"}, []string{"--host=baz", "--port=12345"}, "", ""},
		{anyCmd, []string{"--host=baz", "--url=postgresql://:foo"}, nil, `invalid port ":foo" after host`, ""},
		{anyCmd, []string{"--port=12345", "--url=postgresql://foo"}, []string{"--host=foo", "--port=12345"}, "", ""},
		{anyCmd, []string{"--port=baz", "--url=postgresql://foo"}, []string{"--host=foo", "--port=baz"}, "", `invalid port ":baz" after host`},
		{sqlShell, []string{"--database=baz", "--url=postgresql://foo"}, []string{"--host=foo", "--database=baz"}, "", ""},
		{anyImportCmd, []string{"--database=baz", "--url=postgresql://"}, []string{"--database=baz"}, "", ""},
		{anySQL, []string{"--user=baz", "--url=postgresql://foo"}, []string{"--host=foo", "--user=baz"}, "", ""},

		{anyCmd, []string{"--insecure=false", "--url=postgresql://foo"}, []string{"--host=foo", "--insecure=false"}, "", ""},
		// Only non-SQL lets --insecure bleed into a URL that does not specify sslmode.
		{anyNonSQL, []string{"--insecure", "--url=postgresql://foo"}, []string{"--host=foo", "--insecure"}, "", ""},

		// URL overrides previous flags if component specified.
		{anyCmd, []string{"--host=baz", "--url=postgresql://bar"}, []string{"--host=bar"}, "", ""},
		{anyCmd, []string{"--port=baz", "--url=postgresql://foo:12345"}, []string{"--host=foo", "--port=12345"}, "", ""},
		{anyCmd, []string{"--port=baz", "--url=postgresql://foo:bar"}, nil, `invalid port ":bar" after host`, ""},
		{sqlShell, []string{"--database=baz", "--url=postgresql://foo/bar"}, []string{"--host=foo", "--database=bar"}, "", ""},
		{anyImportCmd, []string{"--database=baz", "--url=postgresql:///bar"}, []string{"--database=bar"}, "", ""},
		{anySQL, []string{"--user=baz", "--url=postgresql://bar@foo"}, []string{"--host=foo", "--user=bar"}, "", ""},

		{anyNonSQL, []string{"--insecure=false", "--url=postgresql://foo?sslmode=disable"}, []string{"--host=foo", "--insecure"}, "", ""},
		{anyCmd, []string{"--insecure", "--url=postgresql://foo?sslmode=verify-full"}, []string{"--host=foo", "--insecure=false"}, "", ""},
		// SQL is special case: specifying sslmode= does not imply insecure mode. So the insecure bit does not get reset.
		{anySQL, []string{"--insecure=false", "--url=postgresql://foo?sslmode=disable"}, []string{"--host=foo"}, "", ""},

		// Discrete flag overrides URL if specified afterwards.
		{anyCmd, []string{"--url=postgresql://bar", "--host=baz"}, []string{"--host=baz"}, "", ""},
		{anyCmd, []string{"--url=postgresql://foo:12345", "--port=5678"}, []string{"--host=foo", "--port=5678"}, "", ""},
		{anyCmd, []string{"--url=postgresql://foo:12345", "--port=baz"}, []string{"--host=foo", "--port=baz"}, "", `invalid port ":baz" after host`},
		{anyCmd, []string{"--url=postgresql://foo:bar", "--port=baz"}, nil, `invalid port ":bar" after host`, ""},
		{sqlShell, []string{"--url=postgresql://foo/bar", "--database=baz"}, []string{"--host=foo", "--database=baz"}, "", ""},
		{anySQL, []string{"--url=postgresql://bar@foo", "--user=baz"}, []string{"--host=foo", "--user=baz"}, "", ""},

		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=disable", "--insecure=false"}, []string{"--host=foo", "--insecure=false"}, "", ""},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full", "--insecure"}, []string{"--host=foo", "--insecure"}, "", ""},

		// Check that the certs dir is extracted properly.
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslrootcert=" + testCertsDirPath + "/ca.crt"}, []string{"--host=foo", "--certs-dir=" + testCertsDirPath}, "", ""},
		{anyNonSQL, []string{"--certs-dir=blah", "--url=postgresql://foo?sslmode=verify-full&sslrootcert=blih/ca.crt"}, nil, "non-homogeneous certificate directory", ""},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslrootcert=blih/ca.crt&sslcert=blah/client.root.crt"}, nil, "non-homogeneous certificate directory", ""},

		// Check the cert component file names are checked.
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslrootcert=blih/loh.crt"}, nil, `invalid file name for "sslrootcert": expected .* got .*`, ""},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslcert=blih/loh.crt"}, nil, `invalid file name for "sslcert": expected .* got .*`, ""},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslkey=blih/loh.crt"}, nil, `invalid file name for "sslkey": expected .* got .*`, ""},
	}

	type capturedFlags struct {
		connHost     string
		connPort     string
		connUser     string
		connDatabase string
		insecure     bool
		certsDir     string
	}

	// capture saves the parameter values computed in a round of
	// command-line argument parsing.
	capture := func(cmd *cobra.Command) capturedFlags {
		certsDir := defCertsDirPath
		if f := cmd.Flags().Lookup(cliflags.CertsDir.Name); f != nil && f.Changed {
			certsDir = baseCfg.SSLCertsDir
		}
		return capturedFlags{
			insecure:     baseCfg.Insecure,
			connUser:     cliCtx.sqlConnUser,
			connDatabase: cliCtx.sqlConnDBName,
			connHost:     cliCtx.clientConnHost,
			connPort:     cliCtx.clientConnPort,
			certsDir:     certsDir,
		}
	}

	for testNum, test := range testData {
		for _, cmdName := range test.cmds {
			t.Run(fmt.Sprintf("%d/%s/%s", testNum+1, cmdName, strings.Join(test.flags, " ")), func(t *testing.T) {
				cmd, _, _ := cockroachCmd.Find(strings.Split(cmdName, " "))

				// Parse using the URL.
				// This checks the URL parser works and/or that it produces the expected error.
				initCLIDefaults()
				cliCtx.SSLCertsDir = defCertsDirPath
				err := cmd.ParseFlags(test.flags)
				if !testutils.IsError(err, test.expErr) {
					t.Fatalf("expected %q, got %v", test.expErr, err)
				}
				if err != nil {
					return
				}
				urlParams := capture(cmd)
				connURL, err := cliCtx.makeClientConnURL()
				if err != nil {
					t.Fatal(err)
				}
				resultURL := connURL.ToPQ().String()

				// Parse using the discrete flags.
				// We use this to generate the reference parameter values for the comparison below.
				initCLIDefaults()
				cliCtx.SSLCertsDir = defCertsDirPath
				if err := cmd.ParseFlags(test.refargs); err != nil {
					t.Fatal(err)
				}
				discreteParams := capture(cmd)
				connURL, err = cliCtx.makeClientConnURL()
				if err != nil {
					t.Fatal(err)
				}
				defaultURL := connURL.ToPQ().String()

				// Verify that parsing the URL produces the same parameters as parsing the discrete flags.
				if urlParams != discreteParams {
					t.Fatalf("mismatch: URL %q parses\n%+v,\ndiscrete parses\n%+v", resultURL, urlParams, discreteParams)
				}

				// For SQL commands only, test that reconstructing the URL
				// from discrete flags yield equivalent connection parameters.
				// (RPC commands never reconstruct a URL.)
				for _, s := range anyNonSQL {
					if cmdName == s {
						return
					}
				}

				// Re-parse using the derived URL.
				// We'll want to ensure below that the derived URL specifies the same parameters
				// (i.e. check makeClientConnURL does its work properly).
				initCLIDefaults()
				cliCtx.SSLCertsDir = defCertsDirPath
				if err := cmd.ParseFlags([]string{"--url=" + resultURL}); err != nil {
					if !testutils.IsError(err, test.reparseErr) {
						t.Fatal(err)
					}
					return
				}
				urlParams2 := capture(cmd)

				// Verify that makeClientConnURL is still specifying the same things.
				if urlParams2 != discreteParams {
					t.Fatalf("mismatch during reparse: derived URL %q parses\n%+v,\ndiscrete parses\n%+v", resultURL, urlParams2, discreteParams)
				}

				// Re-parse using the derived URL from discrete parameters.
				// We're checking here that makeClientConnURL is also doing its job
				// properly when computing a URL from discrete parameters.
				initCLIDefaults()
				cliCtx.SSLCertsDir = defCertsDirPath
				if err := cmd.ParseFlags([]string{"--url=" + defaultURL}); err != nil {
					t.Fatal(err)
				}
				urlParams3 := capture(cmd)

				if urlParams2 != urlParams3 {
					t.Fatalf("mismatch during reparse 2: derived URL %q parses\n%+v,\ndefault URL %q reparses\n%+v", resultURL, urlParams2, defaultURL, urlParams3)
				}
			})
		}
	}
}

func TestServerConnSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args                  []string
		expectedAddr          string
		expectedAdvertiseAddr string
		expSQLAddr            string
		expSQLAdvAddr         string
		expTenantAddr         string
		expTenantAdvAddr      string
	}{
		{[]string{"start"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			":" + base.DefaultPort, ":" + base.DefaultPort,
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "192.168.0.111"},
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", ":"},
			":", ":",
			":", ":",
			":", ":",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:"},
			"127.0.0.1:", "127.0.0.1:",
			"127.0.0.1:", "127.0.0.1:",
			"127.0.0.1:", "127.0.0.1:",
		},
		{[]string{"start", "--listen-addr", ":12345"},
			":12345", ":12345",
			":12345", ":12345",
			":12345", ":12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345"},
			"127.0.0.1:12345", "127.0.0.1:12345",
			"127.0.0.1:12345", "127.0.0.1:12345",
			"127.0.0.1:12345", "127.0.0.1:12345",
		},
		{[]string{"start", "--listen-addr", "[::1]"},
			"[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort,
			"[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort,
			"[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "[::1]:12345"},
			"[::1]:12345", "[::1]:12345",
			"[::1]:12345", "[::1]:12345",
			"[::1]:12345", "[::1]:12345",
		},
		{[]string{"start", "--listen-addr", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort,
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort,
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort,
		},
		// confirm hostnames will work
		{[]string{"start", "--listen-addr", "my.host.name"},
			"my.host.name:" + base.DefaultPort, "my.host.name:" + base.DefaultPort,
			"my.host.name:" + base.DefaultPort, "my.host.name:" + base.DefaultPort,
			"my.host.name:" + base.DefaultPort, "my.host.name:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "myhostname"},
			"myhostname:" + base.DefaultPort, "myhostname:" + base.DefaultPort,
			"myhostname:" + base.DefaultPort, "myhostname:" + base.DefaultPort,
			"myhostname:" + base.DefaultPort, "myhostname:" + base.DefaultPort,
		},

		// SQL address override.
		{[]string{"start", "--sql-addr", "127.0.0.1"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--sql-addr", ":1234"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			":1234", ":1234",
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--sql-addr", "127.0.0.1:1234"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"127.0.0.1:1234", "127.0.0.1:1234",
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--sql-addr", "[::2]"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"[::2]:" + base.DefaultPort, "[::2]:" + base.DefaultPort,
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--sql-addr", "[::2]:1234"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"[::2]:1234", "[::2]:1234",
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},

		// Configuring the components of the SQL address separately.
		{[]string{"start", "--listen-addr", "127.0.0.1", "--sql-addr", "127.0.0.2"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.2:" + base.DefaultPort, "127.0.0.2:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--sql-addr", ":1234"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.1:1234", "127.0.0.1:1234",
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--sql-addr", "127.0.0.2:1234"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.2:1234", "127.0.0.2:1234",
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "[::2]", "--sql-addr", ":1234"},
			"[::2]:" + base.DefaultPort, "[::2]:" + base.DefaultPort,
			"[::2]:1234", "[::2]:1234",
			"[::2]:" + base.DefaultPort, "[::2]:" + base.DefaultPort,
		},

		// --advertise-addr overrides.
		{[]string{"start", "--advertise-addr", "192.168.0.111"},
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111"},
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345", "--advertise-addr", "192.168.0.111"},
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111:12345"},
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:54321", "--advertise-addr", "192.168.0.111:12345"},
			"127.0.0.1:54321", "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--listen-addr", ":12345"},
			":12345", "192.168.0.111:12345",
			":12345", "192.168.0.111:12345",
			":12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--listen-addr", ":54321"},
			":54321", "192.168.0.111:12345",
			":54321", "192.168.0.111:12345",
			":54321", "192.168.0.111:12345",
		},

		// Show that if the SQL address does not have a name default, its
		// advertised form picks up the RPC advertised address.
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--sql-addr", ":54321"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":54321", "192.168.0.111:54321",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},

		// Show that if the SQL address is overridden, its advertised form picks the
		// advertised RPC address but keeps the port.
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--sql-addr", "127.0.0.1:54321"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:54321",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--sql-addr", "127.0.0.1"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--sql-addr", "127.0.0.1:12345"},
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"127.0.0.1:12345", "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},

		// Backward-compatibility flag combinations.
		{[]string{"start", "--host", "192.168.0.111"},
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--port", "12345"},
			":12345", ":12345",
			":12345", ":12345",
			":12345", ":12345",
		},
		{[]string{"start", "--advertise-host", "192.168.0.111"},
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--port", "12345"},
			"127.0.0.1:12345", "127.0.0.1:12345",
			"127.0.0.1:12345", "127.0.0.1:12345",
			"127.0.0.1:12345", "127.0.0.1:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345", "--port", "55555"},
			"127.0.0.1:55555", "127.0.0.1:55555",
			"127.0.0.1:55555", "127.0.0.1:55555",
			"127.0.0.1:55555", "127.0.0.1:55555",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "12345"},
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "12345"},
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"},
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"},
			"127.0.0.1:54321", "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--port", "12345"},
			":12345", "192.168.0.111:12345",
			":12345", "192.168.0.111:12345",
			":12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"},
			":54321", "192.168.0.111:12345",
			":54321", "192.168.0.111:12345",
			":54321", "192.168.0.111:12345",
		},
	}

	for i, td := range testData {
		t.Run(strings.Join(td.args, " "), func(t *testing.T) {
			initCLIDefaults()
			if err := f.Parse(td.args); err != nil {
				t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
			}

			if err := extraServerFlagInit(startCmd); err != nil {
				t.Fatal(err)
			}
			if td.expectedAddr != serverCfg.Addr {
				t.Errorf("%d. serverCfg.Addr expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expectedAddr, serverCfg.Addr, td.args)
			}
			if td.expectedAdvertiseAddr != serverCfg.AdvertiseAddr {
				t.Errorf("%d. serverCfg.AdvertiseAddr expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expectedAdvertiseAddr, serverCfg.AdvertiseAddr, td.args)
			}

			wantSQLSplit := false
			for _, r := range td.args {
				switch r {
				case "--sql-addr":
					wantSQLSplit = true
				}
			}
			if wantSQLSplit != serverCfg.SplitListenSQL {
				t.Errorf("%d. expected combined RPC/SQL listen = %v, found %v", i, wantSQLSplit, serverCfg.SplitListenSQL)
			}

			if td.expSQLAddr != serverCfg.SQLAddr {
				t.Errorf("%d. serverCfg.SQLAddr expected '%s', got '%s'. td.args was '%#v'.",
					i, td.expSQLAddr, serverCfg.SQLAddr, td.args)
			}
			if td.expSQLAdvAddr != serverCfg.SQLAdvertiseAddr {
				t.Errorf("%d. serverCfg.SQLAdvertiseAddr expected '%s', got '%s'. td.args was '%#v'.",
					i, td.expSQLAdvAddr, serverCfg.SQLAdvertiseAddr, td.args)
			}
		})
	}
}

func TestServerSocketSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args           []string
		expectedSocket string
	}{
		{[]string{"start"}, ""},
		// No socket unless requested.
		{[]string{"start", "--listen-addr=:12345"}, ""},
		// File name is auto-generated.
		{[]string{"start", "--socket-dir=/blah"}, "/blah/.s.PGSQL." + base.DefaultPort},
		{[]string{"start", "--socket-dir=/blah", "--listen-addr=:12345"}, "/blah/.s.PGSQL.12345"},
		// Empty socket dir disables the socket.
		{[]string{"start", "--socket-dir="}, ""},
		{[]string{"start", "--socket-dir=", "--listen-addr=:12345"}, ""},
	}

	for i, td := range testData {
		t.Run(strings.Join(td.args, " "), func(t *testing.T) {
			initCLIDefaults()
			if err := f.Parse(td.args); err != nil {
				t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
			}

			if err := extraServerFlagInit(startCmd); err != nil {
				t.Fatal(err)
			}
			if td.expectedSocket != serverCfg.SocketFile {
				t.Errorf("%d. serverCfg.SocketFile expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expectedSocket, serverCfg.SocketFile, td.args)
			}
		})
	}
}

func TestLocalityAdvAddrFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args                     []string
		expLocalityAdvertiseAddr string
	}{
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5"},
			"[{{tcp 235.0.0.5:26257} zone=1}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5,zone=2@123.0.0.5"},
			"[{{tcp 235.0.0.5:26257} zone=1} {{tcp 123.0.0.5:26257} zone=2}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5:1234"},
			"[{{tcp 235.0.0.5:1234} zone=1}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@[::2]"},
			"[{{tcp [::2]:26257} zone=1}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@[::2],zone=2@123.0.0.5"},
			"[{{tcp [::2]:26257} zone=1} {{tcp 123.0.0.5:26257} zone=2}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@[::2]:1234"},
			"[{{tcp [::2]:1234} zone=1}]"},
	}

	for i, td := range testData {
		t.Run(strings.Join(td.args, " "), func(t *testing.T) {
			initCLIDefaults()
			if err := f.Parse(td.args); err != nil {
				t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
			}
			if err := extraServerFlagInit(startCmd); err != nil {
				t.Fatal(err)
			}
			var locAddrStr strings.Builder
			locAddrStr.WriteString("[")
			for i, a := range serverCfg.LocalityAddresses {
				if i > 0 {
					locAddrStr.WriteString(" ")
				}
				fmt.Fprintf(
					&locAddrStr, "{{%s %s} %s}",
					a.Address.NetworkField, a.Address.AddressField, a.LocalityTier.String(),
				)
			}
			locAddrStr.WriteString("]")
			if td.expLocalityAdvertiseAddr != locAddrStr.String() {
				t.Errorf("%d. serverCfg.expLocalityAdvertiseAddr expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expLocalityAdvertiseAddr, locAddrStr.String(), td.args)
			}
		})
	}
}

func TestServerJoinSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args         []string
		expectedJoin []string
	}{
		{[]string{"start", "--join=a"}, []string{"a:" + base.DefaultPort}},
		{[]string{"start", "--join=:"}, []string{"HOSTNAME:" + base.DefaultPort}},
		{[]string{"start", "--join=:123"}, []string{"HOSTNAME:123"}},
		{[]string{"start", "--join=a,b,c"}, []string{"a:" + base.DefaultPort, "b:" + base.DefaultPort, "c:" + base.DefaultPort}},
		{[]string{"start", "--join=a", "--join=b"}, []string{"a:" + base.DefaultPort, "b:" + base.DefaultPort}},
		{[]string{"start", "--join=127.0.0.1"}, []string{"127.0.0.1:" + base.DefaultPort}},
		{[]string{"start", "--join=127.0.0.1:"}, []string{"127.0.0.1:" + base.DefaultPort}},
		{[]string{"start", "--join=127.0.0.1,abc"}, []string{"127.0.0.1:" + base.DefaultPort, "abc:" + base.DefaultPort}},
		{[]string{"start", "--join=[::1],[::2]"}, []string{"[::1]:" + base.DefaultPort, "[::2]:" + base.DefaultPort}},
		{[]string{"start", "--join=[::1]:123,[::2]"}, []string{"[::1]:123", "[::2]:" + base.DefaultPort}},
		{[]string{"start", "--join=[::1],127.0.0.1"}, []string{"[::1]:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort}},
		{[]string{"start", "--join=[::1]:123", "--join=[::2]"}, []string{"[::1]:123", "[::2]:" + base.DefaultPort}},
	}

	for i, td := range testData {
		initCLIDefaults()
		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		if err := extraClientFlagInit(); err != nil {
			t.Fatal(err)
		}

		var actual []string
		myHostname, _ := os.Hostname()
		for _, addr := range serverCfg.JoinList {
			res, err := resolver.NewResolver(addr)
			if err != nil {
				t.Error(err)
			}
			actualAddr := res.Addr()
			// Normalize the local hostname to make the test location-agnostic.
			actualAddr = strings.ReplaceAll(actualAddr, myHostname, "HOSTNAME")
			actual = append(actual, actualAddr)
		}
		if !reflect.DeepEqual(td.expectedJoin, actual) {
			t.Errorf("%d. serverCfg.JoinList expected %#v, but got %#v. td.args was '%#v'.",
				i, td.expectedJoin, actual, td.args)
		}
	}
}

func TestConnectJoinSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := connectInitCmd.Flags()
	testData := []struct {
		args         []string
		expectedJoin []string
	}{
		{[]string{"connect", "init", "--join=a"},
			[]string{"a:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=a,b,c"},
			[]string{"a:" + base.DefaultPort, "b:" + base.DefaultPort, "c:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=a", "--join=b"},
			[]string{"a:" + base.DefaultPort, "b:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=127.0.0.1"},
			[]string{"127.0.0.1:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=127.0.0.1:"},
			[]string{"127.0.0.1:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=127.0.0.1,abc"},
			[]string{"127.0.0.1:" + base.DefaultPort, "abc:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=[::1],[::2]"},
			[]string{"[::1]:" + base.DefaultPort, "[::2]:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=[::1]:123,[::2]"},
			[]string{"[::1]:123", "[::2]:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=[::1],127.0.0.1"},
			[]string{"[::1]:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort}},
		{[]string{"connect", "init", "--join=[::1]:123", "--join=[::2]"},
			[]string{"[::1]:123", "[::2]:" + base.DefaultPort}},
	}

	for i, td := range testData {
		initCLIDefaults()
		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		if err := extraClientFlagInit(); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(td.expectedJoin, []string(serverCfg.JoinList)) {
			t.Errorf("%d. serverCfg.JoinList expected %#v, but got %#v. td.args was '%#v'.",
				i, td.expectedJoin, serverCfg.JoinList, td.args)
		}
	}
}

func TestClientConnSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// For some reason, when run under stress all these test cases fail due to the
	// `--host` flag being unknown to quitCmd. Just skip this under stress.
	skip.UnderStress(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := quitCmd.Flags()
	testData := []struct {
		args         []string
		expectedAddr string
	}{
		{[]string{"quit"}, ":" + base.DefaultPort},
		{[]string{"quit", "--host", "127.0.0.1"}, "127.0.0.1:" + base.DefaultPort},
		{[]string{"quit", "--host", "192.168.0.111"}, "192.168.0.111:" + base.DefaultPort},
		{[]string{"quit", "--host", ":12345"}, ":12345"},
		{[]string{"quit", "--host", "127.0.0.1:12345"}, "127.0.0.1:12345"},
		// confirm hostnames will work
		{[]string{"quit", "--host", "my.host.name"}, "my.host.name:" + base.DefaultPort},
		{[]string{"quit", "--host", "myhostname"}, "myhostname:" + base.DefaultPort},
		// confirm IPv6 works too
		{[]string{"quit", "--host", "[::1]"}, "[::1]:" + base.DefaultPort},
		{[]string{"quit", "--host", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort},

		// Deprecated syntax.
		{[]string{"quit", "--port", "12345"}, ":12345"},
		{[]string{"quit", "--host", "127.0.0.1", "--port", "12345"}, "127.0.0.1:12345"},
	}

	for i, td := range testData {
		initCLIDefaults()
		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		if err := extraClientFlagInit(); err != nil {
			t.Fatal(err)
		}
		if td.expectedAddr != serverCfg.Addr {
			t.Errorf("%d. serverCfg.Addr expected '%s', but got '%s'. td.args was '%#v'.",
				i, td.expectedAddr, serverCfg.Addr, td.args)
		}
	}
}

func TestHttpHostFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args       []string
		expected   string
		tlsEnabled bool
	}{
		{[]string{"start", "--http-addr", "127.0.0.1"}, "127.0.0.1:" + base.DefaultHTTPPort, true},
		{[]string{"start", "--http-addr", "192.168.0.111"}, "192.168.0.111:" + base.DefaultHTTPPort, true},
		// confirm --http-host still works
		{[]string{"start", "--http-host", "127.0.0.1"}, "127.0.0.1:" + base.DefaultHTTPPort, true},
		{[]string{"start", "--http-addr", ":12345", "--http-host", "192.168.0.111"}, "192.168.0.111:12345", true},
		// confirm --http-port still works
		{[]string{"start", "--http-port", "12345"}, ":12345", true},
		{[]string{"start", "--http-addr", "192.168.0.111", "--" + cliflags.ListenHTTPPort.Name, "12345"}, "192.168.0.111:12345", true},
		// confirm hostnames will work
		{[]string{"start", "--http-addr", "my.host.name"}, "my.host.name:" + base.DefaultHTTPPort, true},
		{[]string{"start", "--http-addr", "myhostname"}, "myhostname:" + base.DefaultHTTPPort, true},
		// confirm IPv6 works too
		{[]string{"start", "--http-addr", "[::1]"}, "[::1]:" + base.DefaultHTTPPort, true},
		{[]string{"start", "--http-addr", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultHTTPPort, true},
		// Confirm that the host part is derived from --listen-addr if not specified.
		{[]string{"start", "--listen-addr=blah:1111"}, "blah:" + base.DefaultHTTPPort, true},
		{[]string{"start", "--listen-addr=blah:1111", "--http-addr=:1234"}, "blah:1234", true},
		{[]string{"start", "--listen-addr=blah", "--http-addr=:1234"}, "blah:1234", true},
		// Confirm that --insecure implies no TLS.
		{[]string{"start", "--http-addr=127.0.0.1", "--insecure"}, "127.0.0.1:" + base.DefaultHTTPPort, false},
		// Confirm that --unencrypted-localhost-http overrides the hostname part (and disables TLS).
		{[]string{"start", "--http-addr=:1234", "--unencrypted-localhost-http"}, "localhost:1234", false},
		{[]string{"start", "--listen-addr=localhost:1111", "--unencrypted-localhost-http"}, "localhost:" + base.DefaultHTTPPort, false},
		{[]string{"start", "--http-addr=127.0.0.1", "--unencrypted-localhost-http"},
			"ERROR: --unencrypted-localhost-http is incompatible with --http-addr=127.0.0.1:8080", false},
		{[]string{"start", "--http-addr=incompatible", "--unencrypted-localhost-http"},
			"ERROR: --unencrypted-localhost-http is incompatible with --http-addr=incompatible:8080", false},
		{[]string{"start", "--http-addr=incompatible:1111", "--unencrypted-localhost-http"},
			"ERROR: --unencrypted-localhost-http is incompatible with --http-addr=incompatible:1111", false},
	}

	for i, td := range testData {
		initCLIDefaults()

		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		err := extraServerFlagInit(startCmd)

		expectErr := strings.HasPrefix(td.expected, "ERROR:")
		expectedErr := strings.TrimPrefix(td.expected, "ERROR: ")
		if err != nil {
			if !expectErr {
				t.Fatalf("%d. error: %v", i, err)
			}
			if !testutils.IsError(err, expectedErr) {
				t.Fatalf("%d. expected error %q, got: %v", i, expectedErr, err)
			}
			continue
		}
		if err == nil && expectErr {
			t.Fatalf("%d expected error %q, got none", i, expectedErr)
		}
		exp := "http"
		if td.tlsEnabled {
			exp = "https"
		}
		if td.expected != serverCfg.HTTPAddr {
			t.Errorf("%d. serverCfg.HTTPAddr expected '%s', but got '%s'. td.args was '%#v'.", i, td.expected, serverCfg.HTTPAddr, td.args)
		}
		if exp != serverCfg.HTTPRequestScheme() {
			t.Errorf("%d. TLS config expected %s, got %s. td.args was '%#v'.", i, exp, serverCfg.HTTPRequestScheme(), td.args)
		}
	}
}

func TestMaxDiskTempStorageFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testData := []struct {
		args     []string
		expected string
	}{
		{nil, "<nil>"},
		{[]string{"--max-disk-temp-storage", "1GiB"}, "1.0 GiB"},
		{[]string{"--max-disk-temp-storage", "1GB"}, "954 MiB"},
	}

	for i, td := range testData {
		initCLIDefaults()

		if err := f.Parse(td.args); err != nil {
			t.Fatal(err)
		}
		tempStorageFlag := f.Lookup("max-disk-temp-storage")
		if tempStorageFlag == nil {
			t.Fatalf("%d. max-disk-temp-storage flag was nil", i)
		}
		if tempStorageFlag.DefValue != "<nil>" {
			t.Errorf("%d. tempStorageFlag.DefValue expected <nil>, got %s", i, tempStorageFlag.DefValue)
		}
		if td.expected != tempStorageFlag.Value.String() {
			t.Errorf("%d. tempStorageFlag.Value expected %v, but got %v", i, td.expected, tempStorageFlag.Value)
		}
	}
}

// TestFlagUsage is a basic test to make sure the fragile
// help template does not break.
func TestFlagUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	expUsage := `Usage:
  cockroach [command]

Available Commands:
  start             start a node in a multi-node cluster
  start-single-node start a single-node cluster
  connect           Create certificates for securely connecting with clusters

  init              initialize a cluster
  cert              create ca, node, and client certs
  sql               open a sql shell
  statement-diag    commands for managing statement diagnostics bundles
  auth-session      log in and out of HTTP sessions
  node              list, inspect, drain or remove nodes

  nodelocal         upload and delete nodelocal files
  userfile          upload, list and delete user scoped files
  import            import a db or table from a local PGDUMP or MYSQLDUMP file
  demo              open a demo sql shell
  convert-url       convert a SQL connection string for use with various client drivers
  gen               generate auxiliary files
  version           output version information
  debug             debugging commands
  sqlfmt            format SQL statements
  workload          generators for data and query loads
  help              Help about any command

Flags:
  -h, --help                     help for cockroach
      --log <string>             
                                  Logging configuration, expressed using YAML syntax. For example, you can
                                  change the default logging directory with: --log='file-defaults: {dir: ...}'.
                                  See the documentation for more options and details.  To preview how the log
                                  configuration is applied, or preview the default configuration, you can use
                                  the 'cockroach debug check-log-config' sub-command.
                                 
      --log-config-file <file>   
                                  File name to read the logging configuration from. This has the same effect as
                                  passing the content of the file via the --log flag.
                                  (default <unset>)
      --version                  version for cockroach

Use "cockroach [command] --help" for more information about a command.
`
	helpExpected := fmt.Sprintf("CockroachDB command-line interface and server.\n\n%s",
		// Due to a bug in spf13/cobra, 'cockroach help' does not include the --version
		// flag. Strangely, 'cockroach --help' does, as well as usage error messages.
		strings.ReplaceAll(expUsage, "      --version                  version for cockroach\n", ""))
	badFlagExpected := fmt.Sprintf("%s\nError: unknown flag: --foo\n", expUsage)

	testCases := []struct {
		flags    []string
		expErr   bool
		expected string
	}{
		{[]string{"help"}, false, helpExpected},    // request help specifically
		{[]string{"--foo"}, true, badFlagExpected}, // unknown flag
	}
	for _, test := range testCases {
		t.Run(strings.Join(test.flags, ","), func(t *testing.T) {
			// Override os.Stdout or os.Stderr with our own.
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			cockroachCmd.SetOutput(w)
			defer cockroachCmd.SetOutput(nil)

			done := make(chan error)
			var buf bytes.Buffer
			// copy the output in a separate goroutine so printing can't block indefinitely.
			go func() {
				// Copy reads 'r' until EOF is reached.
				_, err := io.Copy(&buf, r)
				done <- err
			}()

			if err := Run(test.flags); err != nil {
				fmt.Fprintln(w, "Error:", err)
				if !test.expErr {
					t.Error(err)
				}
			}

			// back to normal state
			w.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}

			// Filter out all test flags.
			testFlagRE := regexp.MustCompile(`--(test\.|vmodule|rewrite)`)
			lines := strings.Split(buf.String(), "\n")
			final := []string{}
			for _, l := range lines {
				if testFlagRE.MatchString(l) {
					continue
				}
				final = append(final, l)
			}
			got := strings.Join(final, "\n")

			assert.Equal(t, test.expected, got)
		})
	}
}
