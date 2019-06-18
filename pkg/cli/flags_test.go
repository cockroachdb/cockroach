// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cli

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/spf13/cobra"
)

func TestStdFlagToPflag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "test.") {
			return
		}
		switch f.Name {
		case logflags.LogDirName,
			logflags.LogFileMaxSizeName,
			logflags.LogFilesCombinedMaxSizeName,
			logflags.LogFileVerbosityThresholdName:
			return
		}
		if pf := cf.Lookup(f.Name); pf == nil {
			t.Errorf("unable to find \"%s\"", f.Name)
		}
	})
}

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		// Raven (Sentry) and the errors library use go/build to determine
		// the list of source directories (used to strip the source prefix
		// in stack trace reports).
		"github.com/cockroachdb/cockroach/vendor/github.com/getsentry/raven-go",
		"github.com/cockroachdb/cockroach/vendor/github.com/cockroachdb/errors/withstack",
	)
}

func TestCacheFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	f := StartCmd.Flags()
	args := []string{"--cache", "100MB"}
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}

	const expectedCacheSize = 100 * 1000 * 1000
	if expectedCacheSize != serverCfg.CacheSize {
		t.Errorf("expected %d, but got %d", expectedCacheSize, serverCfg.CacheSize)
	}
}

func TestSQLMemoryPoolFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the test ends.
	defer initCLIDefaults()

	f := StartCmd.Flags()

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
		if c.expected != serverCfg.SQLMemoryPoolSize {
			t.Errorf("expected %d, but got %d", c.expected, serverCfg.SQLMemoryPoolSize)
		}
	}

	for _, c := range []string{".30", "0.3"} {
		args := []string{"--max-sql-memory", c}
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}

		// Check fractional values.
		maxMem, err := status.GetTotalMemory(context.TODO())
		if err != nil {
			t.Logf("total memory unknown: %v", err)
			return
		}
		expectedLow := (maxMem * 28) / 100
		expectedHigh := (maxMem * 32) / 100
		if serverCfg.SQLMemoryPoolSize < expectedLow || serverCfg.SQLMemoryPoolSize > expectedHigh {
			t.Errorf("expected %d-%d, but got %d", expectedLow, expectedHigh, serverCfg.SQLMemoryPoolSize)
		}
	}
}

func TestClockOffsetFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := StartCmd.Flags()
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
	anySQL := []string{"sql", "dump"}
	sqlShell := []string{"sql"}
	anyNonSQLShell := []string{"dump", "quit"}

	testData := []struct {
		cmds    []string
		flags   []string
		refargs []string
		expErr  string
	}{
		// Check individual URL components.
		{anyCmd, []string{"--url=http://foo"}, nil, `URL scheme must be "postgresql"`},
		{anyCmd, []string{"--url=postgresql:foo/bar"}, nil, `unknown URL format`},

		{anyCmd, []string{"--url=postgresql://foo"}, []string{"--host=foo"}, ""},
		{anyCmd, []string{"--url=postgresql://:foo"}, []string{"--port=foo"}, ""},

		{sqlShell, []string{"--url=postgresql:///foo"}, []string{"--database=foo"}, ""},
		{anyNonSQLShell, []string{"--url=postgresql://foo/bar"}, []string{"--host=foo" /*db ignored*/}, ""},

		{anySQL, []string{"--url=postgresql://foo@"}, []string{"--user=foo"}, ""},
		{anyNonSQL, []string{"--url=postgresql://foo@bar"}, []string{"--host=bar" /*user ignored*/}, ""},

		{sqlShell, []string{"--url=postgresql://a@b:c/d"}, []string{"--user=a", "--host=b", "--port=c", "--database=d"}, ""},
		{anySQL, []string{"--url=postgresql://a@b:c"}, []string{"--user=a", "--host=b", "--port=c"}, ""},
		{anyNonSQL, []string{"--url=postgresql://b:c"}, []string{"--host=b", "--port=c"}, ""},

		{anyCmd, []string{"--url=postgresql://foo?sslmode=disable"}, []string{"--host=foo", "--insecure"}, ""},
		{anySQL, []string{"--url=postgresql://foo?sslmode=require"}, []string{"--host=foo", "--insecure=false"}, ""},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=require"}, nil, "command .* only supports sslmode=disable or sslmode=verify-full"},
		{anyCmd, []string{"--url=postgresql://foo?sslmode=verify-full"}, []string{"--host=foo", "--insecure=false"}, ""},

		// URL picks up previous flags if component not specified.
		{anyCmd, []string{"--host=baz", "--url=postgresql://:foo"}, []string{"--host=baz", "--port=foo"}, ""},
		{anyCmd, []string{"--port=baz", "--url=postgresql://foo"}, []string{"--host=foo", "--port=baz"}, ""},
		{sqlShell, []string{"--database=baz", "--url=postgresql://foo"}, []string{"--host=foo", "--database=baz"}, ""},
		{anySQL, []string{"--user=baz", "--url=postgresql://foo"}, []string{"--host=foo", "--user=baz"}, ""},
		{anyCmd, []string{"--insecure=false", "--url=postgresql://foo"}, []string{"--host=foo", "--insecure=false"}, ""},
		{anyCmd, []string{"--insecure", "--url=postgresql://foo"}, []string{"--host=foo", "--insecure"}, ""},

		// URL overrides previous flags if component specified.
		{anyCmd, []string{"--host=baz", "--url=postgresql://bar"}, []string{"--host=bar"}, ""},
		{anyCmd, []string{"--port=baz", "--url=postgresql://foo:bar"}, []string{"--host=foo", "--port=bar"}, ""},
		{sqlShell, []string{"--database=baz", "--url=postgresql://foo/bar"}, []string{"--host=foo", "--database=bar"}, ""},
		{anySQL, []string{"--user=baz", "--url=postgresql://bar@foo"}, []string{"--host=foo", "--user=bar"}, ""},
		{anyCmd, []string{"--insecure=false", "--url=postgresql://foo?sslmode=disable"}, []string{"--host=foo", "--insecure"}, ""},
		{anyCmd, []string{"--insecure", "--url=postgresql://foo?sslmode=verify-full"}, []string{"--host=foo", "--insecure=false"}, ""},

		// Discrete flag overrides URL if specified afterwards.
		{anyCmd, []string{"--url=postgresql://bar", "--host=baz"}, []string{"--host=baz"}, ""},
		{anyCmd, []string{"--url=postgresql://foo:bar", "--port=baz"}, []string{"--host=foo", "--port=baz"}, ""},
		{sqlShell, []string{"--url=postgresql://foo/bar", "--database=baz"}, []string{"--host=foo", "--database=baz"}, ""},
		{anySQL, []string{"--url=postgresql://bar@foo", "--user=baz"}, []string{"--host=foo", "--user=baz"}, ""},
		{anyCmd, []string{"--url=postgresql://foo?sslmode=disable", "--insecure=false"}, []string{"--host=foo", "--insecure=false"}, ""},
		{anyCmd, []string{"--url=postgresql://foo?sslmode=verify-full", "--insecure"}, []string{"--host=foo", "--insecure"}, ""},

		// Check that the certs dir is extracted properly.
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslrootcert=" + testCertsDirPath + "/ca.crt"}, []string{"--host=foo", "--certs-dir=" + testCertsDirPath}, ""},
		{anyNonSQL, []string{"--certs-dir=blah", "--url=postgresql://foo?sslmode=verify-full&sslrootcert=blih/ca.crt"}, nil, "non-homogeneous certificate directory"},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslrootcert=blih/ca.crt&sslcert=blah/client.root.crt"}, nil, "non-homogeneous certificate directory"},

		// Check the cert component file names are checked.
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslrootcert=blih/loh.crt"}, nil, `invalid file name for "sslrootcert": expected .* got .*`},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslcert=blih/loh.crt"}, nil, `invalid file name for "sslcert": expected .* got .*`},
		{anyNonSQL, []string{"--url=postgresql://foo?sslmode=verify-full&sslkey=blih/loh.crt"}, nil, `invalid file name for "sslkey": expected .* got .*`},
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

	for _, test := range testData {
		for _, cmdName := range test.cmds {
			t.Run(fmt.Sprintf("%s/%s", cmdName, strings.Join(test.flags, " ")), func(t *testing.T) {
				cmd, _, _ := cockroachCmd.Find([]string{cmdName})

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
				resultURL := connURL.String()

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
				defaultURL := connURL.String()

				// Verify that parsing the URL produces the same parameters as parsing the discrete flags.
				if urlParams != discreteParams {
					t.Fatalf("mismatch: URL %q parses\n%+v,\ndiscrete parses\n%+v", resultURL, urlParams, discreteParams)
				}

				// Re-parse using the derived URL.
				// We'll want to ensure below that the derived URL specifies the same parameters
				// (i.e. check makeClientConnURL does its work properly).
				initCLIDefaults()
				cliCtx.SSLCertsDir = defCertsDirPath
				if err := cmd.ParseFlags([]string{"--url=" + resultURL}); err != nil {
					t.Fatal(err)
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

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := StartCmd.Flags()
	testData := []struct {
		args                     []string
		expectedAddr             string
		expectedAdvertiseAddr    string
		expLocalityAdvertiseAddr string
	}{
		{[]string{"start"}, ":" + base.DefaultPort, ":" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1"}, "127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "192.168.0.111"}, "192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", ":12345"}, ":12345", ":12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345"}, "127.0.0.1:12345", "127.0.0.1:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345", "--port", "55555"}, "127.0.0.1:55555", "127.0.0.1:55555", "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111"}, ":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort, "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345"}, ":" + base.DefaultPort, "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111"}, "127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345", "--advertise-addr", "192.168.0.111"}, "127.0.0.1:12345", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111:12345"}, "127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1:54321", "--advertise-addr", "192.168.0.111:12345"}, "127.0.0.1:54321", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--listen-addr", ":12345"}, ":12345", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--listen-addr", ":54321"}, ":54321", "192.168.0.111:12345", "[]"},
		// confirm hostnames will work
		{[]string{"start", "--listen-addr", "my.host.name"}, "my.host.name:" + base.DefaultPort, "my.host.name:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "myhostname"}, "myhostname:" + base.DefaultPort, "myhostname:" + base.DefaultPort, "[]"},
		// confirm IPv6 works too
		{[]string{"start", "--listen-addr", "[::1]"}, "[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[]"},

		// Backward-compatibility flag combinations.
		{[]string{"start", "--host", "192.168.0.111"}, "192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort, "[]"},
		{[]string{"start", "--port", "12345"}, ":12345", ":12345", "[]"},
		{[]string{"start", "--advertise-host", "192.168.0.111"}, ":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort, "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"}, ":" + base.DefaultPort, "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "::1"}, "[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "2622:6221:e663:4922:fc2b:788b:fadd:7b48", "[]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--port", "12345"}, "127.0.0.1:12345", "127.0.0.1:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "12345"}, "127.0.0.1:12345", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "12345"}, "127.0.0.1:12345", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"}, "127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345", "[]"},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"}, "127.0.0.1:54321", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--port", "12345"}, ":12345", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"}, ":" + base.DefaultPort, "192.168.0.111:12345", "[]"},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"}, ":54321", "192.168.0.111:12345", "[]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5"}, "127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort, "[{{tcp 235.0.0.5:26257} zone=1}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5,zone=2@123.0.0.5"}, "127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort, "[{{tcp 235.0.0.5:26257} zone=1} {{tcp 123.0.0.5:26257} zone=2}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5:1234"}, "127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort, "[{{tcp 235.0.0.5:1234} zone=1}]"},
	}

	for i, td := range testData {
		t.Run(strings.Join(td.args, " "), func(t *testing.T) {
			initCLIDefaults()
			if err := f.Parse(td.args); err != nil {
				t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
			}

			extraServerFlagInit()
			if td.expectedAddr != serverCfg.Addr {
				t.Errorf("%d. serverCfg.Addr expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expectedAddr, serverCfg.Addr, td.args)
			}
			if td.expectedAdvertiseAddr != serverCfg.AdvertiseAddr {
				t.Errorf("%d. serverCfg.AdvertiseAddr expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expectedAdvertiseAddr, serverCfg.AdvertiseAddr, td.args)
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

func TestClientConnSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// For some reason, when run under stress all these test cases fail due to the
	// `--host` flag being unknown to quitCmd. Just skip this under stress.
	if testutils.NightlyStress() {
		t.Skip()
	}

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
		{[]string{"quit", "--host", "::1"}, "[::1]:" + base.DefaultPort},
		{[]string{"quit", "--host", "2622:6221:e663:4922:fc2b:788b:fadd:7b48"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort},
	}

	for i, td := range testData {
		initCLIDefaults()
		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		extraClientFlagInit()
		if td.expectedAddr != serverCfg.Addr {
			t.Errorf("%d. serverCfg.Addr expected '%s', but got '%s'. td.args was '%#v'.",
				i, td.expectedAddr, serverCfg.Addr, td.args)
		}
	}
}

func TestHttpHostFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := StartCmd.Flags()
	testData := []struct {
		args     []string
		expected string
	}{
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "127.0.0.1"}, "127.0.0.1:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "192.168.0.111"}, "192.168.0.111:" + base.DefaultHTTPPort},
		// confirm --http-host still works
		{[]string{"start", "--" + cliflags.ListenHTTPAddrAlias.Name, "127.0.0.1"}, "127.0.0.1:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, ":12345", "--" + cliflags.ListenHTTPAddrAlias.Name, "192.168.0.111"}, "192.168.0.111:12345"},
		// confirm --http-port still works
		{[]string{"start", "--" + cliflags.ListenHTTPPort.Name, "12345"}, ":12345"},
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "192.168.0.111", "--" + cliflags.ListenHTTPPort.Name, "12345"}, "192.168.0.111:12345"},
		// confirm hostnames will work
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "my.host.name"}, "my.host.name:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "myhostname"}, "myhostname:" + base.DefaultHTTPPort},
		// confirm IPv6 works too
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "::1"}, "[::1]:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ListenHTTPAddr.Name, "2622:6221:e663:4922:fc2b:788b:fadd:7b48"}, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultHTTPPort},
	}

	for i, td := range testData {
		initCLIDefaults()

		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		extraServerFlagInit()
		if td.expected != serverCfg.HTTPAddr {
			t.Errorf("%d. serverCfg.HTTPAddr expected '%s', but got '%s'. td.args was '%#v'.", i, td.expected, serverCfg.HTTPAddr, td.args)
		}
	}
}
