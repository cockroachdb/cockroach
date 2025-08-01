// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestMemoryPoolFlagValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		flag   string
		config *int64
	}{
		{flag: "--max-sql-memory", config: &serverCfg.MemoryPoolSize},
		{flag: "--max-tsdb-memory", config: &serverCfg.TimeSeriesServerConfig.QueryMemoryMax},
	} {
		t.Run(tc.flag, func(t *testing.T) {
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
				args := []string{tc.flag, c.value}
				if err := f.Parse(args); err != nil {
					t.Fatal(err)
				}
				if c.expected != *tc.config {
					t.Errorf("expected %d, but got %d", c.expected, tc.config)
				}
			}

			for _, c := range []string{".30", "0.3"} {
				args := []string{tc.flag, c}
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
				if *tc.config < expectedLow || *tc.config > expectedHigh {
					t.Errorf("expected %d-%d, but got %d", expectedLow, expectedHigh, *tc.config)
				}
			}
		})
	}
}

func TestGetDefaultGoMemLimitValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	maxMem, err := status.GetTotalMemory(context.Background())
	if err != nil {
		t.Logf("total memory unknown: %v", err)
		return
	}
	if maxMem < 1<<30 /* 1GiB */ {
		// The test assumes that it is running on a machine with at least 1GiB
		// of RAM.
		skip.IgnoreLint(t)
	}

	// highCachePercentage is such that --cache would be set too high resulting
	// in the upper bound on the goMemLimit becoming negative, so we'd use the
	// lower bound.
	highCachePercentage := defaultGoMemLimitMaxTotalSystemMemUsage/defaultGoMemLimitCacheSlopMultiple + 0.02

	for i, tc := range []struct {
		maxSQLMemory string
		cache        string
		expected     int64
	}{
		{
			maxSQLMemory: "100MiB",
			cache:        "100MiB",
			// The default calculation says 225MiB which is smaller than the
			// lower bound, so we use the latter.
			expected: defaultGoMemLimitMinValue,
		},
		{
			maxSQLMemory: "200MiB",
			cache:        "100MiB",
			// The default 2.25x of --max-sql-memory.
			expected: defaultGoMemLimitSQLMultiple * 200 << 20, /* 450MiB */
		},
		{
			maxSQLMemory: "200MiB",
			cache:        fmt.Sprintf("%.2f", highCachePercentage),
			expected:     defaultGoMemLimitMinValue,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			// Avoid leaking configuration changes after the test ends.
			defer initCLIDefaults()

			f := startCmd.Flags()

			args := []string{
				"--max-sql-memory", tc.maxSQLMemory,
				"--cache", tc.cache,
			}
			if err := f.Parse(args); err != nil {
				t.Fatal(err)
			}
			limit := getDefaultGoMemLimit(context.Background())
			// Allow for some imprecision since we're dealing with float
			// arithmetic but the result is converted to integer.
			if diff := tc.expected - limit; diff > 1 || diff < -1 {
				t.Errorf("expected %d, but got %d", tc.expected, limit)
			}
		})
	}
}

func TestClockOffsetFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := startCmd.Flags()
	testCases := []struct {
		args            []string
		expectMax       time.Duration
		expectTolerated time.Duration
	}{
		{nil, 500 * time.Millisecond, 400 * time.Millisecond},
		{[]string{"--max-offset", "100ms"}, 100 * time.Millisecond, 80 * time.Millisecond},
		{[]string{"--disable-max-offset-check"}, base.DefaultMaxClockOffset, 0},
		{[]string{"--max-offset", "100ms", "--disable-max-offset-check"}, 100 * time.Millisecond, 0},
	}

	for _, tc := range testCases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			initCLIDefaults()

			require.NoError(t, f.Parse(tc.args))
			require.Equal(t, tc.expectMax, time.Duration(serverCfg.MaxOffset))
			require.Equal(t, tc.expectTolerated, serverCfg.ToleratedOffset())
		})
	}
}

func TestClientURLFlagEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	// Prepare a dummy default certificate directory.
	defCertsDirPath := t.TempDir()
	defCertsDirPath, _ = filepath.Abs(defCertsDirPath)
	cleanup := securitytest.CreateTestCerts(defCertsDirPath)
	defer func() { _ = cleanup() }()

	// Prepare a custom certificate directory.
	testCertsDirPath := t.TempDir()
	testCertsDirPath, _ = filepath.Abs(testCertsDirPath)
	cleanup2 := securitytest.CreateTestCerts(testCertsDirPath)
	defer func() { _ = cleanup2() }()

	anyCmd := []string{"sql", "node drain"}
	anyClientNonSQLCmd := []string{"gen haproxy"}
	anyNonSQL := []string{"node drain", "init"}
	anySQL := []string{"sql"}
	sqlShell := []string{"sql"}
	anyNonSQLShell := []string{"node drain"}
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
		{anyNonSQL, []string{"--url=postgresql://foo@bar"}, []string{"--user=foo", "--host=bar"}, "", ""},

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
		// Check that client commands take username into account when enforcing
		// client cert names. Concretely, it's looking for 'timapples' in the client
		// cert key file name, not 'root'.
		{anyClientNonSQLCmd, []string{"--url=postgresql://timapples@foo?sslmode=verify-full&sslkey=/certs/client.roachprod.key"}, nil, `invalid file name for "sslkey": expected "client.timapples.key", got .*`, ""},

		// Check that not specifying a certs dir will cause Go to use root trust store.
		{anyCmd, []string{"--url=postgresql://foo?sslmode=verify-full"}, []string{"--host=foo"}, "", ""},
		{anySQL, []string{"--url=postgresql://foo?sslmode=verify-ca"}, []string{"--host=foo"}, "", ""},
		{anySQL, []string{"--url=postgresql://foo?sslmode=require"}, []string{"--host=foo"}, "", ""},
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
			connUser:     cliCtx.clientOpts.User,
			connDatabase: cliCtx.clientOpts.Database,
			connHost:     cliCtx.clientOpts.ServerHost,
			connPort:     cliCtx.clientOpts.ServerPort,
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
	}{
		{[]string{"start"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "192.168.0.111"},
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", ":"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			":" + base.DefaultPort, ":" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", ":12345"},
			":12345", ":12345",
			":12345", ":12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345"},
			"127.0.0.1:12345", "127.0.0.1:12345",
			"127.0.0.1:12345", "127.0.0.1:12345",
		},
		{[]string{"start", "--listen-addr", "[::1]"},
			"[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort,
			"[::1]:" + base.DefaultPort, "[::1]:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "[::1]:12345"},
			"[::1]:12345", "[::1]:12345",
			"[::1]:12345", "[::1]:12345",
		},
		{[]string{"start", "--listen-addr", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort,
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort,
		},
		// confirm hostnames will work
		{[]string{"start", "--listen-addr", "my.host.name"},
			"my.host.name:" + base.DefaultPort, "my.host.name:" + base.DefaultPort,
			"my.host.name:" + base.DefaultPort, "my.host.name:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "myhostname"},
			"myhostname:" + base.DefaultPort, "myhostname:" + base.DefaultPort,
			"myhostname:" + base.DefaultPort, "myhostname:" + base.DefaultPort,
		},

		// SQL address override.
		{[]string{"start", "--sql-addr", "127.0.0.1"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
		},
		{[]string{"start", "--sql-addr", ":1234"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			":1234", ":1234",
		},
		{[]string{"start", "--sql-addr", "127.0.0.1:1234"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"127.0.0.1:1234", "127.0.0.1:1234",
		},
		{[]string{"start", "--sql-addr", "[::2]"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"[::2]:" + base.DefaultPort, "[::2]:" + base.DefaultPort,
		},
		{[]string{"start", "--sql-addr", "[::2]:1234"},
			":" + base.DefaultPort, ":" + base.DefaultPort,
			"[::2]:1234", "[::2]:1234",
		},

		// Configuring the components of the SQL address separately.
		{[]string{"start", "--listen-addr", "127.0.0.1", "--sql-addr", "127.0.0.2"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.2:" + base.DefaultPort, "127.0.0.2:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--sql-addr", ":1234"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.1:1234", "127.0.0.1:1234",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--sql-addr", "127.0.0.2:1234"},
			"127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort,
			"127.0.0.2:1234", "127.0.0.2:1234",
		},
		{[]string{"start", "--listen-addr", "[::2]", "--sql-addr", ":1234"},
			"[::2]:" + base.DefaultPort, "[::2]:" + base.DefaultPort,
			"[::2]:1234", "[::2]:1234",
		},

		// --advertise-addr overrides.
		{[]string{"start", "--advertise-addr", "192.168.0.111"},
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111"},
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345", "--advertise-addr", "192.168.0.111"},
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111:12345"},
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:54321", "--advertise-addr", "192.168.0.111:12345"},
			"127.0.0.1:54321", "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--listen-addr", ":12345"},
			":12345", "192.168.0.111:12345",
			":12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--listen-addr", ":54321"},
			":54321", "192.168.0.111:12345",
			":54321", "192.168.0.111:12345",
		},

		// Show that if the SQL address does not have a name default, its
		// advertised form picks up the RPC advertised address.
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--sql-addr", ":54321"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":54321", "192.168.0.111:54321",
		},

		// Show that if the SQL address is overridden, its advertised form picks the
		// advertised RPC address but keeps the port.
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--sql-addr", "127.0.0.1:54321"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:54321",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111:12345", "--sql-addr", "127.0.0.1"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--sql-addr", "127.0.0.1:12345"},
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"127.0.0.1:12345", "192.168.0.111:12345",
		},

		// Backward-compatibility flag combinations.
		{[]string{"start", "--host", "192.168.0.111"},
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			"192.168.0.111:" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--port", "12345"},
			":12345", ":12345",
			":12345", ":12345",
		},
		{[]string{"start", "--advertise-host", "192.168.0.111"},
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
			":" + base.DefaultPort, "192.168.0.111:" + base.DefaultPort,
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--port", "12345"},
			"127.0.0.1:12345", "127.0.0.1:12345",
			"127.0.0.1:12345", "127.0.0.1:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1:12345", "--port", "55555"},
			"127.0.0.1:55555", "127.0.0.1:55555",
			"127.0.0.1:55555", "127.0.0.1:55555",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "12345"},
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "12345"},
			"127.0.0.1:12345", "192.168.0.111:12345",
			"127.0.0.1:12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"},
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
			"127.0.0.1:" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"},
			"127.0.0.1:54321", "192.168.0.111:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--port", "12345"},
			":12345", "192.168.0.111:12345",
			":12345", "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--advertise-port", "12345"},
			":" + base.DefaultPort, "192.168.0.111:12345",
			":" + base.DefaultPort, "192.168.0.111:12345",
		},
		{[]string{"start", "--advertise-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"},
			":54321", "192.168.0.111:12345",
			":54321", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-sql-addr", "192.168.0.111", "--port", "54321", "--advertise-port", "12345"},
			"127.0.0.1:54321", "127.0.0.1:12345",
			"127.0.0.1:54321", "192.168.0.111:12345",
		},
		{[]string{"start", "--listen-addr", "127.0.0.1", "--advertise-sql-addr", "192.168.0.111:12345", "--port", "54321"},
			"127.0.0.1:54321", "127.0.0.1:54321",
			"127.0.0.1:54321", "192.168.0.111:12345",
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

func TestLocalityFileFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// These files have leading and trailing whitespaces to test the logic where
	// we trim those before parsing.

	emptyLocalityFile, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(emptyLocalityFile.Name(), []byte("  "), 0777))

	invalidLocalityFile, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(invalidLocalityFile.Name(), []byte("  invalid  "), 0777))

	validLocalityFile, err := os.CreateTemp(tmpDir, "")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(validLocalityFile.Name(), []byte("  region=us-east1,az=1  \n"), 0777))

	mtf := mtStartSQLCmd.Flags()
	f := startCmd.Flags()
	testData := []struct {
		args        []string
		expLocality roachpb.Locality
		expError    string
	}{
		// Both flags are incompatible.
		{
			args:     []string{"start", "--locality=region=us-east1", "--locality-file=foo"},
			expError: `--locality is incompatible with --locality-file`,
		},
		// File does not exist.
		{
			args:     []string{"start", "--locality-file=donotexist"},
			expError: `invalid argument .* no such file or directory`,
		},
		// File is empty.
		{
			args:     []string{"start", fmt.Sprintf("--locality-file=%s", emptyLocalityFile.Name())},
			expError: `invalid locality data "" .* can't have empty locality`,
		},
		// File is invalid.
		{
			args:     []string{"start", fmt.Sprintf("--locality-file=%s", invalidLocalityFile.Name())},
			expError: `invalid locality data "invalid" .* tier must be in the form "key=value"`,
		},
		// mt start-sql with --locality-file and --tenant-id-file should defer.
		{
			args:        []string{"mt", "start-sql", "--tenant-id-file=tid", fmt.Sprintf("--locality-file=%s", validLocalityFile.Name())},
			expLocality: roachpb.Locality{},
		},
		// mt start-sql with --locality-file.
		{
			args: []string{"mt", "start-sql", fmt.Sprintf("--locality-file=%s", validLocalityFile.Name())},
			expLocality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east1"},
					{Key: "az", Value: "1"},
				},
			},
		},
		// Only --locality.
		{
			args: []string{"start", "--locality=region=us-central1"},
			expLocality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-central1"},
				},
			},
		},
		// Only --locality-file.
		{
			args: []string{"start", fmt.Sprintf("--locality-file=%s", validLocalityFile.Name())},
			expLocality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{Key: "region", Value: "us-east1"},
					{Key: "az", Value: "1"},
				},
			},
		},
	}
	for i, td := range testData {
		t.Run(strings.Join(td.args, " "), func(t *testing.T) {
			initCLIDefaults()
			var err error
			if td.args[0] == "start" {
				require.NoError(t, f.Parse(td.args))
				err = extraServerFlagInit(startCmd)
			} else {
				require.NoError(t, mtf.Parse(td.args))
				err = extraServerFlagInit(mtStartSQLCmd)
			}
			if td.expError == "" {
				require.NoError(t, err)
				require.Equal(t, td.expLocality, serverCfg.Locality)
			} else {
				require.Regexp(t, td.expError, err.Error(),
					"%d. expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expError, err.Error(), td.args)
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
		for _, j := range serverCfg.JoinList {
			addr := util.MakeUnresolvedAddrWithDefaults("tcp", j, base.DefaultPort)

			// Normalize the local hostname to make the test location-agnostic.
			actual = append(actual, strings.ReplaceAll(addr.String(), myHostname, "HOSTNAME"))
		}
		if !reflect.DeepEqual(td.expectedJoin, actual) {
			t.Errorf("%d. serverCfg.JoinList expected %#v, but got %#v. td.args was '%#v'.",
				i, td.expectedJoin, actual, td.args)
		}
	}
}

func TestClientConnSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// For some reason, when run under stress all these test cases fail due to the
	// `--host` flag being unknown to quitCmd. Just skip this under stress.
	// TODO(knz): Check if this skip still applies.
	skip.UnderStress(t)

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	f := drainNodeCmd.Flags()
	testData := []struct {
		args         []string
		expectedAddr string
	}{
		{[]string{"node", "drain"}, "localhost:" + base.DefaultPort},
		{[]string{"node", "drain", "--host", "127.0.0.1"}, "127.0.0.1:" + base.DefaultPort},
		{[]string{"node", "drain", "--host", "192.168.0.111"}, "192.168.0.111:" + base.DefaultPort},
		{[]string{"node", "drain", "--host", ":12345"}, ":12345"},
		{[]string{"node", "drain", "--host", "127.0.0.1:12345"}, "127.0.0.1:12345"},
		// confirm hostnames will work
		{[]string{"node", "drain", "--host", "my.host.name"}, "my.host.name:" + base.DefaultPort},
		{[]string{"node", "drain", "--host", "myhostname"}, "myhostname:" + base.DefaultPort},
		// confirm IPv6 works too
		{[]string{"node", "drain", "--host", "[::1]"}, "[::1]:" + base.DefaultPort},
		{[]string{"node", "drain", "--host", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]"},
			"[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultPort},

		// Deprecated syntax.
		{[]string{"node", "drain", "--port", "12345"}, "localhost:12345"},
		{[]string{"node", "drain", "--host", "127.0.0.1", "--port", "12345"}, "127.0.0.1:12345"},
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

func TestHttpAdvertiseAddrFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer initCLIDefaults()

	// Prepare some reference strings that will be checked in the
	// test below.
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}

	f := startCmd.Flags()
	for i, tc := range []struct {
		args                        []string
		expected                    string
		expectedAfterAddrValidation string
		expectedServerHTTPAddr      string
		tlsEnabled                  bool
	}{
		{[]string{"start"},
			":" + base.DefaultHTTPPort,
			hostname + ":" + base.DefaultHTTPPort,
			":" + base.DefaultHTTPPort,
			true},
		{[]string{"start", "--http-addr", hostAddr},
			hostAddr + ":" + base.DefaultHTTPPort,
			hostAddr + ":" + base.DefaultHTTPPort,
			hostAddr + ":" + base.DefaultHTTPPort,
			true},
		{[]string{"start", "--advertise-addr", "adv.example.com", "--http-addr", "http.example.com"},
			"adv.example.com:" + base.DefaultHTTPPort,
			"adv.example.com:" + base.DefaultHTTPPort,
			"http.example.com:" + base.DefaultHTTPPort,
			true},
		{[]string{"start", "--advertise-addr", "adv.example.com:2345", "--http-addr", "http.example.com:1234"},
			"adv.example.com:1234",
			"adv.example.com:1234",
			"http.example.com:1234",
			true},
		{[]string{"start", "--advertise-addr", "example.com"},
			"example.com:" + base.DefaultHTTPPort,
			"example.com:" + base.DefaultHTTPPort,
			":" + base.DefaultHTTPPort,
			true},
		{[]string{"start", "--advertise-http-addr", "example.com"},
			"example.com:" + base.DefaultHTTPPort,
			"example.com:" + base.DefaultHTTPPort,
			":" + base.DefaultHTTPPort,
			true},
		{[]string{"start", "--http-addr", "http.example.com", "--advertise-http-addr", "example.com"},
			"example.com:" + base.DefaultHTTPPort,
			"example.com:" + base.DefaultHTTPPort,
			"http.example.com:" + base.DefaultHTTPPort,
			true},
		{[]string{"start", "--advertise-addr", "adv.example.com", "--advertise-http-addr", "example.com"},
			"example.com:" + base.DefaultHTTPPort,
			"example.com:" + base.DefaultHTTPPort,
			":" + base.DefaultHTTPPort,

			true},
		{[]string{"start", "--advertise-addr", "adv.example.com:1234", "--http-addr", "http.example.com:2345", "--advertise-http-addr", "example.com:3456"},
			"example.com:3456",
			"example.com:3456",
			"http.example.com:2345",
			true},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			initCLIDefaults()
			require.NoError(t, f.Parse(tc.args))

			err := extraServerFlagInit(startCmd)
			require.NoError(t, err)

			exp := "http"
			if tc.tlsEnabled {
				exp = "https"
			}
			require.Equal(t, tc.expected, serverCfg.HTTPAdvertiseAddr,
				"serverCfg.HTTPAdvertiseAddr expected '%s', but got '%s'. td.args was '%#v'.",
				tc.expected, serverCfg.HTTPAdvertiseAddr, tc.args)
			require.Equal(t, exp, serverCfg.HTTPRequestScheme(),
				"TLS config expected %s, got %s. td.args was '%#v'.", exp, serverCfg.HTTPRequestScheme(), tc.args)

			ctx := context.Background()
			err = serverCfg.ValidateAddrs(ctx)
			if err != nil {
				// Don't care about resolution failures
				if !strings.Contains(err.Error(), "invalid --http-addr") {
					t.Errorf("unexpected error: %s", err)
				}
			}
			require.Equal(t, tc.expectedAfterAddrValidation, serverCfg.HTTPAdvertiseAddr, "http advertise addr after validation")
			require.Equal(t, tc.expectedServerHTTPAddr, serverCfg.HTTPAddr, "http addr after validation")
		})
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

	commandPrefix := "CockroachDB command-line interface and server.\n\n"
	expUsage := `Usage:
  cockroach [command]

Available Commands:
  start             start a node in a multi-node cluster
  start-single-node start a single-node cluster
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
  encode-uri        encode a CRDB connection URL
  help              Help about any command

Flags:
  -h, --help                               help for cockroach
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
      --log-config-vars strings            
                                            Environment variables that will be expanded if present in the body of the
                                            logging configuration.
                                           
      --log-dir <string>                   
                                            --log-dir=XXX is an alias for --log='file-defaults: {dir: XXX}'.
                                           
      --logtostderr <severity>[=DEFAULT]   
                                            --logtostderr=XXX is an alias for --log='sinks: {stderr: {filter: XXX}}'. If
                                            no value is specified, the default value for the command is inferred: INFO for
                                            server commands, WARNING for client commands.
                                            (default UNKNOWN)
      --redactable-logs                    
                                            --redactable-logs=XXX is an alias for --log='file-defaults: {redactable:
                                            XXX}}'.
                                           
      --version                            version for cockroach

Use "cockroach [command] --help" for more information about a command.
`
	// Due to a bug in spf13/cobra, 'cockroach help' does not include the --version
	// flag *the first time the test runs*.
	// (But it does if the test is run a second time.)
	// Strangely, 'cockroach --help' does, as well as usage error messages.
	helpExpected1 := commandPrefix + expUsage
	helpExpected2 := strings.ReplaceAll(helpExpected1, "      --version                            version for cockroach\n", "")
	badFlagExpected := fmt.Sprintf("%s\nError: unknown flag: --foo\n", expUsage)

	testCases := []struct {
		flags         []string
		expErr        bool
		expectHelp    bool
		expectBadFlag bool
	}{
		{[]string{"help"}, false, true, false}, // request help specifically
		{[]string{"--foo"}, true, false, true}, // unknown flag
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

			if test.expectBadFlag {
				assert.Equal(t, badFlagExpected, got)
			}
			if test.expectHelp {
				if got != helpExpected1 && got != helpExpected2 {
					diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
						A:        difflib.SplitLines(strings.ReplaceAll(helpExpected1, "\n", "$\n")),
						B:        difflib.SplitLines(strings.ReplaceAll(got, "\n", "$\n")),
						FromFile: "Expected",
						ToFile:   "Actual",
						Context:  1,
					})
					t.Errorf("Diff:\n%s", diff)
				}
			}
		})
	}
}

func TestSQLPodStorageDefaults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer initCLIDefaults()

	expectedDefaultDir, err := base.GetAbsoluteFSPath("", "cockroach-data-tenant-9")
	if err != nil {
		t.Fatal(err)
	}

	for _, td := range []struct {
		args        []string
		storePath   string
		expectedErr string
	}{
		{
			args:      []string{"mt", "start-sql", "--tenant-id", "9"},
			storePath: expectedDefaultDir,
		},
		{
			args:      []string{"mt", "start-sql", "--tenant-id", "9", "--store", "/tmp/data"},
			storePath: "/tmp/data",
		},
		{
			args:      []string{"mt", "start-sql", "--tenant-id-file", "foo", "--store", "/tmp/data"},
			storePath: "/tmp/data",
		},
		{
			args:        []string{"mt", "start-sql", "--tenant-id-file", "foo"},
			expectedErr: "--store must be explicitly supplied when using --tenant-id-file",
		},
	} {
		t.Run(strings.Join(td.args, ","), func(t *testing.T) {
			initCLIDefaults()
			f := mtStartSQLCmd.Flags()
			require.NoError(t, f.Parse(td.args))
			err := mtStartSQLCmd.PersistentPreRunE(mtStartSQLCmd, td.args)
			if td.expectedErr == "" {
				require.NoError(t, err)
				assert.Equal(t, td.storePath, serverCfg.Stores.Specs[0].Path)
				for _, s := range serverCfg.Stores.Specs {
					assert.Zero(t, s.BallastSize.Capacity)
					assert.Zero(t, s.BallastSize.Percent)
				}
			} else {
				require.EqualError(t, err, td.expectedErr)
			}
		})
	}
}

func TestTenantID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name        string
		arg         string
		errContains string
	}{
		{"empty tenant id text", "", "invalid tenant ID: strconv.ParseUint"},
		{"tenant id text not integer", "abc", "invalid tenant ID: strconv.ParseUint"},
		{"tenant id is 0", "0", "invalid tenant ID"},
		{"tenant id is valid", "2", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgTenantID, err := tenantID(tt.arg)
			if tt.errContains == "" {
				assert.NoError(t, err)
				assert.Equal(t, tt.arg, cfgTenantID.String())
			} else {
				assert.ErrorContains(t, err, tt.errContains)
				assert.Equal(t, roachpb.TenantID{}, cfgTenantID)
			}
		})
	}
}

func TestTenantName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name        string
		arg         string
		errContains string
	}{
		{"empty tenant name text", "", "invalid tenant name: \"\""},
		{"tenant name not valid", "a+bc", "invalid tenant name: \"a+bc\""},
		{"tenant name \"abc\" is valid", "abc", ""},
		{"tenant name \"system\" is valid", "system", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tns := tenantNameSetter{tenantNames: &[]roachpb.TenantName{}}
			err := tns.Set(tt.arg)
			if tt.errContains == "" {
				assert.NoError(t, err)
				assert.Equal(t, tt.arg, tns.String())
			} else {
				assert.True(t, strings.Contains(err.Error(), tt.errContains))
			}
		})
	}
}

func TestTenantIDFromFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	createTempFileWithData := func(t *testing.T, data string) *os.File {
		t.Helper()
		// Put the file in a nested directory within the root temp directory.
		// That way, we don't end up using the default root temp directory as
		// the directory to watch as there will be lots of files created during
		// a stress test, and this will slow down the watcher.
		tmpDir, err := os.MkdirTemp("", "")
		require.NoError(t, err)
		file, err := os.CreateTemp(tmpDir, "")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(file.Name(), []byte(data), 0777))
		t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
		return file
	}

	writeFile := func(t *testing.T, filename, data string) {
		t.Helper()
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
		require.NoError(t, err)
		defer file.Close()
		_, err = file.WriteString(data)
		require.NoError(t, err)
	}

	t.Run("unrelated files do not trigger a read",
		func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "")
			require.NoError(t, err)
			defer func() { _ = os.RemoveAll(tmpDir) }()
			// Two unrelated files, and one file of concern.
			file1 := filepath.Join(tmpDir, "file1")
			file2 := filepath.Join(tmpDir, "file2")
			filename := filepath.Join(tmpDir, "TENANT_ID_FILE")

			watcherWaitCount := atomic.Uint32{}
			watcherEventCount := atomic.Uint32{}
			watcherReadCount := atomic.Uint32{}
			runSuccessfuly := atomic.Bool{}
			go func() {
				cfgTenantID, err := tenantIDFromFile(ctx, filename, &watcherWaitCount, &watcherEventCount, &watcherReadCount)
				require.NoError(t, err)
				require.EqualValues(t, 123, cfgTenantID.ToUint64())
				runSuccessfuly.Store(true)
			}()
			require.Eventually(t, func() bool { return watcherWaitCount.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 0, watcherEventCount.Load())
			require.Equal(t, false, runSuccessfuly.Load())

			// Create a temporary file, which we will swap into file3.
			var tmpFile3, tmpTenantFile string
			func() {
				f := createTempFileWithData(t, "test contents\n")
				defer f.Close()
				tmpFile3 = f.Name()

				tenantFile := createTempFileWithData(t, "123\n")
				defer tenantFile.Close()
				tmpTenantFile = tenantFile.Name()
			}()

			// Write to file1, file2, and file3.
			writeFile(t, file1, "foo")
			require.NoError(t, os.Rename(tmpFile3, filepath.Join(tmpDir, "file3")))
			writeFile(t, file1, "bar")
			writeFile(t, file2, "testing\n")
			writeFile(t, file1, "\n")

			// The watcher events may be in any order, so we'll make sure all
			// the events for files 1-3 are received before writing the actual
			// file which stops the watcher.
			require.Eventually(t, func() bool { return watcherEventCount.Load() >= 3 }, 10*time.Second, 10*time.Millisecond)

			// Finally, write to the actual file.
			require.NoError(t, os.Rename(tmpTenantFile, filename))

			// Check that there are at least 4 events, one for each file,
			// and only two reads (one initial, and another during the CREATE
			// event).
			require.Eventually(t, func() bool { return watcherEventCount.Load() >= 4 }, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return runSuccessfuly.Load() }, 10*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 2, watcherReadCount.Load())
		})

	t.Run("file does not exists, has incomplete first row, waits for it to complete and after completion reads valid tenant id",
		func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "")
			require.NoError(t, err)
			defer func() { _ = os.RemoveAll(tmpDir) }()
			filename := filepath.Join(tmpDir, "TENANT_ID_FILE")

			watcherWaitCount := atomic.Uint32{}
			watcherEventCount := atomic.Uint32{}
			runSuccessfuly := atomic.Bool{}
			go func() {
				cfgTenantID, err := tenantIDFromFile(ctx, filename, &watcherWaitCount, &watcherEventCount, nil)
				require.NoError(t, err)
				require.EqualValues(t, 123, cfgTenantID.ToUint64())
				runSuccessfuly.Store(true)
			}()
			require.Eventually(t, func() bool { return watcherWaitCount.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 0, watcherEventCount.Load())
			require.Equal(t, false, runSuccessfuly.Load())

			// Write a file partially.
			writeFile(t, filename, "12")
			require.Eventually(t, func() bool { return watcherWaitCount.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return watcherEventCount.Load() >= 1 }, 10*time.Second, 10*time.Millisecond)

			// Complete the remaining of the file.
			writeFile(t, filename, "3\n")
			require.Eventually(t, func() bool { return watcherEventCount.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return runSuccessfuly.Load() }, 10*time.Second, 10*time.Millisecond)
		})

	t.Run("file exists, has complete first row, but the value is an invalid tenant id",
		func(t *testing.T) {
			file := createTempFileWithData(t, "abc\n")
			defer file.Close()
			cfgTenantID, err := tenantIDFromFile(ctx, file.Name(), nil, nil, nil)
			require.ErrorContains(t, err, "invalid tenant ID: strconv.ParseUint")
			require.Equal(t, roachpb.TenantID{}, cfgTenantID)
		})

	t.Run("file exists, has incomplete first row, waits for it to complete and after completion with invalid tenant id fails",
		func(t *testing.T) {
			file := createTempFileWithData(t, "abc")
			defer file.Close()
			watcherWaitCount := atomic.Uint32{}
			watcherEventCount := atomic.Uint32{}
			generatedError := atomic.Bool{}
			go func() {
				cfgTenantID, err := tenantIDFromFile(ctx, file.Name(), &watcherWaitCount, &watcherEventCount, nil)
				require.ErrorContains(t, err, "invalid tenant ID: strconv.ParseUint")
				require.Equal(t, roachpb.TenantID{}, cfgTenantID)
				generatedError.Store(true)
			}()
			require.Eventually(t, func() bool { return watcherWaitCount.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 0, watcherEventCount.Load())
			require.Equal(t, false, generatedError.Load())
			require.NoError(t, os.WriteFile(file.Name(), []byte("abc\n"), 0777))
			require.Eventually(t, func() bool { return watcherEventCount.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return generatedError.Load() }, 10*time.Second, 10*time.Millisecond)
		})

	t.Run("file exists, has complete first row, it has tenant id and it is set to valid value",
		func(t *testing.T) {
			file := createTempFileWithData(t, "123\n")
			defer file.Close()
			cfgTenantID, err := tenantIDFromFile(ctx, file.Name(), nil, nil, nil)
			require.NoError(t, err)
			require.EqualValues(t, 123, cfgTenantID.ToUint64())
		})

	t.Run("file exists, has incomplete first row, waits for it to complete and after completion reads valid tenant id",
		func(t *testing.T) {
			file := createTempFileWithData(t, "abc")
			defer file.Close()
			watcherWaitCount := atomic.Uint32{}
			watcherEventCount := atomic.Uint32{}
			runSuccessfuly := atomic.Bool{}
			go func() {
				cfgTenantID, err := tenantIDFromFile(ctx, file.Name(), &watcherWaitCount, &watcherEventCount, nil)
				require.NoError(t, err)
				require.EqualValues(t, 123, cfgTenantID.ToUint64())
				runSuccessfuly.Store(true)
			}()
			require.Eventually(t, func() bool { return watcherWaitCount.Load() == 1 }, 10*time.Second, 10*time.Millisecond)
			require.EqualValues(t, 0, watcherEventCount.Load())
			require.Equal(t, false, runSuccessfuly.Load())
			require.NoError(t, os.WriteFile(file.Name(), []byte("123\n"), 0777))
			require.Eventually(t, func() bool { return watcherEventCount.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return runSuccessfuly.Load() }, 10*time.Second, 10*time.Millisecond)
		})
}
