// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStdFlagToPflag(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		})
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
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5"}, "127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort, "[{{tcp 235.0.0.5} zone=1}]"},
		{[]string{"start", "--host", "127.0.0.1", "--locality-advertise-addr", "zone=1@235.0.0.5,zone=2@123.0.0.5"}, "127.0.0.1:" + base.DefaultPort, "127.0.0.1:" + base.DefaultPort, "[{{tcp 235.0.0.5} zone=1} {{tcp 123.0.0.5} zone=2}]"},
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
			if td.expLocalityAdvertiseAddr != fmt.Sprintf("%s", serverCfg.LocalityIPAddresses) {
				t.Errorf("%d. serverCfg.expLocalityAdvertiseAddr expected '%s', but got '%s'. td.args was '%#v'.",
					i, td.expLocalityAdvertiseAddr, serverCfg.LocalityIPAddresses, td.args)
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
