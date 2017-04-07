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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
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
		"github.com/cockroachdb/cockroach", true,
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

func TestSQLMemoryPoolFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()
	args := []string{"--max-sql-memory", "100MB"}
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}

	const expectedSQLMemSize = 100 * 1000 * 1000
	if expectedSQLMemSize != serverCfg.SQLMemoryPoolSize {
		t.Errorf("expected %d, but got %d", expectedSQLMemSize, serverCfg.SQLMemoryPoolSize)
	}
}

func TestRaftTickIntervalFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()
	testData := []struct {
		args     []string
		expected time.Duration
	}{
		{nil, base.DefaultRaftTickInterval},
		{[]string{"--raft-tick-interval", "200ms"}, 200 * time.Millisecond},
	}

	for i, td := range testData {
		if err := f.Parse(td.args); err != nil {
			t.Fatal(err)
		}
		if td.expected != serverCfg.RaftTickInterval {
			t.Errorf("%d. RaftTickInterval expected %d, but got %d", i, td.expected, serverCfg.RaftTickInterval)
		}
	}
}

func TestServerConnSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	resetGlobals := func() {
		// Ensure each test case starts with empty package-level variables and
		// that we don't leak non-default values of them from this test.
		// Resetting serverConnPort overwrites the default written by the
		// package-level init method, or else none of the cases above would have
		// an empty port.
		serverConnHost = ""
		serverAdvertiseHost = ""
		serverConnPort = ""
	}
	defer resetGlobals()

	f := startCmd.Flags()
	testData := []struct {
		args                  []string
		expectedAddr          string
		expectedAdvertiseAddr string
	}{
		{[]string{"start"}, ":", ":"},
		{[]string{"start", "--host", "127.0.0.1"}, "127.0.0.1:", "127.0.0.1:"},
		{[]string{"start", "--host", "192.168.0.111"}, "192.168.0.111:", "192.168.0.111:"},
		{[]string{"start", "--port", "12345"}, ":12345", ":12345"},
		{[]string{"start", "--host", "127.0.0.1", "--port", "12345"}, "127.0.0.1:12345", "127.0.0.1:12345"},
		{[]string{"start", "--advertise-host", "192.168.0.111"}, ":", "192.168.0.111:"},
		{[]string{"start", "--host", "127.0.0.1", "--advertise-host", "192.168.0.111"}, "127.0.0.1:", "192.168.0.111:"},
		{[]string{"start", "--host", "127.0.0.1", "--advertise-host", "192.168.0.111", "--port", "12345"}, "127.0.0.1:12345", "192.168.0.111:12345"},
		{[]string{"start", "--advertise-host", "192.168.0.111", "--port", "12345"}, ":12345", "192.168.0.111:12345"},
		// confirm hostnames will work
		{[]string{"start", "--host", "my.host.name"}, "my.host.name:", "my.host.name:"},
		{[]string{"start", "--host", "myhostname"}, "myhostname:", "myhostname:"},
		// confirm IPv6 works too
		{[]string{"start", "--host", "::1"}, "[::1]:", "[::1]:"},
		{[]string{"start", "--host", "2622:6221:e663:4922:fc2b:788b:fadd:7b48"}, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:", "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:"},
	}

	for i, td := range testData {
		resetGlobals()
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
	}
}

func TestClientConnSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// For some reason, when run under stress all these test cases fail due to the
	// `--host` flag being unknown to quitCmd. Just skip this under stress.
	if testutils.Stress() {
		t.Skip()
	}

	resetGlobals := func() {
		clientConnHost = ""
		clientConnPort = ""
	}
	defer resetGlobals()

	f := quitCmd.Flags()
	testData := []struct {
		args         []string
		expectedAddr string
	}{
		{[]string{"quit"}, ":"},
		{[]string{"quit", "--host", "127.0.0.1"}, "127.0.0.1:"},
		{[]string{"quit", "--host", "192.168.0.111"}, "192.168.0.111:"},
		{[]string{"quit", "--port", "12345"}, ":12345"},
		{[]string{"quit", "--host", "127.0.0.1", "--port", "12345"}, "127.0.0.1:12345"},
		// confirm hostnames will work
		{[]string{"quit", "--host", "my.host.name"}, "my.host.name:"},
		{[]string{"quit", "--host", "myhostname"}, "myhostname:"},
		// confirm IPv6 works too
		{[]string{"quit", "--host", "::1"}, "[::1]:"},
		{[]string{"quit", "--host", "2622:6221:e663:4922:fc2b:788b:fadd:7b48"}, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:"},
	}

	for i, td := range testData {
		resetGlobals()
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

	f := startCmd.Flags()
	testData := []struct {
		args     []string
		expected string
	}{
		{[]string{"start", "--" + cliflags.ServerHTTPHost.Name, "127.0.0.1"}, "127.0.0.1:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ServerHTTPHost.Name, "192.168.0.111"}, "192.168.0.111:" + base.DefaultHTTPPort},
		// confirm hostnames will work
		{[]string{"start", "--" + cliflags.ServerHTTPHost.Name, "my.host.name"}, "my.host.name:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ServerHTTPHost.Name, "myhostname"}, "myhostname:" + base.DefaultHTTPPort},
		// confirm IPv6 works too
		{[]string{"start", "--" + cliflags.ServerHTTPHost.Name, "::1"}, "[::1]:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.ServerHTTPHost.Name, "2622:6221:e663:4922:fc2b:788b:fadd:7b48"}, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultHTTPPort},
	}

	for i, td := range testData {
		// Ensure each test case starts with an empty package-level variable.
		serverHTTPHost = ""

		if err := f.Parse(td.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", td.args, err)
		}

		extraServerFlagInit()
		if td.expected != serverCfg.HTTPAddr {
			t.Errorf("%d. serverCfg.HTTPAddr expected '%s', but got '%s'. td.args was '%#v'.", i, td.expected, serverCfg.HTTPAddr, td.args)
		}
	}
}
