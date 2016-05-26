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

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/cli/cliflags"
	"github.com/cockroachdb/cockroach/testutils/buildutil"
	"github.com/cockroachdb/cockroach/util/leaktest"
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
			"github.com/cockroachdb/cockroach/security/securitytest", // contains certificates
		},
		[]string{
			"github.com/cockroachdb/cockroach/testutils", // meant for testing code only
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
	if expectedCacheSize != serverCtx.CacheSize {
		t.Errorf("expected %d, but got %d", expectedCacheSize, serverCtx.CacheSize)
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
		if td.expected != serverCtx.RaftTickInterval {
			t.Errorf("%d. RaftTickInterval expected %d, but got %d", i, td.expected, serverCtx.RaftTickInterval)
		}
	}
}

func TestHttpAddrFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()
	testData := []struct {
		args     []string
		expected string
	}{
		{[]string{"start", "--" + cliflags.HTTPAddrName, "127.0.0.1"}, "127.0.0.1:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.HTTPAddrName, "192.168.0.111"}, "192.168.0.111:" + base.DefaultHTTPPort},
		// confirm hostnames will work
		{[]string{"start", "--" + cliflags.HTTPAddrName, "my.host.name"}, "my.host.name:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.HTTPAddrName, "myhostname"}, "myhostname:" + base.DefaultHTTPPort},
		// confirm IPv6 works too
		{[]string{"start", "--" + cliflags.HTTPAddrName, "::1"}, "[::1]:" + base.DefaultHTTPPort},
		{[]string{"start", "--" + cliflags.HTTPAddrName, "2622:6221:e663:4922:fc2b:788b:fadd:7b48"}, "[2622:6221:e663:4922:fc2b:788b:fadd:7b48]:" + base.DefaultHTTPPort},
	}

	for i, td := range testData {
		if err := f.Parse(td.args); err != nil {
			t.Fatal(err)
		}

		extraFlagInit()
		if td.expected != serverCtx.HTTPAddr {
			t.Errorf("%d. serverCtx.HTTPAddr expected '%s', but got '%s'. td.args was '%#v'.", i, td.expected, serverCtx.HTTPAddr, td.args)
		}
	}
}
