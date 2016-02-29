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
// Author: Marc Berhault (marc@cockroachlabs.com)

package base_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestClientSSLSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	certsDir := security.EmbeddedCertsDir

	testCases := []struct {
		// args
		insecure bool
		certs    string
		user     string
		// output
		requestScheme string
		configSuccess bool
		nilConfig     bool
		noCAs         bool
	}{
		{true, "foobar", "node", "http", true, true, false},
		{true, certsDir, "not-a-user", "http", true, true, false},
		{false, certsDir, "not-a-user", "https", false, true, false},
		{false, "", "node", "https", true, false, true},
		{false, certsDir, "node", "https", true, false, false},
		{false, "/dev/null", "node", "https", false, false, false},
	}

	for tcNum, tc := range testCases {
		ctx := &base.Context{Insecure: tc.insecure, Certs: tc.certs, User: tc.user}
		if ctx.HTTPRequestScheme() != tc.requestScheme {
			t.Fatalf("#%d: expected HTTPRequestScheme=%s, got: %s", tcNum, tc.requestScheme, ctx.HTTPRequestScheme())
		}
		tlsConfig, err := ctx.GetClientTLSConfig()
		if (err == nil) != tc.configSuccess {
			t.Fatalf("#%d: expected GetClientTLSConfig success=%t, got err=%v", tcNum, tc.configSuccess, err)
		}
		if err != nil {
			continue
		}
		if (tlsConfig == nil) != tc.nilConfig {
			t.Fatalf("#%d: expected nil config=%t, got: %+v", tcNum, tc.nilConfig, tlsConfig)
		}
		if tlsConfig == nil {
			continue
		}
		if (tlsConfig.RootCAs == nil) != tc.noCAs {
			t.Fatalf("#%d: expected nil RootCAs: %t, got: %+v", tcNum, tc.noCAs, tlsConfig.RootCAs)
		}
	}
}

func TestServerSSLSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	certsDir := security.EmbeddedCertsDir

	testCases := []struct {
		// args
		insecure bool
		certs    string
		// output
		requestScheme string
		configSuccess bool
		nilConfig     bool
	}{
		{true, "foobar", "http", true, true},
		{false, "", "https", false, false},
		{false, certsDir, "https", true, false},
		{false, "/dev/null", "https", false, false},
	}

	for tcNum, tc := range testCases {
		ctx := &base.Context{Insecure: tc.insecure, Certs: tc.certs, User: security.NodeUser}
		if ctx.HTTPRequestScheme() != tc.requestScheme {
			t.Fatalf("#%d: expected HTTPRequestScheme=%s, got: %s", tcNum, tc.requestScheme, ctx.HTTPRequestScheme())
		}
		tlsConfig, err := ctx.GetServerTLSConfig()
		if (err == nil) != tc.configSuccess {
			t.Fatalf("#%d: expected GetServerTLSConfig success=%t, got err=%v", tcNum, tc.configSuccess, err)
		}
		if err != nil {
			continue
		}
		if (tlsConfig == nil) != tc.nilConfig {
			t.Fatalf("#%d: expected nil config=%t, got: %+v", tcNum, tc.nilConfig, tlsConfig)
		}
	}
}
