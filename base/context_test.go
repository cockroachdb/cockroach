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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package base_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
)

func TestClientSSLSettings(t *testing.T) {
	certsDir := security.EmbeddedCertsDir

	testCases := []struct {
		// args
		insecure bool
		certs    string
		// output
		requestScheme string
		configSuccess bool
		nilConfig     bool
		noCAs         bool
	}{
		{true, "foobar", "http", true, true, false},
		{false, "", "https", true, false, true},
		{false, certsDir, "https", true, false, false},
		{false, "/dev/null", "https", false, false, false},
	}

	for tcNum, tc := range testCases {
		ctx := &base.Context{Insecure: tc.insecure, Certs: tc.certs}
		if ctx.RequestScheme() != tc.requestScheme {
			t.Fatalf("#%d: expected RequestScheme=%s, got: %s", tcNum, tc.requestScheme, ctx.RequestScheme())
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
		ctx := &base.Context{Insecure: tc.insecure, Certs: tc.certs}
		if ctx.RequestScheme() != tc.requestScheme {
			t.Fatalf("#%d: expected RequestScheme=%s, got: %s", tcNum, tc.requestScheme, ctx.RequestScheme())
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
