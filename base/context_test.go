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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func fillCertPaths(context *base.Context, user string) {
	context.SSLCA = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	context.SSLCAKey = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCAKey)
	context.SSLCert = filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.crt", user))
	context.SSLCertKey = filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.key", user))
}

func TestClientSSLSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		// args
		insecure bool
		hasCerts bool
		user     string
		// output
		requestScheme string
		configSuccess bool
		nilConfig     bool
		noCAs         bool
	}{
		{true, false, security.NodeUser, "http", true, true, false},
		{true, true, "not-a-user", "http", true, true, false},
		{false, true, "not-a-user", "https", false, true, false},
		{false, false, security.NodeUser, "https", true, false, true},
		{false, true, security.NodeUser, "https", true, false, false},
		{false, true, "bad-user", "https", false, false, false},
	}

	for tcNum, tc := range testCases {
		ctx := &base.Context{Insecure: tc.insecure, User: tc.user}
		if tc.hasCerts {
			fillCertPaths(ctx, tc.user)
		}
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

	testCases := []struct {
		// args
		insecure bool
		hasCerts bool
		// output
		requestScheme string
		configSuccess bool
		nilConfig     bool
	}{
		{true, false, "http", true, true},
		{false, false, "https", false, false},
		{false, true, "https", true, false},
		{false, false, "https", false, false},
	}

	for tcNum, tc := range testCases {
		ctx := &base.Context{Insecure: tc.insecure, User: security.NodeUser}
		if tc.hasCerts {
			fillCertPaths(ctx, security.NodeUser)
		}
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
