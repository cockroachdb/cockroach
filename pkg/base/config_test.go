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

package base_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestClientSSLSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const clientCertNotFound = "problem with client cert for user .*: not found"
	const certDirNotFound = "problem loading certs directory"

	testCases := []struct {
		// args
		insecure bool
		hasCerts bool
		user     string
		// output
		requestScheme string
		configErr     string
		nilConfig     bool
		noCAs         bool
	}{
		{true, false, security.NodeUser, "http", "", true, false},
		{true, true, "not-a-user", "http", "", true, false},
		{false, true, "not-a-user", "https", clientCertNotFound, true, false},
		{false, false, security.NodeUser, "https", certDirNotFound, false, true},
		{false, true, security.NodeUser, "https", "", false, false},
		{false, true, "bad-user", "https", clientCertNotFound, false, false},
	}

	for tcNum, tc := range testCases {
		cfg := &base.Config{Insecure: tc.insecure, User: tc.user}
		if tc.hasCerts {
			testutils.FillCerts(cfg)
		}
		if cfg.HTTPRequestScheme() != tc.requestScheme {
			t.Fatalf("#%d: expected HTTPRequestScheme=%s, got: %s", tcNum, tc.requestScheme, cfg.HTTPRequestScheme())
		}
		tlsConfig, err := cfg.GetClientTLSConfig()
		if !testutils.IsError(err, tc.configErr) {
			t.Fatalf("#%d: expected err=%s, got err=%v", tcNum, tc.configErr, err)
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
		cfg := &base.Config{Insecure: tc.insecure, User: security.NodeUser}
		if tc.hasCerts {
			testutils.FillCerts(cfg)
		}
		if cfg.HTTPRequestScheme() != tc.requestScheme {
			t.Fatalf("#%d: expected HTTPRequestScheme=%s, got: %s", tcNum, tc.requestScheme, cfg.HTTPRequestScheme())
		}
		tlsConfig, err := cfg.GetServerTLSConfig()
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
