// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestClientSSLSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const clientCertNotFound = "problem with client cert for user .*: not found"
	const certDirNotFound = "no certificates found"
	invalidUser := username.MakeSQLUsernameFromPreNormalizedString("not-a-user")
	badUser := username.MakeSQLUsernameFromPreNormalizedString("bad-user")

	testCases := []struct {
		// args
		insecure bool
		hasCerts bool
		user     username.SQLUsername
		// output
		requestScheme string
		configErr     string
		nilConfig     bool
		noCAs         bool
	}{
		{true, false, username.NodeUserName(), "http", "", true, false},
		{true, true, invalidUser, "http", "", true, false},
		{false, true, invalidUser, "https", clientCertNotFound, true, false},
		{false, false, username.NodeUserName(), "https", certDirNotFound, false, true},
		{false, true, username.NodeUserName(), "https", "", false, false},
		{false, true, badUser, "https", clientCertNotFound, false, false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			opts := DefaultContextOptions()
			opts.Insecure = tc.insecure
			opts.User = tc.user
			if tc.hasCerts {
				opts.SSLCertsDir = certnames.EmbeddedCertsDir
			} else {
				// We can't leave this empty because otherwise it refers to the cwd which
				// always exists.
				opts.SSLCertsDir = "i-do-not-exist"
			}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			opts.ClientOnly = true
			opts.Stopper = stopper
			opts.Settings = cluster.MakeTestingClusterSettings()
			rpcContext := NewContext(ctx, opts)

			if expected, actual := tc.requestScheme, rpcContext.SecurityContext.HTTPRequestScheme(); expected != actual {
				t.Fatalf("expected HTTPRequestScheme=%s, got: %s", expected, actual)
			}
			tlsConfig, err := rpcContext.GetClientTLSConfig()
			if !testutils.IsError(err, tc.configErr) {
				t.Fatalf("expected err=%s, got err=%v", tc.configErr, err)
			}
			if err != nil {
				return
			}
			if (tlsConfig == nil) != tc.nilConfig {
				t.Fatalf("expected nil config=%t, got: %+v", tc.nilConfig, tlsConfig)
			}
			if tlsConfig == nil {
				return
			}
			if (tlsConfig.RootCAs == nil) != tc.noCAs {
				t.Fatalf("expected nil RootCAs: %t, got: %+v", tc.noCAs, tlsConfig.RootCAs)
			}
		})
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
		t.Run("", func(t *testing.T) {
			opts := DefaultContextOptions()
			opts.Insecure = tc.insecure
			if !tc.hasCerts {
				opts.SSLCertsDir = "i-do-not-exist"
			}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			opts.Stopper = stopper
			opts.Settings = cluster.MakeTestingClusterSettings()
			rpcContext := NewContext(ctx, opts)
			if actual, expected := rpcContext.HTTPRequestScheme(), tc.requestScheme; actual != expected {
				t.Fatalf("#%d: expected HTTPRequestScheme=%s, got: %s", tcNum, expected, actual)
			}
			tlsConfig, err := rpcContext.GetServerTLSConfig()
			if (err == nil) != tc.configSuccess {
				t.Fatalf("#%d: expected GetServerTLSConfig success=%t, got err=%v", tcNum, tc.configSuccess, err)
			}
			if err != nil {
				return
			}
			if (tlsConfig == nil) != tc.nilConfig {
				t.Fatalf("#%d: expected nil config=%t, got: %+v", tcNum, tc.nilConfig, tlsConfig)
			}
		})
	}
}
