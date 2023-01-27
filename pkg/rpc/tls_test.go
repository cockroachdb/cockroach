// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
			cfg := &base.Config{Insecure: tc.insecure, User: tc.user}
			if tc.hasCerts {
				cfg.SSLCertsDir = certnames.EmbeddedCertsDir
			} else {
				// We can't leave this empty because otherwise it refers to the cwd which
				// always exists.
				cfg.SSLCertsDir = "i-do-not-exist"
			}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			rpcContext := NewContext(ctx, ContextOptions{
				TenantID:        roachpb.SystemTenantID,
				ClientOnly:      true,
				Clock:           &timeutil.DefaultTimeSource{},
				ToleratedOffset: time.Nanosecond,
				Stopper:         stopper,
				Settings:        cluster.MakeTestingClusterSettings(),
				Config:          cfg,
			})

			if cfg.HTTPRequestScheme() != tc.requestScheme {
				t.Fatalf("expected HTTPRequestScheme=%s, got: %s", tc.requestScheme, cfg.HTTPRequestScheme())
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
			cfg := &base.Config{Insecure: tc.insecure, User: username.NodeUserName()}
			if tc.hasCerts {
				cfg.SSLCertsDir = certnames.EmbeddedCertsDir
			}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			rpcContext := NewContext(ctx, ContextOptions{
				TenantID:        roachpb.SystemTenantID,
				Clock:           &timeutil.DefaultTimeSource{},
				ToleratedOffset: time.Nanosecond,
				Stopper:         stopper,
				Settings:        cluster.MakeTestingClusterSettings(),
				Config:          cfg,
			})
			if cfg.HTTPRequestScheme() != tc.requestScheme {
				t.Fatalf("#%d: expected HTTPRequestScheme=%s, got: %s", tcNum, tc.requestScheme, cfg.HTTPRequestScheme())
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
