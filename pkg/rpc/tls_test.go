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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestClientSSLSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const clientCertNotFound = "problem with client cert for user .*: not found"
	const certDirNotFound = "no certificates found"
	invalidUser := security.MakeSQLUsernameFromPreNormalizedString("not-a-user")
	badUser := security.MakeSQLUsernameFromPreNormalizedString("bad-user")

	testCases := []struct {
		// args
		insecure bool
		hasCerts bool
		user     security.SQLUsername
		// output
		requestScheme string
		configErr     string
		nilConfig     bool
		noCAs         bool
	}{
		{true, false, security.NodeUserName(), "http", "", true, false},
		{true, true, invalidUser, "http", "", true, false},
		{false, true, invalidUser, "https", clientCertNotFound, true, false},
		{false, false, security.NodeUserName(), "https", certDirNotFound, false, true},
		{false, true, security.NodeUserName(), "https", "", false, false},
		{false, true, badUser, "https", clientCertNotFound, false, false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			cfg := &base.Config{Insecure: tc.insecure, User: tc.user}
			if tc.hasCerts {
				testutils.FillCerts(cfg)
			} else {
				// We can't leave this empty because otherwise it refers to the cwd which
				// always exists.
				cfg.SSLCertsDir = "i-do-not-exist"
			}
			stopper := stop.NewStopper()
			defer stopper.Stop(context.Background())
			rpcContext := NewContext(ContextOptions{
				TenantID: roachpb.SystemTenantID,
				Clock:    hlc.NewClock(hlc.UnixNano, 1),
				Stopper:  stopper,
				Settings: cluster.MakeTestingClusterSettings(),
				Config:   cfg,
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
			cfg := &base.Config{Insecure: tc.insecure, User: security.NodeUserName()}
			if tc.hasCerts {
				testutils.FillCerts(cfg)
			}
			stopper := stop.NewStopper()
			defer stopper.Stop(context.Background())
			rpcContext := NewContext(ContextOptions{
				TenantID: roachpb.SystemTenantID,
				Clock:    hlc.NewClock(hlc.UnixNano, 1),
				Stopper:  stopper,
				Settings: cluster.MakeTestingClusterSettings(),
				Config:   cfg,
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
