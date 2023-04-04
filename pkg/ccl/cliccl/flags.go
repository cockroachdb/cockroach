// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
)

func init() {
	// Multi-tenancy proxy command flags.
	{
		f := mtStartSQLProxyCmd.Flags()
		cliflagcfg.StringFlag(f, &proxyContext.Denylist, cliflags.DenyList)
		cliflagcfg.StringFlag(f, &proxyContext.Allowlist, cliflags.AllowList)
		cliflagcfg.StringFlag(f, &proxyContext.ListenAddr, cliflags.ProxyListenAddr)
		cliflagcfg.StringFlag(f, &proxyContext.ListenCert, cliflags.ListenCert)
		cliflagcfg.StringFlag(f, &proxyContext.ListenKey, cliflags.ListenKey)
		cliflagcfg.StringFlag(f, &proxyContext.MetricsAddress, cliflags.ListenMetrics)
		cliflagcfg.StringFlag(f, &proxyContext.RoutingRule, cliflags.RoutingRule)
		cliflagcfg.StringFlag(f, &proxyContext.DirectoryAddr, cliflags.DirectoryAddr)
		cliflagcfg.BoolFlag(f, &proxyContext.SkipVerify, cliflags.SkipVerify)
		cliflagcfg.BoolFlag(f, &proxyContext.Insecure, cliflags.InsecureBackend)
		cliflagcfg.DurationFlag(f, &proxyContext.ValidateAccessInterval, cliflags.ValidateAccessInterval)
		cliflagcfg.DurationFlag(f, &proxyContext.PollConfigInterval, cliflags.PollConfigInterval)
		cliflagcfg.DurationFlag(f, &proxyContext.ThrottleBaseDelay, cliflags.ThrottleBaseDelay)
		cliflagcfg.BoolFlag(f, &proxyContext.DisableConnectionRebalancing, cliflags.DisableConnectionRebalancing)
		cliflagcfg.BoolFlag(f, &proxyContext.RequireProxyProtocol, cliflags.RequireProxyProtocol)
	}

	// Multi-tenancy test directory command flags.
	cli.RegisterFlags(func() {
		f := mtTestDirectorySvr.Flags()
		cliflagcfg.IntFlag(f, &testDirectorySvrContext.port, cliflags.TestDirectoryListenPort)
		cliflagcfg.StringFlag(f, &testDirectorySvrContext.certsDir, cliflags.TestDirectoryTenantCertsDir)
		cliflagcfg.StringFlag(f, &testDirectorySvrContext.tenantBaseDir, cliflags.TestDirectoryTenantBaseDir)
		// Use StringFlagDepth to avoid conflicting with the already registered KVAddrs env var.
		cliflagcfg.StringFlagDepth(1, f, &testDirectorySvrContext.kvAddrs, cliflags.KVAddrs)
	})
}
