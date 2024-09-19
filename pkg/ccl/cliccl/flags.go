// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/fipsccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type requireFipsFlag bool

// Type implements the pflag.Value interface.
func (f *requireFipsFlag) Type() string {
	return "bool"
}

// String implements the pflag.Value interface.
func (f *requireFipsFlag) String() string {
	return strconv.FormatBool(bool(*f))
}

// Set implements the pflag.Value interface.
func (f *requireFipsFlag) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	// We implement the logic of this check in the flag setter itself because it
	// applies to all commands and we do not have another good way to inject
	// this behavior globally (PersistentPreRun functions don't help because
	// they are inherited across different levels of the command hierarchy only
	// if that level does not have its own hook).
	if v && !fipsccl.IsFIPSReady() {
		err := errors.WithHint(errors.New("FIPS readiness checks failed"), "Run `cockroach debug enterprise-check-fips` for details")
		clierror.OutputError(os.Stderr, err, true, false)
		exit.WithCode(exit.UnspecifiedError())
	}
	*f = requireFipsFlag(v)
	return nil
}

var _ pflag.Value = (*requireFipsFlag)(nil)

// IsBoolFlag implements a non-public pflag interface to indicate that this
// flag is used without an explicit value.
func (*requireFipsFlag) IsBoolFlag() bool {
	return true
}

func init() {
	// Multi-tenancy proxy command flags.
	{
		f := mtStartSQLProxyCmd.Flags()
		cliflagcfg.StringFlag(f, &proxyContext.Denylist, cliflags.DenyList)
		cliflagcfg.StringFlag(f, &proxyContext.Allowlist, cliflags.AllowList)
		cliflagcfg.StringFlag(f, &proxyContext.ListenAddr, cliflags.ProxyListenAddr)
		cliflagcfg.StringFlag(f, &proxyContext.ProxyProtocolListenAddr, cliflags.ProxyProtocolListenAddr)
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

	// FIPS verification flags.
	cli.RegisterFlags(func() {
		cmd := cli.CockroachCmd()
		var requireFips = requireFipsFlag(false)
		flag := cmd.PersistentFlags().VarPF(&requireFips, "enterprise-require-fips-ready", "", "abort if FIPS readiness checks fail")
		flag.NoOptDefVal = "true"
	})
}
