// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl"
)

func init() {
	setProxyContextDefaults()
	setTestDirectorySvrContextDefaults()
}

// proxyContext captures the command-line parameters of the `mt start-proxy` command.
var proxyContext sqlproxyccl.ProxyOptions

func setProxyContextDefaults() {
	proxyContext.Denylist = ""
	proxyContext.ListenAddr = "127.0.0.1:46257"
	proxyContext.ListenCert = ""
	proxyContext.ListenKey = ""
	proxyContext.MetricsAddress = "0.0.0.0:8080"
	proxyContext.RoutingRule = ""
	proxyContext.DirectoryAddr = ""
	proxyContext.SkipVerify = false
	proxyContext.Insecure = false
	proxyContext.RatelimitBaseDelay = 50 * time.Millisecond
	proxyContext.ValidateAccessInterval = 30 * time.Second
	proxyContext.PollConfigInterval = 30 * time.Second
	proxyContext.ThrottleBaseDelay = time.Second
	proxyContext.DisableConnectionRebalancing = false
	proxyContext.RequireProxyProtocol = false
}

var testDirectorySvrContext struct {
	port          int
	certsDir      string
	kvAddrs       string
	tenantBaseDir string
}

func setTestDirectorySvrContextDefaults() {
	testDirectorySvrContext.port = 36257
}
