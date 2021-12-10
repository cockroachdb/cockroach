// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import "github.com/cockroachdb/cockroach/pkg/security"

// SecurityConfig is the type implemented by RPC configurations.
type SecurityConfig interface {
	Common() CommonConfig
}

// CommonConfig holds the parameters common to all RPC configurations.
type CommonConfig struct {
	// Insecure tells the RPC code to bypass authentication and TLS.
	Insecure bool
	// CertsDir indicates where to find TLS certificates.
	CertsDir string
}

// serverSecurityConfig is suitable for use when creating a RPC
// context for a server.
type serverSecurityConfig struct {
	CommonConfig

	// serverParameters holds the configuration for server processes.
	serverParameters
}

// Common implements the SecurityConfig interface.
func (sctx serverSecurityConfig) Common() CommonConfig { return sctx.CommonConfig }

// ClientSecurityConfig is a configuration suitable for RPC clients
// other than CockroachDB nodes.
type ClientSecurityConfig struct {
	CommonConfig

	// User is the SQL username to use to connect to a remote RPC context.
	User security.SQLUsername
}

// Common implements the SecurityConfig interface.
func (c ClientSecurityConfig) Common() CommonConfig { return c.CommonConfig }
