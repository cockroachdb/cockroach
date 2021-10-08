// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
)

// NewNodeTestBaseContext creates a base context for testing. This uses
// embedded certs and the default node user. The default node user has both
// server and client certificates.
func NewNodeTestBaseContext() *base.Config {
	return NewTestBaseContext(security.NodeUserName())
}

// NewTestBaseContext creates a secure base context for user.
func NewTestBaseContext(user security.SQLUsername) *base.Config {
	cfg := &base.Config{
		Insecure: false,
		User:     user,
	}
	FillCerts(cfg)
	return cfg
}

// FillCerts sets the certs on a base.Config.
func FillCerts(cfg *base.Config) {
	cfg.SSLCertsDir = security.EmbeddedCertsDir
}
