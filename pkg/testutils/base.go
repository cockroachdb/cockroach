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

package testutils

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
)

// NewNodeTestBaseConfig creates a base config for testing. This uses embedded
// certs and the default node user. The default node user has both server and
// client certificates.
func NewNodeTestBaseConfig() *base.Config {
	return NewTestBaseConfig(security.NodeUser)
}

// NewTestBaseConfig creates a secure base config for user.
func NewTestBaseConfig(user string) *base.Config {
	cfg := &base.Config{
		Insecure: false,
		User:     user,
	}
	FillCerts(cfg)
	return cfg
}

// FillCerts sets the certs on a base.Config.
func FillCerts(cfg *base.Config) {
	cfg.SSLCA = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)

	// The NodeUser has a combined server/client certificate/key pair, all other users
	// need client certs.
	if cfg.User == security.NodeUser {
		cfg.SSLCert = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeCert)
		cfg.SSLCertKey = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedNodeKey)
	} else {
		cfg.SSLCert = filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.crt", cfg.User))
		cfg.SSLCertKey = filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.key", cfg.User))
	}
}
