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
	return &base.Config{
		Insecure:   false,
		SSLCA:      filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert),
		SSLCert:    filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.crt", user)),
		SSLCertKey: filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.key", user)),
		User:       user,
	}
}
