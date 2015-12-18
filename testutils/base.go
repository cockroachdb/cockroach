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
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
)

// NewNodeTestBaseContext creates a base context for testing.
// This uses embedded certs and the "node" user (default node user).
// The "node" user has both server and client certificates.
func NewNodeTestBaseContext() *base.Context {
	return NewTestBaseContext(security.NodeUser)
}

// NewTestBaseContext creates a secure base context for 'user'.
func NewTestBaseContext(user string) *base.Context {
	return &base.Context{
		Certs: security.EmbeddedCertsDir,
		User:  user,
	}
}
