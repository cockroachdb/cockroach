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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package testutils

import (
	"net/http"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
)

// NewTestBaseContext creates a base context for testing.
// The certs file loader is overriden in individual main_test files.
func NewTestBaseContext() *base.Context {
	return &base.Context{
		Certs: security.EmbeddedCertsDir,
	}
}

// NewTestHTTPClient creates a HTTP client on the fly using a test context.
// Useful when contexts don't need to be reused.
func NewTestHTTPClient() (*http.Client, error) {
	return NewTestBaseContext().GetHTTPClient()
}
