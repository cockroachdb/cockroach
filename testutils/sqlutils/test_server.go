// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlutils

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server/testingshim"
)

// SetupServerWithParams creates a test server with the given params and sets up
// a gosql DB connection.
func SetupServerWithParams(t *testing.T, params testingshim.TestServerParams) (
	server testingshim.TestServerInterface, goDB *gosql.DB, kvClient *client.DB, cleanupFn func(),
) {
	server = testingshim.NewTestServer(params)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}

	kvClient = server.KVClient().(*client.DB)
	url, cleanupGoDB := PGUrl(t, server.ServingAddr(), security.RootUser, "SetupServer")
	goDB, err := gosql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}

	cleanupFn = func() {
		_ = goDB.Close()
		cleanupGoDB()
		server.Stop()
	}
	return server, goDB, kvClient, cleanupFn
}

// SetupServer creates a test server with default parameters and sets up a gosql
// DB connection.
func SetupServer(t *testing.T) (
	server testingshim.TestServerInterface, goDB *gosql.DB, kvClient *client.DB, cleanupFn func(),
) {
	return SetupServerWithParams(t, testingshim.TestServerParams{})
}
