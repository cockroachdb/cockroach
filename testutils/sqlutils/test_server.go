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

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server/testingshim"
	"github.com/cockroachdb/cockroach/util/stop"
)

// SetupServer creates a test server and sets up a gosql DB connection.
// The server should be stopped by calling server.Stopper().Stop().
func SetupServer(t testing.TB, params testingshim.TestServerParams) (
	testingshim.TestServerInterface, *gosql.DB, *client.DB,
) {
	server, err := testingshim.StartServerRaw(params)
	if err != nil {
		t.Fatal(err)
	}

	kvClient := server.KVClient().(*client.DB)
	pgURL, cleanupGoDB := PGUrl(t, server.ServingAddr(), security.RootUser, "SetupServer")
	pgURL.Path = params.UseDatabase
	goDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	server.Stopper().AddCloser(
		stop.CloserFn(func() {
			_ = goDB.Close()
			cleanupGoDB()
		}))
	return server, goDB, kvClient
}
