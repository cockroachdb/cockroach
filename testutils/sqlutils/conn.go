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
// Author: Matt Tracy (matt@cockroachlabs.com)

package sqlutils

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
)

// PGUrl returns a postgres connection url which connects to this server with
// the given user. Returns the url and a cleanup function which must be called
// after the connection string is no longer needed.
//
// In order to connect securely using postgres, this method will create
// temporary on-disk copies of certain embedded security certificates. The
// certificates will be created as temporary files in the provided directory,
// and their filenames will have the provided prefix. The returned cleanup
// function will delete these temporary files.
func PGUrl(ts *server.TestServer, t util.Tester, user, tempDir, prefix string) (url.URL, func()) {
	host, port, err := net.SplitHostPort(ts.PGAddr())
	if err != nil {
		t.Fatal(err)
	}

	caPath := filepath.Join(security.EmbeddedCertsDir, "ca.crt")
	certPath := security.ClientCertPath(security.EmbeddedCertsDir, user)
	keyPath := security.ClientKeyPath(security.EmbeddedCertsDir, user)

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCAPath, tempCACleanup := securitytest.TempRestrictedCopy(t, caPath, tempDir, "TestLogic_ca")
	tempCertPath, tempCertCleanup := securitytest.TempRestrictedCopy(t, certPath, tempDir, "TestLogic_cert")
	tempKeyPath, tempKeyCleanup := securitytest.TempRestrictedCopy(t, keyPath, tempDir, "TestLogic_key")

	return url.URL{
			Scheme: "postgres",
			User:   url.User(user),
			Host:   net.JoinHostPort(host, port),
			RawQuery: fmt.Sprintf("sslmode=verify-full&sslrootcert=%s&sslcert=%s&sslkey=%s",
				url.QueryEscape(tempCAPath),
				url.QueryEscape(tempCertPath),
				url.QueryEscape(tempKeyPath),
			),
		}, func() {
			tempCACleanup()
			tempCertCleanup()
			tempKeyCleanup()
		}
}
