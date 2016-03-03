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
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
)

// PGUrl returns a postgres connection url which connects to this server with the given user, and a
// cleanup function which must be called after all connections created using the connection url have
// been closed.
//
// In order to connect securely using postgres, this method will create temporary on-disk copies of
// certain embedded security certificates. The certificates will be created in a new temporary
// directory. The returned cleanup function will delete this temporary directory.
// Note that two calls to this function for the same `user` will generate different
// copies of the certificates, so the cleanup function must always be called.
//
// Args:
//  prefix: A prefix to be prepended to the temp file names generated, for debugging.
func PGUrl(t testing.TB, ts *server.TestServer, user, prefix string) (url.URL, func()) {
	host, port, err := net.SplitHostPort(ts.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}

	tempDir, err := ioutil.TempDir("", prefix)
	if err != nil {
		t.Fatal(err)
	}

	caPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	certPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.crt", user))
	keyPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.key", user))

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCAPath := securitytest.RestrictedCopy(t, caPath, tempDir, "ca")
	tempCertPath := securitytest.RestrictedCopy(t, certPath, tempDir, "cert")
	tempKeyPath := securitytest.RestrictedCopy(t, keyPath, tempDir, "key")
	options := url.Values{}
	options.Add("sslmode", "verify-full")
	options.Add("sslrootcert", tempCAPath)
	options.Add("sslcert", tempCertPath)
	options.Add("sslkey", tempKeyPath)

	return url.URL{
			Scheme:   "postgres",
			User:     url.User(user),
			Host:     net.JoinHostPort(host, port),
			RawQuery: options.Encode(),
		}, func() {
			if err := os.RemoveAll(tempDir); err != nil {
				// Not Fatal() because we might already be panicking.
				t.Error(err)
			}
		}
}
