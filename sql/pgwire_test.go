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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/pgwire"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func trivialQuery(datasource string) error {
	db, err := sql.Open("postgres", datasource)
	if err != nil {
		return err
	}
	defer db.Close()
	{
		_, err := db.Exec("SELECT 1")
		return err
	}
}

func TestPGWire(t *testing.T) {
	defer leaktest.AfterTest(t)

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	certDir := filepath.Join(filepath.Dir(dir), "resource", security.EmbeddedCertsDir)

	certUser := server.TestUser
	certPath := security.ClientCertPath(certDir, certUser)
	keyPath := security.ClientKeyPath(certDir, certUser)

	// `github.com/lib/pq` requires that private key file permissions are
	// "u=rw (0600) or less".
	tempDir, err := ioutil.TempDir(os.TempDir(), "TestPGWire")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			// Not Fatal() because we might already be panicking.
			t.Error(err)
		}
	}()
	tmpKeyPath := tempRestrictedCopy(t, keyPath, tempDir)

	for _, insecure := range [...]bool{true, false} {
		ctx := server.NewTestContext()
		ctx.Insecure = insecure
		s := setupTestServerWithContext(t, ctx)

		host, port, err := net.SplitHostPort(s.PGAddr())
		if err != nil {
			t.Fatal(err)
		}

		basePgUrl := url.URL{
			Scheme: "postgres",
			Host:   net.JoinHostPort(host, port),
		}
		if err := trivialQuery(basePgUrl.String()); err != nil {
			if insecure {
				if err != pq.ErrSSLNotSupported {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, "no client certificates in request") {
					t.Fatal(err)
				}
			}
		}

		{
			disablePgUrl := basePgUrl
			disablePgUrl.RawQuery = "sslmode=disable"
			err := trivialQuery(disablePgUrl.String())
			if insecure {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, pgwire.ErrSSLRequired) {
					t.Fatal(err)
				}
			}
		}

		{
			requirePgUrlNoCert := basePgUrl
			requirePgUrlNoCert.RawQuery = "sslmode=require"
			err := trivialQuery(requirePgUrlNoCert.String())
			if insecure {
				if err != pq.ErrSSLNotSupported {
					t.Fatal(err)
				}
			} else {
				if !testutils.IsError(err, "no client certificates in request") {
					t.Fatal(err)
				}
			}
		}

		{
			for _, optUser := range []string{certUser, security.RootUser} {
				requirePgUrlWithCert := basePgUrl
				requirePgUrlWithCert.User = url.User(optUser)
				requirePgUrlWithCert.RawQuery = fmt.Sprintf("sslmode=require&sslcert=%s&sslkey=%s",
					url.QueryEscape(certPath),
					url.QueryEscape(tmpKeyPath),
				)
				err := trivialQuery(requirePgUrlWithCert.String())
				if insecure {
					if err != pq.ErrSSLNotSupported {
						t.Fatal(err)
					}
				} else {
					if optUser == certUser {
						if err != nil {
							t.Fatal(err)
						}
					} else {
						if !testutils.IsError(err, `requested user is \w+, but certificate is for \w+`) {
							t.Fatal(err)
						}
					}
				}
			}
		}

		cleanupTestServer(s)
	}
}
