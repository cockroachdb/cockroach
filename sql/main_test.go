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

package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func init() {
	security.SetReadFileFn(securitytest.Asset)
}

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	leaktest.TestMainWithLeakCheck(m)
}

// checkEndTransactionTrigger verifies that an EndTransactionRequest
// that includes intents for the SystemDB keys sets the proper trigger.
func checkEndTransactionTrigger(_ roachpb.StoreID, req roachpb.Request, _ roachpb.Header) error {
	args, ok := req.(*roachpb.EndTransactionRequest)
	if !ok {
		return nil
	}

	if !args.Commit {
		// This is a rollback: skip trigger verification.
		return nil
	}

	modifiedSpanTrigger := args.InternalCommitTrigger.GetModifiedSpanTrigger()
	modifiedSystemSpan := modifiedSpanTrigger != nil && modifiedSpanTrigger.SystemDBSpan

	var hasSystemKey bool
	for _, span := range args.IntentSpans {
		addr := keys.Addr(span.Key)
		if bytes.Compare(addr, keys.SystemDBSpan.Key) >= 0 && bytes.Compare(addr, keys.SystemDBSpan.EndKey) < 0 {
			hasSystemKey = true
			break
		}
	}
	if hasSystemKey != modifiedSystemSpan {
		return util.Errorf("EndTransaction hasSystemKey=%t, but hasSystemDBTrigger=%t",
			hasSystemKey, modifiedSystemSpan)
	}

	return nil
}

func setupTestServer(t *testing.T) *server.TestServer {
	return setupTestServerWithContext(t, server.NewTestContext())
}

func setupTestServerWithContext(t *testing.T, ctx *server.Context) *server.TestServer {
	storage.TestingCommandFilter = checkEndTransactionTrigger
	s := &server.TestServer{Ctx: ctx}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}

func setup(t *testing.T) (*server.TestServer, *sql.DB, *client.DB) {
	return setupWithContext(t, server.NewTestContext())
}

func setupWithContext(t *testing.T, ctx *server.Context) (*server.TestServer, *sql.DB, *client.DB) {
	s := setupTestServer(t)
	// SQL requests use "root" which has ALL permissions on everything.
	sqlDB, err := sql.Open("cockroach", fmt.Sprintf("https://%s@%s?certs=%s",
		security.RootUser, s.ServingAddr(), security.EmbeddedCertsDir))
	if err != nil {
		t.Fatal(err)
	}
	// All KV requests need "node" certs.
	kvDB, err := client.Open(s.Stopper(), fmt.Sprintf("rpcs://%s@%s?certs=%s",
		security.NodeUser, s.ServingAddr(), security.EmbeddedCertsDir))
	if err != nil {
		t.Fatal(err)
	}

	return s, sqlDB, kvDB
}

func cleanupTestServer(s *server.TestServer) {
	s.Stop()
	storage.TestingCommandFilter = nil
}

func cleanup(s *server.TestServer, db *sql.DB) {
	_ = db.Close()
	cleanupTestServer(s)
}

func tempRestrictedCopy(tb testing.TB, tempdir, path, prefix string) (string, func()) {
	contents, err := securitytest.Asset(path)
	if err != nil {
		tb.Fatal(err)
	}

	tempFile, err := ioutil.TempFile(tempdir, prefix)
	if err != nil {
		tb.Fatal(err)
	}
	if err := tempFile.Close(); err != nil {
		tb.Fatal(err)
	}
	tempPath := tempFile.Name()
	if err := os.Remove(tempPath); err != nil {
		tb.Fatal(err)
	}
	// `github.com/lib/pq` requires that private key file permissions are
	// "u=rw (0600) or less".
	if err := ioutil.WriteFile(tempPath, contents, 0600); err != nil {
		tb.Fatal(err)
	}
	return tempPath, func() {
		if err := os.Remove(tempPath); err != nil {
			// Not Fatal() because we might already be panicking.
			tb.Error(err)
		}
	}
}

func pgURL(tb testing.TB, s *server.TestServer, user, tempDir, prefix string) (url.URL, func()) {
	host, port, err := net.SplitHostPort(s.PGAddr())
	if err != nil {
		tb.Fatal(err)
	}

	caPath := filepath.Join(security.EmbeddedCertsDir, "ca.crt")
	certPath := security.ClientCertPath(security.EmbeddedCertsDir, user)
	keyPath := security.ClientKeyPath(security.EmbeddedCertsDir, user)

	// Copy these assets to disk from embedded strings, so this test can
	// run from a standalone binary.
	tempCAPath, tempCACleanup := tempRestrictedCopy(tb, tempDir, caPath, "TestLogic_ca")
	tempCertPath, tempCertCleanup := tempRestrictedCopy(tb, tempDir, certPath, "TestLogic_cert")
	tempKeyPath, tempKeyCleanup := tempRestrictedCopy(tb, tempDir, keyPath, "TestLogic_key")

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
