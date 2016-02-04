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

package sql_test

import (
	"bytes"
	"database/sql"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
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
	modifiedSystemConfigSpan := modifiedSpanTrigger != nil && modifiedSpanTrigger.SystemConfigSpan

	var hasSystemKey bool
	for _, span := range args.IntentSpans {
		addr := keys.Addr(span.Key)
		if bytes.Compare(addr, keys.SystemConfigSpan.Key) >= 0 &&
			bytes.Compare(addr, keys.SystemConfigSpan.EndKey) < 0 {
			hasSystemKey = true
			break
		}
	}
	// If the transaction in question has intents in the system span, then
	// modifiedSystemConfigSpan should always be true. However, it is possible
	// for modifiedSystemConfigSpan to be set, even though no system keys are
	// present. This can occur with certain conditional DDL statements (e.g.
	// "CREATE TABLE IF NOT EXISTS"), which set the SystemConfigTrigger
	// aggressively but may not actually end up changing the system DB depending
	// on the current state.
	// For more information, see the related comment at the beginning of
	// planner.makePlan().
	if hasSystemKey && !modifiedSystemConfigSpan {
		return util.Errorf("EndTransaction hasSystemKey=%t, but hasSystemConfigTrigger=%t",
			hasSystemKey, modifiedSystemConfigSpan)
	}

	return nil
}

type testServer struct {
	server.TestServer
	cleanupFn func()
}

func setupTestServer(t *testing.T) *testServer {
	return setupTestServerWithContext(t, server.NewTestContext())
}

func setupTestServerWithContext(t *testing.T, ctx *server.Context) *testServer {
	storage.TestingCommandFilter = checkEndTransactionTrigger
	s := &testServer{TestServer: server.TestServer{Ctx: ctx}}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s
}

func setup(t *testing.T) (*testServer, *sql.DB, *client.DB) {
	return setupWithContext(t, server.NewTestContext())
}

func setupWithContext(t *testing.T, ctx *server.Context) (*testServer, *sql.DB, *client.DB) {
	s := setupTestServer(t)

	// SQL requests use "root" which has ALL permissions on everything.
	url, cleanupFn := sqlutils.PGUrl(t, &s.TestServer, security.RootUser, os.TempDir(), "setupWithContext")
	sqlDB, err := sql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	s.cleanupFn = cleanupFn

	return s, sqlDB, s.DB()
}

func cleanupTestServer(s *testServer) {
	s.Stop()
	if s.cleanupFn != nil {
		s.cleanupFn()
	}
	storage.TestingCommandFilter = nil
}

func cleanup(s *testServer, db *sql.DB) {
	_ = db.Close()
	cleanupTestServer(s)
}
