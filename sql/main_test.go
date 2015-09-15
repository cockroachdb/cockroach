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
	"database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
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
func checkEndTransactionTrigger(req proto.Request) error {
	args, ok := req.(*proto.EndTransactionRequest)
	if !ok {
		return nil
	}

	if !args.Commit {
		// This is a rollback: skip trigger verification.
		return nil
	}

	hasTrigger := args.GetInternalCommitTrigger().GetModifiedSpanTrigger().GetSystemDBSpan()

	var hasSystemKey bool
	for _, it := range args.Intents {
		if keys.SystemDBSpan.ContainsKey(it.GetKey()) {
			hasSystemKey = true
			break
		}
	}
	if hasSystemKey != hasTrigger {
		return util.Errorf("EndTransaction hasSystemKey=%t, but hasSystemDBTrigger=%t",
			hasSystemKey, hasTrigger)
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
	sqlDB, err := sql.Open("cockroach", fmt.Sprintf("https://%s@%s?certs=test_certs",
		security.RootUser, s.ServingAddr()))
	if err != nil {
		t.Fatal(err)
	}
	// All KV requests need "node" certs.
	kvDB, err := client.Open(s.Stopper(), fmt.Sprintf("rpcs://%s@%s?certs=test_certs",
		security.NodeUser, s.ServingAddr()))
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
