// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func createTestClient(t *testing.T, stopper *stop.Stopper, addr string) *client.DB {
	return createTestClientForUser(t, stopper, addr, security.NodeUser)
}

func createTestClientForUser(t *testing.T, stopper *stop.Stopper, addr, user string) *client.DB {
	db, err := client.Open(stopper, fmt.Sprintf("rpcs://%s@%s?certs=%s&failfast=1",
		user, addr, security.EmbeddedCertsDir))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestKVDBCoverage verifies that all methods may be invoked on the
// key value database.
func TestKVDBCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	db := createTestClient(t, s.Stopper(), s.ServingAddr())
	key := roachpb.Key("a")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Put first value at key.
	if err := db.Put(key, value1); err != nil {
		t.Fatal(err)
	}

	// Verify put.
	if gr, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Error("expected key to exist")
	}

	// Conditional put should succeed, changing value1 to value2.
	if err := db.CPut(key, value2, value1); err != nil {
		t.Fatal(err)
	}

	// Verify get by looking up conditional put value.
	if gr, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(gr.ValueBytes(), value2) {
		t.Errorf("expected get to return %q; got %q", value2, gr.ValueBytes())
	}

	// Increment.
	if ir, err := db.Inc("i", 10); err != nil {
		t.Fatal(err)
	} else if ir.ValueInt() != 10 {
		t.Errorf("expected increment new value of %d; got %d", 10, ir.ValueInt())
	}

	// Delete conditional put value.
	if err := db.Del(key); err != nil {
		t.Fatal(err)
	}
	if gr, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Error("expected key to not exist after delete")
	}

	// Put values in anticipation of scan & delete range.
	keyValues := []roachpb.KeyValue{
		{Key: roachpb.Key("a"), Value: roachpb.MakeValueFromBytes(value1)},
		{Key: roachpb.Key("b"), Value: roachpb.MakeValueFromBytes(value2)},
		{Key: roachpb.Key("c"), Value: roachpb.MakeValueFromBytes(value3)},
	}
	for _, kv := range keyValues {
		if err := db.Put(kv.Key, kv.Value.GetBytes()); err != nil {
			t.Fatal(err)
		}
	}
	if rows, err := db.Scan("a", "d", 0); err != nil {
		t.Fatal(err)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			if !bytes.Equal(rows[i].ValueBytes(), kv.Value.GetBytes()) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[i].ValueBytes(), kv.Value.GetBytes())
			}
		}
	}

	// Test reverse scan.
	if rows, err := db.ReverseScan("a", "d", 0); err != nil {
		t.Fatal(err)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			if !bytes.Equal(rows[len(keyValues)-1-i].ValueBytes(), kv.Value.GetBytes()) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[len(keyValues)-i].ValueBytes(), kv.Value.GetBytes())
			}
		}
	}

	if err := db.DelRange("a", "c"); err != nil {
		t.Fatal(err)
	}
}

// TestKVDBInternalMethods verifies no internal methods are available
// HTTP DB interface.
func TestKVDBInternalMethods(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("test broken & disabled; obsolete after after #2271")
	s := server.StartTestServer(t)
	defer s.Stop()

	testCases := []roachpb.Request{
		&roachpb.HeartbeatTxnRequest{},
		&roachpb.GCRequest{},
		&roachpb.PushTxnRequest{},
		&roachpb.RangeLookupRequest{},
		&roachpb.ResolveIntentRequest{},
		&roachpb.ResolveIntentRangeRequest{},
		&roachpb.MergeRequest{},
		&roachpb.TruncateLogRequest{},
		&roachpb.LeaderLeaseRequest{},

		&roachpb.EndTransactionRequest{
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{},
		},
	}
	// Verify internal methods experience bad request errors.
	db := createTestClient(t, s.Stopper(), s.ServingAddr())
	for i, args := range testCases {
		args.Header().Key = roachpb.Key("a")
		if roachpb.IsRange(args) {
			args.Header().EndKey = args.Header().Key.Next()
		}
		b := &client.Batch{}
		b.InternalAddRequest(args)
		err := db.Run(b)
		if err == nil {
			t.Errorf("%d: unexpected success calling %s", i, args.Method())
		} else if !testutils.IsError(err, "(couldn't find method|contains commit trigger)") {
			t.Errorf("%d: expected missing method %s; got %s", i, args.Method(), err)
		}
	}
}

// TestKVDBTransaction verifies that transactions work properly over
// the KV DB endpoint.
func TestKVDBTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	key := roachpb.Key("db-txn-test")
	value := []byte("value")
	err := db.Txn(func(txn *client.Txn) error {
		// Use snapshot isolation so non-transactional read can always push.
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(key, value); err != nil {
			t.Fatal(err)
		}

		// Attempt to read outside of txn.
		if gr, err := db.Get(key); err != nil {
			t.Fatal(err)
		} else if gr.Exists() {
			t.Errorf("expected nil value; got %+v", gr.Value)
		}

		// Read within the transaction.
		if gr, err := txn.Get(key); err != nil {
			t.Fatal(err)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
			t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected success on commit; got %s", err)
	}

	// Verify the value is now visible after commit.
	if gr, err := db.Get(key); err != nil {
		t.Errorf("expected success reading value; got %s", err)
	} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
		t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
	}
}

// TestAuthentication tests authentication for the KV endpoint.
func TestAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	arg := &roachpb.PutRequest{}
	arg.Header().Key = roachpb.Key("a")
	b := &client.Batch{}
	b.InternalAddRequest(arg)

	// Create a "node" client and call Run() on it which lets us build
	// our own request, specifying the user.
	db1 := createTestClientForUser(t, s.Stopper(), s.ServingAddr(), security.NodeUser)
	if err := db1.Run(b); err != nil {
		t.Fatal(err)
	}

	// Try again, but this time with certs for a non-node user (even the root
	// user has no KV permissions).
	db2 := createTestClientForUser(t, s.Stopper(), s.ServingAddr(), security.RootUser)
	if err := db2.Run(b); err == nil {
		t.Fatal("Expected error!")
	}
}
