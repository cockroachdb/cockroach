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
	"github.com/cockroachdb/cockroach/proto"
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
	key := proto.Key("a")
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
		t.Error("expected key to exist after delete")
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
	keyValues := []proto.KeyValue{
		{Key: proto.Key("a"), Value: proto.Value{Bytes: value1}},
		{Key: proto.Key("b"), Value: proto.Value{Bytes: value2}},
		{Key: proto.Key("c"), Value: proto.Value{Bytes: value3}},
	}
	for _, kv := range keyValues {
		if err := db.Put(kv.Key, kv.Value.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	if rows, err := db.Scan("a", "d", 0); err != nil {
		t.Fatal(err)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			if !bytes.Equal(rows[i].ValueBytes(), kv.Value.Bytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[i].ValueBytes(), kv.Value.Bytes)
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
			if !bytes.Equal(rows[len(keyValues)-1-i].ValueBytes(), kv.Value.Bytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[len(keyValues)-i].ValueBytes(), kv.Value.Bytes)
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
	s := server.StartTestServer(t)
	defer s.Stop()

	testCases := []proto.Request{
		&proto.HeartbeatTxnRequest{},
		&proto.GCRequest{},
		&proto.PushTxnRequest{},
		&proto.RangeLookupRequest{},
		&proto.ResolveIntentRequest{},
		&proto.ResolveIntentRangeRequest{},
		&proto.MergeRequest{},
		&proto.TruncateLogRequest{},
		&proto.LeaderLeaseRequest{},

		&proto.EndTransactionRequest{
			InternalCommitTrigger: &proto.InternalCommitTrigger{},
		},
	}
	// Verify internal methods experience bad request errors.
	db := createTestClient(t, s.Stopper(), s.ServingAddr())
	for i, args := range testCases {
		args.Header().Key = proto.Key("a")
		if proto.IsRange(args) {
			args.Header().EndKey = args.Header().Key.Next()
		}
		b := &client.Batch{}
		b.InternalAddCall(proto.Call{Args: args, Reply: args.CreateReply()})
		err := db.Run(b)
		if err == nil {
			t.Errorf("%d: unexpected success calling %s", i, args.Method())
		} else if !testutils.IsError(err, "(couldn't find method|contains commit trigger)") {
			t.Errorf("%d: expected missing method %s; got %s", i, args.Method(), err)
		}

		// Verify same but within a Batch request.
		bArgs := &proto.BatchRequest{}
		bArgs.Add(args)
		b = &client.Batch{}
		b.InternalAddCall(proto.Call{Args: bArgs, Reply: &proto.BatchResponse{}})

		if err := db.Run(b); err == nil {
			t.Errorf("%d: unexpected success calling %s", i, args.Method())
		} else if !testutils.IsError(err, "(contains an internal request|contains commit trigger)") {
			t.Errorf("%d: expected disallowed method error %s; got %s", i, args.Method(), err)
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

	key := proto.Key("db-txn-test")
	value := []byte("value")
	err := db.Txn(func(txn *client.Txn) error {
		// Use snapshot isolation so non-transactional read can always push.
		txn.SetSnapshotIsolation()

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

	arg := &proto.PutRequest{}
	arg.Header().Key = proto.Key("a")
	reply := &proto.PutResponse{}
	b := &client.Batch{}
	b.InternalAddCall(proto.Call{Args: arg, Reply: reply})

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
