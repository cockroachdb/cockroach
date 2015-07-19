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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func createTestClient(t *testing.T, addr string) *client.DB {
	db, err := client.Open("https://root@" + addr + "?certs=" + security.EmbeddedCertsDir)
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

	db := createTestClient(t, s.ServingAddr())
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

	testCases := []struct {
		args  proto.Request
		reply proto.Response
	}{
		{&proto.InternalRangeLookupRequest{}, &proto.InternalRangeLookupResponse{}},
		{&proto.InternalGCRequest{}, &proto.InternalGCResponse{}},
		{&proto.InternalHeartbeatTxnRequest{}, &proto.InternalHeartbeatTxnResponse{}},
		{&proto.InternalPushTxnRequest{}, &proto.InternalPushTxnResponse{}},
		{&proto.InternalResolveIntentRequest{}, &proto.InternalResolveIntentResponse{}},
		{&proto.InternalResolveIntentRangeRequest{}, &proto.InternalResolveIntentRangeResponse{}},
		{&proto.InternalMergeRequest{}, &proto.InternalMergeResponse{}},
		{&proto.InternalTruncateLogRequest{}, &proto.InternalTruncateLogResponse{}},
	}
	// Verify non-public methods experience bad request errors.
	db := createTestClient(t, s.ServingAddr())
	for i, test := range testCases {
		test.args.Header().Key = proto.Key("a")
		if proto.IsRange(test.args) {
			test.args.Header().EndKey = test.args.Header().Key.Next()
		}
		b := &client.Batch{}
		b.InternalAddCall(proto.Call{Args: test.args, Reply: test.reply})
		err := db.Run(b)
		if err == nil {
			t.Errorf("%d: unexpected success calling %s", i, test.args.Method())
		} else if !strings.Contains(err.Error(), "404 Not Found") {
			t.Errorf("%d: expected 404; got %s", i, err)
		}
	}
}

// TestKVDBEndTransactionWithTriggers verifies that triggers are
// disallowed on call to EndTransaction.
func TestKVDBEndTransactionWithTriggers(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	db := createTestClient(t, s.ServingAddr())
	err := db.Txn(func(txn *client.Txn) error {
		// Make an EndTransaction request which would fail if not
		// stripped. In this case, we set the start key to "bar" for a
		// split of the default range; start key must be "" in this case.
		b := &client.Batch{}
		b.InternalAddCall(proto.Call{
			Args: &proto.EndTransactionRequest{
				RequestHeader: proto.RequestHeader{Key: proto.Key("foo")},
				Commit:        true,
				InternalCommitTrigger: &proto.InternalCommitTrigger{
					SplitTrigger: &proto.SplitTrigger{
						UpdatedDesc: proto.RangeDescriptor{StartKey: proto.Key("bar")},
					},
				},
			},
			Reply: &proto.EndTransactionResponse{},
		})
		return txn.Run(b)
	})
	if err == nil {
		t.Errorf("expected 400 bad request error on commit")
	}
}

// TestKVDBTransaction verifies that transactions work properly over
// the KV DB endpoint.
func TestKVDBTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	db := createTestClient(t, s.ServingAddr())

	key := proto.Key("db-txn-test")
	value := []byte("value")
	if err := db.Txn(func(txn *client.Txn) error {
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
	}); err != nil {
		t.Errorf("expected success on commit; got %s", err)
	}

	// Verify the value is now visible after commit.
	if gr, err := db.Get(key); err != nil {
		t.Errorf("expected success reading value; got %s", err)
	} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
		t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
	}
}

// TestHTTPAuthentication tests authentication for the KV http endpoint.
func TestHTTPAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	// createTestClient creates a "root" client.
	db := createTestClient(t, s.ServingAddr())

	// We call Run() on the client which lets us build our own request,
	// specifying the user.
	arg := &proto.PutRequest{}
	arg.Header().Key = proto.Key("a")
	arg.Header().User = security.RootUser
	reply := &proto.PutResponse{}
	b := &client.Batch{}
	b.InternalAddCall(proto.Call{Args: arg, Reply: reply})
	err := db.Run(b)
	if err != nil {
		t.Fatal(err)
	}

	// Try again, but this time with arg.User = "foo".
	arg.Header().User = "foo"
	b = &client.Batch{}
	b.InternalAddCall(proto.Call{Args: arg, Reply: reply})
	err = db.Run(b)
	if err == nil {
		t.Fatal("Expected error!")
	}
}
