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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
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
	var ctx base.Context
	ctx.InitDefaults()
	ctx.User = user
	ctx.SSLCA = filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	ctx.SSLCert = filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.crt", user))
	ctx.SSLCertKey = filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("%s.key", user))
	sender, err := client.NewSender(rpc.NewContext(&ctx, nil, stopper), addr)
	if err != nil {
		t.Fatal(err)
	}
	return client.NewDB(sender)
}

// TestKVDBCoverage verifies that all methods may be invoked on the
// key value database.
func TestKVDBCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := server.StartTestServer(t)
	defer s.Stop()

	db := createTestClient(t, s.Stopper(), s.ServingAddr())
	key := roachpb.Key("a")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Put first value at key.
	if pErr := db.Put(key, value1); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify put.
	if gr, pErr := db.Get(key); pErr != nil {
		t.Fatal(pErr)
	} else if !gr.Exists() {
		t.Error("expected key to exist")
	}

	// Conditional put should succeed, changing value1 to value2.
	if pErr := db.CPut(key, value2, value1); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify get by looking up conditional put value.
	if gr, pErr := db.Get(key); pErr != nil {
		t.Fatal(pErr)
	} else if !bytes.Equal(gr.ValueBytes(), value2) {
		t.Errorf("expected get to return %q; got %q", value2, gr.ValueBytes())
	}

	// Increment.
	if ir, pErr := db.Inc("i", 10); pErr != nil {
		t.Fatal(pErr)
	} else if ir.ValueInt() != 10 {
		t.Errorf("expected increment new value of %d; got %d", 10, ir.ValueInt())
	}

	// Delete conditional put value.
	if pErr := db.Del(key); pErr != nil {
		t.Fatal(pErr)
	}
	if gr, pErr := db.Get(key); pErr != nil {
		t.Fatal(pErr)
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
		valueBytes, pErr := kv.Value.GetBytes()
		if pErr != nil {
			t.Fatal(pErr)
		}
		if pErr := db.Put(kv.Key, valueBytes); pErr != nil {
			t.Fatal(pErr)
		}
	}
	if rows, pErr := db.Scan("a", "d", 0); pErr != nil {
		t.Fatal(pErr)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			valueBytes, pErr := kv.Value.GetBytes()
			if pErr != nil {
				t.Fatal(pErr)
			}
			if !bytes.Equal(rows[i].ValueBytes(), valueBytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[i].ValueBytes(), valueBytes)
			}
		}
	}

	// Test reverse scan.
	if rows, pErr := db.ReverseScan("a", "d", 0); pErr != nil {
		t.Fatal(pErr)
	} else if len(rows) != len(keyValues) {
		t.Fatalf("expected %d rows in scan; got %d", len(keyValues), len(rows))
	} else {
		for i, kv := range keyValues {
			valueBytes, pErr := kv.Value.GetBytes()
			if pErr != nil {
				t.Fatal(pErr)
			}
			if !bytes.Equal(rows[len(keyValues)-1-i].ValueBytes(), valueBytes) {
				t.Errorf("%d: key %q, values %q != %q", i, kv.Key, rows[len(keyValues)-i].ValueBytes(), valueBytes)
			}
		}
	}

	if pErr := db.DelRange("a", "c"); pErr != nil {
		t.Fatal(pErr)
	}
}

func TestKVDBInternalMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		{
			header := args.Header()
			header.Key = roachpb.Key("a")
			args.SetHeader(header)
		}
		if roachpb.IsRange(args) {
			header := args.Header()
			header.EndKey = args.Header().Key.Next()
			args.SetHeader(header)
		}
		b := &client.Batch{}
		b.InternalAddRequest(args)
		pErr := db.Run(b)
		if pErr == nil {
			t.Errorf("%d: unexpected success calling %s", i, args.Method())
		} else if !testutils.IsPError(pErr, "contains an internal request|contains commit trigger") {
			t.Errorf("%d: unexpected error for %s: %s", i, args.Method(), pErr)
		}
	}
}

// TestKVDBTransaction verifies that transactions work properly over
// the KV DB endpoint.
func TestKVDBTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := server.StartTestServer(t)
	defer s.Stop()

	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	key := roachpb.Key("db-txn-test")
	value := []byte("value")
	pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		// Use snapshot isolation so non-transactional read can always push.
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}

		if pErr := txn.Put(key, value); pErr != nil {
			t.Fatal(pErr)
		}

		// Attempt to read outside of txn.
		if gr, pErr := db.Get(key); pErr != nil {
			t.Fatal(pErr)
		} else if gr.Exists() {
			t.Errorf("expected nil value; got %+v", gr.Value)
		}

		// Read within the transaction.
		if gr, pErr := txn.Get(key); pErr != nil {
			t.Fatal(pErr)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
			t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
		}
		return nil
	})
	if pErr != nil {
		t.Errorf("expected success on commit; got %s", pErr)
	}

	// Verify the value is now visible after commit.
	if gr, pErr := db.Get(key); pErr != nil {
		t.Errorf("expected success reading value; got %s", pErr)
	} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
		t.Errorf("expected value %q; got %q", value, gr.ValueBytes())
	}
}

// TestAuthentication tests authentication for the KV endpoint.
func TestAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := server.StartTestServer(t)
	defer s.Stop()

	var b1 client.Batch
	b1.Put("a", "b")

	// Create a node user client and call Run() on it which lets us build our own
	// request, specifying the user.
	db1 := createTestClientForUser(t, s.Stopper(), s.ServingAddr(), security.NodeUser)
	if pErr := db1.Run(&b1); pErr != nil {
		t.Fatal(pErr)
	}

	var b2 client.Batch
	b2.Put("c", "d")

	// Try again, but this time with certs for a non-node user (even the root
	// user has no KV permissions).
	db2 := createTestClientForUser(t, s.Stopper(), s.ServingAddr(), security.RootUser)
	if pErr := db2.Run(&b2); !testutils.IsPError(pErr, "is not allowed") {
		t.Fatal(pErr)
	}
}
