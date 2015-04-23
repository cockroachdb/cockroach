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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// NOTE: these tests are in package kv_test to avoid a circular
// dependency between the server and kv packages. These tests rely on
// starting a TestServer, which creates a "real" node and employs a
// distributed sender server-side.

// TestRangeLookupWithOpenTransaction verifies that range lookups are
// done in such a way (e.g. using inconsistent reads) that they
// proceed in the event that a write intent is extant at the meta
// index record being read.
func TestRangeLookupWithOpenTransaction(t *testing.T) {
	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	db := createTestClient(s.ServingAddr())
	db.User = storage.UserRoot

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax)
	now := s.Clock().Now()
	txn := proto.NewTransaction("txn", proto.Key("foobar"), 0, proto.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(s.Engine, nil, key, now, txn, &proto.RangeDescriptor{}); err != nil {
		t.Fatal(err)
	}

	// Now, with an intent pending, attempt (asynchronously) to read
	// from an arbitrary key. This will cause the distributed sender to
	// do a range lookup, which will encounter the intent. We're
	// verifying here that the range lookup doesn't fail with a write
	// intent error. If it did, it would go into a deadloop attempting
	// to push the transaction, which in turn requires another range
	// lookup, etc, ad nauseam.
	success := make(chan struct{})
	go func() {
		if err := db.Run(client.GetCall(proto.Key("a"))); err != nil {
			t.Fatal(err)
		}
		close(success)
	}()

	select {
	case <-success:
		// Hurrah!
	case <-time.After(5 * time.Second):
		t.Errorf("get request did not succeed in face of range metadata intent")
	}
}

// setupMultipleRanges creates a test server and splits the
// key range at key "b". Returns the test server and client.
// The caller is responsible for stopping the server and
// closing the client.
func setupMultipleRanges(t *testing.T) (*server.TestServer, *client.KV) {
	s := &server.TestServer{}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	db := createTestClient(s.ServingAddr())
	db.User = storage.UserRoot

	// Split the keyspace at "b".
	if err := db.Run(client.Call{
		Args: &proto.AdminSplitRequest{
			RequestHeader: proto.RequestHeader{
				Key: proto.Key("b"),
			},
			SplitKey: proto.Key("b"),
		},
		Reply: &proto.AdminSplitResponse{},
	}); err != nil {
		t.Fatal(err)
	}

	return s, db
}

// TestMultiRangeScan verifies operation of a scan across ranges.
func TestMultiRangeScan(t *testing.T) {
	s, db := setupMultipleRanges(t)
	defer s.Stop()

	// Write keys "a" and "b".
	for _, key := range []proto.Key{proto.Key("a"), proto.Key("b")} {
		if err := db.Run(client.PutCall(key, []byte("value"))); err != nil {
			t.Fatal(err)
		}
	}

	call := client.ScanCall(proto.Key("a"), proto.Key("c"), 0)
	sr := call.Reply.(*proto.ScanResponse)
	if err := db.Run(call); err != nil {
		t.Fatalf("unexpected error on scan: %s", err)
	}
	if l := len(sr.Rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
}

// TestMultiRangeScanInconsistent verifies that a scan across ranges
// that doesn't require read consistency will set a timestamp using
// the clock local to the distributed sender.
func TestMultiRangeScanInconsistent(t *testing.T) {
	s, db := setupMultipleRanges(t)
	defer s.Stop()

	// Write keys "a" and "b".
	keys := []proto.Key{proto.Key("a"), proto.Key("b")}
	ts := []proto.Timestamp{}
	for _, key := range keys {
		call := client.PutCall(key, []byte("value"))
		pr := call.Reply.(*proto.PutResponse)
		if err := db.Run(call); err != nil {
			t.Fatal(err)
		}
		ts = append(ts, pr.Timestamp)
	}

	// Do an inconsistent scan from a new dist sender and verify it does
	// the read at it's local clock and doesn't receive an
	// OpRequiresTxnError. We set the local clock to the timestamp of
	// the first key to verify it's used to read only key "a".
	manual := hlc.NewManualClock(ts[1].WallTime - 1)
	clock := hlc.NewClock(manual.UnixNano)
	ds := kv.NewDistSender(&kv.DistSenderContext{Clock: clock}, s.Gossip())
	call := client.ScanCall(proto.Key("a"), proto.Key("c"), 0)
	sr := call.Reply.(*proto.ScanResponse)
	sa := call.Args.(*proto.ScanRequest)
	sa.ReadConsistency = proto.INCONSISTENT
	sa.User = storage.UserRoot
	ds.Send(call)
	if err := sr.GoError(); err != nil {
		t.Fatal(err)
	}
	if l := len(sr.Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
	if key := sr.Rows[0].Key; !key.Equal(keys[0]) {
		t.Errorf("expected key %q; got %q", keys[0], key)
	}
}
