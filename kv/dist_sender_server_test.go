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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
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
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(t, s.ServingAddr())

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := keys.MakeKey(keys.Meta1Prefix, proto.KeyMax)
	now := s.Clock().Now()
	txn := proto.NewTransaction("txn", proto.Key("foobar"), 0, proto.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(s.Engines[0], nil, key, now, txn, &proto.RangeDescriptor{}); err != nil {
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
		if _, err := db.Get("a"); err != nil {
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
func setupMultipleRanges(t *testing.T) (*server.TestServer, *client.DB) {
	s := server.StartTestServer(t)
	db := createTestClient(t, s.ServingAddr())

	// Split the keyspace at "b".
	if err := db.AdminSplit("b"); err != nil {
		t.Fatal(err)
	}

	return s, db
}

// TestMultiRangeScan verifies operation of a scan across ranges.
func TestMultiRangeScan(t *testing.T) {
	s, db := setupMultipleRanges(t)
	defer s.Stop()

	// Write keys "a" and "b".
	for _, key := range []string{"a", "b"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	if rows, err := db.Scan("a", "c", 0); err != nil {
		t.Fatalf("unexpected error on scan: %s", err)
	} else if l := len(rows); l != 2 {
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
	keys := []string{"a", "b"}
	ts := []time.Time{}
	b := &client.Batch{}
	for _, key := range keys {
		b.Put(key, "value")
	}
	if err := db.Run(b); err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		ts = append(ts, b.Results[i].Rows[0].Timestamp)
		log.Infof("%d: %s", i, b.Results[i].Rows[0].Timestamp)
	}

	// Do an inconsistent scan from a new dist sender and verify it does
	// the read at its local clock and doesn't receive an
	// OpRequiresTxnError. We set the local clock to the timestamp of
	// the first key to verify it's used to read only key "a".
	manual := hlc.NewManualClock(ts[1].UnixNano() - 1)
	clock := hlc.NewClock(manual.UnixNano)
	ds := kv.NewDistSender(&kv.DistSenderContext{Clock: clock}, s.Gossip())
	call := proto.ScanCall(proto.Key("a"), proto.Key("c"), 0)
	sr := call.Reply.(*proto.ScanResponse)
	sa := call.Args.(*proto.ScanRequest)
	sa.ReadConsistency = proto.INCONSISTENT
	sa.User = storage.UserRoot
	ds.Send(context.Background(), call)
	if err := sr.GoError(); err != nil {
		t.Fatal(err)
	}
	if l := len(sr.Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
	if key := string(sr.Rows[0].Key); keys[0] != key {
		t.Errorf("expected key %q; got %q", keys[0], key)
	}
}

// TestStartEqualsEndKeyScan verifies that specifying start==end on scan
// returns an empty set.
func TestStartEqualsEndKeyScan(t *testing.T) {
	s := server.StartTestServer(t)
	db := createTestClient(t, s.ServingAddr())
	defer s.Stop()

	// Write key "a".
	if err := db.Put("a", "value"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Scan("a", "a", 0); err == nil {
		t.Fatalf("expected error on scan with startkey == endkey")
	}
}

// TestSplitByMeta2KeyMax check range splitting at key Meta2KeyMax should
// fail as Meta2KeyMax is not a valid split key.
func TestSplitByMeta2KeyMax(t *testing.T) {
	s := server.StartTestServer(t)
	db := createTestClient(t, s.ServingAddr())
	defer s.Stop()

	ch := make(chan struct{})
	go func() {
		if err := db.AdminSplit(keys.Meta2KeyMax); err == nil {
			t.Fatalf("range split on Meta2KeyMax should fail")
		}
		close(ch)
	}()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Error("range split on Meta2KeyMax timed out")
	}
}
