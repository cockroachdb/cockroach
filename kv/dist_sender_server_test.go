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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv_test

import (
	"sync/atomic"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/uuid"
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
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := testutils.MakeKey(keys.Meta1Prefix, roachpb.KeyMax)
	now := s.Clock().Now()
	txn := roachpb.NewTransaction("txn", roachpb.Key("foobar"), 0, enginepb.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(
		context.Background(), s.(*server.TestServer).Ctx.Engines[0],
		nil, key, now, txn, &roachpb.RangeDescriptor{}); err != nil {
		t.Fatal(err)
	}

	// Now, with an intent pending, attempt (asynchronously) to read
	// from an arbitrary key. This will cause the distributed sender to
	// do a range lookup, which will encounter the intent. We're
	// verifying here that the range lookup doesn't fail with a write
	// intent error. If it did, it would go into a deadloop attempting
	// to push the transaction, which in turn requires another range
	// lookup, etc, ad nauseam.
	if _, err := db.Get("a"); err != nil {
		t.Fatal(err)
	}
}

// setupMultipleRanges creates a test server and splits the
// key range at the given keys. Returns the test server and client.
// The caller is responsible for stopping the server and
// closing the client.
func setupMultipleRanges(
	t *testing.T, ts serverutils.TestServerInterface, splitAt ...string,
) *client.DB {
	db := createTestClient(t, ts.Stopper(), ts.ServingAddr())

	// Split the keyspace at the given keys.
	for _, key := range splitAt {
		if err := db.AdminSplit(key); err != nil {
			// Don't leak server goroutines.
			t.Fatal(err)
		}
	}

	return db
}

var errInfo = testutils.MakeCaller(3, 2)

func checkScanResults(t *testing.T, results []client.Result, expResults [][]string, expCount int) {
	if len(expResults) != len(results) {
		t.Fatalf("only got %d results, wanted %d", len(expResults), len(results))
	}
	// Ensure all the keys returned align properly with what is expected.
	count := 0
	for i, res := range results {
		count += len(res.Rows)
		for j, kv := range res.Rows {
			if key, expKey := string(kv.Key), expResults[i][j]; key != expKey {
				t.Errorf("%s: expected scan key (%d, %d) to be %q; got %q", errInfo(), i, j, expKey, key)
			}
		}
	}
	// The bound was respected.
	if count != expCount {
		t.Errorf("count = %d, expCount = %d", count, expCount)
	}
}

func TestMultiRangeBoundedBatchScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	// These are the expected results if there is no bound.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1"},
		{"b1", "b2", "c1"},
		{"c1", "c2", "d1", "f1", "f2", "f3"},
		{"f2"},
	}
	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	for bound := 1; bound <= 20; bound++ {
		b := db.NewBatch()
		b.Header.MaxSpanRequestKeys = int64(bound)

		b.Scan("a", "e")
		b.Scan("b", "c2")
		b.Scan("c", "g")
		b.Scan("f1a", "f2a")
		if err := db.Run(b); err != nil {
			t.Fatal(err)
		}

		expCount := maxExpCount
		if bound < maxExpCount {
			expCount = bound
		}
		checkScanResults(t, b.Results, expResults, expCount)
	}
}

func TestMultiRangeBoundedBatchReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	// These are the expected results if there is no bound
	expResults := [][]string{
		{"b2", "b1", "a3", "a2", "a1"},
		{"c1", "b2", "b1"},
		{"f3", "f2", "f1", "d1", "c2", "c1"},
		{"f2"},
	}
	maxExpCount := 0
	for _, res := range expResults {
		maxExpCount += len(res)
	}

	for bound := 1; bound <= 20; bound++ {
		b := db.NewBatch()
		b.Header.MaxSpanRequestKeys = int64(bound)

		b.ReverseScan("a", "c")
		b.ReverseScan("b", "c2")
		b.ReverseScan("c", "g")
		b.ReverseScan("f1a", "f2a")
		if err := db.Run(b); err != nil {
			t.Fatal(err)
		}

		expCount := maxExpCount
		if bound < maxExpCount {
			expCount = bound
		}
		checkScanResults(t, b.Results, expResults, expCount)
	}
}

// TestMultiRangeBoundedBatchScanUnsortedOrder runs two non-overlapping
// scan requests out of order and shows how the batch response can
// contain two partial responses.
func TestMultiRangeBoundedBatchScanUnsortedOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "b3", "b4", "b5", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	bound := 6
	b := db.NewBatch()
	b.Header.MaxSpanRequestKeys = int64(bound)
	// Two non-overlapping requests out of order.
	b.Scan("b3", "c2")
	b.Scan("a", "b3")
	if err := db.Run(b); err != nil {
		t.Fatal(err)
	}
	// See incomplete results for the two requests.
	expResults := [][]string{
		{"b3", "b4", "b5"},
		{"a1", "a2", "a3"},
	}
	checkScanResults(t, b.Results, expResults, bound)
}

// TestMultiRangeBoundedBatchScanSortedOverlapping runs two overlapping
// ordered (by start key) scan requests, and shows how the batch response can
// contain two partial responses.
func TestMultiRangeBoundedBatchScanSortedOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2", "c1", "c2", "d1", "f1", "f2", "f3"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	bound := 6
	b := db.NewBatch()
	b.Header.MaxSpanRequestKeys = int64(bound)
	// Two ordered overlapping requests.
	b.Scan("a", "d")
	b.Scan("b", "g")
	if err := db.Run(b); err != nil {
		t.Fatal(err)
	}
	// See incomplete results for the two requests.
	expResults := [][]string{
		{"a1", "a2", "a3", "b1", "b2"},
		{"b1"},
	}
	checkScanResults(t, b.Results, expResults, bound)
}

// TestMultiRangeEmptyAfterTruncate exercises a code path in which a
// multi-range request deals with a range without any active requests after
// truncation. In that case, the request is skipped.
func TestMultiRangeEmptyAfterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := setupMultipleRanges(t, s, "c", "d")

	// Delete the keys within a transaction. The range [c,d) doesn't have
	// any active requests.
	if err := db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "b", false)
		b.DelRange("e", "f", false)
		return txn.CommitInBatch(b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}
}

// TestMultiRangeScanReverseScanDeleteResolve verifies that Scan, ReverseScan,
// DeleteRange and ResolveIntentRange work across ranges.
func TestMultiRangeScanReverseScanDeleteResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := setupMultipleRanges(t, s, "b")

	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}
	// Scan to retrieve the keys just written.
	if rows, err := db.Scan("a", "q", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Scan in reverse order to retrieve the keys just written.
	if rows, err := db.ReverseScan("a", "q", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Delete the keys within a transaction. Implicitly, the intents are
	// resolved via ResolveIntentRange upon completion.
	if err := db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "d", false)
		return txn.CommitInBatch(b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}

	// Scan consistently to make sure the intents are gone.
	if rows, err := db.Scan("a", "q", 0); err != nil {
		t.Fatalf("unexpected error on Scan: %s", err)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}

	// ReverseScan consistently to make sure the intents are gone.
	if rows, err := db.ReverseScan("a", "q", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}
}

// TestMultiRangeScanReverseScanInconsistent verifies that a Scan/ReverseScan
// across ranges that doesn't require read consistency will set a timestamp
// using the clock local to the distributed sender.
func TestMultiRangeScanReverseScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := setupMultipleRanges(t, s, "b")

	// Write keys "a" and "b", the latter of which is the first key in the
	// second range.
	keys := [2]string{"a", "b"}
	ts := [2]hlc.Timestamp{}
	for i, key := range keys {
		b := &client.Batch{}
		b.Put(key, "value")
		if err := db.Run(b); err != nil {
			t.Fatal(err)
		}
		ts[i] = s.Clock().Now()
		log.Infof(context.TODO(), "%d: %s %d", i, key, ts[i])
		if i == 0 {
			util.SucceedsSoon(t, func() error {
				// Enforce that when we write the second key, it's written
				// with a strictly higher timestamp. We're dropping logical
				// ticks and the clock may just have been pushed into the
				// future, so that's necessary. See #3122.
				if ts[0].WallTime >= s.Clock().Now().WallTime {
					return errors.New("time stands still")
				}
				return nil
			})
		}
	}

	// Do an inconsistent Scan/ReverseScan from a new DistSender and verify
	// it does the read at its local clock and doesn't receive an
	// OpRequiresTxnError. We set the local clock to the timestamp of
	// just above the first key to verify it's used to read only key "a".
	for i, request := range []roachpb.Request{
		roachpb.NewScan(roachpb.Key("a"), roachpb.Key("c")),
		roachpb.NewReverseScan(roachpb.Key("a"), roachpb.Key("c")),
	} {
		manual := hlc.NewManualClock(ts[0].WallTime + 1)
		clock := hlc.NewClock(manual.UnixNano)
		ds := kv.NewDistSender(&kv.DistSenderContext{Clock: clock, RPCContext: s.RPCContext()}, s.(*server.TestServer).Gossip())

		reply, err := client.SendWrappedWith(ds, nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, request)
		if err != nil {
			t.Fatal(err)
		}

		var rows []roachpb.KeyValue
		switch r := reply.(type) {
		case *roachpb.ScanResponse:
			rows = r.Rows
		case *roachpb.ReverseScanResponse:
			rows = r.Rows
		default:
			t.Fatalf("unexpected response %T: %v", reply, reply)
		}

		if l := len(rows); l != 1 {
			t.Fatalf("%d: expected 1 row; got %d\n%s", i, l, rows)
		}
		if key := string(rows[0].Key); keys[0] != key {
			t.Errorf("expected key %q; got %q", keys[0], key)
		}
	}
}

func initReverseScanTestEnv(s serverutils.TestServerInterface, t *testing.T) *client.DB {
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Set up multiple ranges:
	// ["", "b"),["b", "e") ,["e", "g") and ["g", "\xff\xff").
	for _, key := range []string{"b", "e", "g"} {
		// Split the keyspace at the given key.
		if err := db.AdminSplit(key); err != nil {
			t.Fatal(err)
		}
	}
	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestSingleRangeReverseScan verifies that ReverseScan gets the right results
// on a single range.
func TestSingleRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := initReverseScanTestEnv(s, t)

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, err := db.ReverseScan("b", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	// Case 2: Request.EndKey is equal to the EndKey of the range.
	if rows, pErr := db.ReverseScan("e", "g", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	// Case 3: Test roachpb.TableDataMin. Expected to return "g" and "h".
	wanted := 2
	if rows, pErr := db.ReverseScan("g", keys.TableDataMin, 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != wanted {
		t.Errorf("expected %d rows; got %d", wanted, l)
	}
	// Case 4: Test keys.SystemMax
	// This span covers the system DB keys. Note sql.GetInitialSystemValues
	// returns one key before keys.SystemMax, but our scan is including one key
	// (\xffa) created for the test.
	if rows, pErr := db.ReverseScan(keys.SystemMax, "b", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 1 {
		t.Errorf("expected 1 row; got %d", l)
	}
}

// TestMultiRangeReverseScan verifies that ReverseScan gets the right results
// across multiple ranges.
func TestMultiRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := initReverseScanTestEnv(s, t)

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, pErr := db.ReverseScan("a", "d", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
	// Case 2: Request.EndKey is equal to the EndKey of the range.
	if rows, pErr := db.ReverseScan("d", "g", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

// TestReverseScanWithSplitAndMerge verifies that ReverseScan gets the right results
// across multiple ranges while range splits and merges happen.
func TestReverseScanWithSplitAndMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := initReverseScanTestEnv(s, t)

	// Case 1: An encounter with a range split.
	// Split the range ["b", "e") at "c".
	if err := db.AdminSplit("c"); err != nil {
		t.Fatal(err)
	}

	// The ReverseScan will run into a stale descriptor.
	if rows, err := db.ReverseScan("a", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Case 2: encounter with range merge .
	// Merge the range ["e", "g") and ["g", "\xff\xff") .
	if err := db.AdminMerge("e"); err != nil {
		t.Fatal(err)
	}
	if rows, err := db.ReverseScan("d", "g", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

func TestBadRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Write key "a".
	if err := db.Put("a", "value"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Scan("a", "a", 0); !testutils.IsError(err, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on scan with startkey == endkey: %v", err)
	}

	if _, err := db.ReverseScan("a", "a", 0); !testutils.IsError(err, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on reverse scan with startkey == endkey: %v", err)
	}

	if err := db.DelRange("x", "a"); !testutils.IsError(err, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on deletion on [x, a): %v", err)
	}

	if err := db.DelRange("", "z"); !testutils.IsError(err, "must be greater than LocalMax") {
		t.Fatalf("unexpected error on deletion on [KeyMin, z): %v", err)
	}
}

// TestNoSequenceCachePutOnRangeMismatchError verifies that the
// sequence cache is not updated with RangeKeyMismatchError. This is a
// higher-level version of TestSequenceCacheShouldCache.
func TestNoSequenceCachePutOnRangeMismatchError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := setupMultipleRanges(t, s, "b", "c")

	// The requests in the transaction below will be chunked and
	// sent to replicas in the following way:
	// 1) A batch request containing a BeginTransaction and a
	//    put on "a" are sent to a replica owning range ["a","b").
	// 2) A next batch request containing a put on "b" and a put
	//    on "c" are sent to a replica owning range ["b","c").
	//   (The range cache has a stale range descriptor.)
	// 3) The put request on "c" causes a RangeKeyMismatchError.
	// 4) The dist sender re-sends a request to the same replica.
	//    This time the request contains only the put on "b" to the
	//    same replica.
	// 5) The command succeeds since the sequence cache has not yet been updated.
	epoch := 0
	if err := db.Txn(func(txn *client.Txn) error {
		epoch++
		b := txn.NewBatch()
		b.Put("a", "val")
		b.Put("b", "val")
		b.Put("c", "val")
		return txn.CommitInBatch(b)
	}); err != nil {
		t.Errorf("unexpected error on transactional Puts: %s", err)
	}

	if epoch != 1 {
		t.Errorf("unexpected epoch; the txn must not be retried, but got %d retries", epoch)
	}
}

// TestPropagateTxnOnError verifies that DistSender.sendChunk properly
// propagates the txn data to a next iteration. Use txn.Writing field to
// verify that.
func TestPropagateTxnOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var storeKnobs storage.StoreTestingKnobs
	// Set up a filter to so that the first CPut operation will
	// get a ReadWithinUncertaintyIntervalError.
	targetKey := roachpb.Key("b")
	var numGets int32
	storeKnobs.TestingCommandFilter =
		func(fArgs storagebase.FilterArgs) *roachpb.Error {
			_, ok := fArgs.Req.(*roachpb.ConditionalPutRequest)
			if ok && fArgs.Req.Header().Key.Equal(targetKey) {
				if atomic.AddInt32(&numGets, 1) == 1 {
					z := hlc.ZeroTimestamp
					pErr := roachpb.NewReadWithinUncertaintyIntervalError(z, z)
					return roachpb.NewErrorWithTxn(pErr, fArgs.Hdr.Txn)
				}
			}
			return nil
		}
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &storeKnobs}})
	defer s.Stopper().Stop()

	db := setupMultipleRanges(t, s, "b")

	// Set the initial value on the target key "b".
	origVal := "val"
	if err := db.Put(targetKey, origVal); err != nil {
		t.Fatal(err)
	}

	// The following txn creates a batch request that is split
	// into two requests: Put and CPut. The CPut operation will
	// get a ReadWithinUncertaintyIntervalError and the txn will be
	// retried.
	epoch := 0
	if err := db.Txn(func(txn *client.Txn) error {
		epoch++
		if epoch >= 2 {
			// Writing must be true since we ran the BeginTransaction command.
			if !txn.Proto.Writing {
				t.Errorf("unexpected non-writing txn")
			}
		} else {
			// Writing must be false since we haven't run any write command.
			if txn.Proto.Writing {
				t.Errorf("unexpected writing txn")
			}
		}

		b := txn.NewBatch()
		b.Put("a", "val")
		b.CPut(targetKey, "new_val", origVal)
		err := txn.CommitInBatch(b)
		if epoch == 1 {
			if retErr, ok := err.(*roachpb.RetryableTxnError); ok {
				if _, ok := retErr.Cause.(*roachpb.ReadWithinUncertaintyIntervalError); ok {
					if !retErr.Transaction.Writing {
						t.Errorf("unexpected non-writing txn on error")
					}
				} else {
					t.Errorf("expected ReadWithinUncertaintyIntervalError, but got: %s", retErr.Cause)
				}
			} else {
				t.Errorf("expected a retryable error, but got: %s", err)
			}
		}
		return err
	}); err != nil {
		t.Errorf("unexpected error on transactional Puts: %s", err)
	}

	if epoch != 2 {
		t.Errorf("unexpected epoch; the txn must be retried exactly once, but got %d", epoch)
	}
}

func assertTransactionPushErrorWithTxnIDSet(t *testing.T, e error) *uuid.UUID {
	if retErr, ok := e.(*roachpb.RetryableTxnError); ok {
		if _, ok := retErr.Cause.(*roachpb.TransactionPushError); ok {
			if retErr.TxnID == nil {
				t.Fatalf("txn ID is not set unexpectedly: %s", retErr)
			}
			return retErr.TxnID
		}
		t.Fatalf("expected a TransactionPushError, but got %s (%T)", retErr.Cause, retErr.Cause)
	} else {
		t.Fatalf("expected a retryable error, but got %s (%T)", e, e)
	}
	panic("not reached")
}

// TestPropagateTxnOnPushError is similar to TestPropagateTxnOnError,
// but verifies that txn data are propagated to the next iteration on
// TransactionPushError.
func TestPropagateTxnOnPushError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	db := setupMultipleRanges(t, s, "b")

	waitForWriteIntent := make(chan struct{})
	waitForTxnRestart := make(chan struct{})
	waitForTxnCommit := make(chan struct{})
	lowPriority := int32(1)
	highPriority := int32(10)
	key := "a"
	// Create a goroutine that creates a write intent and waits until
	// another txn created in this test is restarted.
	go func() {
		if err := db.Txn(func(txn *client.Txn) error {
			// Set high priority so that the intent will not be pushed.
			txn.InternalSetPriority(highPriority)
			log.Infof(context.TODO(), "Creating a write intent with high priority")
			if err := txn.Put(key, "val"); err != nil {
				return err
			}
			close(waitForWriteIntent)
			// Wait until another txn in this test is
			// restarted by a push txn error.
			log.Infof(context.TODO(), "Waiting for the txn restart")
			<-waitForTxnRestart
			return txn.CommitInBatch(txn.NewBatch())
		}); err != nil {
			t.Errorf("unexpected error on transactional Puts: %s", err)
		}
		close(waitForTxnCommit)
	}()

	// Wait until a write intent is created by the above goroutine.
	log.Infof(context.TODO(), "Waiting for the write intent creation")
	<-waitForWriteIntent

	// The transaction below is restarted exactly once. The restart is
	// caused by the write intent created on key "a" by the above goroutine.
	// When the txn is retried, the error propagates the txn ID to the next
	// iteration.
	epoch := 0
	var txnID *uuid.UUID
	if err := db.Txn(func(txn *client.Txn) error {
		// Set low priority so that a write from this txn will not push others.
		txn.InternalSetPriority(lowPriority)

		epoch++

		if epoch == 2 {
			close(waitForTxnRestart)
			// Wait until the txn created by the goroutine is committed.
			log.Infof(context.TODO(), "Waiting for the txn commit")
			<-waitForTxnCommit
			if !roachpb.TxnIDEqual(txn.Proto.ID, txnID) {
				t.Errorf("txn ID is not propagated; got %s", txn.Proto.ID)
			}
		}

		// The commit returns an error, and it will pass
		// the txn data to the next iteration.
		err := txn.Put(key, "val")
		if epoch == 1 {
			txnID = assertTransactionPushErrorWithTxnIDSet(t, err)
		}
		return err
	}); err != nil {
		t.Errorf("unexpected error on transactional Puts: %s", err)
	}

	if e := 2; epoch != e {
		t.Errorf("unexpected epoch; the txn must be attempted %d times, but got %d attempts", e, epoch)
		if epoch == 1 {
			// Wait for the completion of the goroutine to see if it successfully commits the txn.
			close(waitForTxnRestart)
			log.Infof(context.TODO(), "Waiting for the txn commit")
			<-waitForTxnCommit
		}
	}
}

// TestRequestToUninitializedRange tests the behavior when a request
// is sent to a node which should be a replica of the correct range
// but has not yet received its initial snapshot. This would
// previously panic due to a malformed error response from the server,
// as seen in https://github.com/cockroachdb/cockroach/issues/6027.
//
// Prior to the other changes in the commit that introduced it, this
// test would reliable trigger the panic from #6027. However, it
// relies on some hacky tricks to both trigger the panic and shut down
// cleanly. If this test needs a lot of maintenance in the future we
// should be willing to get rid of it.
func TestRequestToUninitializedRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{StoresPerNode: 2})
	defer srv.Stopper().Stop()
	s := srv.(*server.TestServer)

	// Choose a range ID that is much larger than any that would be
	// created by initial splits.
	const rangeID = roachpb.RangeID(1000)

	// Set up a range with replicas on two stores of the same node. This
	// ensures that the DistSender will consider both replicas healthy
	// and will try to talk to both (so we can get a non-retryable error
	// from the second store).
	replica1 := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	replica2 := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   2,
		ReplicaID: 2,
	}

	// HACK: remove the second store from the node to generate a
	// non-retryable error when we try to talk to it.
	store2, err := s.Stores().GetStore(2)
	if err != nil {
		t.Fatal(err)
	}
	s.Stores().RemoveStore(store2)

	// Create the uninitialized range by sending an isolated raft
	// message to the first store.
	conn, err := s.RPCContext().GRPCDial(s.ServingAddr())
	if err != nil {
		t.Fatal(err)
	}
	raftClient := storage.NewMultiRaftClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := raftClient.RaftMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := storage.RaftMessageRequest{
		RangeID:     rangeID,
		ToReplica:   replica1,
		FromReplica: replica2,
		Message: raftpb.Message{
			Type: raftpb.MsgApp,
			To:   1,
		},
	}
	if err := stream.Send(&msg); err != nil {
		t.Fatal(err)
	}

	// Make sure the replica was created.
	store1, err := s.Stores().GetStore(1)
	if err != nil {
		t.Fatal(err)
	}
	util.SucceedsSoon(t, func() error {
		if replica, err := store1.GetReplica(rangeID); err != nil {
			return errors.Errorf("failed to look up replica: %s", err)
		} else if replica.IsInitialized() {
			return errors.Errorf("expected replica to be uninitialized")
		}
		return nil
	})

	// Create our own DistSender so we can force some requests to the
	// bogus range. The DistSender needs to be in scope for its own
	// MockRangeDescriptorDB closure.
	var sender *kv.DistSender
	sender = kv.NewDistSender(&kv.DistSenderContext{
		Clock:      s.Clock(),
		RPCContext: s.RPCContext(),
		RangeDescriptorDB: kv.MockRangeDescriptorDB(
			func(key roachpb.RKey, considerIntents, useReverseScan bool,
			) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
				if key.Equal(roachpb.RKeyMin) {
					// Pass through requests for the first range to the real sender.
					desc, err := sender.FirstRange()
					if err != nil {
						return nil, nil, roachpb.NewError(err)
					}
					return []roachpb.RangeDescriptor{*desc}, nil, nil
				}
				return []roachpb.RangeDescriptor{{
					RangeID:  rangeID,
					StartKey: roachpb.RKey(keys.Meta2Prefix),
					EndKey:   roachpb.RKeyMax,
					Replicas: []roachpb.ReplicaDescriptor{replica1, replica2},
				}}, nil, nil
			}),
	}, s.Gossip())
	// Only inconsistent reads triggered the panic in #6027.
	hdr := roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}
	req := roachpb.NewGet(roachpb.Key("asdf"))
	// Repeat the test a few times: due to the randomization between the
	// two replicas, each attempt only had a 50% chance of triggering
	// the panic.
	for i := 0; i < 5; i++ {
		_, pErr := client.SendWrappedWith(sender, context.Background(), hdr, req)
		// Each attempt fails with "store 2 not found" because that is the
		// non-retryable error.
		if !testutils.IsPError(pErr, "store 2 not found") {
			t.Fatal(pErr)
		}
	}
}
