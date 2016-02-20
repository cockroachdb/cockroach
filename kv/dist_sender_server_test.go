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
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
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
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := testutils.MakeKey(keys.Meta1Prefix, roachpb.KeyMax)
	now := s.Clock().Now()
	txn := roachpb.NewTransaction("txn", roachpb.Key("foobar"), 0, roachpb.SERIALIZABLE, now, 0)
	if err := engine.MVCCPutProto(s.Ctx.Engines[0], nil, key, now, txn, &roachpb.RangeDescriptor{}); err != nil {
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
func setupMultipleRanges(t *testing.T, ts *server.TestServer, splitAt ...string) *client.DB {
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

func TestMultiRangeBatchBounded(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := setupMultipleRanges(t, s, "a", "b", "c", "d", "e", "f")
	for _, key := range []string{"a", "aa", "aaa", "b", "bb", "cc", "d", "dd", "ff"} {
		if err := db.Put(key, "value"); err != nil {
			t.Fatal(err)
		}
	}

	expResults := [][]string{
		{"aaa", "b", "bb"},
		{"a", "aa"},
		{"cc", "d", "dd"},
	}

	b := db.NewBatch()
	b.Scan("aaa", "dd", 3)
	b.Scan("a", "z", 2)
	b.Scan("cc", "ff", 3)
	if err := db.Run(b); err != nil {
		t.Fatal(err)
	}

	if len(expResults) != len(b.Results) {
		t.Fatalf("only got %d results, wanted %d", len(expResults), len(b.Results))
	}
	for i, res := range b.Results {
		expRes := expResults[i]
		var actRes []string
		for _, k := range res.Rows {
			actRes = append(actRes, string(k.Key))
		}
		if !reflect.DeepEqual(actRes, expRes) {
			t.Errorf("%d: got %v, wanted %v", i, actRes, expRes)
		}
	}
}

// TestMultiRangeEmptyAfterTruncate exercises a code path in which a
// multi-range request deals with a range without any active requests after
// truncation. In that case, the request is skipped.
func TestMultiRangeEmptyAfterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := setupMultipleRanges(t, s, "c", "d")

	// Delete the keys within a transaction. The range [c,d) doesn't have
	// any active requests.
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		b := txn.NewBatch()
		b.DelRange("a", "b", false)
		b.DelRange("e", "f", false)
		return txn.CommitInBatch(b)
	}); pErr != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", pErr)
	}
}

// TestMultiRangeScanReverseScanDeleteResolve verifies that Scan, ReverseScan,
// DeleteRange and ResolveIntentRange work across ranges.
func TestMultiRangeScanReverseScanDeleteResolve(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := setupMultipleRanges(t, s, "b")

	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c"} {
		if pErr := db.Put(key, "value"); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// Scan to retrieve the keys just written.
	if rows, pErr := db.Scan("a", "q", 0); pErr != nil {
		t.Fatalf("unexpected pError on Scan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Scan in reverse order to retrieve the keys just written.
	if rows, pErr := db.ReverseScan("a", "q", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Delete the keys within a transaction. Implicitly, the intents are
	// resolved via ResolveIntentRange upon completion.
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		b := txn.NewBatch()
		b.DelRange("a", "d", false)
		return txn.CommitInBatch(b)
	}); pErr != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", pErr)
	}

	// Scan consistently to make sure the intents are gone.
	if rows, pErr := db.Scan("a", "q", 0); pErr != nil {
		t.Fatalf("unexpected error on Scan: %s", pErr)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}

	// ReverseScan consistently to make sure the intents are gone.
	if rows, pErr := db.ReverseScan("a", "q", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 0 {
		t.Errorf("expected 0 rows; got %d", l)
	}
}

// TestMultiRangeScanReverseScanInconsistent verifies that a Scan/ReverseScan
// across ranges that doesn't require read consistency will set a timestamp
// using the clock local to the distributed sender.
func TestMultiRangeScanReverseScanInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)

	s := server.StartTestServer(t)
	defer s.Stop()
	db := setupMultipleRanges(t, s, "b")

	// Write keys "a" and "b", the latter of which is the first key in the
	// second range.
	keys := [2]string{"a", "b"}
	ts := [2]time.Time{}
	for i, key := range keys {
		b := &client.Batch{}
		b.Put(key, "value")
		if pErr := db.Run(b); pErr != nil {
			t.Fatal(pErr)
		}
		ts[i] = b.Results[0].Rows[0].Timestamp()
		log.Infof("%d: %d.%d", i, ts[i].Unix(), ts[i].Nanosecond())
		if i == 0 {
			util.SucceedsWithin(t, time.Second, func() error {
				// Enforce that when we write the second key, it's written
				// with a strictly higher timestamp. We're dropping logical
				// ticks and the clock may just have been pushed into the
				// future, so that's necessary. See #3122.
				if !s.Clock().PhysicalTime().After(ts[0]) {
					return errors.New("time stands still")
				}
				return nil
			})
		}
	}

	// Do an inconsistent Scan/ReverseScan from a new DistSender and verify
	// it does the read at its local clock and doesn't receive an
	// OpRequiresTxnError. We set the local clock to the timestamp of
	// the first key to verify it's used to read only key "a".
	manual := hlc.NewManualClock(ts[1].UnixNano() - 1)
	clock := hlc.NewClock(manual.UnixNano)
	ds := kv.NewDistSender(&kv.DistSenderContext{Clock: clock, RPCContext: s.RPCContext()}, s.Gossip())

	// Scan.
	sa := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("c"), 0).(*roachpb.ScanRequest)
	reply, pErr := client.SendWrappedWith(ds, nil, roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, sa)
	if pErr != nil {
		t.Fatal(pErr)
	}
	sr := reply.(*roachpb.ScanResponse)

	if l := len(sr.Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
	if key := string(sr.Rows[0].Key); keys[0] != key {
		t.Errorf("expected key %q; got %q", keys[0], key)
	}

	// ReverseScan.
	rsa := roachpb.NewReverseScan(roachpb.Key("a"), roachpb.Key("c"), 0).(*roachpb.ReverseScanRequest)
	reply, pErr = client.SendWrappedWith(ds, nil, roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, rsa)
	if pErr != nil {
		t.Fatal(pErr)
	}
	rsr := reply.(*roachpb.ReverseScanResponse)
	if l := len(rsr.Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
	if key := string(rsr.Rows[0].Key); keys[0] != key {
		t.Errorf("expected key %q; got %q", keys[0], key)
	}
}

func initReverseScanTestEnv(s *server.TestServer, t *testing.T) *client.DB {
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Set up multiple ranges:
	// ["", "b"),["b", "e") ,["e", "g") and ["g", "\xff\xff").
	for _, key := range []string{"b", "e", "g"} {
		// Split the keyspace at the given key.
		if pErr := db.AdminSplit(key); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// Write keys before, at, and after the split key.
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		if pErr := db.Put(key, "value"); pErr != nil {
			t.Fatal(pErr)
		}
	}
	return db
}

// TestSingleRangeReverseScan verifies that ReverseScan gets the right results
// on a single range.
func TestSingleRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := initReverseScanTestEnv(s, t)

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, pErr := db.ReverseScan("b", "d", 0); pErr != nil {
		t.Fatalf("unexpected pError on ReverseScan: %s", pErr)
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
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
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
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := initReverseScanTestEnv(s, t)

	// Case 1: An encounter with a range split.
	// Split the range ["b", "e") at "c".
	if pErr := db.AdminSplit("c"); pErr != nil {
		t.Fatal(pErr)
	}

	// The ReverseScan will run into a stale descriptor.
	if rows, pErr := db.ReverseScan("a", "d", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}

	// Case 2: encounter with range merge .
	// Merge the range ["e", "g") and ["g", "\xff\xff") .
	if pErr := db.AdminMerge("e"); pErr != nil {
		t.Fatal(pErr)
	}
	if rows, pErr := db.ReverseScan("d", "g", 0); pErr != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", pErr)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

func TestBadRequest(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	db := createTestClient(t, s.Stopper(), s.ServingAddr())
	defer s.Stop()

	// Write key "a".
	if pErr := db.Put("a", "value"); pErr != nil {
		t.Fatal(pErr)
	}

	if _, pErr := db.Scan("a", "a", 0); !testutils.IsPError(pErr, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on scan with startkey == endkey: %v", pErr)
	}

	if _, pErr := db.ReverseScan("a", "a", 0); !testutils.IsPError(pErr, "truncation resulted in empty batch") {
		t.Fatalf("unexpected pError on reverse scan with startkey == endkey: %v", pErr)
	}

	if pErr := db.DelRange("x", "a"); !testutils.IsPError(pErr, "truncation resulted in empty batch") {
		t.Fatalf("unexpected error on deletion on [x, a): %v", pErr)
	}

	if pErr := db.DelRange("", "z"); !testutils.IsPError(pErr, "must be greater than LocalMax") {
		t.Fatalf("unexpected error on deletion on [KeyMin, z): %v", pErr)
	}
}

// TestNoSequenceCachePutOnRangeMismatchError verifies that the
// sequence cache is not updated with RangeKeyMismatchError. This is a
// higher-level version of TestSequenceCacheShouldCache.
func TestNoSequenceCachePutOnRangeMismatchError(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
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
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		epoch++
		b := txn.NewBatch()
		b.Put("a", "val")
		b.Put("b", "val")
		b.Put("c", "val")
		return txn.CommitInBatch(b)
	}); pErr != nil {
		t.Errorf("unexpected error on transactional Puts: %s", pErr)
	}

	if epoch != 1 {
		t.Errorf("unexpected epoch; the txn must not be retried, but got %d retries", epoch)
	}
}

// TestPropagateTxnOnError verifies that DistSender.sendChunk properly
// propagates the txn data to a next iteration. Use txn.Writing field to
// verify that.
func TestPropagateTxnOnError(t *testing.T) {
	defer leaktest.AfterTest(t)

	// Set up a filter to so that the first CPut operation will
	// get a ReadWithinUncertaintyIntervalError.
	targetKey := roachpb.Key("b")
	var numGets int32
	storage.TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, h roachpb.Header) error {
		if _, ok := args.(*roachpb.ConditionalPutRequest); ok && args.Header().Key.Equal(targetKey) {
			if atomic.AddInt32(&numGets, 1) == 1 {
				return &roachpb.ReadWithinUncertaintyIntervalError{
					Timestamp:         h.Timestamp,
					ExistingTimestamp: h.Timestamp,
				}

			}
		}
		return nil
	}
	defer func() {
		storage.TestingCommandFilter = nil
	}()

	s := server.StartTestServer(t)
	defer s.Stop()
	db := setupMultipleRanges(t, s, "b")

	// Set the initial value on the target key "b".
	origVal := "val"
	if pErr := db.Put(targetKey, origVal); pErr != nil {
		t.Fatal(pErr)
	}

	// The following txn creates a batch request that is split
	// into two requests: Put and CPut. The CPut operation will
	// get a ReadWithinUncertaintyIntervalError and the txn will be
	// retried.
	epoch := 0
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
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
		pErr := txn.CommitInBatch(b)
		if epoch == 1 {
			if _, ok := pErr.GetDetail().(*roachpb.ReadWithinUncertaintyIntervalError); ok {
				if !pErr.GetTxn().Writing {
					t.Errorf("unexpected non-writing txn on error")
				}
			} else {
				t.Errorf("expected ReadWithinUncertaintyIntervalError, but got: %s", pErr)
			}
		}
		return pErr
	}); pErr != nil {
		t.Errorf("unexpected error on transactional Puts: %s", pErr)
	}

	if epoch != 2 {
		t.Errorf("unexpected epoch; the txn must be retried exactly once, but got %d", epoch)
	}
}

// TestPropagateTxnOnPushError is similar to TestPropagateTxnOnError,
// but verifies that txn data are propagated to the next iteration on
// TransactionPushError.
func TestPropagateTxnOnPushError(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := setupMultipleRanges(t, s, "b")

	waitForWriteIntent := make(chan struct{})
	waitForTxnRestart := make(chan struct{})
	waitForTxnCommit := make(chan struct{})
	// Create a goroutine that creates a write intent and waits until
	// another txn created in this test is restarted.
	go func() {
		if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
			if pErr := txn.Put("b", "val"); pErr != nil {
				return pErr
			}
			close(waitForWriteIntent)
			// Wait until another txn in this test is
			// restarted by a push txn error.
			<-waitForTxnRestart
			return txn.CommitInBatch(txn.NewBatch())
		}); pErr != nil {
			t.Errorf("unexpected error on transactional Puts: %s", pErr)
		}
		close(waitForTxnCommit)
	}()

	// Wait until a write intent is created by the above goroutine.
	<-waitForWriteIntent

	// The transaction below is restarted multiple times.
	// - The first retry is caused by the write intent created on key "b" by the above goroutine.
	// - The subsequent retries are caused by the write conflict on key "a". Since the txn
	//   ID is not propagated, a txn of a new epoch always has a new txn ID different
	//   from the previous txn's. So, the write intent made by the txn of the previous epoch
	//   is treated as a write made by some different txn.
	epoch := 0
	var txnID *uuid.UUID
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		// Set low priority so that the intent will not be pushed.
		txn.InternalSetPriority(1)

		epoch++

		if epoch == 2 {
			close(waitForTxnRestart)
			// Wait until the txn created by the goroutine is committed.
			<-waitForTxnCommit
			if !roachpb.TxnIDEqual(txn.Proto.ID, txnID) {
				t.Errorf("txn ID is not propagated; got %s", txn.Proto.ID)
			}
		}

		b := txn.NewBatch()
		b.Put("a", "val")
		b.Put("b", "val")
		// The commit returns an error, but it will not be
		// passed to the next iteration. txnSender.Send() does
		// not update the txn data since
		// TransactionPushError.Transaction() returns nil.
		pErr := txn.CommitInBatch(b)
		if epoch == 1 {
			if tErr, ok := pErr.GetDetail().(*roachpb.TransactionPushError); ok {
				if pErr.GetTxn().ID == nil {
					t.Errorf("txn ID is not set unexpectedly: %s", tErr)
				}
				txnID = pErr.GetTxn().ID
			} else {
				t.Errorf("expected TransactionRetryError, but got: %s", pErr)
			}
		}
		return pErr
	}); pErr != nil {
		t.Errorf("unexpected error on transactional Puts: %s", pErr)
	}

	if e := 2; epoch != e {
		t.Errorf("unexpected epoch; the txn must be attempted %d times, but got %d attempts", e, epoch)
	}
}
