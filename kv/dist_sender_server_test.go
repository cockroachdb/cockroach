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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
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
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Create an intent on the meta1 record by writing directly to the
	// engine.
	key := keys.MakeKey(keys.Meta1Prefix, roachpb.KeyMax)
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
func setupMultipleRanges(t *testing.T, splitAt ...string) (*server.TestServer, *client.DB) {
	s := server.StartTestServer(t)
	db := createTestClient(t, s.Stopper(), s.ServingAddr())

	// Split the keyspace at the given keys.
	for _, key := range splitAt {
		if err := db.AdminSplit(key); err != nil {
			t.Fatal(err)
		}
	}

	return s, db
}

func TestMultiRangeBatchBounded(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setupMultipleRanges(t, "a", "b", "c", "d", "e", "f")
	defer s.Stop()
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
// multi-range requests deals with a range without any active requests after
// truncation. In that case, the request is skipped.
func TestMultiRangeEmptyAfterTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setupMultipleRanges(t, "c", "d")
	defer s.Stop()

	// Delete the keys within a transaction. Implicitly, the intents are
	// resolved via ResolveIntentRange upon completion.
	if err := db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		b.DelRange("a", "b")
		b.DelRange("e", "f")
		b.DelRange(keys.LocalMax, roachpb.KeyMax)
		return txn.CommitInBatch(b)
	}); err != nil {
		t.Fatalf("unexpected error on transactional DeleteRange: %s", err)
	}
}

// TestMultiRangeScanReverseScanDeleteResolve verifies that Scan, ReverseScan,
// DeleteRange and ResolveIntentRange work across ranges.
func TestMultiRangeScanReverseScanDeleteResolve(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setupMultipleRanges(t, "b")
	defer s.Stop()

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
		b.DelRange("a", "d")
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
	defer leaktest.AfterTest(t)
	s, db := setupMultipleRanges(t, "b")
	defer s.Stop()

	// Write keys "a" and "b", the latter of which is the first key in the
	// second range.
	keys := [2]string{"a", "b"}
	ts := [2]time.Time{}
	// This outer loop may seem awkward, but we've actually had issue #3122
	// since the two timestamps ended up equal. This can happen (very rarely)
	// since both timestamps are HLC internally and then have their logical
	// component dropped. Lease changes can push the HLC ahead of the wall
	// time for short amounts of time, so that losing the logical tick matters.
	for !ts[1].After(ts[0]) {
		for i, key := range keys {
			b := &client.Batch{}
			b.Put(key, "value")
			if err := db.Run(b); err != nil {
				t.Fatal(err)
			}
			ts[i] = b.Results[0].Rows[0].Timestamp()
			log.Infof("%d: %s", i, b.Results[0].Rows[0].Timestamp())
		}
	}

	// Do an inconsistent Scan/ReverseScan from a new DistSender and verify
	// it does the read at its local clock and doesn't receive an
	// OpRequiresTxnError. We set the local clock to the timestamp of
	// the first key to verify it's used to read only key "a".
	manual := hlc.NewManualClock(ts[1].UnixNano() - 1)
	clock := hlc.NewClock(manual.UnixNano)
	ds := kv.NewDistSender(&kv.DistSenderContext{Clock: clock}, s.Gossip())

	// Scan.
	sa := roachpb.NewScan(roachpb.Key("a"), roachpb.Key("c"), 0).(*roachpb.ScanRequest)
	reply, err := client.SendWrappedWith(ds, nil, roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, sa)
	if err != nil {
		t.Fatal(err)
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
	reply, err = client.SendWrappedWith(ds, nil, roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, rsa)
	if err != nil {
		t.Fatal(err)
	}
	rsr := reply.(*roachpb.ReverseScanResponse)
	if l := len(rsr.Rows); l != 1 {
		t.Fatalf("expected 1 row; got %d", l)
	}
	if key := string(rsr.Rows[0].Key); keys[0] != key {
		t.Errorf("expected key %q; got %q", keys[0], key)
	}
}

func initReverseScanTestEnv(t *testing.T) (*server.TestServer, *client.DB) {
	s := server.StartTestServer(t)
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
	return s, db
}

// TestSingleRangeReverseScan verifies that ReverseScan gets the right results
// on a single range.
func TestSingleRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := initReverseScanTestEnv(t)
	defer s.Stop()

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, err := db.ReverseScan("b", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	// Case 2: Request.EndKey is equal to the EndKey of the range.
	if rows, err := db.ReverseScan("e", "g", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 2 {
		t.Errorf("expected 2 rows; got %d", l)
	}
	// Case 3: Test roachpb.KeyMax
	// This span covers the system DB keys.
	wanted := 1 + len(sql.MakeMetadataSchema().GetInitialValues())
	if rows, err := db.ReverseScan("g", roachpb.KeyMax, 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != wanted {
		t.Errorf("expected %d rows; got %d", wanted, l)
	}
	// Case 4: Test keys.SystemMax
	if rows, err := db.ReverseScan(keys.SystemMax, "b", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 1 {
		t.Errorf("expected 1 row; got %d", l)
	}
}

// TestMultiRangeReverseScan verifies that ReverseScan gets the right results
// across multiple ranges.
func TestMultiRangeReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := initReverseScanTestEnv(t)
	defer s.Stop()

	// Case 1: Request.EndKey is in the middle of the range.
	if rows, err := db.ReverseScan("a", "d", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
	// Case 2: Request.EndKey is equal to the EndKey of the range.
	if rows, err := db.ReverseScan("d", "g", 0); err != nil {
		t.Fatalf("unexpected error on ReverseScan: %s", err)
	} else if l := len(rows); l != 3 {
		t.Errorf("expected 3 rows; got %d", l)
	}
}

// TestReverseScanWithSplitAndMerge verifies that ReverseScan gets the right results
// across multiple ranges while range splits and merges happen.
func TestReverseScanWithSplitAndMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := initReverseScanTestEnv(t)
	defer s.Stop()

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
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	db := createTestClient(t, s.Stopper(), s.ServingAddr())
	defer s.Stop()

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
	defer leaktest.AfterTest(t)
	s, db := setupMultipleRanges(t, "b", "c")
	defer s.Stop()

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

	s, db := setupMultipleRanges(t, "b")
	defer s.Stop()

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
			if tErr, ok := err.(*roachpb.ReadWithinUncertaintyIntervalError); ok {
				if !tErr.Txn.Writing {
					t.Errorf("unexpected non-writing txn on error")
				}
			} else {
				t.Errorf("expected ReadWithinUncertaintyIntervalError, but got: %s", err)
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

// TestPropagateTxnOnPushError is similar to TestPropagateTxnOnError,
// but verifies that txn data are propagated to the next iteration on
// TransactionPushError.
func TestPropagateTxnOnPushError(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, db := setupMultipleRanges(t, "b")
	defer s.Stop()

	waitForWriteIntent := make(chan struct{})
	waitForTxnRestart := make(chan struct{})
	waitForTxnCommit := make(chan struct{})
	// Create a goroutine that creates a write intent and waits until
	// another txn created in this test is restarted.
	go func() {
		if err := db.Txn(func(txn *client.Txn) error {
			if err := txn.Put("b", "val"); err != nil {
				return err
			}
			close(waitForWriteIntent)
			// Wait until another txn in this test is
			// restarted by a push txn error.
			<-waitForTxnRestart
			return txn.CommitInBatch(txn.NewBatch())
		}); err != nil {
			t.Errorf("unexpected error on transactional Puts: %s", err)
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
	var txnID []byte
	if err := db.Txn(func(txn *client.Txn) error {
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
		err := txn.CommitInBatch(b)
		if epoch == 1 {
			if tErr, ok := err.(*roachpb.TransactionPushError); ok {
				if len(tErr.Txn.ID) == 0 {
					t.Errorf("txn ID is not set unexpectedly: %s", tErr)
				}
				txnID = tErr.Txn.ID
			} else {
				t.Errorf("expected TransactionRetryError, but got: %s", err)
			}
		}
		return err
	}); err != nil {
		t.Errorf("unexpected error on transactional Puts: %s", err)
	}

	if e := 2; epoch != e {
		t.Errorf("unexpected epoch; the txn must be attempted %d times, but got %d attempts", e, epoch)
	}
}
