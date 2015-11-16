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

package storage

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var (
	batchR = roachpb.BatchResponse{}
)

func init() {
	incR := roachpb.IncrementResponse{
		NewValue: 1,
	}
	batchR.Add(&incR)
}

// createTestSequenceCache creates an in-memory engine and
// returns a sequence cache using the supplied Range ID.
func createTestSequenceCache(t *testing.T, rangeID roachpb.RangeID, stopper *stop.Stopper) (*SequenceCache, engine.Engine) {
	return NewSequenceCache(rangeID), engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
}

const testTxnEpo = 5

var testTxnID = uuid.UUID([]byte("0ce61c17-5eb4-4587-8c36-dcf4062ada4c"))
var testTxnKey = []byte("a")
var testTxnTimestamp = roachpb.ZeroTimestamp.Add(123, 456)
var testEntry = roachpb.SequenceCacheEntry{Key: testTxnKey, Timestamp: testTxnTimestamp}

func TestSequenceCacheEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)
	const rangeID = 123
	const expSeq = 987
	key := keys.SequenceCacheKey(rangeID, testTxnID, testTxnEpo, expSeq)
	id, epoch, seq, err := decodeSequenceCacheKey(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(id, testTxnID) {
		t.Fatalf("expected id %q, got %q", testTxnID, id)
	}
	if epoch != testTxnEpo {
		t.Fatalf("expected epoch %d, got %d", testTxnEpo, epoch)
	}
	if seq != expSeq {
		t.Fatalf("expected sequence %d, got %d", expSeq, seq)
	}
}

// TestSequenceCachePutGetClearData tests basic get & put functionality as well as
// clearing the cache.
func TestSequenceCachePutGetClearData(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sc, e := createTestSequenceCache(t, 1, stopper)
	// Start with a get for an unseen id/sequence combo.
	if seq, _, readErr := sc.Get(e, testTxnID, nil); seq > 0 {
		t.Errorf("expected no response for id %s", testTxnID)
	} else if readErr != nil {
		t.Fatalf("unxpected read error: %s", readErr)
	}
	// Cache the test response.
	const seq = 123
	if err := sc.PutSequence(e, testTxnID, testTxnEpo, seq, testTxnKey, testTxnTimestamp, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	tryHit := func(expSeq, expEpo uint32) {
		var entry roachpb.SequenceCacheEntry
		if actEpo, actSeq, readErr := sc.Get(e, testTxnID, &entry); readErr != nil {
			t.Errorf("unexpected failure getting response: %s", readErr)
		} else if (expSeq > 0 || actSeq > 0) && expSeq != actSeq {
			t.Errorf("wanted hit: %t, got actual %d vs expected %d", expSeq > 0, actSeq, expSeq)
		} else if expSeq > 0 {
			if !reflect.DeepEqual(testEntry, entry) {
				t.Fatalf("wanted %v, got %v", testEntry, entry)
			}
			if expEpo != actEpo {
				t.Fatalf("expected epoch %d, got %d", expEpo, actEpo)
			}
		}
	}

	tryHit(seq, testTxnEpo)
	if err := sc.ClearData(e); err != nil {
		t.Error(err)
	}
	tryHit(0, 0)

	if err := sc.PutSequence(e, testTxnID, testTxnEpo, 2*seq, testTxnKey, testTxnTimestamp, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	tryHit(2*seq, testTxnEpo)

	if err := sc.PutSequence(e, testTxnID, 2*testTxnEpo, 2*seq, testTxnKey, testTxnTimestamp, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	tryHit(2*seq, 2*testTxnEpo)

	if err := sc.PutSequence(e, testTxnID, testTxnEpo-1, 2*seq, testTxnKey, testTxnTimestamp, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	tryHit(2*seq, 2*testTxnEpo)
}

// TestSequenceCacheEmptyParams tests operation with empty parameters.
func TestSequenceCacheEmptyParams(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sc, e := createTestSequenceCache(t, 1, stopper)
	// Put value for test response.
	if err := sc.PutSequence(e, testTxnID, testTxnEpo, 0, testTxnKey, testTxnTimestamp, nil); err != errEmptyID {
		t.Errorf("unexpected error putting response: %v", err)
	}
	if err := sc.PutSequence(e, nil, testTxnEpo, 10, testTxnKey, testTxnTimestamp, nil); err != errEmptyID {
		t.Errorf("unexpected error putting response: %v", err)
	}
	if _, _, readErr := sc.Get(e, nil, nil); readErr != errEmptyID {
		t.Fatalf("unxpected read error: %v", readErr)
	}
}

// TestSequenceCacheCopyInto tests that entries in one cache get
// transferred correctly to another cache using CopyInto().
func TestSequenceCacheCopyInto(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc1, e := createTestSequenceCache(t, 1, stopper)
	rc2, _ := createTestSequenceCache(t, 2, stopper)
	const seq = 123
	// Store an increment with new value one in the first cache.
	if err := rc1.PutSequence(e, testTxnID, testTxnEpo, seq, testTxnKey, testTxnTimestamp, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Copy the first cache into the second.
	if err := rc1.CopyInto(e, rc2.rangeID); err != nil {
		t.Errorf("unexpected error while copying sequence cache: %s", err)
	}
	for _, cache := range []*SequenceCache{rc1, rc2} {
		var entry roachpb.SequenceCacheEntry
		// Get should return 1 for both caches.
		if _, actSeq, readErr := cache.Get(e, testTxnID, &entry); readErr != nil {
			t.Errorf("unexpected failure getting response from source: %s", readErr)
		} else if actSeq != seq {
			t.Fatalf("unxpected cache miss")
		} else if !reflect.DeepEqual(testEntry, entry) {
			t.Fatalf("wanted %v, got %v", testEntry, entry)
		}
	}
}

// TestSequenceCacheCopyFrom tests that entries in one cache get
// transferred correctly to another cache using CopyFrom().
func TestSequenceCacheCopyFrom(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc1, e := createTestSequenceCache(t, 1, stopper)
	rc2, _ := createTestSequenceCache(t, 2, stopper)
	const seq = 321
	// Store an increment with new value one in the first cache.
	if err := rc1.PutSequence(e, testTxnID, testTxnEpo, seq, testTxnKey, testTxnTimestamp, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	// Copy the first cache into the second.
	if err := rc2.CopyFrom(e, rc1.rangeID); err != nil {
		t.Errorf("unexpected error while copying sequence cache: %s", err)
	}

	// Get should hit both caches.
	for i, cache := range []*SequenceCache{rc1, rc2} {
		var entry roachpb.SequenceCacheEntry
		if _, actSeq, readErr := cache.Get(e, testTxnID, &entry); readErr != nil {
			t.Fatalf("%d: unxpected read error: %s", i, readErr)
		} else if actSeq != seq {
			t.Errorf("%d: unexpected cache miss: wanted %d, got %d", i, seq, actSeq)
		} else if !reflect.DeepEqual(entry, testEntry) {
			t.Fatalf("expected %v, got %v", testEntry, entry)
		}
	}
}

// TestSequenceCacheShouldCache verifies conditions for caching responses.
func TestSequenceCacheShouldCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sc, _ := createTestSequenceCache(t, 1, stopper)

	testCases := []struct {
		err         error
		shouldCache bool
	}{
		{nil, true},
		{&roachpb.ReadWithinUncertaintyIntervalError{}, true},
		{&roachpb.TransactionAbortedError{}, true},
		{&roachpb.TransactionPushError{}, true},
		{&roachpb.TransactionRetryError{}, true},
		{&roachpb.RangeNotFoundError{}, true},
		{&roachpb.RangeKeyMismatchError{}, true},
		{&roachpb.TransactionStatusError{}, true},
		{&roachpb.ConditionFailedError{}, true},
		{&roachpb.WriteIntentError{}, false},
		{&roachpb.WriteTooOldError{}, false},
		{&roachpb.NotLeaderError{}, false},
	}

	reply := roachpb.PutResponse{}

	for i, test := range testCases {
		br := &roachpb.BatchResponse{}
		br.Add(&reply)
		if shouldCache := sc.shouldCacheError(test.err); shouldCache != test.shouldCache {
			t.Errorf("%d: expected cache? %t; got %t", i, test.shouldCache, shouldCache)
		}
	}
}
