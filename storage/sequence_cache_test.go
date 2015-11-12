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
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
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

var family = []byte("testfamily")

// TestSequenceCachePutGetClearData tests basic get & put functionality as well as
// clearing the cache.
func TestSequenceCachePutGetClearData(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, e := createTestSequenceCache(t, 1, stopper)
	// Start with a get for an unseen id/sequence combo.
	if seq, readErr := rc.GetSequence(e, family); seq > 0 {
		t.Errorf("expected no response for family %s", family)
	} else if readErr != nil {
		t.Fatalf("unxpected read error: %s", readErr)
	}
	// Cache the test response.
	const seq = 123
	if err := rc.PutSequence(e, family, seq, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	tryHit := func(shouldHit bool) {
		if actSeq, readErr := rc.GetSequence(e, family); readErr != nil {
			t.Errorf("unexpected failure getting response: %s", readErr)
		} else if shouldHit != (actSeq == seq) {
			t.Errorf("wanted hit: %t, got actual %d vs expected %d", shouldHit, actSeq, seq)
		}
	}

	tryHit(true)
	if err := rc.ClearData(e); err != nil {
		t.Error(err)
	}
	tryHit(false)
}

// TestSequenceCacheEmptyParams tests operation with empty parameters.
func TestSequenceCacheEmptyParams(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, e := createTestSequenceCache(t, 1, stopper)
	// Put value for test response.
	if err := rc.PutSequence(e, family, 0, nil); err != errEmptyID {
		t.Errorf("unexpected error putting response: %v", err)
	}
	if err := rc.PutSequence(e, nil, 10, nil); err != errEmptyID {
		t.Errorf("unexpected error putting response: %v", err)
	}
	if _, readErr := rc.GetSequence(e, nil); readErr != errEmptyID {
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
	if err := rc1.PutSequence(e, family, seq, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Copy the first cache into the second.
	if err := rc1.CopyInto(e, rc2.rangeID); err != nil {
		t.Errorf("unexpected error while copying sequence cache: %s", err)
	}
	for _, cache := range []*SequenceCache{rc1, rc2} {
		// Get should return 1 for both caches.
		if actSeq, readErr := cache.GetSequence(e, family); readErr != nil {
			t.Errorf("unexpected failure getting response from source: %s", readErr)
		} else if actSeq != seq {
			t.Fatalf("unxpected cache miss")
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
	if err := rc1.PutSequence(e, family, seq, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	// Copy the first cache into the second.
	if err := rc2.CopyFrom(e, rc1.rangeID); err != nil {
		t.Errorf("unexpected error while copying sequence cache: %s", err)
	}

	// Get should hit both caches.
	for _, cache := range []*SequenceCache{rc1, rc2} {
		if actSeq, readErr := cache.GetSequence(e, family); readErr != nil {
			t.Fatalf("unxpected read error: %s", readErr)
		} else if actSeq != seq {
			t.Errorf("unexpected cache miss")
		}
	}
}

// TestSequenceCacheShouldCache verifies conditions for caching responses.
func TestSequenceCacheShouldCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, _ := createTestSequenceCache(t, 1, stopper)

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
		if shouldCache := rc.shouldCacheError(test.err); shouldCache != test.shouldCache {
			t.Errorf("%d: expected cache? %t; got %t", i, test.shouldCache, shouldCache)
		}
	}
}
