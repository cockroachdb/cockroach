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

// createTestResponseCache creates an in-memory engine and
// returns a response cache using the supplied Range ID.
func createTestResponseCache(t *testing.T, rangeID roachpb.RangeID, stopper *stop.Stopper) (*ResponseCache, engine.Engine) {
	return NewResponseCache(rangeID), engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
}

func makeCmdID(wallTime, random int64) roachpb.ClientCmdID {
	return roachpb.ClientCmdID{
		WallTime: wallTime,
		Random:   random,
	}
}

// TestResponseCachePutGetClearData tests basic get & put functionality as well as
// clearing the cache.
func TestResponseCachePutGetClearData(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, e := createTestResponseCache(t, 1, stopper)
	cmdID := makeCmdID(1, 1)
	// Start with a get for an unseen cmdID.
	if isHit, readErr := rc.GetResponse(e, cmdID); isHit {
		t.Errorf("expected no response for id %+v", cmdID)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
	// Cache the test response.
	if err := rc.PutResponse(e, cmdID, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	tryHit := func(shouldHit bool) {
		if isHit, readErr := rc.GetResponse(e, cmdID); readErr != nil {
			t.Errorf("unexpected failure getting response: %s", readErr)
		} else if isHit != shouldHit {
			t.Errorf("wanted hit: %t, got hit: %t", shouldHit, isHit)
		}
	}

	tryHit(true)
	if err := rc.ClearData(e); err != nil {
		t.Error(err)
	}
	tryHit(false)
}

// TestResponseCacheEmptyCmdID tests operation with empty client
// command id.
func TestResponseCacheEmptyCmdID(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, e := createTestResponseCache(t, 1, stopper)
	cmdID := roachpb.ClientCmdID{}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, nil); err != errEmptyCmdID {
		t.Errorf("unexpected error putting response: %v", err)
	}
	if _, readErr := rc.GetResponse(e, cmdID); readErr != errEmptyCmdID {
		t.Fatalf("unxpected read error: %v", readErr)
	}
}

// TestResponseCacheCopyInto tests that responses cached in one cache get
// transferred correctly to another cache using CopyInto().
func TestResponseCacheCopyInto(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc1, e := createTestResponseCache(t, 1, stopper)
	rc2, _ := createTestResponseCache(t, 2, stopper)
	cmdID := makeCmdID(1, 1)
	// Store an increment with new value one in the first cache.
	if err := rc1.PutResponse(e, cmdID, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Copy the first cache into the second.
	if err := rc1.CopyInto(e, rc2.rangeID); err != nil {
		t.Errorf("unexpected error while copying response cache: %s", err)
	}
	for _, cache := range []*ResponseCache{rc1, rc2} {
		// Get should return 1 for both caches.
		if isHit, readErr := cache.GetResponse(e, cmdID); readErr != nil {
			t.Errorf("unexpected failure getting response from source: %s", readErr)
		} else if !isHit {
			t.Fatalf("unxpected cache miss")
		}
	}
}

// TestResponseCacheCopyFrom tests that responses cached in one cache get
// transferred correctly to another cache using CopyFrom().
func TestResponseCacheCopyFrom(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc1, e := createTestResponseCache(t, 1, stopper)
	rc2, _ := createTestResponseCache(t, 2, stopper)
	cmdID := makeCmdID(1, 1)
	// Store an increment with new value one in the first cache.
	if err := rc1.PutResponse(e, cmdID, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	// Copy the first cache into the second.
	if err := rc2.CopyFrom(e, rc1.rangeID); err != nil {
		t.Errorf("unexpected error while copying response cache: %s", err)
	}

	// Get should hit both caches.
	for _, cache := range []*ResponseCache{rc1, rc2} {
		if isHit, readErr := cache.GetResponse(e, cmdID); readErr != nil {
			t.Fatalf("unxpected read error :%s", readErr)
		} else if !isHit {
			t.Errorf("unexpected cache miss")
		}
	}
}

// TestResponseCacheShouldCache verifies conditions for caching responses.
func TestResponseCacheShouldCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, _ := createTestResponseCache(t, 1, stopper)

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
