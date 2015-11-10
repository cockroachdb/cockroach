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
	"time"

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
	if replyWithErr, readErr := rc.GetResponse(e, cmdID); replyWithErr.Reply != nil || replyWithErr.Err != nil {
		t.Errorf("expected no response for id %+v; got %+v, %s", cmdID, replyWithErr.Reply, replyWithErr.Err)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, roachpb.ResponseWithError{Reply: &batchR, Err: nil}); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Get should now return 1.
	replyWithErr, readErr := rc.GetResponse(e, cmdID)
	incReply := replyWithErr.Reply.Responses[0].GetInner().(*roachpb.IncrementResponse)
	if readErr != nil || replyWithErr.Reply == nil {
		t.Errorf("unexpected failure getting response: %s", readErr)
	} else if incReply.NewValue != 1 || replyWithErr.Err != nil {
		t.Errorf("unexpected response: %s, %s", incReply, replyWithErr.Err)
	}
	if err := rc.ClearData(e); err != nil {
		t.Error(err)
	}
	// The previously present response should be gone.
	if replyWithErr, readErr := rc.GetResponse(e, cmdID); replyWithErr.Reply != nil || replyWithErr.Err != nil {
		t.Errorf("unexpected success getting response: %s, %+v", replyWithErr.Err, replyWithErr.Reply)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
}

// TestResponseCacheEmptyCmdID tests operation with empty client
// command id. All calls should be noops.
func TestResponseCacheEmptyCmdID(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, e := createTestResponseCache(t, 1, stopper)
	cmdID := roachpb.ClientCmdID{}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, roachpb.ResponseWithError{Reply: &batchR, Err: nil}); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Get should return !ok.
	if replyWithErr, readErr := rc.GetResponse(e, cmdID); replyWithErr.Reply != nil || replyWithErr.Err != nil {
		t.Errorf("unexpected success getting response: %s, %+v", replyWithErr.Err, replyWithErr.Reply)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
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
	if err := rc1.PutResponse(e, cmdID, roachpb.ResponseWithError{Reply: &batchR, Err: nil}); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Copy the first cache into the second.
	if err := rc1.CopyInto(e, rc2.rangeID); err != nil {
		t.Errorf("unexpected error while copying response cache: %s", err)
	}
	// Get should return 1 for both caches.
	replyWithErr, readErr := rc1.GetResponse(e, cmdID)
	incReply := replyWithErr.Reply.Responses[0].GetInner().(*roachpb.IncrementResponse)
	if replyWithErr.Reply == nil && replyWithErr.Err == nil || incReply.NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %s, %+v", replyWithErr.Err, incReply)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
	replyWithErr, readErr = rc2.GetResponse(e, cmdID)
	incReply = replyWithErr.Reply.Responses[0].GetInner().(*roachpb.IncrementResponse)
	if replyWithErr.Reply == nil && replyWithErr.Err == nil || incReply.NewValue != 1 {
		t.Errorf("unexpected failure getting response from destination: %s, %+v", replyWithErr.Err, incReply)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
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
	if err := rc1.PutResponse(e, cmdID, roachpb.ResponseWithError{Reply: &batchR, Err: nil}); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	// Copy the first cache into the second.
	if err := rc2.CopyFrom(e, rc1.rangeID); err != nil {
		t.Errorf("unexpected error while copying response cache: %s", err)
	}

	// Get should return 1 for both caches.
	replyWithErr, readErr := rc1.GetResponse(e, cmdID)
	incReply := replyWithErr.Reply.Responses[0].GetInner().(*roachpb.IncrementResponse)
	if replyWithErr.Reply == nil && replyWithErr.Err == nil || incReply.NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %s, %+v", replyWithErr.Err, incReply)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
	replyWithErr, readErr = rc2.GetResponse(e, cmdID)
	incReply = replyWithErr.Reply.Responses[0].GetInner().(*roachpb.IncrementResponse)
	if replyWithErr.Reply == nil && replyWithErr.Err == nil || incReply.NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %s, %+v", replyWithErr.Err, incReply)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
}

// TestResponseCacheInflight verifies concurrent GetResponse
// invocations do not block on same keys.
func TestResponseCacheInflight(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc, e := createTestResponseCache(t, 1, stopper)
	cmdID := makeCmdID(1, 1)
	// Add inflight for cmdID.
	if replyWithErr, readErr := rc.GetResponse(e, cmdID); replyWithErr.Reply != nil || replyWithErr.Err != nil {
		t.Errorf("unexpected response or error: %s", replyWithErr.Err)
	} else if readErr != nil {
		t.Fatalf("unxpected read error :%s", readErr)
	}
	// Make two requests for response from cmdID, which is inflight.
	doneChans := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for _, done := range doneChans {
		doneChan := done
		go func() {
			if replyWithErr, readErr := rc.GetResponse(e, cmdID); replyWithErr.Reply != nil || replyWithErr.Err != nil {
				t.Errorf("unexpectedly found value for response cache entry %+v: %s", cmdID, replyWithErr.Err)
			} else if readErr != nil {
				t.Fatalf("unxpected read error :%s", readErr)
			}
			close(doneChan)
		}()
	}
	for _, doneChan := range doneChans {
		select {
		case <-doneChan:
			break
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("concurrent gets to cache did not complete")
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
		if shouldCache := rc.shouldCacheResponse(roachpb.ResponseWithError{Reply: br, Err: test.err}); shouldCache != test.shouldCache {
			t.Errorf("%d: expected cache? %t; got %t", i, test.shouldCache, shouldCache)
		}
	}
}
