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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var incR = proto.IncrementResponse{
	NewValue: 1,
}

// createTestResponseCache creates an in-memory engine and
// returns a response cache using the supplied Raft ID.
func createTestResponseCache(t *testing.T, raftID proto.RaftID) (*ResponseCache, engine.Engine) {
	return NewResponseCache(raftID), engine.NewInMem(proto.Attributes{}, 1<<20)
}

func makeCmdID(wallTime, random int64) proto.ClientCmdID {
	return proto.ClientCmdID{
		WallTime: wallTime,
		Random:   random,
	}
}

// TestResponseCachePutGetClearData tests basic get & put functionality as well as
// clearing the cache.
func TestResponseCachePutGetClearData(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc, e := createTestResponseCache(t, 1)
	cmdID := makeCmdID(1, 1)
	// Start with a get for an unseen cmdID.
	if val, err, corruptionErr := rc.GetResponse(e, cmdID); val != nil || err != nil {
		t.Errorf("expected no response for id %+v; got %+v, %s", cmdID, val, err)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, &incR, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Get should now return 1.
	if val, err, corruptionErr := rc.GetResponse(e, cmdID); corruptionErr != nil {
		t.Errorf("unexpected failure getting response: %s", corruptionErr)
	} else if val == nil || val.(*proto.IncrementResponse).NewValue != 1 || err != nil {
		t.Errorf("unexpected response: %s, %s", val, err)
	}
	if err := rc.ClearData(e); err != nil {
		t.Error(err)
	}
	// The previously present response should be gone.
	if val, err, corruptionErr := rc.GetResponse(e, cmdID); val != nil || err != nil {
		t.Errorf("unexpected success getting response: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
}

// TestResponseCacheEmptyCmdID tests operation with empty client
// command id. All calls should be noops.
func TestResponseCacheEmptyCmdID(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc, e := createTestResponseCache(t, 1)
	cmdID := proto.ClientCmdID{}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, &incR, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Get should return !ok.
	if val, err, corruptionErr := rc.GetResponse(e, cmdID); val != nil || err != nil {
		t.Errorf("unexpected success getting response: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
}

// TestResponseCacheCopyInto tests that responses cached in one cache get
// transferred correctly to another cache using CopyInto().
func TestResponseCacheCopyInto(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc1, e := createTestResponseCache(t, 1)
	rc2, _ := createTestResponseCache(t, 2)
	cmdID := makeCmdID(1, 1)
	// Store an increment with new value one in the first cache.
	if err := rc1.PutResponse(e, cmdID, &incR, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	// Copy the first cache into the second.
	if err := rc1.CopyInto(e, rc2.raftID); err != nil {
		t.Errorf("unexpected error while copying response cache: %s", err)
	}
	// Get should return 1 for both caches.
	if val, err, corruptionErr := rc1.GetResponse(e, cmdID); val == nil && err == nil || val.(*proto.IncrementResponse).NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
	if val, err, corruptionErr := rc2.GetResponse(e, cmdID); val == nil && err == nil || val.(*proto.IncrementResponse).NewValue != 1 {
		t.Errorf("unexpected failure getting response from destination: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
}

// TestResponseCacheCopyFrom tests that responses cached in one cache get
// transferred correctly to another cache using CopyFrom().
func TestResponseCacheCopyFrom(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc1, e := createTestResponseCache(t, 1)
	rc2, _ := createTestResponseCache(t, 2)
	cmdID := makeCmdID(1, 1)
	// Store an increment with new value one in the first cache.
	if err := rc1.PutResponse(e, cmdID, &incR, nil); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	// Copy the first cache into the second.
	if err := rc2.CopyFrom(e, rc1.raftID); err != nil {
		t.Errorf("unexpected error while copying response cache: %s", err)
	}

	// Get should return 1 for both caches.
	if val, err, corruptionErr := rc1.GetResponse(e, cmdID); val == nil && err == nil || val.(*proto.IncrementResponse).NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
	if val, err, corruptionErr := rc2.GetResponse(e, cmdID); val == nil && err == nil || val.(*proto.IncrementResponse).NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
}

// TestResponseCacheInflight verifies concurrent GetResponse
// invocations do not block on same keys.
func TestResponseCacheInflight(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc, e := createTestResponseCache(t, 1)
	cmdID := makeCmdID(1, 1)
	// Add inflight for cmdID.
	if val, err, corruptionErr := rc.GetResponse(e, cmdID); val != nil || err != nil {
		t.Errorf("unexpected response or error: %s", err)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
	// Make two requests for response from cmdID, which is inflight.
	doneChans := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for _, done := range doneChans {
		doneChan := done
		go func() {
			if val, err, corruptionErr := rc.GetResponse(e, cmdID); val != nil || err != nil {
				t.Errorf("unexpectedly found value for response cache entry %+v: %s", cmdID, err)
			} else if corruptionErr != nil {
				t.Fatalf("unxpected corruption error :%s", corruptionErr)
			}
			close(doneChan)
		}()
	}
	for count := 0; count < 2; count++ {
		select {
		case <-doneChans[0]:
			count++
		case <-doneChans[1]:
			count++
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("concurrent gets to cache did not complete")
		}
	}
}

// TestResponseCacheShouldCache verifies conditions for caching responses.
func TestResponseCacheShouldCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc, _ := createTestResponseCache(t, 1)

	testCases := []struct {
		err         error
		shouldCache bool
	}{
		{nil, true},
		{&proto.ReadWithinUncertaintyIntervalError{}, true},
		{&proto.TransactionAbortedError{}, true},
		{&proto.TransactionPushError{}, true},
		{&proto.TransactionRetryError{}, true},
		{&proto.Error{}, true},
		{&proto.RangeNotFoundError{}, true},
		{&proto.RangeKeyMismatchError{}, true},
		{&proto.TransactionStatusError{}, true},
		{&proto.ConditionFailedError{}, true},
		{&proto.WriteIntentError{}, false},
		{&proto.WriteTooOldError{}, false},
		{&proto.NotLeaderError{}, false},
	}

	reply := proto.PutResponse{}

	for i, test := range testCases {
		if shouldCache := rc.shouldCacheResponse(&reply, test.err); shouldCache != test.shouldCache {
			t.Errorf("%d: expected cache? %t; got %t", i, test.shouldCache, shouldCache)
		}
	}
}

// TestResponseCacheGC verifies that response cache entries are
// garbage collected periodically.
func TestResponseCacheGC(t *testing.T) {
	defer leaktest.AfterTest(t)
	eng := engine.NewInMem(proto.Attributes{Attrs: []string{"ssd"}}, 1<<30)
	defer eng.Close()

	rc := NewResponseCache(1)
	cmdID := makeCmdID(1, 1)

	// Add response for cmdID with timestamp at time=1ns.
	copyIncR := incR
	copyIncR.Timestamp.WallTime = 1
	if err := rc.PutResponse(eng, cmdID, &copyIncR, nil); err != nil {
		t.Fatalf("unexpected error putting responpse: %s", err)
	}
	eng.SetGCTimeouts(0, 0) // avoids GC
	eng.CompactRange(nil, nil)
	if val, err, corruptionErr := rc.GetResponse(eng, cmdID); val == nil && err == nil || val.(*proto.IncrementResponse).NewValue != 1 {
		t.Fatalf("unexpected response or error: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}

	// Now set minRCacheTS to 1, which will GC.
	eng.SetGCTimeouts(0, 1)
	eng.CompactRange(nil, nil)
	if val, err, corruptionErr := rc.GetResponse(eng, cmdID); val != nil && err != nil {
		t.Fatalf("unexpected response or error: %s, %+v", err, val)
	} else if corruptionErr != nil {
		t.Fatalf("unxpected corruption error :%s", corruptionErr)
	}
}
