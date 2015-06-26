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
	val := proto.IncrementResponse{}
	// Start with a get for an unseen cmdID.
	if ok, err := rc.GetResponse(e, cmdID, &val); ok || err != nil {
		t.Errorf("expected no response for id %+v; got %+v, %v", cmdID, val, err)
	}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, &incR); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}
	// Get should now return 1.
	if ok, err := rc.GetResponse(e, cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Errorf("unexpected failure getting response: %t, %v, %+v", ok, err, val)
	}
	if err := rc.ClearData(e); err != nil {
		t.Error(err)
	}
	// The previously present response should be gone.
	if ok, err := rc.GetResponse(e, cmdID, &val); ok {
		t.Errorf("unexpected success getting response: %t, %v, %+v", ok, err, val)
	}
}

// TestResponseCacheEmptyCmdID tests operation with empty client
// command id. All calls should be noops.
func TestResponseCacheEmptyCmdID(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc, e := createTestResponseCache(t, 1)
	cmdID := proto.ClientCmdID{}
	val := proto.IncrementResponse{}
	// Put value of 1 for test response.
	if err := rc.PutResponse(e, cmdID, &incR); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}
	// Get should return !ok.
	if ok, err := rc.GetResponse(e, cmdID, &val); ok || err != nil {
		t.Errorf("unexpected success getting response: %v, %v, %+v", ok, err, val)
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
	val := proto.IncrementResponse{}
	if err := rc1.PutResponse(e, cmdID, &incR); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}
	// Copy the first cache into the second.
	if err := rc1.CopyInto(e, rc2.raftID); err != nil {
		t.Errorf("unexpected error while copying response cache: %v", err)
	}
	// Get should return 1 for both caches.
	if ok, err := rc1.GetResponse(e, cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %t, %v, %+v", ok, err, val)
	}
	if ok, err := rc2.GetResponse(e, cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Errorf("unexpected failure getting response from destination: %t, %v, %+v", ok, err, val)
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
	val := proto.IncrementResponse{}
	if err := rc1.PutResponse(e, cmdID, &incR); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}

	// Copy the first cache into the second.
	if err := rc2.CopyFrom(e, rc1.raftID); err != nil {
		t.Errorf("unexpected error while copying response cache: %v", err)
	}

	// Get should return 1 for both caches.
	if ok, err := rc1.GetResponse(e, cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %t, %v, %+v", ok, err, val)
	}
	if ok, err := rc2.GetResponse(e, cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Errorf("unexpected failure getting response from source: %t, %v, %+v", ok, err, val)
	}
}

// TestResponseCacheInflight verifies concurrent GetResponse
// invocations do not block on same keys.
func TestResponseCacheInflight(t *testing.T) {
	defer leaktest.AfterTest(t)
	rc, e := createTestResponseCache(t, 1)
	cmdID := makeCmdID(1, 1)
	val := proto.IncrementResponse{}
	// Add inflight for cmdID.
	if ok, err := rc.GetResponse(e, cmdID, &val); ok || err != nil {
		t.Errorf("unexpected response or error: %t, %v", ok, err)
	}
	// Make two requests for response from cmdID, which is inflight.
	doneChans := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for _, done := range doneChans {
		doneChan := done
		go func() {
			val2 := proto.IncrementResponse{}
			if ok, err := rc.GetResponse(e, cmdID, &val2); ok || err != nil {
				t.Errorf("unexpectedly found value for response cache entry %+v: %s", cmdID, err)
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

	for i, test := range testCases {
		reply := &proto.PutResponse{}
		reply.SetGoError(test.err)
		if shouldCache := rc.shouldCacheResponse(reply); shouldCache != test.shouldCache {
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
	if err := rc.PutResponse(eng, cmdID, &copyIncR); err != nil {
		t.Fatalf("unexpected error putting responpse: %v", err)
	}
	eng.SetGCTimeouts(0, 0) // avoids GC
	eng.CompactRange(nil, nil)
	val := proto.IncrementResponse{}
	if ok, err := rc.GetResponse(eng, cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Fatalf("unexpected response or error: %t, %v, %+v", ok, err, val)
	}

	// Now set minRCacheTS to 1, which will GC.
	eng.SetGCTimeouts(0, 1)
	eng.CompactRange(nil, nil)
	if ok, err := rc.GetResponse(eng, cmdID, &val); ok || err != nil {
		t.Errorf("unexpected response or error: %t, %v", ok, err)
	}
}
