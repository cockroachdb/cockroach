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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"testing"
	"time"
)

// createTestResponseCache creates an in-memory engine and
// returns a response cache using the engine for range ID 1.
func createTestResponseCache(t *testing.T) *ResponseCache {
	return NewResponseCache(1, NewInMem(Attributes{}, 1<<20))
}

func makeCmdID(wallTime, random int64) ClientCmdID {
	return ClientCmdID{
		WallTime: wallTime,
		Random:   random,
	}
}

// TestResponseCachePutAndGet tests basic get & put functionality.
func TestResponseCachePutAndGet(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID := makeCmdID(1, 1)
	var val int64
	// Start with a get for an unseen cmdID.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("expected no response for id %+v; got %+v, %v", cmdID, val, err)
	}
	// Put value of 1 for test response.
	if err := rc.PutResponse(cmdID, int64(1)); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}
	// Get should now return 1.
	if ok, err := rc.GetResponse(cmdID, &val); !ok || err != nil || val != 1 {
		t.Errorf("unexpected failure getting response: %b, %v, %+v", ok, err, val)
	}
}

// TestResponseCacheEmptyCmdID tests operation with empty client
// command id. All calls should be noops.
func TestResponseCacheEmptyCmdID(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID := ClientCmdID{}
	var val int64
	// Put value of 1 for test response.
	if err := rc.PutResponse(cmdID, int64(1)); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}
	// Add inflight, which would otherwise block the get.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("unexpected success getting response: %v, %v, %+v", ok, err, val)
	}
	// Get should return !ok.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("unexpected success getting response: %v, %v, %+v", ok, err, val)
	}
}

// TestResponseCacheInflight verifies GetResponse invocations
// block on inflight requests.
func TestResponseCacheInflight(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID1 := makeCmdID(1, 1)
	cmdID2 := makeCmdID(1, 2)
	var val int64
	// Add inflight for cmdID1.
	if ok, err := rc.GetResponse(cmdID1, &val); ok || err != nil {
		t.Errorf("unexpected response or error: %b, %v", ok, err)
	}
	// No blocking for cmdID2.
	if ok, err := rc.GetResponse(cmdID2, &val); ok || err != nil {
		t.Errorf("unexpected success getting response: %v, %v, %+v", ok, err, val)
	}
	// Make two blocking requests for response from cmdID1, which is inflight.
	doneChans := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for _, done := range doneChans {
		doneChan := done
		go func() {
			if ok, err := rc.GetResponse(cmdID1, &val); !ok || err != nil || val != 1 {
				t.Errorf("unexpected error: %b, %v, %+v", ok, err, val)
			}
			close(doneChan)
		}()
	}
	// Wait for 2ms to verify both gets are blocked.
	select {
	case <-doneChans[0]:
		t.Fatal("1st get should not complete; it blocks until we put")
	case <-doneChans[1]:
		t.Fatal("2nd get should not complete; it blocks until we put")
	case <-time.After(2 * time.Millisecond):
		if err := rc.PutResponse(cmdID1, int64(1)); err != nil {
			t.Fatalf("unexpected error putting responpse: %v", err)
		}
	}
	// After putting response, verify that get is unblocked.
	for _, done := range doneChans {
		select {
		case <-done:
			// Success!
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("get response failed to complete in 500ms")
		}
	}
}

// TestResponseCacheTwoInflights verifies panic in the event
// that AddInflight is called twice for same command ID.
func TestResponseCacheTwoInflights(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic due to two successive calls to AddInflight")
		}
	}()
	rc := createTestResponseCache(t)
	cmdID := makeCmdID(1, 1)
	rc.addInflightLocked(cmdID)
	rc.addInflightLocked(cmdID)
}

// TestResponseCacheClear verifies that inflight waiters are
// signaled in the event the cache is cleared.
func TestResponseCacheClear(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID := makeCmdID(1, 1)
	var val int64
	// Add inflight for cmdID.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("unexpected error: %b, %v", ok, err)
	}
	done := make(chan struct{})
	go func() {
		if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
			t.Errorf("unexpected error: %b, %v", ok, err)
		}
		close(done)
	}()
	// Clear the response cache, which should unblock request.
	rc.ClearInflight()
	select {
	case <-done:
		// Success!
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("get response failed to complete in 500ms")
	}
}
