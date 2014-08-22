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
	"github.com/cockroachdb/cockroach/util"
)

var incR = proto.IncrementResponse{
	NewValue: 1,
}

// createTestResponseCache creates an in-memory engine and
// returns a response cache using the engine for range ID 1.
func createTestResponseCache(t *testing.T) *ResponseCache {
	return NewResponseCache(1, engine.NewInMem(engine.Attributes{}, 1<<20))
}

func makeCmdID(wallTime, random int64) proto.ClientCmdID {
	return proto.ClientCmdID{
		WallTime: wallTime,
		Random:   random,
	}
}

// TestResponseCachePutAndGet tests basic get & put functionality.
func TestResponseCachePutAndGet(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID := makeCmdID(1, 1)
	val := proto.IncrementResponse{}
	// Start with a get for an unseen cmdID.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("expected no response for id %+v; got %+v, %v", cmdID, val, err)
	}
	// Put value of 1 for test response.
	if err := rc.PutResponse(cmdID, &incR); err != nil {
		t.Errorf("unexpected error putting response: %v", err)
	}
	// Get should now return 1.
	if ok, err := rc.GetResponse(cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Errorf("unexpected failure getting response: %t, %v, %+v", ok, err, val)
	}
}

// TestResponseCacheEmptyCmdID tests operation with empty client
// command id. All calls should be noops.
func TestResponseCacheEmptyCmdID(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID := proto.ClientCmdID{}
	val := proto.IncrementResponse{}
	// Put value of 1 for test response.
	if err := rc.PutResponse(cmdID, &incR); err != nil {
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

// TestResponseCacheInflight verifies GetResponse invocations block on
// inflight requests.
func TestResponseCacheInflight(t *testing.T) {
	rc := createTestResponseCache(t)
	cmdID := makeCmdID(1, 1)
	val := proto.IncrementResponse{}
	// Add inflight for cmdID.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("unexpected response or error: %t, %v", ok, err)
	}
	// Make two blocking requests for response from cmdID, which is inflight.
	doneChans := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for _, done := range doneChans {
		doneChan := done
		go func() {
			val2 := proto.IncrementResponse{}
			if ok, err := rc.GetResponse(cmdID, &val2); !ok || err != nil || val2.NewValue != 1 {
				t.Errorf("unexpected error: %t, %v, %+v", ok, err, val2)
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
		if err := rc.PutResponse(cmdID, &incR); err != nil {
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
	val := proto.IncrementResponse{}
	// Add inflight for cmdID.
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("unexpected error: %t, %v", ok, err)
	}
	done := make(chan struct{})
	go func() {
		if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
			t.Errorf("unexpected error: %t, %v", ok, err)
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

// TestResponseCacheGC verifies that response cache entries are
// garbage collected periodically.
func TestResponseCacheGC(t *testing.T) {
	loc := util.CreateTempDirectory()
	rocksdb := engine.NewRocksDB(engine.Attributes([]string{"ssd"}), loc)
	if err := rocksdb.Start(); err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(t *testing.T) {
		rocksdb.Close()
		if err := rocksdb.Destroy(); err != nil {
			t.Errorf("could not destroy rocksdb db at %s: %v", loc, err)
		}
	}(t)

	rc := NewResponseCache(1, rocksdb)
	cmdID := makeCmdID(1, 1)

	// Add response for cmdID with timestamp at time=1ns.
	copyIncR := incR
	copyIncR.Timestamp.WallTime = 1
	if err := rc.PutResponse(cmdID, &copyIncR); err != nil {
		t.Fatalf("unexpected error putting responpse: %v", err)
	}
	rocksdb.SetGCTimeouts(func() (minTxnTS, minRCacheTS int64) {
		minRCacheTS = 0 // avoids GC
		return
	})
	rocksdb.CompactRange(nil, nil)
	val := proto.IncrementResponse{}
	if ok, err := rc.GetResponse(cmdID, &val); !ok || err != nil || val.NewValue != 1 {
		t.Fatalf("unexpected response or error: %b, %v, %+v", ok, err, val)
	}

	// Now set minRCacheTS to 1, which will GC.
	rocksdb.SetGCTimeouts(func() (minTxnTS, minRCacheTS int64) {
		minRCacheTS = 1
		return
	})
	rocksdb.CompactRange(nil, nil)
	if ok, err := rc.GetResponse(cmdID, &val); ok || err != nil {
		t.Errorf("unexpected response or error: %b, %v", ok, err)
	}
}
