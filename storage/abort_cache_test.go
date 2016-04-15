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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var (
	batchR           = roachpb.BatchResponse{}
	testTxnID        *uuid.UUID
	testTxnID2       *uuid.UUID
	testTxnKey       = []byte("a")
	testTxnTimestamp = roachpb.ZeroTimestamp.Add(123, 456)
	testTxnPriority  = int32(123)
)

func init() {
	incR := roachpb.IncrementResponse{
		NewValue: 1,
	}
	batchR.Add(&incR)

	var err error
	testTxnID, err = uuid.FromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	if err != nil {
		panic(err)
	}
	testTxnID2, err = uuid.FromString("9ab49d02-eb45-beef-9212-c23a92bc8211")
	if err != nil {
		panic(err)
	}
}

// createTestAbortCache creates an in-memory engine and
// returns a abort cache using the supplied Range ID.
func createTestAbortCache(t *testing.T, rangeID roachpb.RangeID, stopper *stop.Stopper) (*AbortCache, engine.Engine) {
	return NewAbortCache(rangeID), engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
}

// TestAbortCachePutGetClearData tests basic get & put functionality as well as
// clearing the cache.
func TestAbortCachePutGetClearData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sc, e := createTestAbortCache(t, 1, stopper)
	// Start with a get for an uncached id.
	entry := roachpb.AbortCacheEntry{}
	if aborted, readErr := sc.Get(context.Background(), e, testTxnID, &entry); aborted {
		t.Errorf("expected not aborted for id %s", testTxnID)
	} else if readErr != nil {
		t.Fatalf("unxpected read error: %s", readErr)
	}

	entry = roachpb.AbortCacheEntry{
		Key:       testTxnKey,
		Timestamp: testTxnTimestamp,
		Priority:  testTxnPriority,
	}
	if err := sc.Put(context.Background(), e, nil, testTxnID, &entry); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	tryHit := func(expAbort bool, expEntry roachpb.AbortCacheEntry) {
		var actual roachpb.AbortCacheEntry
		if aborted, readErr := sc.Get(context.Background(), e, testTxnID, &actual); readErr != nil {
			t.Errorf("unexpected failure getting response: %s", readErr)
		} else if expAbort != aborted {
			t.Errorf("got aborted: %t; expected %t", aborted, expAbort)
		} else if !reflect.DeepEqual(expEntry, actual) {
			t.Fatalf("wanted %v, got %v", expEntry, actual)
		}
	}

	tryHit(true, entry)
	if err := sc.ClearData(e); err != nil {
		t.Error(err)
	}
	tryHit(false, roachpb.AbortCacheEntry{})
}

// TestAbortCacheEmptyParams tests operation with empty parameters.
func TestAbortCacheEmptyParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	sc, e := createTestAbortCache(t, 1, stopper)

	entry := roachpb.AbortCacheEntry{
		Key:       testTxnKey,
		Timestamp: testTxnTimestamp,
		Priority:  testTxnPriority,
	}
	// Put value for test response.
	if err := sc.Put(context.Background(), e, nil, testTxnID, &entry); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}
	if err := sc.Put(context.Background(), e, nil, nil, &entry); err != errEmptyTxnID {
		t.Errorf("expected errEmptyTxnID error putting response; got %s", err)
	}
	if _, err := sc.Get(context.Background(), e, nil, nil); err != errEmptyTxnID {
		t.Fatalf("expected errEmptyTxnID error; got %s", err)
	}
}

// TestAbortCacheCopyInto tests that entries in one cache get
// transferred correctly to another cache using CopyInto().
func TestAbortCacheCopyInto(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc1, e := createTestAbortCache(t, 1, stopper)
	rc2, _ := createTestAbortCache(t, 2, stopper)
	const seq = 123

	entry := roachpb.AbortCacheEntry{
		Key:       testTxnKey,
		Timestamp: testTxnTimestamp,
		Priority:  testTxnPriority,
	}
	if err := rc1.Put(context.Background(), e, nil, testTxnID, &entry); err != nil {
		t.Errorf("unexpected error putting entry: %s", err)
	}
	// Copy the first cache into the second.
	if count, err := rc1.CopyInto(e, nil, rc2.rangeID); err != nil {
		t.Fatal(err)
	} else if expCount := 1; count != expCount {
		t.Errorf("unexpected number of copied entries: %d", count)
	}
	for _, cache := range []*AbortCache{rc1, rc2} {
		var actual roachpb.AbortCacheEntry
		// Get should return 1 for both caches.
		if aborted, readErr := cache.Get(context.Background(), e, testTxnID, &actual); !aborted || readErr != nil {
			t.Errorf("unexpected failure getting response from source: %t, %s", aborted, readErr)
		} else if !reflect.DeepEqual(entry, actual) {
			t.Fatalf("wanted %v, got %v", entry, actual)
		}
	}
}

// TestAbortCacheCopyFrom tests that entries in one cache get
// transferred correctly to another cache using CopyFrom().
func TestAbortCacheCopyFrom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rc1, e := createTestAbortCache(t, 1, stopper)
	rc2, _ := createTestAbortCache(t, 2, stopper)

	entry := roachpb.AbortCacheEntry{
		Key:       testTxnKey,
		Timestamp: testTxnTimestamp,
		Priority:  testTxnPriority,
	}
	if err := rc1.Put(context.Background(), e, nil, testTxnID, &entry); err != nil {
		t.Errorf("unexpected error putting response: %s", err)
	}

	// Copy the first cache into the second.
	if count, err := rc2.CopyFrom(context.Background(), e, nil, rc1.rangeID); err != nil {
		t.Fatal(err)
	} else if expCount := 1; count != expCount {
		t.Errorf("unexpected number of copied entries: %d", count)
	}

	// Get should hit both caches.
	for i, cache := range []*AbortCache{rc1, rc2} {
		var actual roachpb.AbortCacheEntry
		if aborted, readErr := cache.Get(context.Background(), e, testTxnID, &actual); !aborted || readErr != nil {
			t.Fatalf("%d: unxpected read error: %t, %s", i, aborted, readErr)
		} else if !reflect.DeepEqual(entry, actual) {
			t.Fatalf("expected %v, got %v", entry, actual)
		}
	}
}
