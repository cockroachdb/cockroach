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

package kv

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/biogo/store/llrb"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func mustMeta(k roachpb.RKey) roachpb.RKey {
	m, err := meta(k)
	if err != nil {
		panic(err)
	}
	return m
}

type testDescriptorDB struct {
	data            llrb.Tree
	cache           *RangeDescriptorCache
	lookupCount     int64
	disablePrefetch bool
	pauseChan       chan struct{}
}

type testDescriptorNode struct {
	*roachpb.RangeDescriptor
}

func (a testDescriptorNode) Compare(b llrb.Comparable) int {
	aKey := a.RangeDescriptor.EndKey
	bKey := b.(testDescriptorNode).RangeDescriptor.EndKey
	return bytes.Compare(aKey, bKey)
}

func (db *testDescriptorDB) getDescriptors(
	key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
	rs := make([]roachpb.RangeDescriptor, 0, 1)
	preRs := make([]roachpb.RangeDescriptor, 0, 2)
	for i := 0; i < 3; i++ {
		var endKey roachpb.RKey
		if useReverseScan {
			endKey = key
		} else {
			endKey = key.Next()
		}

		v := db.data.Ceil(testDescriptorNode{
			&roachpb.RangeDescriptor{
				EndKey: endKey,
			},
		})
		if v == nil {
			break
		}
		desc := *(v.(testDescriptorNode).RangeDescriptor)
		if i == 0 {
			rs = append(rs, desc)
			// Fake an intent.
			desc.RangeID++
			rs = append(rs, desc)
		} else if db.disablePrefetch {
			break
		} else {
			preRs = append(preRs, desc)
		}
		// Break to keep from skidding off the end of the available ranges.
		if desc.EndKey.Equal(roachpb.RKeyMax) {
			break
		}

		if useReverseScan {
			key = desc.StartKey
		} else {
			key = desc.EndKey
		}
	}
	return rs, preRs, nil
}

func (db *testDescriptorDB) FirstRange() (*roachpb.RangeDescriptor, error) {
	return nil, nil
}

func (db *testDescriptorDB) RangeLookup(
	ctx context.Context, key roachpb.RKey, _ *roachpb.RangeDescriptor, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
	select {
	case <-db.pauseChan:
	case <-ctx.Done():
		return nil, nil, roachpb.NewError(ctx.Err())
	}
	atomic.AddInt64(&db.lookupCount, 1)
	return db.getDescriptors(stripMeta(key), useReverseScan)
}

func stripMeta(key roachpb.RKey) roachpb.RKey {
	switch {
	case bytes.HasPrefix(key, keys.Meta1Prefix):
		return testutils.MakeKey(roachpb.RKey(keys.Meta2Prefix), key[len(keys.Meta1Prefix):])
	case bytes.HasPrefix(key, keys.Meta2Prefix):
		return key[len(keys.Meta2Prefix):]
	}
	// First range.
	return nil
}

func (db *testDescriptorDB) splitRange(t *testing.T, key roachpb.RKey) {
	v := db.data.Ceil(testDescriptorNode{&roachpb.RangeDescriptor{EndKey: key}})
	if v == nil {
		t.Fatalf("Error splitting range at key %s, range to split not found", string(key))
	}
	val := v.(testDescriptorNode)
	if bytes.Equal(val.EndKey, key) {
		t.Fatalf("Attempt to split existing range at Endkey: %s", string(key))
	}
	db.data.Insert(testDescriptorNode{
		&roachpb.RangeDescriptor{
			StartKey: val.StartKey,
			EndKey:   key,
		},
	})
	db.data.Insert(testDescriptorNode{
		&roachpb.RangeDescriptor{
			StartKey: key,
			EndKey:   val.EndKey,
		},
	})
}

func (db *testDescriptorDB) pauseRangeLookups() {
	db.pauseChan = make(chan struct{})
}

func (db *testDescriptorDB) resumeRangeLookups() {
	close(db.pauseChan)
}

func newTestDescriptorDB() *testDescriptorDB {
	db := &testDescriptorDB{
		pauseChan: make(chan struct{}),
	}
	db.data.Insert(testDescriptorNode{
		&roachpb.RangeDescriptor{
			StartKey: testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMin),
			EndKey:   testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax),
		},
	})
	db.data.Insert(testDescriptorNode{
		&roachpb.RangeDescriptor{
			StartKey: testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax),
			EndKey:   roachpb.RKeyMax,
		},
	})
	db.resumeRangeLookups()
	return db
}

func initTestDescriptorDB(t *testing.T) *testDescriptorDB {
	db := newTestDescriptorDB()
	for i, char := range "abcdefghijklmnopqrstuvwx" {
		db.splitRange(t, roachpb.RKey(string(char)))
		if i > 0 && i%6 == 0 {
			db.splitRange(t, mustMeta(roachpb.RKey(string(char))))
		}
	}
	db.cache = NewRangeDescriptorCache(db, 2<<10)
	return db
}

// assertLookupCountEq fails unless exactly the number of lookups have been observed.
func (db *testDescriptorDB) assertLookupCountEq(t *testing.T, exp int64, key string) {
	if exp != db.lookupCount {
		file, line, _ := caller.Lookup(1)
		t.Errorf("%s:%d: expected lookup count %d after %s, was %d",
			file, line, exp, key, db.lookupCount)
	}
	db.lookupCount = 0
}

// assertLookupCountEq fails unless number of lookups observed is >= from and <= to.
func (db *testDescriptorDB) assertLookupCount(t *testing.T, from, to int64, key string) {
	if from > db.lookupCount || to < db.lookupCount {
		file, line, _ := caller.Lookup(1)
		t.Errorf("%s:%d: expected lookup count in [%d, %d] after %s, was %d",
			file, line, from, to, key, db.lookupCount)
	}
	db.lookupCount = 0
}

func doLookup(
	ctx context.Context, t *testing.T, rc *RangeDescriptorCache, key string,
) (*roachpb.RangeDescriptor, *EvictionToken) {
	return doLookupWithToken(ctx, t, rc, key, nil, false, nil)
}

func doLookupConsideringIntents(
	ctx context.Context, t *testing.T, rc *RangeDescriptorCache, key string,
) (*roachpb.RangeDescriptor, *EvictionToken) {
	return doLookupWithToken(ctx, t, rc, key, nil, false, nil)
}

func doLookupWithToken(
	ctx context.Context,
	t *testing.T,
	rc *RangeDescriptorCache,
	key string,
	evictToken *EvictionToken,
	useReverseScan bool,
	wg *sync.WaitGroup,
) (*roachpb.RangeDescriptor, *EvictionToken) {
	r, returnToken, err := rc.lookupRangeDescriptorInternal(
		ctx, roachpb.RKey(key), evictToken, useReverseScan, wg)
	if err != nil {
		t.Fatalf("Unexpected error from LookupRangeDescriptor: %s", err)
	}
	keyAddr, err := keys.Addr(roachpb.Key(key))
	if err != nil {
		t.Fatal(err)
	}
	if (useReverseScan && !r.ContainsExclusiveEndKey(keyAddr)) || (!useReverseScan && !r.ContainsKey(keyAddr)) {
		t.Fatalf("Returned range did not contain key: %s-%s, %s", r.StartKey, r.EndKey, key)
	}
	return r, returnToken
}

// TestDescriptorDBGetDescriptors verifies that getDescriptors returns correct descriptors.
func TestDescriptorDBGetDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	key := roachpb.RKey("k")
	expectedRspansMap := map[bool][]roachpb.RSpan{
		true: {
			roachpb.RSpan{Key: roachpb.RKey("j"), EndKey: roachpb.RKey("k")}, // real
			roachpb.RSpan{Key: roachpb.RKey("j"), EndKey: roachpb.RKey("k")}, // fake intent
			roachpb.RSpan{Key: roachpb.RKey("i"), EndKey: roachpb.RKey("j")},
			roachpb.RSpan{Key: roachpb.RKey("h"), EndKey: roachpb.RKey("i")},
		},
		false: {
			roachpb.RSpan{Key: roachpb.RKey("k"), EndKey: roachpb.RKey("l")}, // real
			roachpb.RSpan{Key: roachpb.RKey("k"), EndKey: roachpb.RKey("l")}, // fake intent
			roachpb.RSpan{Key: roachpb.RKey("l"), EndKey: roachpb.RKey("m")},
			roachpb.RSpan{Key: roachpb.RKey("m"), EndKey: roachpb.RKey("n")},
		},
	}

	for useReverseScan, expectedRspans := range expectedRspansMap {
		descs, preDescs, pErr := db.getDescriptors(key, useReverseScan)
		if pErr != nil {
			t.Fatal(pErr)
		}

		descSpans := make([]roachpb.RSpan, len(descs))
		for i := range descs {
			descSpans[i] = descs[i].RSpan()
		}
		if !reflect.DeepEqual(descSpans, expectedRspans[:2]) {
			t.Errorf("useReverseScan=%t: expected %s, got %s", useReverseScan, expectedRspans[:2], descSpans)
		}
		preDescSpans := make([]roachpb.RSpan, len(preDescs))
		for i := range preDescs {
			preDescSpans[i] = preDescs[i].RSpan()
		}
		if !reflect.DeepEqual(preDescSpans, expectedRspans[2:]) {
			t.Errorf("useReverseScan=%t: expected %s, got %s", useReverseScan, expectedRspans[2:], preDescSpans)
		}
	}
}

func TestRangeCacheAssumptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	expKeyMin := mustMeta(mustMeta(mustMeta(roachpb.RKey("test"))))
	if !bytes.Equal(expKeyMin, roachpb.RKeyMin) {
		t.Fatalf("RangeCache relies on RangeMetaKey returning KeyMin after two levels, but got %s", expKeyMin)
	}
}

// TestRangeCache is a simple test which verifies that metadata ranges
// are being cached and retrieved properly. It sets up a fake backing
// store for the cache, and measures how often that backing store is
// lookuped when looking up metadata keys through the cache.
func TestRangeCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.TODO()

	doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// Descriptors for the following ranges should be cached.
	doLookup(ctx, t, db.cache, "ab")
	db.assertLookupCountEq(t, 0, "ab")
	doLookup(ctx, t, db.cache, "ba")
	db.assertLookupCountEq(t, 0, "ba")
	doLookup(ctx, t, db.cache, "cz")
	db.assertLookupCountEq(t, 0, "cz")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	doLookup(ctx, t, db.cache, "d")
	db.assertLookupCountEq(t, 1, "d")
	doLookup(ctx, t, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	doLookup(ctx, t, db.cache, "ij")
	db.assertLookupCountEq(t, 1, "ij")
	doLookup(ctx, t, db.cache, "jk")
	db.assertLookupCountEq(t, 0, "jk")
	doLookup(ctx, t, db.cache, "pn")
	db.assertLookupCountEq(t, 1, "pn")

	// Totally uncached ranges
	doLookup(ctx, t, db.cache, "vu")
	db.assertLookupCountEq(t, 2, "vu")
	doLookup(ctx, t, db.cache, "xx")
	db.assertLookupCountEq(t, 0, "xx")

	// Evict clears one level 1 and one level 2 cache
	if err := db.cache.EvictCachedRangeDescriptor(ctx, roachpb.RKey("da"), nil, false); err != nil {
		t.Fatal(err)
	}
	doLookup(ctx, t, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")
	doLookup(ctx, t, db.cache, "da")
	db.assertLookupCountEq(t, 2, "da")

	// Looking up a descriptor that lands on an end-key should work
	// without a cache miss.
	doLookup(ctx, t, db.cache, "a")
	db.assertLookupCountEq(t, 0, "a")

	// Attempt to compare-and-evict with a descriptor that is not equal to the
	// cached one; it should not alter the cache.
	if err := db.cache.EvictCachedRangeDescriptor(ctx, roachpb.RKey("cz"), &roachpb.RangeDescriptor{}, false); err != nil {
		t.Fatal(err)
	}
	_, evictToken := doLookup(ctx, t, db.cache, "cz")
	db.assertLookupCountEq(t, 0, "cz")
	// Now evict with the actual descriptor. The cache should clear the
	// descriptor and the cached meta key.
	if err := evictToken.Evict(ctx); err != nil {
		t.Fatal(err)
	}
	doLookup(ctx, t, db.cache, "cz")
	db.assertLookupCountEq(t, 2, "cz")
}

// TestRangeCacheCoalescedRequests verifies that concurrent lookups for
// the same key will be coalesced onto the same database lookup.
func TestRangeCacheCoalescedRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.TODO()

	pauseLookupResumeAndAssert := func(key string, expected int64) {
		var wg, waitJoin sync.WaitGroup
		db.pauseRangeLookups()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			waitJoin.Add(1)
			go func() {
				doLookupWithToken(ctx, t, db.cache, key, nil, false, &waitJoin)
				wg.Done()
			}()
		}
		waitJoin.Wait()
		db.resumeRangeLookups()
		wg.Wait()
		db.assertLookupCountEq(t, expected, key)
	}

	pauseLookupResumeAndAssert("aa", 2)

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	pauseLookupResumeAndAssert("d", 1)
	pauseLookupResumeAndAssert("fa", 0)
}

// TestRangeCacheContextCancellation tests the behavior that for an ongoing
// RangeDescriptor lookup, if the context passed in gets cancelled the lookup
// returns with an error indicating so. The result of the context cancellation
// differs between requests that lead RangeLookup requests and requests that
// coalesce onto existing RangeLookup requests.
// - If the context of a RangeLookup request follower is cancelled, the follower
//   will stop waiting on the inflight request, but will not have an effect on
//   the inflight request.
// - If the context of a RangeLookup request leader is cancelled, the lookup
//   itself will also be cancelled. This means that any followers waiting on the
//   inflight request will also see the context cancellation. This is ok, though,
//   because DistSender will transparently retry the lookup.
func TestRangeCacheContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	// lookupAndWaitUntilJoin performs a RangeDescriptor lookup in a new
	// goroutine and blocks until the request is added to the inflight request
	// map. It returns a channel that transmits the error return value from the
	// lookup.
	lookupAndWaitUntilJoin := func(ctx context.Context, key roachpb.RKey) chan error {
		errC := make(chan error)
		var waitJoin sync.WaitGroup
		waitJoin.Add(1)
		go func() {
			_, _, err := db.cache.lookupRangeDescriptorInternal(ctx, key, nil, false, &waitJoin)
			errC <- err
		}()
		waitJoin.Wait()
		return errC
	}

	expectContextCancellation := func(t *testing.T, c <-chan error) {
		if err := <-c; err.Error() != context.Canceled.Error() {
			t.Errorf("expected context cancellation error, found %v", err)
		}
	}
	expectNoError := func(t *testing.T, c <-chan error) {
		if err := <-c; err != nil {
			t.Errorf("unexpected error, found %v", err)
		}
	}

	// If a RangeDescriptor lookup joins an inflight RangeLookup, it can cancel
	// its context to stop waiting on the range lookup. This context cancellation
	// will not affect the "leader" of the inflight lookup or any other
	// "followers" who are also waiting on the inflight request.
	t.Run("Follower", func(t *testing.T) {
		ctx1 := context.TODO() // leader
		ctx2, cancel := context.WithCancel(context.TODO())
		ctx3 := context.TODO()

		db.pauseRangeLookups()
		key1 := roachpb.RKey("aa")
		errC1 := lookupAndWaitUntilJoin(ctx1, key1)
		errC2 := lookupAndWaitUntilJoin(ctx2, key1)
		errC3 := lookupAndWaitUntilJoin(ctx3, key1)

		cancel()
		expectContextCancellation(t, errC2)

		db.resumeRangeLookups()
		expectNoError(t, errC1)
		expectNoError(t, errC3)
	})

	// If a RangeDescriptor lookup leads a RangeLookup because there are no
	// inflight lookups when it misses the cache,  the it can cancel it context
	// to cancel the range lookup. This context cancellation will be propagated
	// to all "followers" who are also waiting on the inflight request.
	t.Run("Leader", func(t *testing.T) {
		ctx1, cancel := context.WithCancel(context.TODO()) // leader
		ctx2 := context.TODO()
		ctx3 := context.TODO()

		db.pauseRangeLookups()
		key2 := roachpb.RKey("zz")
		errC1 := lookupAndWaitUntilJoin(ctx1, key2)
		errC2 := lookupAndWaitUntilJoin(ctx2, key2)
		errC3 := lookupAndWaitUntilJoin(ctx3, key2)

		cancel()
		expectContextCancellation(t, errC1)
		expectContextCancellation(t, errC2)
		expectContextCancellation(t, errC3)
	})
}

// TestRangeCacheDetectSplit verifies that when the cache detects a split
// it will properly coalesce all requests to the right half of the split and
// will prefetch the left half of the split.
func TestRangeCacheDetectSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.TODO()

	pauseLookupResumeAndAssert := func(key string, expected int64, evictToken *EvictionToken) {
		var wg, waitJoin sync.WaitGroup
		db.pauseRangeLookups()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			waitJoin.Add(1)
			go func(id int) {
				// Each request goes to a different key.
				doLookupWithToken(ctx, t, db.cache, fmt.Sprintf("%s%d", key, id), evictToken, false, &waitJoin)
				wg.Done()
			}(i)
		}
		waitJoin.Wait()
		db.resumeRangeLookups()
		wg.Wait()
		db.assertLookupCountEq(t, expected, key)
	}

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an") and ["an"-"b").
	db.splitRange(t, roachpb.RKey("an"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	_, evictToken := doLookup(ctx, t, db.cache, "az")
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	if err := evictToken.EvictAndReplace(ctx, mismatchErrRange); err != nil {
		t.Fatal(err)
	}
	pauseLookupResumeAndAssert("az", 2, evictToken)

	// Both sides of the split are now correctly cached.
	doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")
	doLookup(ctx, t, db.cache, "az")
	db.assertLookupCountEq(t, 0, "az")
}

// Verifies that the end key of a stale descriptor is used as a request key
// when the request is for the reverse scan.
func TestRangeCacheDetectSplitReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.TODO()

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an") and ["an"-"b").
	db.splitRange(t, roachpb.RKey("an"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	useReverseScan := true
	_, evictToken := doLookupWithToken(ctx, t, db.cache, "az", nil, useReverseScan, nil)
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	if err := evictToken.EvictAndReplace(ctx, mismatchErrRange); err != nil {
		// Evict the cached descriptor ["a", "b") and insert ["a"-"an")
		t.Fatal(err)
	}

	// Create two lookup requests with key "a" and "az". The lookup on "az" uses
	// the evictToken returned by the previous lookup.
	//
	// The requests will *not* be coalesced, and two different descriptors should
	// be returned ([KeyMin-,"a") and ["an-b")).
	lookups := []struct {
		key        string
		evictToken *EvictionToken
	}{
		{"a", nil},
		{"az", evictToken},
	}
	db.pauseRangeLookups()
	var wg, waitJoin sync.WaitGroup
	for _, lookup := range lookups {
		wg.Add(1)
		waitJoin.Add(1)
		go func(key string, evictToken *EvictionToken) {
			doLookupWithToken(ctx, t, db.cache, key, evictToken, useReverseScan, &waitJoin)
			wg.Done()
		}(lookup.key, lookup.evictToken)
	}
	waitJoin.Wait()
	db.resumeRangeLookups()
	wg.Wait()
	db.assertLookupCount(t, 3, 4, "a and az")

	// Both are now correctly cached.
	doLookupWithToken(ctx, t, db.cache, "a", nil, useReverseScan, nil)
	db.assertLookupCountEq(t, 0, "a")
	doLookupWithToken(ctx, t, db.cache, "az", nil, useReverseScan, nil)
	db.assertLookupCountEq(t, 0, "az")
}

// TestRangeCacheHandleDoubleSplit verifies that when the cache can handle a
// double split.
func TestRangeCacheHandleDoubleSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testRangeCacheHandleDoubleSplit(t, false)
}

func TestRangeCacheHandleDoubleSplitUseReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testRangeCacheHandleDoubleSplit(t, true)
}

func testRangeCacheHandleDoubleSplit(t *testing.T, useReverseScan bool) {
	db := initTestDescriptorDB(t)
	db.disablePrefetch = true
	ctx := context.TODO()

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an"), ["an"-"at"), ["at"-"b").
	db.splitRange(t, roachpb.RKey("an"))
	db.splitRange(t, roachpb.RKey("at"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	_, evictToken := doLookup(ctx, t, db.cache, "az")
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	if err := evictToken.EvictAndReplace(ctx, mismatchErrRange); err != nil {
		t.Fatal(err)
	}

	// Requests to all parts of the split are sent:

	// [reverse case]
	// - "aa" and "an" will hit the cache
	// - all others will join a coalesced request to "az"
	//   + will lookup the meta2 desc
	//   + will lookup the ["at"-"b") desc
	// - "az" will get the right range back
	// - "at" will make a second lookup
	//   + will lookup the ["an"-"at") desc
	//
	// [non-reverse case]
	// - "aa" will hit the cache
	// - all others will join a coalesced request to "an"
	//   + will lookup the meta2 desc
	//   + will lookup the ["an"-"at") desc
	// - "an" and "ao" will get the right range back
	// - "at" and "az" will make a second lookup
	//   + will lookup the ["at"-"b") desc
	var wg, waitJoin sync.WaitGroup
	db.pauseRangeLookups()
	numRetries := int64(0)
	for _, k := range []string{"aa", "an", "ao", "at", "az"} {
		wg.Add(1)
		waitJoin.Add(1)
		go func(key roachpb.RKey) {
			reqEvictToken := evictToken
			waitJoinCopied := &waitJoin
			var desc *roachpb.RangeDescriptor
			for {
				// Each request goes to a different key.
				var err error
				if desc, reqEvictToken, err = db.cache.lookupRangeDescriptorInternal(
					ctx, key, reqEvictToken,
					useReverseScan, waitJoinCopied); err != nil {
					waitJoinCopied = nil
					atomic.AddInt64(&numRetries, 1)
					continue
				}
				break
			}
			if useReverseScan {
				if !desc.ContainsExclusiveEndKey(key) {
					t.Errorf("desc %s does not contain exclusive end key %s", desc, key)
				}
			} else {
				if !desc.ContainsKey(key) {
					t.Errorf("desc %s does not contain key %s", desc, key)
				}
			}

			wg.Done()
		}(roachpb.RKey(k))
	}
	// Wait until all lookup requests hit the cache or join into a coalesced request.
	waitJoin.Wait()
	db.resumeRangeLookups()

	wg.Wait()
	db.assertLookupCountEq(t, 3, "an and az")
	if numRetries == 0 {
		t.Error("expected retry on desc lookup")
	}

	// All three descriptors are now correctly cached.
	doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")
	doLookup(ctx, t, db.cache, "ao")
	db.assertLookupCountEq(t, 0, "ao")
	doLookup(ctx, t, db.cache, "az")
	db.assertLookupCountEq(t, 0, "az")
}

func TestRangeCacheUseIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.TODO()

	// A request initially looks up the range descriptor ["a"-"b") considering intents.
	abDesc, evictToken := doLookupConsideringIntents(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// Perform a lookup now that the cache is populated.
	abDescLookup, _ := doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")

	// The descriptors should be the same.
	if !reflect.DeepEqual(abDesc, abDescLookup) {
		t.Errorf("expected initial range descriptor to be returned from lookup, found %v", abDescLookup)
	}

	// The current descriptor is found to be stale, so it is evicted. The next cache
	// lookup should return the descriptor from the intents, without performing another
	// db lookup.
	if err := evictToken.Evict(ctx); err != nil {
		t.Fatal(err)
	}
	abDescIntent, _ := doLookup(ctx, t, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")

	// The descriptors should be different.
	if reflect.DeepEqual(abDesc, abDescIntent) {
		t.Errorf("expected initial range descriptor to be different from the one from intents, found %v", abDesc)
	}
}

// TestRangeCacheClearOverlapping verifies that existing, overlapping
// cached entries are cleared when adding a new entry.
func TestRangeCacheClearOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()

	defDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
	}

	cache := NewRangeDescriptorCache(nil, 2<<10)
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), defDesc)

	// Now, add a new, overlapping set of descriptors.
	minToBDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("b"),
	}
	bToMaxDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKeyMax,
	}
	if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, minToBDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKey("b"))), minToBDesc)
	if desc, err := cache.GetCachedRangeDescriptor(roachpb.RKey("b"), false); err != nil {
		t.Fatal(err)
	} else if desc != nil {
		t.Errorf("descriptor unexpectedly non-nil: %s", desc)
	}
	if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, bToMaxDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKeyMax)), bToMaxDesc)
	if desc, err := cache.GetCachedRangeDescriptor(roachpb.RKey("b"), false); err != nil {
		t.Fatal(err)
	} else if desc != bToMaxDesc {
		t.Errorf("expected descriptor %s; got %s", bToMaxDesc, desc)
	}

	// Add default descriptor back which should remove two split descriptors.
	if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, defDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), defDesc)
	for _, key := range []roachpb.RKey{roachpb.RKey("a"), roachpb.RKey("b")} {
		if desc, err := cache.GetCachedRangeDescriptor(key, false); err != nil {
			t.Fatal(err)
		} else if desc != defDesc {
			t.Errorf("expected descriptor %s for key %s; got %s", defDesc, key, desc)
		}
	}

	// Insert ["b", "c") and then insert ["a", b"). Verify that the former is not evicted by the latter.
	bToCDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
	}
	if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, bToCDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKey("c"))), bToCDesc)
	if desc, err := cache.GetCachedRangeDescriptor(roachpb.RKey("c"), true); err != nil {
		t.Fatal(err)
	} else if desc != bToCDesc {
		t.Errorf("expected descriptor %s; got %s", bToCDesc, desc)
	}

	aToBDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}
	if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, aToBDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKey("b"))), aToBDesc)
	if desc, err := cache.GetCachedRangeDescriptor(roachpb.RKey("c"), true); err != nil {
		t.Fatal(err)
	} else if desc != bToCDesc {
		t.Errorf("expected descriptor %s; got %s", bToCDesc, desc)
	}
}

// TestRangeCacheClearOverlappingMeta prevents regression of a bug which caused
// a panic when clearing overlapping descriptors for [KeyMin, Meta2Key). The
// issue was that when attempting to clear out descriptors which were subsumed
// by the above range, an iteration over the corresponding meta keys was
// performed, with the left endpoint excluded. This exclusion was incorrect: it
// first incremented the start key (KeyMin) and then formed the meta key; for
// KeyMin this leads to Meta2Prefix\x00. For the above EndKey, the meta key is
// a Meta1key which sorts before Meta2Prefix\x00, causing a panic. The fix was
// simply to increment the meta key for StartKey, not StartKey itself.
func TestRangeCacheClearOverlappingMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()

	firstDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("zzz"),
	}
	restDesc := &roachpb.RangeDescriptor{
		StartKey: firstDesc.StartKey,
		EndKey:   roachpb.RKeyMax,
	}

	cache := NewRangeDescriptorCache(nil, 2<<10)
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(firstDesc.EndKey)),
		firstDesc)
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(restDesc.EndKey)),
		restDesc)

	// Add new range, corresponding to splitting the first range at a meta key.
	metaSplitDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   mustMeta(roachpb.RKey("foo")),
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("invocation of clearOverlappingCachedRangeDescriptors panicked: %v", r)
			}
		}()
		if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, metaSplitDesc); err != nil {
			t.Fatal(err)
		}
	}()
}

// TestGetCachedRangeDescriptorInclusive verifies the correctness of the result
// that is returned by getCachedRangeDescriptor with inclusive=true.
func TestGetCachedRangeDescriptorInclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []*roachpb.RangeDescriptor{
		{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
		{StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("z")},
	}

	cache := NewRangeDescriptorCache(nil, 2<<10)
	for _, rd := range testData {
		cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(rd.EndKey)), rd)
	}

	testCases := []struct {
		queryKey roachpb.RKey
		cacheKey rangeCacheKey
		rng      *roachpb.RangeDescriptor
	}{
		{
			// Check range start key.
			queryKey: roachpb.RKey("a"),
			cacheKey: nil,
			rng:      nil,
		},
		{
			// Check range end key.
			queryKey: roachpb.RKey("c"),
			cacheKey: rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("c"))),
			rng:      &roachpb.RangeDescriptor{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		},
		{
			// Check range middle key.
			queryKey: roachpb.RKey("d"),
			cacheKey: rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("e"))),
			rng:      &roachpb.RangeDescriptor{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
		},
		{
			// Check miss range key.
			queryKey: roachpb.RKey("f"),
			cacheKey: nil,
			rng:      nil,
		},
		{
			// Check range start key with previous range miss.
			queryKey: roachpb.RKey("g"),
			cacheKey: nil,
			rng:      nil,
		},
	}

	for _, test := range testCases {
		cache.rangeCache.RLock()
		cacheKey, targetRange, err := cache.getCachedRangeDescriptorLocked(
			test.queryKey, true /* inclusive */)
		cache.rangeCache.RUnlock()
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(targetRange, test.rng) {
			t.Fatalf("expect range %v, actual get %v", test.rng, targetRange)
		}
		if !reflect.DeepEqual(cacheKey, test.cacheKey) {
			t.Fatalf("expect cache key %v, actual get %v", test.cacheKey, cacheKey)
		}
	}

}
