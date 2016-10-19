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
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package kv

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/net/context"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	cache           *rangeDescriptorCache
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
	_ context.Context, key roachpb.RKey, _ *roachpb.RangeDescriptor, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, *roachpb.Error) {
	<-db.pauseChan
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
			StartKey: roachpb.RKeyMin,
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
	db.cache = newRangeDescriptorCache(context.TODO(), db, 2<<10)
	return db
}

// assertLookupCountEq fails unless exactly the number of lookups have been observed.
func (db *testDescriptorDB) assertLookupCountEq(t *testing.T, exp int64, key string) {
	db.assertLookupCount(t, exp, exp, key)
}

// assertLookupCountEq fails unless number of lookups observed is >= from and <= to.
func (db *testDescriptorDB) assertLookupCount(t *testing.T, from, to int64, key string) {
	if from > db.lookupCount || to < db.lookupCount {
		t.Errorf("Expected lookup count in [%d, %d] after %s, was %d", from, to, key, db.lookupCount)
	}
	db.lookupCount = 0
}

func doLookup(
	t *testing.T, rc *rangeDescriptorCache, key string,
) (*roachpb.RangeDescriptor, *EvictionToken) {
	return doLookupWithToken(t, rc, key, nil, false, nil)
}

func doLookupConsideringIntents(
	t *testing.T, rc *rangeDescriptorCache, key string,
) (*roachpb.RangeDescriptor, *EvictionToken) {
	return doLookupWithToken(t, rc, key, nil, false, nil)
}

func doLookupWithToken(
	t *testing.T,
	rc *rangeDescriptorCache,
	key string,
	evictToken *EvictionToken,
	useReverseScan bool,
	wg *sync.WaitGroup,
) (*roachpb.RangeDescriptor, *EvictionToken) {
	r, returnToken, pErr := rc.lookupRangeDescriptorInternal(
		context.Background(), roachpb.RKey(key), evictToken, useReverseScan, wg)
	if pErr != nil {
		t.Fatalf("Unexpected error from LookupRangeDescriptor: %s", pErr)
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

	doLookup(t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// Descriptors for the following ranges should be cached.
	doLookup(t, db.cache, "ab")
	db.assertLookupCountEq(t, 0, "ab")
	doLookup(t, db.cache, "ba")
	db.assertLookupCountEq(t, 0, "ba")
	doLookup(t, db.cache, "cz")
	db.assertLookupCountEq(t, 0, "cz")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	doLookup(t, db.cache, "d")
	db.assertLookupCountEq(t, 1, "d")
	doLookup(t, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	doLookup(t, db.cache, "ij")
	db.assertLookupCountEq(t, 1, "ij")
	doLookup(t, db.cache, "jk")
	db.assertLookupCountEq(t, 0, "jk")
	doLookup(t, db.cache, "pn")
	db.assertLookupCountEq(t, 1, "pn")

	// Totally uncached ranges
	doLookup(t, db.cache, "vu")
	db.assertLookupCountEq(t, 2, "vu")
	doLookup(t, db.cache, "xx")
	db.assertLookupCountEq(t, 0, "xx")

	// Evict clears one level 1 and one level 2 cache
	if err := db.cache.EvictCachedRangeDescriptor(roachpb.RKey("da"), nil, false); err != nil {
		t.Fatal(err)
	}
	doLookup(t, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")
	doLookup(t, db.cache, "da")
	db.assertLookupCountEq(t, 2, "da")

	// Looking up a descriptor that lands on an end-key should work
	// without a cache miss.
	doLookup(t, db.cache, "a")
	db.assertLookupCountEq(t, 0, "a")

	// Attempt to compare-and-evict with a descriptor that is not equal to the
	// cached one; it should not alter the cache.
	if err := db.cache.EvictCachedRangeDescriptor(roachpb.RKey("cz"), &roachpb.RangeDescriptor{}, false); err != nil {
		t.Fatal(err)
	}
	_, evictToken := doLookup(t, db.cache, "cz")
	db.assertLookupCountEq(t, 0, "cz")
	// Now evict with the actual descriptor. The cache should clear the
	// descriptor and the cached meta key.
	if err := evictToken.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	doLookup(t, db.cache, "cz")
	db.assertLookupCountEq(t, 2, "cz")
}

// TestRangeCacheCoalescedRequests verifies that concurrent lookups for
// the same key will be coalesced onto the same database lookup.
func TestRangeCacheCoalescedRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	pauseLookupResumeAndAssert := func(key string, expected int64) {
		var wg, waitJoin sync.WaitGroup
		db.pauseRangeLookups()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			waitJoin.Add(1)
			go func() {
				doLookupWithToken(t, db.cache, key, nil, false, &waitJoin)
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

// TestRangeCacheDetectSplit verifies that when the cache detects a split
// it will properly coalesce all requests to the right half of the split and
// will prefetch the left half of the split.
func TestRangeCacheDetectSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	pauseLookupResumeAndAssert := func(key string, expected int64, evictToken *EvictionToken) {
		var wg, waitJoin sync.WaitGroup
		db.pauseRangeLookups()
		for i := 0; i < 3; i++ {
			wg.Add(1)
			waitJoin.Add(1)
			go func(id int) {
				// Each request goes to a different key.
				doLookupWithToken(t, db.cache, fmt.Sprintf("%s%d", key, id), evictToken, false, &waitJoin)
				wg.Done()
			}(i)
		}
		waitJoin.Wait()
		db.resumeRangeLookups()
		wg.Wait()
		db.assertLookupCountEq(t, expected, key)
	}

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an") and ["an"-"b").
	db.splitRange(t, roachpb.RKey("an"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	_, evictToken := doLookup(t, db.cache, "az")
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	if err := evictToken.EvictAndReplace(context.Background(), mismatchErrRange); err != nil {
		t.Fatal(err)
	}
	pauseLookupResumeAndAssert("az", 2, evictToken)

	// Both sides of the split are now correctly cached.
	doLookup(t, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")
	doLookup(t, db.cache, "az")
	db.assertLookupCountEq(t, 0, "az")
}

// Verifies that the end key of a stale descriptor is used as a request key
// when the request is for the reverse scan.
func TestRangeCacheDetectSplitReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an") and ["an"-"b").
	db.splitRange(t, roachpb.RKey("an"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	useReverseScan := true
	_, evictToken := doLookupWithToken(t, db.cache, "az", nil, useReverseScan, nil)
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	if err := evictToken.EvictAndReplace(context.Background(), mismatchErrRange); err != nil {
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
			doLookupWithToken(t, db.cache, key, evictToken, useReverseScan, &waitJoin)
			wg.Done()
		}(lookup.key, lookup.evictToken)
	}
	waitJoin.Wait()
	db.resumeRangeLookups()
	wg.Wait()
	db.assertLookupCount(t, 3, 4, "a and az")

	// Both are now correctly cached.
	doLookupWithToken(t, db.cache, "a", nil, useReverseScan, nil)
	db.assertLookupCountEq(t, 0, "a")
	doLookupWithToken(t, db.cache, "az", nil, useReverseScan, nil)
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

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an"), ["an"-"at"), ["at"-"b").
	db.splitRange(t, roachpb.RKey("an"))
	db.splitRange(t, roachpb.RKey("at"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	_, evictToken := doLookup(t, db.cache, "az")
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	if err := evictToken.EvictAndReplace(context.Background(), mismatchErrRange); err != nil {
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
					context.Background(), key, reqEvictToken,
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
	doLookup(t, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")
	doLookup(t, db.cache, "ao")
	db.assertLookupCountEq(t, 0, "ao")
	doLookup(t, db.cache, "az")
	db.assertLookupCountEq(t, 0, "az")
}

func TestRangeCacheUseIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	// A request initially looks up the range descriptor ["a"-"b") considering intents.
	abDesc, evictToken := doLookupConsideringIntents(t, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// The current descriptor is found to be stale, so it is evicted. The next cache
	// lookup should return the descriptor from the intents, without performing another
	// db lookup.
	if err := evictToken.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}
	abDescIntent, _ := doLookup(t, db.cache, "aa")
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

	defDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
	}

	cache := newRangeDescriptorCache(context.TODO(), nil, 2<<10)
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
	if err := cache.clearOverlappingCachedRangeDescriptors(minToBDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKey("b"))), minToBDesc)
	if _, desc, err := cache.getCachedRangeDescriptor(roachpb.RKey("b"), false); err != nil {
		t.Fatal(err)
	} else if desc != nil {
		t.Errorf("descriptor unexpectedly non-nil: %s", desc)
	}
	if err := cache.clearOverlappingCachedRangeDescriptors(bToMaxDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKeyMax)), bToMaxDesc)
	if _, desc, err := cache.getCachedRangeDescriptor(roachpb.RKey("b"), false); err != nil {
		t.Fatal(err)
	} else if desc != bToMaxDesc {
		t.Errorf("expected descriptor %s; got %s", bToMaxDesc, desc)
	}

	// Add default descriptor back which should remove two split descriptors.
	if err := cache.clearOverlappingCachedRangeDescriptors(defDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), defDesc)
	for _, key := range []roachpb.RKey{roachpb.RKey("a"), roachpb.RKey("b")} {
		if _, desc, err := cache.getCachedRangeDescriptor(key, false); err != nil {
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
	if err := cache.clearOverlappingCachedRangeDescriptors(bToCDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKey("c"))), bToCDesc)
	if _, desc, err := cache.getCachedRangeDescriptor(roachpb.RKey("c"), true); err != nil {
		t.Fatal(err)
	} else if desc != bToCDesc {
		t.Errorf("expected descriptor %s; got %s", bToCDesc, desc)
	}

	aToBDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}
	if err := cache.clearOverlappingCachedRangeDescriptors(aToBDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(mustMeta(roachpb.RKey("b"))), aToBDesc)
	if _, desc, err := cache.getCachedRangeDescriptor(roachpb.RKey("c"), true); err != nil {
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

	firstDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("zzz"),
	}
	restDesc := &roachpb.RangeDescriptor{
		StartKey: firstDesc.StartKey,
		EndKey:   roachpb.RKeyMax,
	}

	cache := newRangeDescriptorCache(context.TODO(), nil, 2<<10)
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
		if err := cache.clearOverlappingCachedRangeDescriptors(metaSplitDesc); err != nil {
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

	cache := newRangeDescriptorCache(context.TODO(), nil, 2<<10)
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
		cacheKey, targetRange, err := cache.getCachedRangeDescriptor(test.queryKey, true /* inclusive */)
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
