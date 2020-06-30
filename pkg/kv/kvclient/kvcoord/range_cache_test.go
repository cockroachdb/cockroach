// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testDescriptorDB struct {
	data            llrb.Tree
	cache           *RangeDescriptorCache
	lookupCount     int64
	disablePrefetch bool
	pauseChan       chan struct{}
	// listeners[key] is closed when a lookup on the key happens.
	listeners map[string]chan struct{}
}

type testDescriptorNode struct {
	*roachpb.RangeDescriptor
}

func (a testDescriptorNode) Compare(b llrb.Comparable) int {
	aKey := a.RangeDescriptor.EndKey
	bKey := b.(testDescriptorNode).RangeDescriptor.EndKey
	return bytes.Compare(aKey, bKey)
}

// notifyOn returns a channel that will be closed when the next lookup on key
// happens.
func (db *testDescriptorDB) notifyOn(key roachpb.RKey) <-chan struct{} {
	if db.listeners == nil {
		db.listeners = make(map[string]chan struct{})
	}
	ch := make(chan struct{})
	db.listeners[key.String()] = ch
	return ch
}

// getDescriptors scans the testDescriptorDB starting at the provided key in the
// specified direction and collects the first RangeDescriptors that it finds.
func (db *testDescriptorDB) getDescriptors(
	key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
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
			desc.Generation = desc.Generation + 1
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
	rs, _, err := db.getDescriptors(roachpb.RKeyMin, false /* useReverseScan */)
	if err != nil {
		return nil, err
	}
	return &rs[0], nil
}

func (db *testDescriptorDB) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// Notify the test of the lookup, if the test wants notifications.
	if ch, ok := db.listeners[key.String()]; ok {
		close(ch)
	}
	select {
	case <-db.pauseChan:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	atomic.AddInt64(&db.lookupCount, 1)
	rs, preRs, err := db.getDescriptors(key, useReverseScan)
	if err != nil {
		return nil, nil, err
	}

	if err := db.simulateLookupScan(ctx, key, &rs[0], useReverseScan); err != nil {
		return nil, nil, err
	}
	return rs, preRs, nil
}

// For each RangeLookup, we also perform a cache lookup for the descriptor
// which holds that key. This mimics the behavior of DistSender, which uses
// the cache when performing a ScanRequest over the meta range to find the
// desired descriptor.
//
// This isn't exactly correct, because DistSender will actually keep
// scanning until it prefetches the desired number of descriptors, but it's
// close enough for testing.
func (db *testDescriptorDB) simulateLookupScan(
	ctx context.Context, key roachpb.RKey, foundDesc *roachpb.RangeDescriptor, useReverseScan bool,
) error {
	metaKey := keys.RangeMetaKey(key)
	for {
		tok, err := db.cache.LookupWithEvictionToken(ctx, metaKey, EvictionToken{}, useReverseScan)
		if err != nil {
			return err
		}
		desc := tok.Desc()
		// If the descriptor for metaKey does not contain the EndKey of the
		// descriptor we're going to return, simulate a scan continuation.
		// This can happen in the case of meta2 splits.
		if desc.ContainsKey(keys.RangeMetaKey(foundDesc.EndKey)) {
			break
		}
		metaKey = desc.EndKey
	}
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
	newGen := val.Generation + 1
	db.data.Insert(testDescriptorNode{
		&roachpb.RangeDescriptor{
			StartKey:   val.StartKey,
			EndKey:     key,
			Generation: newGen,
		},
	})
	db.data.Insert(testDescriptorNode{
		&roachpb.RangeDescriptor{
			StartKey:   key,
			EndKey:     val.EndKey,
			Generation: newGen,
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
	// NOTE: The range descriptors created below are not initialized with a
	// generation. The ones created by splitting them will have generations,
	// though. Not putting generations in these initial ones is done for diversity
	// in the tests.
	td1 := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey(keys.Meta2Prefix),
	}
	td2 := &roachpb.RangeDescriptor{
		StartKey: td1.EndKey,
		EndKey:   roachpb.RKey(keys.MetaMax),
	}
	td3 := &roachpb.RangeDescriptor{
		StartKey: td2.EndKey,
		EndKey:   roachpb.RKeyMax,
	}
	db.data.Insert(testDescriptorNode{td1})
	db.data.Insert(testDescriptorNode{td2})
	db.data.Insert(testDescriptorNode{td3})
	db.resumeRangeLookups()
	return db
}

func staticSize(size int64) func() int64 {
	return func() int64 {
		return size
	}
}

func initTestDescriptorDB(t *testing.T) *testDescriptorDB {
	st := cluster.MakeTestingClusterSettings()
	db := newTestDescriptorDB()
	for i, char := range "abcdefghijklmnopqrstuvwx" {
		// Create splits on each character:
		//   [min,a), [a,b), [b,c), [c,d), [d,e), etc.
		db.splitRange(t, roachpb.RKey(string(char)))
		if i > 0 && i%6 == 0 {
			// Create meta2 splits on every 6th character:
			//   [meta(min),meta(g)), [meta(g),meta(m)), [meta(m),meta(s)), etc.
			db.splitRange(t, keys.RangeMetaKey(roachpb.RKey(string(char))))
		}
	}
	db.cache = NewRangeDescriptorCache(st, db, staticSize(2<<10), stop.NewStopper())
	return db
}

// assertLookupCountEq fails unless exactly the number of lookups have been observed.
func (db *testDescriptorDB) assertLookupCountEq(t *testing.T, exp int64, key string) {
	t.Helper()
	if exp != db.lookupCount {
		t.Errorf("expected lookup count %d after %s, was %d", exp, key, db.lookupCount)
	}
	db.lookupCount = 0
}

// assertLookupCountEq fails unless number of lookups observed is >= from and <= to.
func (db *testDescriptorDB) assertLookupCount(t *testing.T, from, to int64, key string) {
	t.Helper()
	if from > db.lookupCount || to < db.lookupCount {
		t.Errorf("expected lookup count in [%d, %d] after %s, was %d", from, to, key, db.lookupCount)
	}
	db.lookupCount = 0
}

func doLookup(
	ctx context.Context, rc *RangeDescriptorCache, key string,
) (*roachpb.RangeDescriptor, EvictionToken) {
	return doLookupWithToken(ctx, rc, key, EvictionToken{}, false)
}

func evict(ctx context.Context, rc *RangeDescriptorCache, entry *kvbase.RangeCacheEntry) bool {
	rc.rangeCache.Lock()
	defer rc.rangeCache.Unlock()
	ok, _ /* updatedEntry */ := rc.evictLocked(ctx, entry)
	return ok
}

func clearOlderOverlapping(
	ctx context.Context, rc *RangeDescriptorCache, desc *roachpb.RangeDescriptor,
) bool {
	ent := &kvbase.RangeCacheEntry{Desc: *desc}
	ok, _ /* newerEntry */ := rc.clearOlderOverlapping(ctx, ent)
	return ok
}

func doLookupWithToken(
	ctx context.Context,
	rc *RangeDescriptorCache,
	key string,
	evictToken EvictionToken,
	useReverseScan bool,
) (*roachpb.RangeDescriptor, EvictionToken) {
	// NOTE: This function panics on errors because it is often called from other
	// goroutines than the test's main one.

	returnToken, err := rc.lookupInternal(
		ctx, roachpb.RKey(key), evictToken, useReverseScan)
	if err != nil {
		panic(fmt.Sprintf("unexpected error from Lookup: %s", err))
	}
	desc := &returnToken.entry.Desc
	keyAddr, err := keys.Addr(roachpb.Key(key))
	if err != nil {
		panic(err)
	}
	if (useReverseScan && !desc.ContainsKeyInverted(keyAddr)) ||
		(!useReverseScan && !desc.ContainsKey(keyAddr)) {
		panic(fmt.Sprintf("Returned range did not contain key: %s-%s, %s",
			desc.StartKey, desc.EndKey, key))
	}
	return desc, returnToken
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
	expKeyMin := keys.RangeMetaKey(keys.RangeMetaKey(keys.RangeMetaKey(roachpb.RKey("test"))))
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
	ctx := context.Background()

	// Totally uncached range.
	//  Retrieves [meta(min),meta(g)) and [a,b).
	//  Prefetches [meta(g),meta(m)), [meta(m),meta(s)), [b,c), and [c,d).
	doLookup(ctx, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// Descriptors for the following ranges should be cached.
	doLookup(ctx, db.cache, "ab")
	db.assertLookupCountEq(t, 0, "ab")
	doLookup(ctx, db.cache, "ba")
	db.assertLookupCountEq(t, 0, "ba")
	doLookup(ctx, db.cache, "cz")
	db.assertLookupCountEq(t, 0, "cz")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	//  Retrieves [d,e).
	//  Prefetches [e,f) and [f,g).
	_, deTok := doLookup(ctx, db.cache, "d")
	db.assertLookupCountEq(t, 1, "d")
	doLookup(ctx, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	//  Retrieves [i,j).
	//  Prefetches [j,k) and [k,l).
	doLookup(ctx, db.cache, "ij")
	db.assertLookupCountEq(t, 1, "ij")
	doLookup(ctx, db.cache, "jk")
	db.assertLookupCountEq(t, 0, "jk")

	// Totally uncached range.
	//  Retrieves [meta(s),meta(max)) and [r,s).
	//  Prefetches [s,t) and [t,u).
	//
	// Notice that the lookup key "ra" will not initially go to
	// [meta(s),meta(max)), but instead will go to [meta(m),meta(s)). This is
	// an example where the RangeLookup scan will continue onto a new range.
	doLookup(ctx, db.cache, "ra")
	db.assertLookupCountEq(t, 2, "ra")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	//  Retrieves [v,w).
	//  Prefetches [w,x) and [x,max).
	doLookup(ctx, db.cache, "vu")
	db.assertLookupCountEq(t, 1, "vu")

	// Evicts [d,e).
	require.True(t, evict(ctx, db.cache, deTok.entry))
	// Evicts [meta(min),meta(g)).
	require.True(t, db.cache.EvictByKey(ctx, keys.RangeMetaKey(roachpb.RKey("da"))))
	doLookup(ctx, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")
	// Totally uncached range.
	//  Retrieves [meta(min),meta(g)) and [d,e).
	//  Prefetches [e,f) and [f,g).
	doLookup(ctx, db.cache, "da")
	db.assertLookupCountEq(t, 2, "da")

	// Looking up a descriptor that lands on an end-key should work
	// without a cache miss.
	doLookup(ctx, db.cache, "a")
	db.assertLookupCountEq(t, 0, "a")

	// Attempt to compare-and-evict with a cache entry that is not equal to the
	// cached one; it should not alter the cache.
	desc, _ := doLookup(ctx, db.cache, "cz")
	require.False(t, evict(ctx, db.cache, &kvbase.RangeCacheEntry{Desc: *desc}))

	_, evictToken := doLookup(ctx, db.cache, "cz")
	db.assertLookupCountEq(t, 0, "cz")
	// Now evict with the actual cache entry, which should succeed.
	//  Evicts [c,d).
	evictToken.Evict(ctx)
	// Meta2 range is cached.
	//  Retrieves [c,d).
	//  Prefetches [c,e) and [e,f).
	doLookup(ctx, db.cache, "cz")
	db.assertLookupCountEq(t, 1, "cz")
}

// TestRangeCacheCoalescedRequests verifies that concurrent lookups for
// the same key will be coalesced onto the same database lookup.
func TestRangeCacheCoalescedRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.Background()

	pauseLookupResumeAndAssert := func(key string, expected int64) {
		var wg sync.WaitGroup
		db.pauseRangeLookups()

		// We're going to perform 3 lookups on the same key, in parallel, while
		// lookups are paused. Either they're all expected to get cache hits (in the
		// case where expected == 0), or there will be one request actually blocked
		// in the db and the other two will get coalesced onto it.
		var coalesced chan struct{}
		if expected > 0 {
			coalesced = make(chan struct{})
			db.cache.coalesced = coalesced
		}
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				doLookupWithToken(ctx, db.cache, key, EvictionToken{}, false)
				wg.Done()
			}()
		}

		// Wait for requests to be coalesced before unblocking the db.
		if coalesced != nil {
			for i := 0; i < 2; i++ {
				<-coalesced
			}
		}

		db.resumeRangeLookups()
		wg.Wait()
		db.assertLookupCountEq(t, expected, key)
	}

	// Totally uncached range.
	//  Retrieves [meta(min),meta(g)) and [a,b).
	//  Prefetches [meta(g),meta(m)), [meta(m),meta(s)), [b,c), and [c,d).
	pauseLookupResumeAndAssert("aa", 2)

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	//  Retrieves [d,e).
	//  Prefetches [e,f) and [f,g).
	pauseLookupResumeAndAssert("d", 1)
	pauseLookupResumeAndAssert("ea", 0)
}

// TestRangeCacheContextCancellation tests the behavior that for an ongoing
// RangeDescriptor lookup, if the context passed in gets canceled the lookup
// returns with an error indicating so. Canceling the ctx does not stop the
// in-flight lookup though (even though the requester has returned from
// lookupInternal()) - other requesters that joined the same
// flight are unaffected by the ctx cancelation.
func TestRangeCacheContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)

	// lookupAndWaitUntilJoin performs a RangeDescriptor lookup in a new
	// goroutine and blocks until the request is added to the inflight request
	// map. It returns a channel that transmits the error return value from the
	// lookup.
	lookupAndWaitUntilJoin := func(ctx context.Context, key roachpb.RKey, expectDBLookup bool) chan error {
		errC := make(chan error)
		var blocked <-chan struct{}
		if expectDBLookup {
			blocked = db.notifyOn(key)
		} else {
			ch := make(chan struct{})
			db.cache.coalesced = ch
			blocked = ch
		}
		go func() {
			_, err := db.cache.lookupInternal(ctx, key, EvictionToken{}, false)
			errC <- err
		}()
		<-blocked
		return errC
	}

	expectContextCancellation := func(t *testing.T, c <-chan error) {
		t.Helper()
		if err := <-c; !errors.Is(err, context.Canceled) {
			t.Errorf("expected context cancellation error, found %v", err)
		}
	}
	expectNoError := func(t *testing.T, c <-chan error) {
		t.Helper()
		if err := <-c; err != nil {
			t.Errorf("unexpected error, found %v", err)
		}
	}

	ctx1, cancel := context.WithCancel(context.Background()) // leader
	ctx2 := context.Background()
	ctx3 := context.Background()

	db.pauseRangeLookups()
	key1 := roachpb.RKey("aa")
	errC1 := lookupAndWaitUntilJoin(ctx1, key1, true)
	errC2 := lookupAndWaitUntilJoin(ctx2, key1, false)

	// Cancel the leader and check that it gets an error.
	cancel()
	expectContextCancellation(t, errC1)

	// While lookups are still blocked, launch another one. This new request
	// should join the flight just like c2.
	errC3 := lookupAndWaitUntilJoin(ctx3, key1, false)

	// Let the flight finish.
	db.resumeRangeLookups()
	expectNoError(t, errC2)
	expectNoError(t, errC3)
}

// TestRangeCacheDetectSplit verifies that when the cache detects a split
// it will properly coalesce all requests to the right half of the split and
// will prefetch the left half of the split.
func TestRangeCacheDetectSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.Background()

	pauseLookupResumeAndAssert := func(key string, expected int64, evictToken EvictionToken) {
		var wg sync.WaitGroup
		db.pauseRangeLookups()

		// We're going to perform 3 lookups on the same key, in parallel, while
		// lookups are paused. Either they're all expected to get cache hits (in the
		// case where expected == 0), or there will be one request actually blocked
		// in the db and the other two will get coalesced onto it.
		var coalesced chan struct{}
		if expected > 0 {
			coalesced = make(chan struct{})
			db.cache.coalesced = coalesced
		}

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(id int) {
				// Each request goes to a different key.
				doLookupWithToken(ctx, db.cache, fmt.Sprintf("%s%d", key, id), evictToken, false)
				wg.Done()
			}(i)
		}
		// Wait for requests to be coalesced before unblocking the db.
		if coalesced != nil {
			for i := 0; i < 2; i++ {
				<-coalesced
			}
		}

		db.resumeRangeLookups()
		wg.Wait()
		db.assertLookupCountEq(t, expected, key)
	}

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(ctx, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an") and ["an"-"b").
	db.splitRange(t, roachpb.RKey("an"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	_, evictToken := doLookup(ctx, db.cache, "az")
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	evictToken.EvictAndReplace(ctx, roachpb.RangeInfo{Desc: mismatchErrRange})
	pauseLookupResumeAndAssert("az", 1, evictToken)

	// Both sides of the split are now correctly cached.
	doLookup(ctx, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")
	doLookup(ctx, db.cache, "az")
	db.assertLookupCountEq(t, 0, "az")
}

// Verifies that the end key of a stale descriptor is used as a request key
// when the request is for the reverse scan.
func TestRangeCacheDetectSplitReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.Background()

	// A request initially looks up the range descriptor ["a"-"b").
	doLookup(ctx, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// A split breaks up the range into ["a"-"an") and ["an"-"b").
	db.splitRange(t, roachpb.RKey("an"))

	// A request is sent to the stale descriptor on the right half
	// such that a RangeKeyMismatchError is returned.
	useReverseScan := true
	_, evictToken := doLookupWithToken(ctx, db.cache, "az", EvictionToken{}, useReverseScan)
	// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
	ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
	if pErr != nil {
		t.Fatal(pErr)
	}
	mismatchErrRange := ranges[0]
	// The stale descriptor is evicted, the new descriptor from the error is
	// replaced, and a new lookup is initialized.
	// Evict the cached descriptor ["a", "b") and insert ["a"-"an")
	evictToken.EvictAndReplace(ctx, roachpb.RangeInfo{Desc: mismatchErrRange})

	// Create two lookup requests with key "a" and "az". The lookup on "az" uses
	// the evictToken returned by the previous lookup.
	//
	// The requests will *not* be coalesced, and two different descriptors should
	// be returned ([KeyMin-,"a") and ["an-b")).
	lookups := []struct {
		key        string
		evictToken EvictionToken
	}{
		{"a", EvictionToken{}},
		{"az", evictToken},
	}
	db.pauseRangeLookups()
	var wg, waitJoin sync.WaitGroup
	for _, lookup := range lookups {
		wg.Add(1)
		blocked := db.notifyOn(roachpb.RKey(lookup.key))
		go func(key string, evictToken EvictionToken) {
			doLookupWithToken(ctx, db.cache, key, evictToken, useReverseScan)
			wg.Done()
		}(lookup.key, lookup.evictToken)
		<-blocked
	}
	waitJoin.Wait()
	db.resumeRangeLookups()
	wg.Wait()
	db.assertLookupCount(t, 2, 3, "a and az")

	// Both are now correctly cached.
	doLookupWithToken(ctx, db.cache, "a", EvictionToken{}, useReverseScan)
	db.assertLookupCountEq(t, 0, "a")
	doLookupWithToken(ctx, db.cache, "az", EvictionToken{}, useReverseScan)
	db.assertLookupCountEq(t, 0, "az")
}

// Test that the range cache deals with situations where requests have to be
// retried internally because they've been wrongly-coalesced the first time
// around.
func TestRangeCacheHandleDoubleSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The tests starts with the descriptor [a-an) in the cache.
	// There are 3 ranges of interest: [a-an)[an-at)[at-b).
	// We're going to start a bunch of range lookups in order, and we'll assert
	// what happens to each one.
	type exp int
	const (
		cacheHit exp = iota
		lookupLeader
		lookupCoalesced
		lookupWronglyCoalesced
	)
	testCases := []struct {
		reverseScan bool
		keys        []struct {
			key string
			exp exp
		}
	}{
		{
			// [forward case]
			// - "aa" will hit the cache
			// - all others will join a coalesced request to "an"
			//   + will lookup the meta2 desc
			//   + will lookup the ["an"-"at") desc
			// - "an" and "ao" will get the correct range back
			// - "at" and "az" will make a second lookup
			//   + will lookup the ["at"-"b") desc
			reverseScan: false,
			keys: []struct {
				key string
				exp exp
			}{
				{key: "aa", exp: cacheHit},
				{key: "an", exp: lookupLeader},
				{key: "ao", exp: lookupCoalesced},
				{key: "at", exp: lookupWronglyCoalesced},
				{key: "az", exp: lookupWronglyCoalesced},
			},
		},
		{
			// [reverse case]
			// - "aa" and "an" will hit the cache
			// - all others will join a coalesced request to "ao"
			//   + will lookup the meta2 desc
			//   + will lookup the ["at"-"b") desc
			// - "ao" will get the right range back
			// - "at" and "az" will make a second lookup
			//   + will lookup the ["an"-"at") desc
			reverseScan: true,
			keys: []struct {
				key string
				exp exp
			}{
				{key: "aa", exp: cacheHit},
				{key: "an", exp: cacheHit},
				{key: "ao", exp: lookupLeader},
				{key: "at", exp: lookupCoalesced},
				{key: "az", exp: lookupWronglyCoalesced},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("reverse=%t", tc.reverseScan), func(t *testing.T) {
			db := initTestDescriptorDB(t)
			db.disablePrefetch = true
			ctx := context.Background()

			// A request initially looks up the range descriptor ["a"-"b").
			doLookup(ctx, db.cache, "aa")
			db.assertLookupCountEq(t, 2, "aa")

			// A split breaks up the range into ["a"-"an"), ["an"-"at"), ["at"-"b").
			db.splitRange(t, roachpb.RKey("an"))
			db.splitRange(t, roachpb.RKey("at"))

			// A request is sent to the stale descriptor on the right half
			// such that a RangeKeyMismatchError is returned.
			_, evictToken := doLookup(ctx, db.cache, "az")
			// mismatchErrRange mocks out a RangeKeyMismatchError.Range response.
			ranges, _, pErr := db.getDescriptors(roachpb.RKey("aa"), false)
			if pErr != nil {
				t.Fatal(pErr)
			}
			mismatchErrRange := ranges[0]
			// The stale descriptor is evicted, the new descriptor from the error is
			// replaced, and a new lookup is initialized.
			evictToken.EvictAndReplace(ctx, roachpb.RangeInfo{Desc: mismatchErrRange})

			// wg will be used to wait for all the lookups to complete.
			wg := sync.WaitGroup{}
			wg.Add(len(tc.keys))

			// lookup will kick of an async range lookup. If the request is expected
			// to block by either going to the db or be coalesced onto another
			// request, this function will wait until the request gets blocked.
			lookup := func(key roachpb.RKey, exp exp) {
				var blocked <-chan struct{}
				var expLog string
				switch exp {
				case lookupLeader:
					blocked = db.notifyOn(key)
				case lookupWronglyCoalesced:
					expLog = "bad lookup coalescing; retrying"
					ch := make(chan struct{})
					db.cache.coalesced = ch
					blocked = ch
				case lookupCoalesced:
					expLog = "coalesced range lookup request onto in-flight one"
					ch := make(chan struct{})
					db.cache.coalesced = ch
					blocked = ch
				}

				go func(ctx context.Context) {
					defer wg.Done()
					var desc *roachpb.RangeDescriptor
					// Each request goes to a different key.
					var err error
					ctx, getRecording, cancel := tracing.ContextWithRecordingSpan(ctx, "test")
					defer cancel()
					tok, err := db.cache.lookupInternal(
						ctx, key, evictToken,
						tc.reverseScan)
					require.NoError(t, err)
					desc = &tok.entry.Desc
					if tc.reverseScan {
						if !desc.ContainsKeyInverted(key) {
							t.Errorf("desc %s does not contain exclusive end key %s", desc, key)
						}
					} else {
						if !desc.ContainsKey(key) {
							t.Errorf("desc %s does not contain key %s", desc, key)
						}
					}
					if expLog != "" {
						rec := getRecording()
						_, ok := rec.FindLogMessage(expLog)
						if !ok {
							t.Errorf("didn't find expected message in trace for %s: %s. Recording:\n%s",
								key, expLog, rec)
						}
					}
				}(ctx)

				// If we're expecting this request to block, wait for that.
				if blocked != nil {
					select {
					case <-blocked:
					case <-time.After(10 * time.Second):
						t.Errorf("request didn't block:%s", key)
					}
				}
				// Reset the notification channel; if the lookup is internally retried
				// we won't be waiting for a 2nd notification.
				db.cache.coalesced = nil
			}

			// Block all the lookups at the db level.
			db.pauseRangeLookups()
			// Kick off all the lookups, in order. The cache hits will finish, the rest
			// will get blocked.
			for _, look := range tc.keys {
				lookup(roachpb.RKey(look.key), look.exp)
			}

			// All the requests that didn't hit the cache are now blocked. Unblock
			// them.
			db.resumeRangeLookups()
			// Wait for all requests to finish.
			wg.Wait()
			db.assertLookupCountEq(t, 2, "an and az")

			// All three descriptors are now correctly cached.
			doLookup(ctx, db.cache, "aa")
			db.assertLookupCountEq(t, 0, "aa")
			doLookup(ctx, db.cache, "ao")
			db.assertLookupCountEq(t, 0, "ao")
			doLookup(ctx, db.cache, "az")
			db.assertLookupCountEq(t, 0, "az")
		})
	}
}

func TestRangeCacheUseIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := initTestDescriptorDB(t)
	ctx := context.Background()

	// A request initially looks up the range descriptor ["a"-"b").
	abDesc, evictToken := doLookup(ctx, db.cache, "aa")
	db.assertLookupCountEq(t, 2, "aa")

	// Perform a lookup now that the cache is populated.
	abDescLookup, _ := doLookup(ctx, db.cache, "aa")
	db.assertLookupCountEq(t, 0, "aa")

	// The descriptors should be the same.
	if !reflect.DeepEqual(abDesc, abDescLookup) {
		t.Errorf("expected initial range descriptor to be returned from lookup, found %v", abDescLookup)
	}

	// The current descriptor is found to be stale, so it is evicted. The next cache
	// lookup should return the descriptor from the intents, without performing another
	// db lookup.
	evictToken.Evict(ctx)
	abDescIntent, _ := doLookup(ctx, db.cache, "aa")
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
	ctx := context.Background()

	defDesc := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKeyMin,
		EndKey:     roachpb.RKeyMax,
		Generation: 0,
	}

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10), stop.NewStopper())
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), &kvbase.RangeCacheEntry{Desc: *defDesc})

	// Now, add a new, overlapping set of descriptors.
	minToBDesc := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKeyMin,
		EndKey:     roachpb.RKey("b"),
		Generation: 1,
	}
	bToMaxDesc := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("b"),
		EndKey:     roachpb.RKeyMax,
		Generation: 1,
	}
	curGeneration := int64(1)
	require.True(t, clearOlderOverlapping(ctx, cache, minToBDesc))
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("b"))), &kvbase.RangeCacheEntry{Desc: *minToBDesc})
	if desc := cache.GetCached(roachpb.RKey("b"), false); desc != nil {
		t.Errorf("descriptor unexpectedly non-nil: %s", desc)
	}

	require.True(t, clearOlderOverlapping(ctx, cache, bToMaxDesc))
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), &kvbase.RangeCacheEntry{Desc: *bToMaxDesc})
	ri := cache.GetCached(roachpb.RKey("b"), false)
	require.Equal(t, *bToMaxDesc, ri.Desc)

	// Add default descriptor back which should remove two split descriptors.
	defDescCpy := *defDesc
	curGeneration++
	defDescCpy.Generation = curGeneration
	require.True(t, clearOlderOverlapping(ctx, cache, &defDescCpy))
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), &kvbase.RangeCacheEntry{Desc: defDescCpy})
	for _, key := range []roachpb.RKey{roachpb.RKey("a"), roachpb.RKey("b")} {
		ri = cache.GetCached(key, false)
		require.Equal(t, defDescCpy, ri.Desc)
	}

	// Insert ["b", "c") and then insert ["a", b"). Verify that the former is not evicted by the latter.
	curGeneration++
	bToCDesc := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("b"),
		EndKey:     roachpb.RKey("c"),
		Generation: curGeneration,
	}
	require.True(t, clearOlderOverlapping(ctx, cache, bToCDesc))
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("c"))), &kvbase.RangeCacheEntry{Desc: *bToCDesc})
	ri = cache.GetCached(roachpb.RKey("c"), true)
	require.Equal(t, *bToCDesc, ri.Desc)

	curGeneration++
	aToBDesc := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("a"),
		EndKey:     roachpb.RKey("b"),
		Generation: curGeneration,
	}
	require.True(t, clearOlderOverlapping(ctx, cache, aToBDesc))
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("b"))), ri)
	ri = cache.GetCached(roachpb.RKey("c"), true)
	require.Equal(t, *bToCDesc, ri.Desc)
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
	ctx := context.Background()

	firstDesc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("zzz"),
	}
	restDesc := roachpb.RangeDescriptor{
		StartKey: firstDesc.EndKey,
		EndKey:   roachpb.RKeyMax,
	}

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10), stop.NewStopper())
	cache.Insert(ctx,
		roachpb.RangeInfo{Desc: firstDesc},
		roachpb.RangeInfo{Desc: restDesc})

	// Add new range, corresponding to splitting the first range at a meta key.
	metaSplitDesc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   keys.RangeMetaKey(roachpb.RKey("foo")),
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("invocation of clearOlderOverlapping panicked: %v", r)
			}
		}()
		cache.clearOlderOverlapping(ctx, &kvbase.RangeCacheEntry{Desc: metaSplitDesc})
	}()
}

// TestGetCachedRangeDescriptorInverted verifies the correctness of the result
// that is returned by getCachedRangeDescriptor with inverted=true.
func TestGetCachedRangeDescriptorInverted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []roachpb.RangeDescriptor{
		{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
		{StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("z")},
	}

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10), stop.NewStopper())
	for _, rd := range testData {
		cache.rangeCache.cache.Add(
			rangeCacheKey(keys.RangeMetaKey(rd.EndKey)), &kvbase.RangeCacheEntry{Desc: rd})
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
		targetRange, entry := cache.getCachedLocked(test.queryKey, true /* inverted */)
		cache.rangeCache.RUnlock()

		if test.rng == nil {
			require.Nil(t, targetRange)
		} else {
			require.NotNil(t, targetRange)
			require.Equal(t, *test.rng, targetRange.Desc)
		}
		var cacheKey rangeCacheKey
		if entry != nil {
			cacheKey = entry.Key.(rangeCacheKey)
		}
		if !reflect.DeepEqual(cacheKey, test.cacheKey) {
			t.Fatalf("expect cache key %v, actual get %v", test.cacheKey, cacheKey)
		}
	}
}

func TestRangeCacheGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	descAM1 := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("a"),
		EndKey:     roachpb.RKey("m"),
		Generation: 1,
	}
	descMZ3 := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("m"),
		EndKey:     roachpb.RKey("z"),
		Generation: 3,
	}

	descBY0 := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("b"),
		EndKey:     roachpb.RKey("y"),
		Generation: 0,
	}
	descBY2 := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("b"),
		EndKey:     roachpb.RKey("y"),
		Generation: 2,
	}
	descBY4 := &roachpb.RangeDescriptor{
		StartKey:   roachpb.RKey("b"),
		EndKey:     roachpb.RKey("y"),
		Generation: 4,
	}

	testCases := []struct {
		name         string
		insertDesc   *roachpb.RangeDescriptor
		queryKeys    []roachpb.RKey
		expectedDesc []*roachpb.RangeDescriptor
	}{
		{
			// descBY0 is ignored since the existing keyspace is covered by
			// descriptors of generations 1 and 3, respectively.
			name:         "evict 0",
			insertDesc:   descBY0,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{descAM1, descMZ3},
		},
		{
			// descBY2 evicts descAM1, but not descMZ3 based on Generation. Since
			// there is an overlapping descriptor with higher Generation (descMZ3),
			// it is not inserted.
			name:         "evict 1",
			insertDesc:   descBY2,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{nil, descMZ3},
		},
		{
			// descBY4 replaces both existing descriptors and it is inserted.
			name:         "evict 2",
			insertDesc:   descBY4,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{descBY4, nil},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10), stop.NewStopper())
			cache.Insert(ctx,
				roachpb.RangeInfo{Desc: *descAM1},
				roachpb.RangeInfo{Desc: *descMZ3},
				roachpb.RangeInfo{Desc: *tc.insertDesc})

			for index, queryKey := range tc.queryKeys {
				ri := cache.GetCached(queryKey, false)
				exp := tc.expectedDesc[index]
				if exp == nil {
					require.Nil(t, ri)
				} else {
					require.NotNil(t, ri)
					require.NotNil(t, *exp, ri.Desc)
				}
			}
		})
	}
}

func TestRangeCacheUpdateLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	rep1 := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	rep2 := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rep3 := roachpb.ReplicaDescriptor{
		NodeID:    3,
		StoreID:   3,
		ReplicaID: 3,
	}
	repNonMember := roachpb.ReplicaDescriptor{
		NodeID:    4,
		StoreID:   4,
		ReplicaID: 4,
	}
	desc1 := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			rep1, rep2,
		},
		Generation: 0,
	}
	desc2 := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			rep2, rep3,
		},
		Generation: 1,
	}
	desc3 := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			rep1, rep2,
		},
		Generation: 2,
	}
	startKey := desc1.StartKey

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10), stop.NewStopper())

	cache.Insert(ctx, roachpb.RangeInfo{
		Desc:  desc1,
		Lease: roachpb.Lease{},
	})

	// Check that initially the cache has an empty lease. Then, we'll UpdateLease().
	tok, err := cache.LookupWithEvictionToken(
		ctx, desc1.StartKey, EvictionToken{}, false /* useReverseScan */)
	require.NoError(t, err)
	require.Nil(t, tok.Lease())

	l := &roachpb.Lease{
		Replica: rep1,
	}
	oldTok := tok
	tok, ok := tok.UpdateLease(ctx, l)
	require.True(t, ok)
	require.Equal(t, oldTok.Desc(), tok.Desc())
	ri := cache.GetCached(startKey, false /* inverted */)
	require.NotNil(t, ri)
	require.Equal(t, rep1, ri.Lease.Replica)

	tok = tok.ClearLease(ctx)
	ri = cache.GetCached(startKey, false /* inverted */)
	require.NotNil(t, ri)
	require.True(t, ri.Lease.Empty())
	require.NotNil(t, tok)

	// Check that trying to update the lease to a non-member replica results
	// in a nil return and the entry's eviction.
	l = &roachpb.Lease{
		Replica: repNonMember,
	}
	tok, ok = tok.UpdateLease(ctx, l)
	require.False(t, ok)
	require.True(t, tok.Empty())
	ri = cache.GetCached(startKey, false /* inverted */)
	require.Nil(t, ri)

	// Check that updating the lease while the cache has a newer descriptor
	// returns the newer descriptor.

	cache.Insert(ctx, roachpb.RangeInfo{
		Desc:  desc1,
		Lease: roachpb.Lease{},
	})
	tok, err = cache.LookupWithEvictionToken(
		ctx, desc1.StartKey, EvictionToken{}, false /* useReverseScan */)
	require.NoError(t, err)

	// Update the cache.
	cache.Insert(ctx, roachpb.RangeInfo{
		Desc:  desc2,
		Lease: roachpb.Lease{},
	})
	tok, ok = tok.UpdateLease(ctx,
		// Specify a lease compatible with desc2.
		&roachpb.Lease{Replica: rep2},
	)
	require.True(t, ok)
	require.NotNil(t, tok)
	require.Equal(t, tok.Desc(), &desc2)
	require.Equal(t, tok.Lease().Replica, rep2)

	// Update the cache again.
	cache.Insert(ctx, roachpb.RangeInfo{
		Desc:  desc3,
		Lease: roachpb.Lease{},
	})
	// This time try to specify a lease that's not compatible with the desc. The
	// entry should end up evicted from the cache.
	tok, ok = tok.UpdateLease(ctx, &roachpb.Lease{Replica: rep3})
	require.False(t, ok)
	require.True(t, tok.Empty())
	ri = cache.GetCached(startKey, false /* inverted */)
	require.Nil(t, ri)
}

func TestRangeCacheEntryUpdateLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rep1 := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}
	rep2 := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	repNonMember := roachpb.ReplicaDescriptor{
		NodeID:    3,
		StoreID:   3,
		ReplicaID: 3,
	}
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			rep1, rep2,
		},
		Generation: 0,
	}

	e := &kvbase.RangeCacheEntry{
		Desc:  desc,
		Lease: roachpb.Lease{},
	}

	// Check that some lease overwrites an empty lease.
	l := &roachpb.Lease{
		Replica:  rep1,
		Sequence: 1,
	}
	ok, e := e.UpdateLease(l)
	require.True(t, ok)
	require.True(t, l.Equal(&e.Lease))

	// Check that a lease with no sequence number overwrites any other lease.
	l = &roachpb.Lease{
		Replica:  rep1,
		Sequence: 0,
	}
	ok, e = e.UpdateLease(l)
	require.True(t, ok)
	require.True(t, l.Equal(e.Lease))

	// Check that another lease with no seq num overwrites a lease with no seq num.
	l = &roachpb.Lease{
		Replica:  rep2,
		Sequence: 0,
	}
	ok, e = e.UpdateLease(l)
	require.True(t, ok)
	require.True(t, l.Equal(e.Lease))

	// Check that another lease with no seq num overwrites a lease with no seq num.
	l = &roachpb.Lease{
		Replica:  rep1,
		Sequence: 0,
	}
	ok, e = e.UpdateLease(l)
	require.True(t, ok)
	require.True(t, l.Equal(e.Lease))

	// Set a lease
	l = &roachpb.Lease{
		Replica:  rep1,
		Sequence: 2,
	}
	ok, e = e.UpdateLease(l)
	require.True(t, ok)
	require.True(t, l.Equal(e.Lease))

	// Check that updating to an older lease doesn't work.
	l = &roachpb.Lease{
		Replica:  rep2,
		Sequence: 1,
	}
	ok, e = e.UpdateLease(l)
	require.False(t, ok)
	require.False(t, l.Equal(e.Lease))

	// Check that updating to a lease at the same sequence as the existing one works.
	l = &roachpb.Lease{
		Replica:  rep2,
		Sequence: 2,
	}
	ok, e = e.UpdateLease(l)
	require.True(t, ok)
	require.True(t, l.Equal(e.Lease))

	// Check that updating the lease to a non-member replica returns a nil
	// entry.
	l = &roachpb.Lease{
		Replica:  repNonMember,
		Sequence: 0,
	}
	ok, e = e.UpdateLease(l)
	require.True(t, ok)
	require.Nil(t, e)
}
