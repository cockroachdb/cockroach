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

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

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
		desc, _, err := db.cache.LookupRangeDescriptorWithEvictionToken(ctx, metaKey, nil, useReverseScan)
		if err != nil {
			return err
		}
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
	db.cache = NewRangeDescriptorCache(st, db, staticSize(2<<10))
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
	if (useReverseScan && !r.ContainsKeyInverted(keyAddr)) || (!useReverseScan && !r.ContainsKey(keyAddr)) {
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
	ctx := context.TODO()

	// Totally uncached range.
	//  Retrieves [meta(min),meta(g)) and [a,b).
	//  Prefetches [meta(g),meta(m)), [meta(m),meta(s)), [b,c), and [c,d).
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
	//  Retrieves [d,e).
	//  Prefetches [e,f) and [f,g).
	doLookup(ctx, t, db.cache, "d")
	db.assertLookupCountEq(t, 1, "d")
	doLookup(ctx, t, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	//  Retrieves [i,j).
	//  Prefetches [j,k) and [k,l).
	doLookup(ctx, t, db.cache, "ij")
	db.assertLookupCountEq(t, 1, "ij")
	doLookup(ctx, t, db.cache, "jk")
	db.assertLookupCountEq(t, 0, "jk")

	// Totally uncached range.
	//  Retrieves [meta(s),meta(max)) and [r,s).
	//  Prefetches [s,t) and [t,u).
	//
	// Notice that the lookup key "ra" will not initially go to
	// [meta(s),meta(max)), but instead will go to [meta(m),meta(s)). This is
	// an example where the RangeLookup scan will continue onto a new range.
	doLookup(ctx, t, db.cache, "ra")
	db.assertLookupCountEq(t, 2, "ra")

	// Metadata 2 ranges aren't cached, metadata 1 range is.
	//  Retrieves [v,w).
	//  Prefetches [w,x) and [x,max).
	doLookup(ctx, t, db.cache, "vu")
	db.assertLookupCountEq(t, 1, "vu")

	// Evicts [d,e).
	if err := db.cache.EvictCachedRangeDescriptor(ctx, roachpb.RKey("da"), nil, false); err != nil {
		t.Fatal(err)
	}
	// Evicts [meta(min),meta(g)).
	if err := db.cache.EvictCachedRangeDescriptor(ctx, keys.RangeMetaKey(roachpb.RKey("da")), nil, false); err != nil {
		t.Fatal(err)
	}
	doLookup(ctx, t, db.cache, "fa")
	db.assertLookupCountEq(t, 0, "fa")
	// Totally uncached range.
	//  Retrieves [meta(min),meta(g)) and [d,e).
	//  Prefetches [e,f) and [f,g).
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
	// descriptor.
	//  Evicts [c,d).
	if err := evictToken.Evict(ctx); err != nil {
		t.Fatal(err)
	}
	// Meta2 range is cached.
	//  Retrieves [c,d).
	//  Prefetches [c,e) and [e,f).
	doLookup(ctx, t, db.cache, "cz")
	db.assertLookupCountEq(t, 1, "cz")
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
// returns with an error indicating so. The result of the context cancellation
// differs between requests that lead RangeLookup requests and requests that
// coalesce onto existing RangeLookup requests.
// - If the context of a RangeLookup request follower is canceled, the follower
//   will stop waiting on the inflight request, but will not have an effect on
//   the inflight request.
// - If the context of a RangeLookup request leader is canceled, the lookup
//   itself will also be canceled. This means that any followers waiting on the
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
		if err := <-c; errors.Cause(err) != context.Canceled {
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
	pauseLookupResumeAndAssert("az", 1, evictToken)

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
	db.assertLookupCount(t, 2, 3, "a and az")

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
	// - "ao" and "at" will make a second lookup
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
				if !desc.ContainsKeyInverted(key) {
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
	db.assertLookupCountEq(t, 2, "an and az")
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

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10))
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
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("b"))), minToBDesc)
	if desc, err := cache.GetCachedRangeDescriptor(roachpb.RKey("b"), false); err != nil {
		t.Fatal(err)
	} else if desc != nil {
		t.Errorf("descriptor unexpectedly non-nil: %s", desc)
	}
	if _, err := cache.clearOverlappingCachedRangeDescriptors(ctx, bToMaxDesc); err != nil {
		t.Fatal(err)
	}
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKeyMax)), bToMaxDesc)
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
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("c"))), bToCDesc)
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
	cache.rangeCache.cache.Add(rangeCacheKey(keys.RangeMetaKey(roachpb.RKey("b"))), aToBDesc)
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

	firstDesc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKey("zzz"),
	}
	restDesc := roachpb.RangeDescriptor{
		StartKey: firstDesc.EndKey,
		EndKey:   roachpb.RKeyMax,
	}

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10))
	if err := cache.InsertRangeDescriptors(ctx, firstDesc, restDesc); err != nil {
		t.Fatal(err)
	}

	// Add new range, corresponding to splitting the first range at a meta key.
	metaSplitDesc := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		EndKey:   keys.RangeMetaKey(roachpb.RKey("foo")),
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

// TestGetCachedRangeDescriptorInverted verifies the correctness of the result
// that is returned by getCachedRangeDescriptor with inverted=true.
func TestGetCachedRangeDescriptorInverted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []*roachpb.RangeDescriptor{
		{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
		{StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("z")},
	}

	st := cluster.MakeTestingClusterSettings()
	cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10))
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
		targetRange, entry, err := cache.getCachedRangeDescriptorLocked(
			test.queryKey, true /* inverted */)
		cache.rangeCache.RUnlock()
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(targetRange, test.rng) {
			t.Fatalf("expect range %v, actual get %v", test.rng, targetRange)
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
	ctx := context.TODO()

	descAM1 := &roachpb.RangeDescriptor{
		StartKey:             roachpb.RKey("a"),
		EndKey:               roachpb.RKey("m"),
		Generation:           proto.Int64(1),
		GenerationComparable: proto.Bool(true),
	}
	descMZ3 := &roachpb.RangeDescriptor{
		StartKey:             roachpb.RKey("m"),
		EndKey:               roachpb.RKey("z"),
		Generation:           proto.Int64(3),
		GenerationComparable: proto.Bool(true),
	}

	descBY0 := &roachpb.RangeDescriptor{
		StartKey:             roachpb.RKey("b"),
		EndKey:               roachpb.RKey("y"),
		Generation:           proto.Int64(0),
		GenerationComparable: proto.Bool(true),
	}
	descBY2 := &roachpb.RangeDescriptor{
		StartKey:             roachpb.RKey("b"),
		EndKey:               roachpb.RKey("y"),
		Generation:           proto.Int64(2),
		GenerationComparable: proto.Bool(true),
	}
	descBY4 := &roachpb.RangeDescriptor{
		StartKey:             roachpb.RKey("b"),
		EndKey:               roachpb.RKey("y"),
		Generation:           proto.Int64(4),
		GenerationComparable: proto.Bool(true),
	}
	descBYIncomparable := &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("y"),
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
			name:         "generation comparable evict 0",
			insertDesc:   descBY0,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{descAM1, descMZ3},
		},
		{
			// descBY2 evicts descAM1, but not descMZ3 based on Generation. Since
			// there is an overlapping descriptor with higher Generation (descMZ3),
			// it is not inserted.
			name:         "generation comparable evict 1",
			insertDesc:   descBY2,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{nil, descMZ3},
		},
		{
			// descBY4 replaces both existing descriptors and it is inserted.
			name:         "generation comparable evict 2",
			insertDesc:   descBY4,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{descBY4, nil},
		},
		{
			// descBYIncomparable has an incomparable Generation, so it evicts all
			// overlapping descriptors. This behavior is clearly less desirable in
			// general, but there's no better option in this case.
			name:         "generation incomparable evict 2",
			insertDesc:   descBYIncomparable,
			queryKeys:    []roachpb.RKey{roachpb.RKey("b"), roachpb.RKey("y")},
			expectedDesc: []*roachpb.RangeDescriptor{descBYIncomparable, nil},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			cache := NewRangeDescriptorCache(st, nil, staticSize(2<<10))
			err := cache.InsertRangeDescriptors(ctx, *descAM1, *descMZ3, *tc.insertDesc)
			if err != nil {
				t.Fatal(err)
			}

			for index, queryKey := range tc.queryKeys {
				if actualDesc, err := cache.GetCachedRangeDescriptor(queryKey, false); err != nil {
					t.Fatal(err)
				} else if !tc.expectedDesc[index].Equal(actualDesc) {
					t.Errorf("expected descriptor %s; got %s", tc.expectedDesc[index], actualDesc)
				}
			}
		})
	}
}
