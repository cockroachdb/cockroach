// Copyright 2017 The Cockroach Authors.
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

package client

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// RangeLookup is used to look up RangeDescriptors - a RangeDescriptor is a
// metadata structure which describes the key range and replica locations of a
// distinct range in the cluster. They map the logical keyspace in cockroach to
// its physical replicas, allowing a node to send requests for a certain key to
// the replicas that contain that key.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix prepended. The Range Metadata
// Key for an ordinary key can be generated with the `keys.RangeMetaKey(key)`
// function. The RangeDescriptor for the range which contains a given key can be
// retrieved by generating its Range Metadata Key and scanning from the key
// forwards until the first RangeDescriptor is found. This is what this function
// does with the provided key.
//
// Note that the Range Metadata Key sent as the StartKey of the lookup scan is
// NOT the key at which the desired RangeDescriptor is stored. Instead, this
// method returns the RangeDescriptor stored at the _lowest_ existing key which
// is _greater_ than the given key. The returned RangeDescriptor will thus
// contain the ordinary key which was provided to this function.
//
// The "Range Metadata Key" for a range is built by appending the end key of the
// range to the respective meta prefix.
//
//
// It is often useful to think of Cockroach's ranges as existing in a three
// level tree:
//
//            [/meta1/,/meta1/max)   <-- always one range, gossipped, start here!
//                     |
//          -----------------------
//          |                     |
//  [/meta2/,/meta2/m)   [/meta2/m,/meta2/max)
//          |                     |
//      ---------             ---------
//      |       |             |       |
//    [a,g)   [g,m)         [m,s)   [s,max)   <- user data
//
// In this analogy, each node (range) contains a number of RangeDescriptors, and
// these descriptors act as pointers to the location of its children. So given a
// key we want to find, we can use the tree structure to find it, starting at
// the tree's root (meta1). But starting at the root, how do we know which
// pointer to follow? This is where RangeMetaKey comes into play - it turns a
// key in one range into a meta key in its parent range. Then, when looking at
// its parent range, we know that the descriptor we want is the first descriptor
// to the right of this meta key in the parent's ordered set of keys.
//
//
// Let's look at a few examples that demonstrate how RangeLookup performs this
// task of finding a user RangeDescriptors from cached meta2 descriptors:
//
// Ex. 1:
//  Meta2 Ranges: [/meta2/a,  /meta2/z)
//  User  Ranges: [a, f) [f, p), [p, z)
//  1.a: RangeLookup(key=f)
//   In this case, we want to look up the range descriptor for the range [f, p)
//   because "f" is in that range. Remember that this descriptor will be stored
//   at "/meta2/p". Of course, when we're performing the RangeLookup, we don't
//   actually know what the bounds of this range are or where exactly it's
//   stored (that's what we're looking up!), so all we have to go off of is the
//   lookup key. So, we first determine the meta key for the lookup key using
//   RangeMetaKey, which is simply "/meta2/f". We then construct the scan bounds
//   for this key using MetaScanBounds. This scan bound will be
//   [/meta2/f.Next(),/meta2/max). The reason that this scan doesn't start at
//   "/meta2/f" is because if this key is the start key of a range (like it is
//   in this example!), the previous range descriptor will be stored at that
//   key. We then issue a forward ScanRequest over this range. Since we're
//   assuming we already cached the meta2 range that contains this span of keys,
//   we send the request directly to that range's replica (if we didn't have
//   this cached, the process would recurse to lookup the meta2 range
//   descriptor). We then find that the first KV pair we see during the scan is
//   at "/meta2/p". This is our desired range descriptor.
//  1.b: RangeLookup(key=m)
//   This case is similar. We construct a scan for this key "m" from
//   [/meta2/m.Next(),/meta2/max) and everything works the same as before.
//  1.b: RangeLookup(key=p)
//   Here, we're looking for the descriptor for the range [p, z), because key "p"
//   is included in that range, but not [f, p). We scan with bounds of
//   [/meta2/p.Next(),/meta2/max) and everything works as expected.
//
// Ex. 2:
//  Meta2 Ranges: [/meta2/a, /meta2/m) [/meta2/m, /meta2/z)
//  User  Ranges: [a, f)           [f, p),           [p, z)
//  2.a: RangeLookup(key=n)
//   In this case, we want to look up the range descriptor for the range [f, p)
//   because "n" is in that range. Remember that this descriptor will be stored
//   at "/meta2/p", which in this case is on the second meta2 range. So, we
//   construct the scan bounds of [/meta2/n.Next(),/meta2/max), send this scan
//   to the second meta2 range, and find that the first descriptor found is the
//   desired descriptor.
//  2.b: RangeLookup(key=g)
//   This is where things get a little tricky. As usual, we construct scan
//   bounds of [/meta2/g.Next(),/meta2/max). However, this scan will be routed
//   to the first meta2 range. It will scan forward and notice that no
//   descriptors are stored between [/meta2/g.Next(),/meta2/m). We then rely on
//   DistSender to continue this scan onto the next meta2 range since the result
//   from the first meta2 range will be empty. Once on the next meta2 range,
//   we'll find the desired descriptor at "/meta2/p".
//
// Ex. 3:
//  Meta2 Ranges: [/meta2/a, /meta2/m)  [/meta2/m, /meta2/z)
//  User  Ranges: [a, f)        [f, m), [m,s)         [p, z)
//  3.a: RangeLookup(key=g)
//   This is a little confusing, but actually behaves the exact same way at 2.b.
//   Notice that the descriptor for [f, m) is actually stored on the second
//   meta2 range! So the lookup scan will start on the first meta2 range and
//   continue onto the second before finding the desired descriptor at /meta2/m.
//   This is an unfortunate result of us storing RangeDescriptors at
//   RangeMetaKey(desc.EndKey) instead of RangeMetaKey(desc.StartKey) even
//   though our ranges are [inclusive,exclusive). Still everything works if we
//   let DistSender do its job when scanning over the meta2 range.
//
//   See #16266 and #17565 for further discussion. Notably, it is not possible
//   to pick meta2 boundaries such that we will never run into this issue. The
//   only way to avoid this completely would be to store RangeDescriptors at
//   RangeMetaKey(desc.StartKey) and only allow meta2 split boundaries at
//   RangeMetaKey(existingSplitBoundary)
//
//
// Lookups for range metadata keys usually want to read inconsistently, but some
// callers need a consistent result; both are supported be specifying the
// ReadConsistencyType. If the lookup is consistent, the Sender provided should
// be a TxnCoordSender.
//
// This method has an important optimization if the prefetchNum arg is larger
// than 0: instead of just returning the request RangeDescriptor, it also
// returns a slice of additional range descriptors immediately consecutive to
// the desired RangeDescriptor. This is intended to serve as a sort of caching
// pre-fetch, so that nodes can aggressively cache RangeDescriptors which are
// likely to be desired by their current workload. The prefetchReverse flag
// specifies whether descriptors are prefetched in descending or ascending
// order.
func RangeLookup(
	ctx context.Context,
	sender Sender,
	key roachpb.Key,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	// RangeLookup scans can span multiple ranges, as discussed above.
	// Traditionally, in order to see a fully-consistent snapshot of multiple
	// ranges, a scan needs to operate in a Txn with a fixed timestamp. Without
	// this, the scan may read results from different ranges at different times,
	// resulting in an inconsistent view. This is why DistSender returns
	// OpRequiresTxnError for consistent scans outside of Txns that span
	// multiple ranges.
	//
	// For RangeLookups, a consistent but outdated result is just as useless as
	// an inconsistent result. Because of this, we allow both inconsistent scans
	// and consistent scans outside of Txns for RangeLookups, and attempt to
	// reconcile any inconsistencies due to races, rescanning if this is not
	// possible.
	//
	// The retry options are set to be very aggressive because we should only
	// need to retry if a scan races with a split which is writing its new
	// RangeDescriptors across two different meta2 ranges. Because these meta2
	// writes are transactional, performing the entire scan again immediately
	// will not run into the same race.
	opts := retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Multiplier:     2,
	}

	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		// Determine the "Range Metadata Key" for the provided key.
		rkey, err := addrForDir(prefetchReverse)(key)
		if err != nil {
			return nil, nil, err
		}

		descs, intentDescs, err := lookupRangeFwdScan(ctx, sender, rkey, rc, prefetchNum, prefetchReverse)
		if err != nil {
			return nil, nil, err
		}
		if prefetchReverse {
			descs, intentDescs, err = lookupRangeRevScan(ctx, sender, rkey, rc, prefetchNum,
				prefetchReverse, descs, intentDescs)
			if err != nil {
				return nil, nil, err
			}
		}

		desiredDesc := containsForDir(prefetchReverse, rkey)
		var matchingRanges []roachpb.RangeDescriptor
		var prefetchedRanges []roachpb.RangeDescriptor
		for _, desc := range descs {
			if desiredDesc(desc) {
				if len(matchingRanges) == 0 {
					matchingRanges = append(matchingRanges, desc)
				} else {
					// Since we support scanning non-transactionally, it's possible
					// that we pick up both the pre- and post-split descriptor for a
					// range. In this case, we can detect the newer version of the
					// descriptor by selecting the smaller range. This is possible
					// by simply looking at the descriptors' EndKeys, which can never
					// be the same or the two options would have been stored at the
					// same key.
					if desc.EndKey.Less(matchingRanges[0].EndKey) {
						matchingRanges[0] = desc
					}
				}
			} else {
				// If this is not the desired descriptor, it must be a prefetched
				// descriptor.
				prefetchedRanges = append(prefetchedRanges, desc)
			}
		}
		for _, desc := range intentDescs {
			if desiredDesc(desc) {
				matchingRanges = append(matchingRanges, desc)
				// We only want up to one intent descriptor.
				break
			}
		}
		if len(matchingRanges) > 0 {
			return matchingRanges, prefetchedRanges, nil
		}

		log.Warningf(ctx, "range lookup of key %s found only non-matching ranges %v; retrying",
			key, prefetchedRanges)
	}

	ctxErr := ctx.Err()
	if ctxErr == nil {
		log.Fatalf(ctx, "retry loop broke before context expired")
	}
	return nil, nil, ctxErr
}

func lookupRangeFwdScan(
	ctx context.Context,
	sender Sender,
	key roachpb.RKey,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	if skipFwd := prefetchReverse && key.Equal(roachpb.KeyMax); skipFwd {
		// Don't attempt a forward scan because it's neither defined nor required.
		//
		// If !prefetchReverse && key.Equal(roachpb.KeyMax), we'll throw an error
		// below in keys.MetaScanBounds.
		return nil, nil, nil
	}

	// We want to search for the metadata key greater than metaKey. Scan for
	// both the requested key and the keys immediately afterwards.
	metaKey := keys.RangeMetaKey(key)
	bounds, err := keys.MetaScanBounds(metaKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not create scan bounds for range lookup")
	}

	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = rc
	if prefetchReverse {
		// Even if we're prefetching in the reverse direction, we still scan
		// forward first to get the first range. There are two cases, which we
		// can't detect in advance:
		// 1. key is not an endpoint of the range.
		// 2. key is the start/end key of the range.
		//
		// In case 1, we need the forward scan to get the first range
		// descriptor, because a reverse scan can't do the work. Imagine we have
		// ranges [a,c) and [c,f), and the reverse RangeLookup request's key is
		// "d". "d" is less than "f", and so the meta row {f->[c,f)} would not
		// be seen by the reverse scan. In this case, the forward scan will pick
		// it up.
		//
		// In case 2, the range descriptor received from the forward scan is not
		// needed. Imagine we have ranges [c,f) and [f,z), and the reverse
		// RangeLookup request's key is "f". The forward scan will start at
		// "f".Next() (see MetaScanBounds for details about the Next call) and
		// will receive the descriptor {z->[f,z)}. Since this is not being asked
		// for, we discard it below in lookupRangeRevScan. In this case, the
		// reverse scan will pick up {f->[c,f)}.
		ba.MaxSpanRequestKeys = 1
	} else {
		ba.MaxSpanRequestKeys = prefetchNum + 1
	}
	ba.Add(&roachpb.ScanRequest{
		Span: bounds.AsRawSpanWithNoLocals(),
		// NOTE (subtle): we want the scan to return intents as well as values
		// when scanning inconsistently. The reason is because it's not clear
		// whether the intent or the previous value points to the correct
		// location of the Range. It gets even more complicated when there are
		// split-related intents or a txn record co-located with a replica
		// involved in the split. Since we cannot know the correct answer, we
		// reply with both the pre- and post- transaction values.
		//
		// This does not count against a maximum range count (per the
		// ReturnIntents contract) because they are possible versions of the
		// same descriptor. In other words, both the current live descriptor and
		// a potentially valid descriptor from observed intents could be
		// returned.
		//
		// We don't need to set this when rc == roachpb.READ_UNCOMMITTED
		// because all read_uncommitted requests will return intents, which
		// is why this option is now deprecated.
		//
		// TODO(nvanbenschoten): remove in version 2.1.
		DeprecatedReturnIntents: rc == roachpb.INCONSISTENT,
	})
	if !TestingIsRangeLookup(ba) {
		log.Fatalf(ctx, "BatchRequest %v not detectable as RangeLookup", ba)
	}

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, nil, pErr.GoError()
	}
	scanRes := br.Responses[0].GetInner().(*roachpb.ScanResponse)

	descs, err := kvsToRangeDescriptors(scanRes.Rows)
	if err != nil {
		return nil, nil, err
	}
	intentDescs, err := kvsToRangeDescriptors(scanRes.IntentRows)
	if err != nil {
		return nil, nil, err
	}

	// If the forward scan did not find the desired descriptor and this is
	// prefetching in reverse, remove forward descriptors if they aren't needed.
	// This occurs in case 2 from above.
	if prefetchReverse {
		desiredDesc := containsForDir(prefetchReverse, key)
		if len(descs) > 0 && !desiredDesc(descs[0]) {
			descs = nil
		}
		if len(intentDescs) > 0 && !desiredDesc(intentDescs[0]) {
			intentDescs = nil
		}
	}
	return descs, intentDescs, nil
}

func lookupRangeRevScan(
	ctx context.Context,
	sender Sender,
	key roachpb.RKey,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
	fwdDescs, fwdIntentDescs []roachpb.RangeDescriptor,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	// If the forward scan already found the desired descriptor, subtract from
	// the ReverseScanRequest's max keys. If this becomes 0, there's no need to
	// do the scan.
	maxKeys := prefetchNum + 1
	if len(fwdDescs) > 0 {
		maxKeys-- // desired desc already found
		if maxKeys == 0 {
			return fwdDescs, fwdIntentDescs, nil
		}
	}

	// We want to search for the metadata key just less or equal to
	// metaKey. Scan in prefetchReverse order for both the requested key and the
	// keys immediately backwards.
	metaKey := keys.RangeMetaKey(key)
	revBounds, err := keys.MetaReverseScanBounds(metaKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not create scan bounds for reverse range lookup")
	}

	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = rc
	ba.MaxSpanRequestKeys = maxKeys
	ba.Add(&roachpb.ReverseScanRequest{
		Span: revBounds.AsRawSpanWithNoLocals(),
		// See explanation above in lookupRangeFwdScan.
		DeprecatedReturnIntents: rc == roachpb.INCONSISTENT,
	})
	if !TestingIsRangeLookup(ba) {
		log.Fatalf(ctx, "BatchRequest %v not detectable as RangeLookup", ba)
	}

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, nil, pErr.GoError()
	}
	revScanRes := br.Responses[0].GetInner().(*roachpb.ReverseScanResponse)

	revDescs, err := kvsToRangeDescriptors(revScanRes.Rows)
	if err != nil {
		return nil, nil, err
	}
	revIntentDescs, err := kvsToRangeDescriptors(revScanRes.IntentRows)
	if err != nil {
		return nil, nil, err
	}
	return append(fwdDescs, revDescs...), append(fwdIntentDescs, revIntentDescs...), nil
}

// LegacyRangeLookup performs the same operation as RangeLookup, but does so
// using a DeprecatedRangeLookupRequest. This request type does not support
// split meta2 splits, and as such, should not be used unless
// cluster.VersionMeta2Splits is active. It is used for compatibility on
// clusters that do not have that version active and therefore cannot perform
// RangeLookup scans with ScanRequests because ScanRequest.ReturnIntents was not
// available yet.
//
// LegacyRangeLookup does not work in all cases with split meta2 ranges. This is
// because a DeprecatedRangeLookupRequest can return no matching
// RangeDescriptors when meta2 ranges split. Remember that the range addressing
// keys are generated from the end key of a range descriptor, not the start key.
// With this in mind, consider the scenario:
//
//   range 1 [/meta2/a, /meta2/d):
//     b -> [a, b)
//     c -> [b, c)
//   range 2 [/meta2/d, /meta2/f):
//     d -> [c, d)
//     e -> [d, e)
//
// Now consider looking up the range containing key `c`. The RangeDescriptorDB
// routing logic would send the RangeLookup request to range 1 since `/meta2/c`
// lies within the bounds of that range. But notice that the range descriptor
// containing `d` lies in range 2. This means that no matching RangeDescriptors
// will be found on range 1 and returned from the first RangeLookup. In fact, a
// RangeLookup for any key between ['c','d') will create this scenario.
//
// Our solution (RangeLookup) is to deprecate RangeLookupRequest and instead use
// a ScanRequest over the entire MetaScanBounds. DistSender will properly scan
// across range boundaries when it doesn't find a descriptor at first, which
// avoids meta2 split complications.
//
// See #16266 and #17565 for further discussion. Notably, it is not possible to
// pick meta2 boundaries such that we will never run into this issue. The only
// way to avoid this completely would be to store RangeDescriptors at
// RangeMetaKey(desc.StartKey) and only allow meta2 split boundaries at
// RangeMetaKey(existingSplitBoundary).
//
// TODO(nvanbenschoten): remove in version 2.1.
func LegacyRangeLookup(
	ctx context.Context,
	sender Sender,
	key roachpb.Key,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	rkey, err := addrForDir(prefetchReverse)(key)
	if err != nil {
		return nil, nil, err
	}

	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = rc
	ba.Add(&roachpb.DeprecatedRangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(rkey).AsRawKey(),
		},
		MaxRanges: int32(prefetchNum + 1),
		Reverse:   prefetchReverse,
	})

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, nil, pErr.GoError()
	}
	if br.Error != nil {
		return nil, nil, br.Error.GoError()
	}
	resp := br.Responses[0].GetInner().(*roachpb.DeprecatedRangeLookupResponse)
	return resp.Ranges, resp.PrefetchedRanges, nil
}

// RangeLookupForVersion performs the same operation as RangeLookup, but does so
// using either RangeLookup or LegacyRangeLookup, depending on the provided
// cluster version.
//
// TODO(nvanbenschoten): remove in version 2.1.
func RangeLookupForVersion(
	ctx context.Context,
	st *cluster.Settings,
	sender Sender,
	key roachpb.Key,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	fn := RangeLookup
	if !st.Version.IsActive(cluster.VersionMeta2Splits) {
		fn = LegacyRangeLookup
	}
	return fn(ctx, sender, key, rc, prefetchNum, prefetchReverse)
}

// addrForDir determines the key addressing function to use for a RangeLookup
// scan in the given direction. With either addressing function, the result is
// the identity fn if the key is not a local key. However, for local keys, we
// want the scan to include the local key. See the respective comments on
// keys.Addr and keys.AddrUpperBound for more detail.
func addrForDir(prefetchReverse bool) func(roachpb.Key) (roachpb.RKey, error) {
	if prefetchReverse {
		return keys.AddrUpperBound
	}
	return keys.Addr
}

func containsForDir(prefetchReverse bool, key roachpb.RKey) func(roachpb.RangeDescriptor) bool {
	return func(desc roachpb.RangeDescriptor) bool {
		contains := roachpb.RangeDescriptor.ContainsKey
		if prefetchReverse {
			contains = roachpb.RangeDescriptor.ContainsKeyInverted
		}
		return contains(desc, key)
	}
}

func kvsToRangeDescriptors(kvs []roachpb.KeyValue) ([]roachpb.RangeDescriptor, error) {
	descs := make([]roachpb.RangeDescriptor, len(kvs))
	for i, kv := range kvs {
		if err := kv.Value.GetProto(&descs[i]); err != nil {
			return nil, err
		}
	}
	return descs, nil
}

// TestingIsRangeLookup returns if the provided BatchRequest looks like a single
// RangeLookup scan. It can return false positives and should only be used in
// tests.
func TestingIsRangeLookup(ba roachpb.BatchRequest) bool {
	if ba.IsSingleRequest() {
		return TestingIsRangeLookupRequest(ba.Requests[0].GetInner())
	}
	return false
}

// These spans bounds the start and end keys of the spans returned from
// MetaScanBounds and MetaReverseScanBounds. Next is called on each span's
// EndKey to make it end-inclusive so that ContainsKey works as expected.
var rangeLookupStartKeyBounds = roachpb.Span{
	Key:    keys.Meta1Prefix,
	EndKey: keys.Meta2KeyMax.Next(),
}
var rangeLookupEndKeyBounds = roachpb.Span{
	Key:    keys.Meta1Prefix.Next(),
	EndKey: keys.SystemPrefix.Next(),
}

// TestingIsRangeLookupRequest returns if the provided Request looks like a single
// RangeLookup scan. It can return false positives and should only be used in
// tests.
func TestingIsRangeLookupRequest(req roachpb.Request) bool {
	switch req.(type) {
	case *roachpb.ScanRequest:
	case *roachpb.ReverseScanRequest:
	default:
		return false
	}
	s := req.Header()
	return rangeLookupStartKeyBounds.ContainsKey(s.Key) &&
		rangeLookupEndKeyBounds.ContainsKey(s.EndKey)
}
