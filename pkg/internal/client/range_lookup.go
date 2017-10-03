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
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// LookupRange is used to look up RangeDescriptors - a RangeDescriptor is a
// metadata structure which describes the key range and replica locations of a
// distinct range in the cluster.
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
// Lookups for range metadata keys usually want to read inconsistently, but some
// callers need a consistent result; both are supported be specifying the
// ReadConsistencyType.
//
// This method has an important optimization if the prefetchNum arg is larger
// than 0: instead of just returning the request RangeDescriptor, it also
// returns a slice of additional range descriptors immediately consecutive to
// the desired RangeDescriptor. This is intended to serve as a sort of caching
// pre-fetch, so that nodes can aggressively cache RangeDescriptors which are
// likely to be desired by their current workload. The prefetchReverse flag
// specifies whether descriptors are prefetched in descending or ascending
// order.
func LookupRange(
	ctx context.Context,
	sender Sender,
	key roachpb.Key,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	// Determine the "Range Metadata Key" for the provided key.
	rkey, err := addrForDir(prefetchReverse)(key)
	if err != nil {
		return nil, nil, err
	}

	descs, intentDescs, err := lookupRangeFwdScan(ctx, sender, rkey, rc, prefetchNum, prefetchReverse)
	if err != nil {
		return nil, nil, err
	}
	descs, intentDescs, err = lookupRangeRevScan(ctx, sender, rkey, rc, prefetchNum,
		prefetchReverse, descs, intentDescs)
	if err != nil {
		return nil, nil, err
	}

	desiredDesc := containsForDir(prefetchReverse, rkey)
	var matchingRanges []roachpb.RangeDescriptor
	var prefetchedRanges []roachpb.RangeDescriptor
	for _, desc := range descs {
		if desiredDesc(desc) {
			matchingRanges = append(matchingRanges, desc)
		} else {
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
	if len(matchingRanges) == 0 {
		log.Fatalf(ctx, "range lookup of key %s found only non-matching ranges", key)
	}
	return matchingRanges, prefetchedRanges, nil
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
		// See case 2b below.
		return nil, nil, nil
	}

	// We want to search for the metadata key greater than metaKey. Scan for
	// both the requested key and the keys immediately afterwards.
	metaKey := keys.RangeMetaKey(key)
	bounds, err := keys.MetaScanBounds(metaKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get meta scan bounds for range lookup")
	}

	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = rc
	if prefetchReverse {
		// Even if we're prefetching in the reverse direction, we still scan
		// forward first to get the first range. There are three cases:
		// 1. key is not an endpoint of the range.
		// 2a. key is the start/end key of the range.
		// 2b. key is roachpb.KeyMax.
		//
		// In case 1, we need the forward ScanRequest to get the first range
		// descriptor, because ReverseScan can't do the work. If we have ranges
		// [a,c) and [c,f) and the reverse scan request's key range is [b,d),
		// then "d" is less than "f", and so the meta row {f->[c,f)} would be
		// ignored by the ReverseScanRequest.
		//
		// In case 2a, the range descriptor received by ScanRequest is not
		// needed. With ranges [c,f) and [f,z), reverse scan on [d,f) receives
		// the descriptor {z->[f,z)}, which is discarded below since it's not
		// being asked for.
		//
		// Finally, in case 2b, we don't even attempt the forward scan because
		// it's neither defined nor required. Note that Meta2KeyMax is
		// admissible and a forward scan handles it correctly.
		ba.MaxSpanRequestKeys = 1
	} else {
		ba.MaxSpanRequestKeys = prefetchNum + 1
	}
	ba.Add(&roachpb.ScanRequest{
		Span: bounds.AsRawSpanWithNoLocals(),
		// NOTE (subtle): we want the scan to return intents as well as values
		// when scanning inconsistency. The reason is because it's not clear
		// whether the intent or the previous value point to the correct
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
		ReturnIntents: rc == roachpb.INCONSISTENT,
	})

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
	if !prefetchReverse {
		return fwdDescs, fwdIntentDescs, nil
	}

	// If the forward scan already found the desired descriptor, subtract from
	// the ReverseScanRequest's max keys. If this becomes 0, there's no need to
	// do the scan.
	//
	// If the forward scan did not find the desired descriptor, remove forward
	// descriptors if they aren't needed. This occurs in the case 2a from above.
	// Also, detect if the reverse scan
	maxKeys := prefetchNum + 1
	desiredDesc := containsForDir(prefetchReverse, key)
	if len(fwdDescs) > 0 {
		if desiredDesc(fwdDescs[0]) {
			maxKeys-- // desired desc already found
			if maxKeys == 0 {
				return fwdDescs, fwdIntentDescs, nil
			}
		} else {
			fwdDescs = nil
		}
	}
	if len(fwdIntentDescs) == 0 || !desiredDesc(fwdIntentDescs[0]) {
		fwdIntentDescs = nil
	}

	// We want to search for the metadata key just less or equal to
	// metaKey. Scan in prefetchReverse order for both the requested key and the
	// keys immediately backwards.
	metaKey := keys.RangeMetaKey(key)
	revBounds, err := keys.MetaReverseScanBounds(metaKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get meta prefetchReverse scan bounds for range lookup")
	}

	ba := roachpb.BatchRequest{}
	ba.ReadConsistency = rc
	ba.MaxSpanRequestKeys = maxKeys
	ba.Add(&roachpb.ReverseScanRequest{
		Span:          revBounds.AsRawSpanWithNoLocals(),
		ReturnIntents: rc == roachpb.INCONSISTENT,
	})

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

// LookupRangeCompat performs the same operation as LookupRange, but does so
// using a RangeLookupRequest. This request type does not support split meta2
// splits, and as such, should not be used unless cluster.VersionMeta2Splits is
// active. It is used for compatibility on clusters that do not have that
// version active and therefore cannot perform RangeLookup scans with
// ScanRequests because ScanRequest.ReturnIntents was not available yet.
//
// LookupRangeCompat does not work in all cases with split meta2 ranges. This is
// because a RangeLookupRequest can return no matching RangeDescriptors when
// meta2 ranges split. Remember that the range addressing keys are generated
// from the end key of a range descriptor, not the start key. With this in mind,
// consider the scenario:
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
// RangeLookup for any key between ['c','d') will create this scenerio.
//
// Our solution (LookupRange) is to deprecate RangeLookupRequest and instead use
// a ScanRequest over the entire MetaScanBounds. DistSender will properly scan
// across range boundaries when it doesn't find a descriptor at first, which
// avoids meta2 split complications.
//
// See #16266 and #17565 for further discussion. Notably, it is not possible to
// pick meta2 boundaries such that we will never run into this issue. The only
// way to avoid this completely would be to store RangeDescriptors at
// RangeMetaKey(desc.StartKey) and only allow meta2 split boundaries at
// RangeMetaKey(existingSplitBoundary)
func LookupRangeCompat(
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
	ba.Add(&roachpb.RangeLookupRequest{
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
	// TODO during review: is this actually needed?
	if br.Error != nil {
		return nil, nil, br.Error.GoError()
	}
	resp := br.Responses[0].GetInner().(*roachpb.RangeLookupResponse)
	return resp.Ranges, resp.PrefetchedRanges, nil
}

// LookupRangeForVersion performs the same operation as LookupRange, but does so
// using either LookupRange or LookupRangeCompat, depending on the provided
// cluster version.
func LookupRangeForVersion(
	ctx context.Context,
	st *cluster.Settings,
	sender Sender,
	key roachpb.Key,
	rc roachpb.ReadConsistencyType,
	prefetchNum int64,
	prefetchReverse bool,
) (rs, preRs []roachpb.RangeDescriptor, err error) {
	fn := LookupRange
	if !st.Version.IsActive(cluster.VersionMeta2Splits) {
		fn = LookupRangeCompat
	}
	return fn(ctx, sender, key, rc, prefetchNum, prefetchReverse)
}

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

// IsRangeLookup returns if the provided BatchRequest is a RangeLookup scan.
func IsRangeLookup(ba roachpb.BatchRequest) bool {
	if ba.IsSingleRequest() {
		return IsRangeLookupRequest(ba.Requests[0].GetInner())
	}
	return false
}

// IsRangeLookupRequest returns if the provided Request is a RangeLookup scan.
func IsRangeLookupRequest(req roachpb.Request) bool {
	if s, ok := req.(*roachpb.ScanRequest); ok {
		return s.Key.Compare(keys.Meta2KeyMax) <= 0 && s.EndKey.Compare(keys.MetaMax) <= 0
	}
	return false
}
