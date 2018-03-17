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

package batcheval

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.DeprecatedRangeLookup, declareKeysDeprecatedRangeLookup, DeprecatedRangeLookup)
}

func declareKeysDeprecatedRangeLookup(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	lookupReq := req.(*roachpb.DeprecatedRangeLookupRequest)

	if keys.IsLocal(lookupReq.Key) {
		// Error will be thrown during evaluation.
		return
	}
	key := roachpb.RKey(lookupReq.Key)

	// RangeLookupRequests depend on the range descriptor because they need
	// to determine which range descriptors are within the local range.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})

	// Both forward and reverse RangeLookupRequests scan forward initially. We are
	// unable to bound this initial scan any further than from the lookup key to
	// the end of the current descriptor.
	if !lookupReq.Reverse || key.Less(roachpb.RKey(keys.Meta2KeyMax)) {
		scanBounds, err := rangeLookupScanBounds(&desc, key, false /* reverse */)
		if err != nil {
			// Errors will be caught during evaluation.
			return
		}
		spans.Add(spanset.SpanReadOnly, roachpb.Span{
			Key:    scanBounds.Key.AsRawKey(),
			EndKey: scanBounds.EndKey.AsRawKey(),
		})
	}

	// A reverse RangeLookupRequest also scans backwards.
	if lookupReq.Reverse {
		revScanBounds, err := rangeLookupScanBounds(&desc, key, true /* reverse */)
		if err != nil {
			// Errors will be caught during evaluation.
			return
		}
		spans.Add(spanset.SpanReadOnly, roachpb.Span{
			Key:    revScanBounds.Key.AsRawKey(),
			EndKey: revScanBounds.EndKey.AsRawKey(),
		})
	}
}

// rangeLookupScanBounds returns the range [start,end) within the bounds of the
// provided RangeDescriptor which the desired meta record can be found by means
// of an engine scan.
func rangeLookupScanBounds(
	desc *roachpb.RangeDescriptor, key roachpb.RKey, reverse bool,
) (roachpb.RSpan, error) {
	boundsFn := keys.MetaScanBounds
	if reverse {
		boundsFn = keys.MetaReverseScanBounds
	}
	span, err := boundsFn(key)
	if err != nil {
		return roachpb.RSpan{}, err
	}
	return span.Intersect(desc)
}

// DeprecatedRangeLookup is used to look up RangeDescriptors - a RangeDescriptor
// is a metadata structure which describes the key range and replica locations
// of a distinct range in the cluster.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix prepended. The Range Metadata
// Key for an ordinary key can be generated with the `keys.RangeMetaKey(key)`
// function. The RangeDescriptor for the range which contains a given key can be
// retrieved by generating its Range Metadata Key and dispatching it to
// RangeLookup.
//
// Note that the Range Metadata Key sent to RangeLookup is NOT the key
// at which the desired RangeDescriptor is stored. Instead, this method returns
// the RangeDescriptor stored at the _lowest_ existing key which is _greater_
// than the given key. The returned RangeDescriptor will thus contain the
// ordinary key which was originally used to generate the Range Metadata Key
// sent to RangeLookup.
//
// The "Range Metadata Key" for a range is built by appending the end key of
// the range to the respective meta prefix.
//
// Lookups for range metadata keys usually want to read inconsistently, but
// some callers need a consistent result; both are supported.
//
// This method has an important optimization in the inconsistent case: instead
// of just returning the request RangeDescriptor, it also returns a slice of
// additional range descriptors immediately consecutive to the desired
// RangeDescriptor. This is intended to serve as a sort of caching pre-fetch,
// so that the requesting nodes can aggressively cache RangeDescriptors which
// are likely to be desired by their current workload. The Reverse flag
// specifies whether descriptors are prefetched in descending or ascending
// order.
func DeprecatedRangeLookup(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	log.Event(ctx, "RangeLookup")
	args := cArgs.Args.(*roachpb.DeprecatedRangeLookupRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DeprecatedRangeLookupResponse)

	if keys.IsLocal(args.Key) {
		return result.Result{}, errors.Errorf("illegal lookup of range-local key %q", args.Key)
	}
	key := roachpb.RKey(args.Key)

	ts, txn, consistent, rangeCount := h.Timestamp, h.Txn, h.ReadConsistency == roachpb.CONSISTENT, int64(args.MaxRanges)
	if rangeCount < 1 {
		return result.Result{}, errors.Errorf("range lookup specified invalid maximum range count %d: must be > 0", rangeCount)
	}

	desc := cArgs.EvalCtx.Desc()

	var checkAndUnmarshal func(roachpb.Value) (*roachpb.RangeDescriptor, error)

	var kvs []roachpb.KeyValue // kv descriptor pairs in scan order
	var intents []roachpb.Intent
	if !args.Reverse {
		// If scanning forward, there's no special "checking": Just decode the
		// descriptor and return it.
		checkAndUnmarshal = func(v roachpb.Value) (*roachpb.RangeDescriptor, error) {
			var rd roachpb.RangeDescriptor
			if err := v.GetProto(&rd); err != nil {
				return nil, err
			}
			return &rd, nil
		}

		// We want to search for the metadata key greater than
		// args.Key. Scan for both the requested key and the keys immediately
		// afterwards, up to MaxRanges.
		span, err := rangeLookupScanBounds(desc, key, false /* reverse */)
		if err != nil {
			return result.Result{}, err
		}

		// Scan for descriptors.
		kvs, _, intents, err = engine.MVCCScan(
			ctx, batch, span.Key.AsRawKey(), span.EndKey.AsRawKey(), rangeCount, ts, consistent, txn,
		)
		if err != nil {
			// An error here is likely a WriteIntentError when reading consistently.
			return result.Result{}, err
		}
	} else {
		// Use MVCCScan to get the first range. There are three cases:
		// 1. args.Key is not an endpoint of the range.
		// 2a. args.Key is the start/end key of the range.
		// 2b. args.Key is roachpb.KeyMax.
		// In the first case, we need use the MVCCScan() to get the first
		// range descriptor, because ReverseScan can't do the work. If we
		// have ranges [a,c) and [c,f) and the reverse scan request's key
		// range is [b,d), then d.Next() is less than "f", and so the meta
		// row {f->[c,f)} would be ignored by MVCCReverseScan. In case 2a,
		// the range descriptor received by MVCCScan will be filtered before
		// results are returned: With ranges [c,f) and [f,z), reverse scan
		// on [d,f) receives the descriptor {z->[f,z)}, which is discarded
		// below since it's not being asked for. Finally, in case 2b, we
		// don't even attempt the forward scan because it's neither defined
		// nor required.
		// Note that Meta1KeyMax is admissible: it means we're looking for
		// the range descriptor that houses Meta2KeyMax, and a forward scan
		// handles it correctly.
		// In this case, checkAndUnmarshal is more complicated: It needs
		// to weed out descriptors from the forward scan above, which could
		// return a result or an intent we're not supposed to return.
		checkAndUnmarshal = func(v roachpb.Value) (*roachpb.RangeDescriptor, error) {
			var rd roachpb.RangeDescriptor
			if err := v.GetProto(&rd); err != nil {
				return nil, err
			}
			startKeyAddr := keys.RangeMetaKey(rd.StartKey)
			if !startKeyAddr.Less(key) {
				// This is the case in which we've picked up an extra descriptor
				// we don't want.
				return nil, nil
			}
			// We actually want this descriptor.
			return &rd, nil
		}

		if key.Less(roachpb.RKey(keys.Meta2KeyMax)) {
			span, err := rangeLookupScanBounds(desc, key, false /* reverse */)
			if err != nil {
				return result.Result{}, err
			}

			kvs, _, intents, err = engine.MVCCScan(
				ctx, batch, span.Key.AsRawKey(), span.EndKey.AsRawKey(), 1, ts, consistent, txn,
			)
			if err != nil {
				return result.Result{}, err
			}
		}
		// We want to search for the metadata key just less or equal to
		// args.Key. Scan in reverse order for both the requested key and the
		// keys immediately backwards, up to MaxRanges.
		span, err := rangeLookupScanBounds(desc, key, true /* reverse */)
		if err != nil {
			return result.Result{}, err
		}

		// Reverse scan for descriptors.
		revKVs, _, revIntents, err := engine.MVCCReverseScan(
			ctx, batch, span.Key.AsRawKey(), span.EndKey.AsRawKey(), rangeCount, ts, consistent, txn,
		)
		if err != nil {
			// An error here is likely a WriteIntentError when reading consistently.
			return result.Result{}, err
		}

		// Merge the results, the total ranges may be bigger than rangeCount.
		kvs = append(kvs, revKVs...)
		intents = append(intents, revIntents...)
	}

	userKey := keys.UserKey(key)
	containsFn := roachpb.RangeDescriptor.ContainsKey
	if args.Reverse {
		containsFn = roachpb.RangeDescriptor.ContainsKeyInverted
	}

	for _, kv := range kvs {
		// TODO(tschottdorf): Candidate for a ReplicaCorruptionError.
		rd, err := checkAndUnmarshal(kv.Value)
		if err != nil {
			return result.Result{}, err
		}
		if rd != nil {
			// Add the first valid descriptor to the desired range descriptor
			// list in the response, add all others to the prefetched list.
			if len(reply.Ranges) == 0 && containsFn(*rd, userKey) {
				reply.Ranges = append(reply.Ranges, *rd)
			} else {
				reply.PrefetchedRanges = append(reply.PrefetchedRanges, *rd)
			}
		}
	}

	// NOTE (subtle): dangling intents on meta records are peculiar: It's not
	// clear whether the intent or the previous value point to the correct
	// location of the Range. It gets even more complicated when there are
	// split-related intents or a txn record co-located with a replica
	// involved in the split. Since we cannot know the correct answer, we
	// reply with both the pre- and post- transaction values.
	//
	// This does not count against a maximum range count because they are
	// possible versions of the same descriptor. In other words, both the
	// current live descriptor and a potentially valid descriptor from
	// observed intents could be returned.
	for _, intent := range intents {
		val, _, err := engine.MVCCGetAsTxn(
			ctx, batch, intent.Key, intent.Txn.Timestamp, intent.Txn,
		)
		if err != nil {
			return result.Result{}, err
		}

		if val == nil {
			// Intent is a deletion.
			continue
		}
		rd, err := checkAndUnmarshal(*val)
		if err != nil {
			return result.Result{}, err
		}
		if rd != nil {
			if containsFn(*rd, userKey) {
				reply.Ranges = append(reply.Ranges, *rd)
				break
			}
		}
	}

	if len(reply.Ranges) == 0 {
		// No matching results were returned from the scan. This can happen when
		// meta2 ranges split (so for now such splitting is disabled). Remember
		// that the range addressing keys are generated from the end key of a range
		// descriptor, not the start key. Consider the scenario:
		//
		//   range 1 [a, e):
		//     b -> [a, b)
		//     c -> [b, c)
		//     d -> [c, d)
		//   range 2 [e, g):
		//     e -> [d, e)
		//     f -> [e, f)
		//     g -> [f, g)
		//
		// Now consider looking up the range containing key `d`. The DistSender
		// routing logic would send the RangeLookup request to range 1 since `d`
		// lies within the bounds of that range. But notice that the range
		// descriptor containing `d` lies in range 2. Boom! A real fix will involve
		// additional logic in the RangeDescriptorCache range lookup state machine.
		//
		// See #16266.
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "range lookup of meta key '%[1]s' [%[1]x] found only non-matching ranges:",
			args.Key)
		for _, desc := range reply.PrefetchedRanges {
			buf.WriteByte('\n')
			buf.WriteString(desc.String())
		}
		log.Fatal(ctx, buf.String())
	}

	if preCount := int64(len(reply.PrefetchedRanges)); 1+preCount > rangeCount {
		// We've possibly picked up an extra descriptor if we're in reverse
		// mode due to the initial forward scan.
		//
		// Here, we only count the desired range descriptors as a single
		// descriptor against the rangeCount limit, even if multiple versions
		// of the same descriptor were found in intents. In practice, we should
		// only get multiple desired range descriptors when prefetching is disabled
		// anyway (see above), so this should never actually matter.
		reply.PrefetchedRanges = reply.PrefetchedRanges[:rangeCount-1]
	}

	return result.FromIntents(intents, args), nil
}
