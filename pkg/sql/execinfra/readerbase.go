// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// We ignore any limits that are higher than this value to avoid integer
// overflows. See limitHint for how this bound is used.
const readerOverflowProtection = 1000000000000000 /* 10^15 */

// LimitHint returns the limit hint to set for a KVFetcher based on
// the spec's limit hint and the PostProcessSpec.
func LimitHint(specLimitHint int64, post *execinfrapb.PostProcessSpec) (limitHint int64) {
	// We prioritize the post process's limit since ProcOutputHelper
	// will tell us to stop once we emit enough rows.
	if post.Limit != 0 && post.Limit <= readerOverflowProtection {
		limitHint = int64(post.Limit)
	} else if specLimitHint != 0 && specLimitHint <= readerOverflowProtection {
		// If it turns out that limiHint rows are sufficient for our consumer, we
		// want to avoid asking for another batch. Currently, the only way for us to
		// "stop" is if we block on sending rows and the consumer sets
		// ConsumerDone() on the RowChannel while we block. So we want to block
		// *after* sending all the rows in the limit hint; to do this, we request
		// rowChannelBufSize + 1 more rows:
		//  - rowChannelBufSize rows guarantee that we will fill the row channel
		//    even after limitHint rows are consumed
		//  - the extra row gives us chance to call Push again after we unblock,
		//    which will notice that ConsumerDone() was called.
		//
		// This flimsy mechanism is only useful in the (optimistic) case that the
		// processor that only needs this many rows is our direct, local consumer.
		// If we have a chain of processors and RowChannels, or remote streams, this
		// reasoning goes out the door.
		//
		// TODO(radu, andrei): work on a real mechanism for limits.
		limitHint = specLimitHint + RowChannelBufSize + 1
	}

	if !post.Filter.Empty() {
		// We have a filter so we will likely need to read more rows.
		limitHint *= 2
	}

	return limitHint
}

// MisplannedRanges queries the range cache for all the passed-in spans and
// returns the list of ranges whose leaseholder is not on the indicated node.
// Ranges with unknown leases are not included in the result.
func MisplannedRanges(
	ctx context.Context,
	spans []roachpb.Span,
	nodeID roachpb.NodeID,
	rdc *kvcoord.RangeDescriptorCache,
) (misplannedRanges []roachpb.RangeInfo) {
	log.VEvent(ctx, 2, "checking range cache to see if range info updates should be communicated to the gateway")
	misplanned := make(map[roachpb.RangeID]struct{})
	for _, sp := range spans {
		rSpan, err := keys.SpanAddr(sp)
		if err != nil {
			panic(err)
		}
		overlapping := rdc.GetCachedOverlapping(ctx, rSpan)

		for _, ri := range overlapping {
			if _, ok := misplanned[ri.Desc().RangeID]; ok {
				// We're already returning info about this range.
				continue
			}
			// Ranges with unknown leases are not returned, as the current node might
			// actually have the lease without the local cache knowing about it.
			l := ri.Lease()
			if l != nil && l.Replica.NodeID != nodeID {
				misplannedRanges = append(misplannedRanges, roachpb.RangeInfo{
					Desc:  *ri.Desc(),
					Lease: *l,
				})
			}
		}
	}

	if len(misplannedRanges) != 0 {
		var msg string
		if len(misplannedRanges) < 3 {
			msg = fmt.Sprintf("%+v", misplannedRanges[0].Desc)
		} else {
			msg = fmt.Sprintf("%+v...", misplannedRanges[:3])
		}
		log.VEventf(ctx, 2, "misplanned ranges: %s", msg)
	}

	return misplannedRanges
}
