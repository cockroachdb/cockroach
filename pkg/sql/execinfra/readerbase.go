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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
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
		limitHint = specLimitHint
	}

	return limitHint
}

// MisplannedRanges queries the range cache for all the passed-in spans and
// returns the list of ranges whose leaseholder is not on the indicated node.
// Ranges with unknown leases are not included in the result.
func MisplannedRanges(
	ctx context.Context, spans []roachpb.Span, nodeID roachpb.NodeID, rdc *rangecache.RangeCache,
) (misplannedRanges []roachpb.RangeInfo) {
	log.VEvent(ctx, 2, "checking range cache to see if range info updates should be communicated to the gateway")
	var misplanned map[roachpb.RangeID]struct{}
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
					Desc:                  *ri.Desc(),
					Lease:                 *l,
					ClosedTimestampPolicy: ri.ClosedTimestampPolicy(),
				})

				if misplanned == nil {
					misplanned = make(map[roachpb.RangeID]struct{})
				}
				misplanned[ri.Desc().RangeID] = struct{}{}
			}
		}
	}

	if len(misplannedRanges) != 0 && log.ExpensiveLogEnabled(ctx, 2) {
		var b strings.Builder
		for i := range misplannedRanges {
			if i > 0 {
				b.WriteString(", ")
			}
			if i > 3 {
				b.WriteString("...")
				break
			}
			fmt.Fprintf(&b, "%+v", misplannedRanges[i])
		}
		log.VEventf(ctx, 2, "misplanned ranges: %s", b.String())
	}

	return misplannedRanges
}
