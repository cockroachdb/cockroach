// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
)

// computeNetworkRTTBasedOnPolicy converts a closed timestamp policy to an estimated
// network RTT. Returns default max RTT for no latency info, or midpoint RTT for
// known latency buckets. Panics on unknown policy.
func computeNetworkRTTBasedOnPolicy(policy ctpb.RangeClosedTimestampPolicy) time.Duration {
	switch {
	case policy == ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO:
		return defaultMaxNetworkRTT
	case policy >= ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS &&
		policy <= ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS:
		// Calculate bucket number and return its midpoint RTT. Midpoint RTT for
		// bucket N is (N+0.5)*interval.
		bucket := int(policy) - int(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO) - 1
		return time.Duration((float64(bucket) + 0.5) * float64(closedTimestampPolicyBucketWidth.Nanoseconds()))
	default:
		panic(fmt.Sprintf("unknown closed timestamp policy: %s", policy))
	}
}
