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
		// Calculate bucket number and return its midpoint RTT
		bucket := int(policy) - int(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO)
		return time.Duration(bucket) * closedTimestampPolicyLatencyInterval / 2
	default:
		panic(fmt.Sprintf("unknown closed timestamp policy: %s", policy))
	}
}
