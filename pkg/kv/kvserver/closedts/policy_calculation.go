// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package closedts

import (
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
)

// FindBucketBasedOnNetworkRTT maps a network RTT to a closed timestamp policy bucket.
// It divides RTT by policy interval, adds 1 for zero-based indexing, and offsets by
// the base policy enum value.
func FindBucketBasedOnNetworkRTT(networkRTT time.Duration) ctpb.RangeClosedTimestampPolicy {
	if networkRTT < 0 {
		return ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO
	}
	bucketNum := int32(math.Floor(float64(networkRTT)/float64(closedTimestampPolicyLatencyInterval))) + 1
	policy := ctpb.RangeClosedTimestampPolicy(
		bucketNum + int32(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO))
	return min(ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS, policy)
}

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
