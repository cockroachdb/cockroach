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

// FindBucketBasedOnNetworkRTTWithDampening calculates a new closed timestamp policy
// based on the old policy, the network RTT, and a boundary percentage.
//
// The new policy is only changed if the network RTT has crossed the boundary of
// the new policy.
func FindBucketBasedOnNetworkRTTWithDampening(
	oldPolicy ctpb.RangeClosedTimestampPolicy, networkRTT time.Duration, boundaryPercent float64,
) ctpb.RangeClosedTimestampPolicy {
	// Calculate the new policy based on network RTT.
	newPolicy := FindBucketBasedOnNetworkRTT(networkRTT)

	if newPolicy == ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO ||
		oldPolicy == ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO {
		return newPolicy
	}

	// Apply the new policy if it's different from the old one, or if there's a
	// non-adjacent bucket jump.
	if newPolicy == oldPolicy || math.Abs(float64(newPolicy-oldPolicy)) > 1 {
		return newPolicy
	}

	// Calculate bucket number by subtracting base policy and adjusting for
	// zero-based indexing.
	bucket := int(newPolicy) - int(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO) - 1
	intervalNanos := float64(closedTimestampPolicyBucketWidth.Nanoseconds())
	switch {
	case oldPolicy < newPolicy:
		// The new policy has a higher latency threshold. Only switch to the
		// higher latency bucket if the RTT exceeds the bucket boundary.
		if higherLatencyBucketThreshold := time.Duration((float64(bucket) + boundaryPercent) * intervalNanos); networkRTT >= higherLatencyBucketThreshold {
			return newPolicy
		}
		return oldPolicy
	case oldPolicy > newPolicy:
		// The new policy has a lower latency threshold. Only switch to the lower
		// latency bucket if the RTT is below the bucket boundary.
		if lowerLatencyBucketThreshold := time.Duration((float64(bucket) + 1 - boundaryPercent) * intervalNanos); networkRTT <= lowerLatencyBucketThreshold {
			return newPolicy
		}
		return oldPolicy
	default:
		panic("unexpected condition")
	}
}

// FindBucketBasedOnNetworkRTT maps a network RTT to a closed timestamp policy
// bucket.
func FindBucketBasedOnNetworkRTT(networkRTT time.Duration) ctpb.RangeClosedTimestampPolicy {
	// If maxLatency is negative (i.e. no peer latency is provided), return
	// LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO
	if networkRTT < 0 {
		return ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO
	}
	if networkRTT >= 300*time.Millisecond {
		return ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS
	}
	// Divide RTT by policy interval, add 1 for zero-based indexing, and offset by
	// the base policy enum value.
	bucketNum := int32(math.Floor(float64(networkRTT)/float64(closedTimestampPolicyBucketWidth))) + 1
	return ctpb.RangeClosedTimestampPolicy(bucketNum + int32(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO))
}

// computeNetworkRTTBasedOnPolicy converts a closed timestamp policy to an estimated
// network RTT.
func computeNetworkRTTBasedOnPolicy(policy ctpb.RangeClosedTimestampPolicy) time.Duration {
	switch {
	case policy == ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO:
		// If no latency info is available, return the default max RTT.
		return DefaultMaxNetworkRTT
	case policy >= ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_LESS_THAN_20MS &&
		policy <= ctpb.LEAD_FOR_GLOBAL_READS_LATENCY_EQUAL_OR_GREATER_THAN_300MS:
		// For known latency buckets, we return the midpoint RTT for the bucket.
		// The midpointRTT for bucket N is (N+0.5)*interval.
		bucket := int(policy) - int(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LATENCY_INFO) - 1
		return time.Duration((float64(bucket) + 0.5) * float64(closedTimestampPolicyBucketWidth.Nanoseconds()))
	default:
		panic(fmt.Sprintf("unknown closed timestamp policy: %s", policy))
	}
}
