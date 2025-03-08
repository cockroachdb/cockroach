// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sidetransport

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// mapClosedTSPolicyToLocality maps a closed timestamp policy and locality info
// to a specific policy+locality combination used for closed timestamp
// propagation.
func mapClosedTSPolicyToLocality(
	policy roachpb.RangeClosedTimestampPolicy, locality roachpb.LocalityComparisonType,
) ctpb.RangeClosedTimestampByPolicyLocality {
	switch policy {
	case roachpb.LAG_BY_CLUSTER_SETTING:
		return ctpb.LAG_BY_CLUSTER_SETTING
	case roachpb.LEAD_FOR_GLOBAL_READS:
		switch locality {
		case roachpb.LocalityComparisonType_UNDEFINED:
			return ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY
		case roachpb.LocalityComparisonType_CROSS_REGION:
			return ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION
		case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
			return ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE
		case roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE:
			return ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE
		default:
			panic(fmt.Sprintf("unknown locality: %s", locality))
		}
	default:
		panic(fmt.Sprintf("unknown policy: %s", policy))
	}
}

// GetTargetAndPolicyLocality returns the target timestamp and policy locality
// for a replica. It first attempts to find a target using both policy and
// locality. If unavailable (e.g. during cluster upgrade or when auto-tuning is
// disabled), it falls back to using just the policy. The targetByPolicy map is
// treated as authoritative - if a target is not found, it falls back to basic
// closed timestamp policies without considering replica localities.
func GetTargetAndPolicyLocality(
	policy roachpb.RangeClosedTimestampPolicy,
	locality roachpb.LocalityComparisonType,
	targetByPolicy map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp,
) (targetTS hlc.Timestamp, policyLocality ctpb.RangeClosedTimestampByPolicyLocality) {
	// Try policy+locality combination first.
	policyLocality = mapClosedTSPolicyToLocality(policy, locality)
	if targetTS, ok := targetByPolicy[policyLocality]; ok {
		return targetTS, policyLocality
	}

	// Fall back to just policy if policy+locality target not found.
	policyLocality = ctpb.RangeClosedTimestampByPolicyLocality(policy)
	return targetByPolicy[policyLocality], policyLocality
}
