// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package closedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TargetForPolicy returns the target closed timestamp for a range with the
// given policy.
func TargetForPolicy(
	now hlc.ClockTimestamp,
	maxClockOffset time.Duration,
	lagTargetDuration time.Duration,
	leadTargetOverride time.Duration,
	sideTransportCloseInterval time.Duration,
	policy roachpb.RangeClosedTimestampPolicy,
) hlc.Timestamp {
	var res hlc.Timestamp
	switch policy {
	case roachpb.LAG_BY_CLUSTER_SETTING:
		// Simple calculation: lag now by desired duration.
		res = now.ToTimestamp().Add(-lagTargetDuration.Nanoseconds(), 0)
	case roachpb.LEAD_FOR_GLOBAL_READS:
		// The LEAD_FOR_GLOBAL_READS calculation is more complex. Instead of the
		// policy defining an offset from the publisher's perspective, the
		// policy defines a goal from the consumer's perspective - the goal
		// being that present time reads (with a possible uncertainty interval)
		// can be served from all followers. To accomplish this, we must work
		// backwards to establish a lead time to publish closed timestamps at.
		//
		// The calculation looks something like the following:
		//
		//  # This should be sufficient for any present-time transaction,
		//  # because its global uncertainty limit should be <= this time.
		//  # For more, see (*Transaction).RequiredFrontier.
		//  closed_ts_at_follower = now + max_offset
		//
		//  # The sender must account for the time it takes to propagate a
		//  # closed timestamp update to its followers.
		//  closed_ts_at_sender = closed_ts_at_follower + propagation_time
		//
		//  # Closed timestamps propagate in two ways. Both need to make it to
		//  # followers in time.
		//  propagation_time = max(raft_propagation_time, side_propagation_time)
		//
		//  # Raft propagation takes 3 network hops to go from a leader proposing
		//  # a write (with a closed timestamp update) to the write being applied.
		//  # 1. leader sends MsgProp with entry
		//  # 2. followers send MsgPropResp with vote
		//  # 3. leader sends MsgProp with higher commit index
		//  #
		//  # We also add on a small bit of overhead for request evaluation, log
		//  # sync, and state machine apply latency.
		//  raft_propagation_time = max_network_rtt * 1.5 + raft_overhead
		//
		//  # Side-transport propagation takes 1 network hop, as there is no voting.
		//  # However, it is delayed by the full side_transport_close_interval in
		//  # the worst-case.
		//  side_propagation_time = max_network_rtt * 0.5 + side_transport_close_interval
		//
		//  # Combine, we get the following result
		//  closed_ts_at_sender = now + max_offset + max(
		//    max_network_rtt * 1.5 + raft_overhead,
		//    max_network_rtt * 0.5 + side_transport_close_interval,
		//  )
		//
		// By default, this leads to a closed timestamp target that leads the
		// senders current clock by 800ms.
		//
		// NOTE: this calculation takes into consideration maximum clock skew as
		// it relates to a transaction's uncertainty interval, but it does not
		// take into consideration "effective" clock skew as it relates to a
		// follower replica having a faster clock than a leaseholder and
		// therefore needing the leaseholder to publish even further into the
		// future. Since the effect of getting this wrong is reduced performance
		// (i.e. missed follower reads) and not a correctness violation (i.e.
		// stale reads), we can be less strict here. We also expect that even
		// when two nodes have skewed physical clocks, the "stability" property
		// of HLC propagation when nodes are communicating should reduce the
		// effective HLC clock skew.

		// TODO(nvanbenschoten): make this dynamic, based on the measured
		// network latencies recorded by the RPC context. This isn't trivial and
		// brings up a number of questions. For instance, how far into the tail
		// do we care about? Do we place upper and lower bounds on this value?
		const maxNetworkRTT = 150 * time.Millisecond

		// See raft_propagation_time.
		const raftTransportOverhead = 20 * time.Millisecond
		raftTransportPropTime := (maxNetworkRTT*3)/2 + raftTransportOverhead

		// See side_propagation_time.
		sideTransportPropTime := maxNetworkRTT/2 + sideTransportCloseInterval

		// See propagation_time.
		maxTransportPropTime := sideTransportPropTime
		if maxTransportPropTime < raftTransportPropTime {
			maxTransportPropTime = raftTransportPropTime
		}

		// Include a small amount of extra margin to smooth out temporary
		// network blips or anything else that slows down closed timestamp
		// propagation momentarily.
		const bufferTime = 25 * time.Millisecond
		leadTimeAtSender := maxTransportPropTime + maxClockOffset + bufferTime

		// Override entirely with cluster setting, if necessary.
		if leadTargetOverride != 0 {
			leadTimeAtSender = leadTargetOverride
		}

		// Mark as synthetic, because this time is in the future.
		res = now.ToTimestamp().Add(leadTimeAtSender.Nanoseconds(), 0).WithSynthetic(true)
	default:
		panic("unexpected RangeClosedTimestampPolicy")
	}
	// We truncate the logical part in order to save a few bytes over the network,
	// and also because arithmetic with logical timestamp doesn't make much sense.
	res.Logical = 0
	return res
}
