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

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MaxSafeTimestampInterval specifies the duration of the interval
// after which reading from "up-to-date" follower replicas is safe.
//
// Nodes send a max safe timestamp with coalesced heartbeats. Receipt
// of a heartbeat from a node which is both the leaseholder and the
// Raft leader means the maxSafeTS can be trusted to apply to each
// follower replica which has committed at or over a max safe log
// index, a new value supplied with heartbeats.
//
// Nodes keep track of their min proposal timestamp. On every
// heartbeat, the min proposal is updated and persisted locally to
// ensure monotonicity on node restart. At startup, a node reads the
// last persisted min proposal, and forwards the HLC clock if
// necessary. On command evaluation, the batch timestamp is forwarded
// if less than the minmum proposal timestamp.
//
// Things get more interesting when a range quiesces. Replicas of
// quiesced ranges no longer receive heartbeats. However, if a replica
// is quiesced, we can continue to rely on the most recent maxSafeTS
// supplied in coalesced heartbeats, so long as the liveness epoch
// (reported with heartbeats) remains stable. Because Raft leadership
// can change while leaseholdership remains stable, the coalesced
// heartbeat protocol includes "unsafe" range IDs to which the node's
// maxSafeTS no longer applies. Any range where the leaseholder has to
// forward commands to the Raft leader, or where the lease has
// changed, is reported as unsafe. Removed replicas are also reported
// as unsafe.
//
// Nodes maintain a map from node/store ID to a maxSafeTS, which
// contains a maxSafe timestamp and a map from range ID to "safe" log
// indexes. Any replica which has committed to the "safe" log index
// can serve follower reads at timestamps earlier than the maxSafeTS,
// if the node which reported it is the valid leaseholder (requires an
// epoch lease). In the event a node reports that its maxSafeTS no
// longer applies to a range ID, that range ID is removed from the
// set.
//
// As previously mentioned, ranges are marked as "unsafe" on lease
// update, any leaseholder-not-leader writes, and on replica removal.
// Untrusted ranges are kept in a map and sent with coalesced
// heartbeats. They remain in the untrusted map until they're
// explicitly acknowledged by the receiver to avoid allowing the
// intended recipient to continue trusting the range on a missed
// heartbeat.
//
// Note that on leaseholder change, the new leaseholder will never
// allow a write at an earlier timestamp than a previously reported
// max safe timestamp. This is due to the low water timestamp in the
// timestamp cache being reset on leaseholder transfer to prevent
// rewriting history in general.
const MaxSafeTimestampInterval = base.MaxTransactionAge

// maxSafeTS contains information about which ranges can be safely
// read from if the range epoch lease is valid and the local replica
// has committed its log to at least the specified min commit index.
type maxSafeTS struct {
	hlc.Timestamp
	epoch      int64
	minCommits map[roachpb.RangeID]uint64
}
