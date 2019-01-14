// Copyright 2019 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// evaluateProposal generates a Result from the given request by
// evaluating it, returning both state which is held only on the
// proposer and that which is to be replicated through Raft. The
// return value is ready to be inserted into Replica's proposal map
// and subsequently passed to submitProposalLocked.
//
// The method also returns a flag indicating if the request needs to
// be proposed through Raft and replicated. This flag will be false
// either if the request was a no-op or if it hit an error. In this
// case, the result can be sent directly back to the client without
// going through Raft, but while still handling LocalEvalResult.
//
// Replica.mu must not be held.
func (r *Replica) evaluateProposal(
	ctx context.Context, idKey storagebase.CmdIDKey, ba roachpb.BatchRequest, spans *spanset.SpanSet,
) (*result.Result, bool, *roachpb.Error) {
	if ba.Timestamp == (hlc.Timestamp{}) {
		return nil, false, roachpb.NewErrorf("can't propose Raft command with zero timestamp")
	}

	// Evaluate the commands. If this returns without an error, the batch should
	// be committed. Note that we don't hold any locks at this point. This is
	// important since evaluating a proposal is expensive.
	// TODO(tschottdorf): absorb all returned values in `res` below this point
	// in the call stack as well.
	batch, ms, br, res, pErr := r.evaluateWriteBatch(ctx, idKey, ba, spans)

	// Note: reusing the proposer's batch when applying the command on the
	// proposer was explored as an optimization but resulted in no performance
	// benefit.
	defer batch.Close()

	if pErr != nil {
		pErr = r.maybeSetCorrupt(ctx, pErr)

		txn := pErr.GetTxn()
		if txn != nil && ba.Txn == nil {
			log.Fatalf(ctx, "error had a txn but batch is non-transactional. Err txn: %s", txn)
		}

		// Failed proposals can't have any Result except for what's
		// whitelisted here.
		intents := res.Local.DetachIntents()
		endTxns := res.Local.DetachEndTxns(true /* alwaysOnly */)
		res.Local = result.LocalResult{
			Intents: &intents,
			EndTxns: &endTxns,
			Metrics: res.Local.Metrics,
		}
		res.Replicated.Reset()
		return &res, false /* needConsensus */, pErr
	}

	// Set the local reply, which is held only on the proposing replica and is
	// returned to the client after the proposal completes, or immediately if
	// replication is not necessary.
	res.Local.Reply = br

	// needConsensus determines if the result needs to be replicated and
	// proposed through Raft. This is necessary if at least one of the
	// following conditions is true:
	// 1. the request created a non-empty write batch.
	// 2. the request had an impact on the MVCCStats. NB: this is possible
	//    even with an empty write batch when stats are recomputed.
	// 3. the request has replicated side-effects.
	// 4. the cluster is in "clockless" mode, in which case consensus is
	//    used to enforce a linearization of all reads and writes.
	needConsensus := !batch.Empty() ||
		ms != (enginepb.MVCCStats{}) ||
		!res.Replicated.Equal(storagepb.ReplicatedEvalResult{}) ||
		r.store.Clock().MaxOffset() == timeutil.ClocklessMaxOffset

	if needConsensus {
		// Set the proposal's WriteBatch, which is the serialized representation of
		// the proposals effect on RocksDB.
		res.WriteBatch = &storagepb.WriteBatch{
			Data: batch.Repr(),
		}

		// Set the proposal's replicated result, which contains metadata and
		// side-effects that are to be replicated to all replicas.
		res.Replicated.IsLeaseRequest = ba.IsLeaseRequest()
		res.Replicated.Timestamp = ba.Timestamp
		if r.store.cfg.Settings.Version.IsActive(cluster.VersionMVCCNetworkStats) {
			res.Replicated.Delta = ms.ToStatsDelta()
		} else {
			res.Replicated.DeprecatedDelta = &ms
		}
		// If the RangeAppliedState key is not being used and the cluster version is
		// high enough to guarantee that all current and future binaries will
		// understand the key, we send the migration flag through Raft. Because
		// there is a delay between command proposal and application, we may end up
		// setting this migration flag multiple times. This is ok, because the
		// migration is idempotent.
		// TODO(nvanbenschoten): This will be baked in to 2.1, so it can be removed
		// in the 2.2 release.
		r.mu.RLock()
		usingAppliedStateKey := r.mu.state.UsingAppliedStateKey
		r.mu.RUnlock()
		if !usingAppliedStateKey &&
			r.ClusterSettings().Version.IsMinSupported(cluster.VersionRangeAppliedStateKey) {
			if res.Replicated.State == nil {
				res.Replicated.State = &storagepb.ReplicaState{}
			}
			res.Replicated.State.UsingAppliedStateKey = true
		}
	}

	return &res, needConsensus, nil
}
