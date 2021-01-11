// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package observedts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// LimitTxnMaxTimestamp limits the transaction's max timestamp so that it
// respects any timestamp already observed on the leaseholder's node. This
// prevents unnecessary uncertainty interval restarts caused by reading a value
// written at a timestamp between txn.Timestamp and txn.MaxTimestamp.
//
// The lease's start time is also taken into consideration to ensure that a
// lease transfer does not result in the observed timestamp for this node being
// inapplicable to data previously written by the former leaseholder. To wit:
//
//  1. put(k on leaseholder n1), gateway chooses t=1.0
//  2. begin; read(unrelated key on n2); gateway chooses t=0.98
//  3. pick up observed timestamp for n2 of t=0.99
//  4. n1 transfers lease for range with k to n2 @ t=1.1
//  5. read(k) on leaseholder n2 at ReadTimestamp=0.98 should get
//     ReadWithinUncertaintyInterval because of the write in step 1, so
//     even though we observed n2's timestamp in step 3 we must expand
//     the uncertainty interval to the lease's start time, which is
//     guaranteed to be greater than any write which occurred under
//     the previous leaseholder.
//
func LimitTxnMaxTimestamp(
	ctx context.Context, txn *roachpb.Transaction, status kvserverpb.LeaseStatus,
) *roachpb.Transaction {
	if txn == nil || status.State != kvserverpb.LeaseState_VALID {
		return txn
	}
	// For calls that read data within a txn, we keep track of timestamps
	// observed from the various participating nodes' HLC clocks. If we have a
	// timestamp on file for the leaseholder's node which is smaller than
	// MaxTimestamp, we can lower MaxTimestamp accordingly. If MaxTimestamp
	// drops below ReadTimestamp, we effectively can't see uncertainty restarts
	// anymore.
	//
	// Note that we care about an observed timestamp from the leaseholder's
	// node, even if this is a follower read on a different node. See the
	// comment in doc.go about "Follower Reads" for more.
	obsClockTS, ok := txn.GetObservedTimestamp(status.Lease.Replica.NodeID)
	if !ok {
		return txn
	}
	// If the lease is valid, we use the greater of the observed timestamp and
	// the lease start time, up to the max timestamp. This ensures we avoid
	// incorrect assumptions about when data was written, in absolute time on a
	// different node, which held the lease before this replica acquired it.
	obsClockTS.Forward(status.Lease.Start)
	obsTS := obsClockTS.ToTimestamp()
	// If the observed timestamp reduces the transaction's uncertainty interval,
	// update the transacion proto.
	if obsTS.Less(txn.MaxTimestamp) {
		// Copy-on-write to protect others we might be sharing the Txn with.
		txnClone := txn.Clone()
		// The uncertainty window is [ReadTimestamp, maxTS), so if that window
		// is empty, there won't be any uncertainty restarts.
		if obsTS.LessEq(txn.ReadTimestamp) {
			log.Event(ctx, "read has no clock uncertainty")
		}
		txnClone.MaxTimestamp.Backward(obsTS)
		txn = txnClone
	}
	return txn
}
