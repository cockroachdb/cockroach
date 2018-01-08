// Copyright 2014 The Cockroach Authors.
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

package batcheval

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/txnwait"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.PushTxn, declareKeysPushTransaction, PushTxn)
}

func declareKeysPushTransaction(
	_ roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	pr := req.(*roachpb.PushTxnRequest)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(pr.PusheeTxn.Key, pr.PusheeTxn.ID)})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, pr.PusheeTxn.ID)})
}

// PushTxn resolves conflicts between concurrent txns (or
// between a non-transactional reader or writer and a txn) in several
// ways depending on the statuses and priorities of the conflicting
// transactions. The PushTxn operation is invoked by a
// "pusher" (the writer trying to abort a conflicting txn or the
// reader trying to push a conflicting txn's commit timestamp
// forward), who attempts to resolve a conflict with a "pushee"
// (args.PushTxn -- the pushee txn whose intent(s) caused the
// conflict). A pusher is either transactional, in which case
// PushTxn is completely initialized, or not, in which case the
// PushTxn has only the priority set.
//
// Txn already committed/aborted: If pushee txn is committed or
// aborted return success.
//
// Txn Timeout: If pushee txn entry isn't present or its LastHeartbeat
// timestamp isn't set, use its as LastHeartbeat. If current time -
// LastHeartbeat > 2 * DefaultHeartbeatInterval, then the pushee txn
// should be either pushed forward, aborted, or confirmed not pending,
// depending on value of Request.PushType.
//
// Old Txn Epoch: If persisted pushee txn entry has a newer Epoch than
// PushTxn.Epoch, return success, as older epoch may be removed.
//
// Lower Txn Priority: If pushee txn has a lower priority than pusher,
// adjust pushee's persisted txn depending on value of
// args.PushType. If args.PushType is PUSH_ABORT, set txn.Status to
// ABORTED, and priority to one less than the pusher's priority and
// return success. If args.PushType is PUSH_TIMESTAMP, set
// txn.Timestamp to just after PushTo.
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionPushError. Transaction will be retried
// with priority one less than the pushee's higher priority.
//
// If the pusher is non-transactional, args.PusherTxn is an empty
// proto with only the priority set.
//
// If the pushee is aborted, its timestamp will be forwarded to match its last
// client activity timestamp (i.e. last heartbeat), if available. This is done
// so that the updated timestamp populates the AbortSpan, allowing the GC
// queue to purge entries for which the transaction coordinator must have found
// out via its heartbeats that the transaction has failed.
func PushTxn(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.PushTxnRequest)
	reply := resp.(*roachpb.PushTxnResponse)

	if cArgs.Header.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	if args.Now == (hlc.Timestamp{}) {
		return result.Result{}, errors.Errorf("the field Now must be provided")
	}
	if args.PushType == roachpb.PUSH_QUERY {
		return result.Result{}, errors.Errorf("PUSH_QUERY no longer supported")
	}

	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		return result.Result{}, errors.Errorf("request key %s should match pushee's txn key %s", args.Key, args.PusheeTxn.Key)
	}
	key := keys.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction; if missing, we're allowed to abort.
	existTxn := &roachpb.Transaction{}
	ok, err := engine.MVCCGetProto(ctx, batch, key, hlc.Timestamp{},
		true /* consistent */, nil /* txn */, existTxn)
	if err != nil {
		return result.Result{}, err
	}
	// There are three cases in which there is no transaction entry:
	//
	// * the pushee is still active but the BeginTransaction was delayed
	//   for long enough that a write intent from this txn to another
	//   range is causing another reader or writer to push.
	// * the pushee resolved its intents synchronously on successful commit;
	//   in this case, the transaction record of the pushee is also removed.
	//   Note that in this case, the intent which prompted this PushTxn
	//   doesn't exist any more.
	// * the pushee timed out or was aborted and the intent not cleaned up,
	//   but the transaction record was garbage collected.
	//
	// We currently make no attempt at guessing which one it is, though we
	// could (see #1939). Instead, a new aborted entry is always written.
	//
	// TODO(tschottdorf): we should actually improve this when we
	// garbage-collect aborted transactions, or we run the risk of a push
	// recreating a GC'ed transaction as PENDING, which is an error if it
	// has open intents (which is likely if someone pushes it).
	if !ok {
		// The transaction doesn't exist on disk; we're allowed to abort it.
		// TODO(tschottdorf): especially for SNAPSHOT transactions, there's
		// something to win here by not aborting, but instead pushing the
		// timestamp. For SERIALIZABLE it's less important, but still better
		// to have them restart than abort. See #3344.
		// TODO(tschottdorf): double-check for problems emanating from
		// using a trivial Transaction proto here. Maybe some fields ought
		// to receive dummy values.
		reply.PusheeTxn.Status = roachpb.ABORTED
		reply.PusheeTxn.TxnMeta = args.PusheeTxn
		reply.PusheeTxn.Timestamp = args.Now // see method comment
		// Setting OrigTimestamp bumps LastActive(); see #9265.
		reply.PusheeTxn.OrigTimestamp = args.Now
		result := result.Result{}
		result.Local.UpdatedTxns = &[]*roachpb.Transaction{&reply.PusheeTxn}
		return result, engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, &reply.PusheeTxn)
	}
	// Start with the persisted transaction record as final transaction.
	reply.PusheeTxn = existTxn.Clone()

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status != roachpb.PENDING {
		// Trivial noop.
		return result.Result{}, nil
	}

	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if args.PushType == roachpb.PUSH_TIMESTAMP && args.PushTo.Less(reply.PusheeTxn.Timestamp) {
		// Trivial noop.
		return result.Result{}, nil
	}

	// The pusher might be aware of a newer version of the pushee.
	reply.PusheeTxn.Timestamp.Forward(args.PusheeTxn.Timestamp)
	if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
		reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
	}
	reply.PusheeTxn.UpgradePriority(args.PusheeTxn.Priority)

	var pusherWins bool
	var reason string

	switch {
	case txnwait.IsExpired(args.Now, &reply.PusheeTxn):
		reason = "pushee is expired"
		// When cleaning up, actually clean up (as opposed to simply pushing
		// the garbage in the path of future writers).
		args.PushType = roachpb.PUSH_ABORT
		pusherWins = true
	case args.PushType == roachpb.PUSH_TOUCH:
		// If just attempting to cleanup old or already-committed txns,
		// pusher always fails.
		pusherWins = false
	case args.PushType == roachpb.PUSH_TIMESTAMP &&
		reply.PusheeTxn.Isolation == enginepb.SNAPSHOT:
		// Can always push a SNAPSHOT txn's timestamp.
		reason = "pushee is SNAPSHOT"
		pusherWins = true
	case CanPushWithPriority(&args.PusherTxn, &reply.PusheeTxn):
		reason = "pusher has priority"
		pusherWins = true
	case args.Force:
		reason = "forced push"
		pusherWins = true
	}

	if log.V(1) && reason != "" {
		s := "pushed"
		if !pusherWins {
			s = "failed to push"
		}
		log.Infof(ctx, "%s "+s+" %s: %s (pushee last active: %s)",
			args.PusherTxn.Short(), args.PusheeTxn.Short(),
			reason, reply.PusheeTxn.LastActive())
	}

	if !pusherWins {
		err := roachpb.NewTransactionPushError(reply.PusheeTxn)
		if log.V(1) {
			log.Infof(ctx, "%v", err)
		}
		return result.Result{}, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(args.PusherTxn.Priority - 1)

	// If aborting transaction, set new status and return success.
	if args.PushType == roachpb.PUSH_ABORT {
		reply.PusheeTxn.Status = roachpb.ABORTED
		// Forward the timestamp to accommodate AbortSpan GC. See method
		// comment for details.
		reply.PusheeTxn.Timestamp.Forward(reply.PusheeTxn.LastActive())
	} else if args.PushType == roachpb.PUSH_TIMESTAMP {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PusheeTxn.Timestamp = args.PushTo
		reply.PusheeTxn.Timestamp.Logical++
	}

	// Persist the pushed transaction using zero timestamp for inline value.
	if err := engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, &reply.PusheeTxn); err != nil {
		return result.Result{}, err
	}
	result := result.Result{}
	result.Local.UpdatedTxns = &[]*roachpb.Transaction{&reply.PusheeTxn}
	return result, nil
}
