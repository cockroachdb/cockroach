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

package batcheval

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.RecoverTxn, declareKeysRecoverTransaction, RecoverTxn)
}

func declareKeysRecoverTransaction(
	_ roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	rr := req.(*roachpb.RecoverTxnRequest)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(rr.Txn.Key, rr.Txn.ID)})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, rr.Txn.ID)})
}

// RecoverTxn attempts to recover the specified transaction from an
// indeterminate commit state. Transactions enter this state when abandoned
// after updating their transaction record with a STAGING status. The
// RecoveryTxn operation is invoked by a caller who encounters a transaction in
// this state after they have already queried all of the STAGING transaction's
// declared in-flight writes. The caller specifies whether all of these
// in-flight writes were found to have succeeded or not. This is used by
// RecoveryTxn to determine whether the result of the recovery should be
// committing the abandoned transaction or aborting it.
func RecoverTxn(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RecoverTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.RecoverTxnResponse)

	if cArgs.Header.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	if !bytes.Equal(args.Key, args.Txn.Key) {
		return result.Result{}, errors.Errorf("request key %s does not match txn key %s", args.Key, args.Txn.Key)
	}
	if h.Timestamp.Less(args.Txn.Timestamp) {
		// This condition must hold for the timestamp cache update to be safe.
		return result.Result{}, errors.Errorf("request timestamp %v less than txn timestamp %v", h.Timestamp, args.Txn.Timestamp)
	}
	key := keys.TransactionKey(args.Txn.Key, args.Txn.ID)

	// Fetch transaction record; if missing, attempt to synthesize one.
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.Timestamp{}, &reply.RecoveredTxn, engine.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// The transaction's record must have been removed already.
		// Synthesize it from the provided TxnMeta to have something
		// to return. The synthesized record should have an ABORTED
		// status because it was already GCed. If not, something went
		// wrong for us to get to this point.
		synthTxn := SynthesizeTxnFromMeta(cArgs.EvalCtx, args.Txn)
		if synthTxn.Status != roachpb.ABORTED {
			err := errors.Errorf("txn record synthesized with non-ABORTED status: %v", synthTxn)
			return result.Result{}, err
		}
		reply.RecoveredTxn = synthTxn
		return result.Result{}, nil
	}

	// If already committed or aborted, return success.
	if reply.RecoveredTxn.Status.IsFinalized() {
		// Trivial noop.
		return result.Result{}, nil
	}

	// If the transaction has changed epoch or timestamp, return its current
	// state unchanged. The recovery was not needed after all because the
	// transaction is still in progress.
	if args.Txn.Epoch < reply.RecoveredTxn.Epoch ||
		args.Txn.Timestamp.Less(reply.RecoveredTxn.Timestamp) {
		// Recovery not immediately needed.
		return result.Result{}, nil
	}

	// Recover the transaction based on whether or not all of its writes
	// succeeded. If all of the writes succeeded then the transaction was
	// implicitly committed and an acknowledgement of success may have already
	// been returned to clients. If not, then we should have prevented the
	// transaction from ever becoming implicitly committed at this timestamp
	// using a QueryIntent(IfMissing=PREVENT), so we're free to abort the
	// transaction record.
	if args.AllWritesFound {
		reply.RecoveredTxn.Status = roachpb.COMMITTED
	} else {
		reply.RecoveredTxn.Status = roachpb.ABORTED
	}
	txnRecord := reply.RecoveredTxn.AsRecord()
	if err := engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
		return result.Result{}, err
	}

	result := result.Result{}
	result.Local.UpdatedTxns = &[]*roachpb.Transaction{&reply.RecoveredTxn}
	return result, nil
}
