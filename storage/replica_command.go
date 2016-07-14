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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var errTransactionUnsupported = errors.New("not supported within a transaction")

// executeCmd switches over the method and multiplexes to execute the appropriate storage API
// command. It returns the response, an error, and a post commit trigger which
// may be actionable even in the case of an error.
// maxKeys is the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func (r *Replica) executeCmd(
	ctx context.Context,
	raftCmdID storagebase.CmdIDKey,
	index int,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	maxKeys int64,
	args roachpb.Request,
	reply roachpb.Response,
) (*PostCommitTrigger, *roachpb.Error) {
	ts := h.Timestamp

	if _, ok := args.(*roachpb.NoopRequest); ok {
		return nil, nil
	}

	if err := r.checkCmdHeader(args.Header()); err != nil {
		return nil, roachpb.NewErrorWithTxn(err, h.Txn)
	}

	// If a unittest filter was installed, check for an injected error; otherwise, continue.
	if filter := r.store.ctx.TestingKnobs.TestingCommandFilter; filter != nil {
		filterArgs := storagebase.FilterArgs{Ctx: ctx, CmdID: raftCmdID, Index: index,
			Sid: r.store.StoreID(), Req: args, Hdr: h}
		if pErr := filter(filterArgs); pErr != nil {
			log.Infof(ctx, "%s: test injecting error: %s", r, pErr)
			return nil, pErr
		}
	}

	// Update the node clock with the serviced request. This maintains a
	// high water mark for all ops serviced, so that received ops
	// without a timestamp specified are guaranteed one higher than any
	// op already executed for overlapping keys.
	r.store.Clock().Update(ts)

	var err error
	var trigger *PostCommitTrigger

	// Note that responses are populated even when an error is returned.
	// TODO(tschottdorf): Change that. IIRC there is nontrivial use of it currently.
	switch tArgs := args.(type) {
	case *roachpb.GetRequest:
		resp := reply.(*roachpb.GetResponse)
		*resp, trigger, err = r.Get(ctx, batch, h, *tArgs)
	case *roachpb.PutRequest:
		resp := reply.(*roachpb.PutResponse)
		*resp, err = r.Put(ctx, batch, ms, h, *tArgs)
	case *roachpb.ConditionalPutRequest:
		resp := reply.(*roachpb.ConditionalPutResponse)
		*resp, err = r.ConditionalPut(ctx, batch, ms, h, *tArgs)
	case *roachpb.InitPutRequest:
		resp := reply.(*roachpb.InitPutResponse)
		*resp, err = r.InitPut(ctx, batch, ms, h, *tArgs)
	case *roachpb.IncrementRequest:
		resp := reply.(*roachpb.IncrementResponse)
		*resp, err = r.Increment(ctx, batch, ms, h, *tArgs)
	case *roachpb.DeleteRequest:
		resp := reply.(*roachpb.DeleteResponse)
		*resp, err = r.Delete(ctx, batch, ms, h, *tArgs)
	case *roachpb.DeleteRangeRequest:
		resp := reply.(*roachpb.DeleteRangeResponse)
		*resp, err = r.DeleteRange(ctx, batch, ms, h, *tArgs)
	case *roachpb.ScanRequest:
		resp := reply.(*roachpb.ScanResponse)
		*resp, trigger, err = r.Scan(ctx, batch, h, maxKeys, *tArgs)
	case *roachpb.ReverseScanRequest:
		resp := reply.(*roachpb.ReverseScanResponse)
		*resp, trigger, err = r.ReverseScan(ctx, batch, h, maxKeys, *tArgs)
	case *roachpb.BeginTransactionRequest:
		resp := reply.(*roachpb.BeginTransactionResponse)
		*resp, err = r.BeginTransaction(ctx, batch, ms, h, *tArgs)
	case *roachpb.EndTransactionRequest:
		resp := reply.(*roachpb.EndTransactionResponse)
		*resp, trigger, err = r.EndTransaction(ctx, batch, ms, h, *tArgs)
	case *roachpb.RangeLookupRequest:
		resp := reply.(*roachpb.RangeLookupResponse)
		*resp, trigger, err = r.RangeLookup(ctx, batch, h, *tArgs)
	case *roachpb.HeartbeatTxnRequest:
		resp := reply.(*roachpb.HeartbeatTxnResponse)
		*resp, err = r.HeartbeatTxn(ctx, batch, ms, h, *tArgs)
	case *roachpb.GCRequest:
		resp := reply.(*roachpb.GCResponse)
		*resp, trigger, err = r.GC(ctx, batch, ms, h, *tArgs)
	case *roachpb.PushTxnRequest:
		resp := reply.(*roachpb.PushTxnResponse)
		*resp, err = r.PushTxn(ctx, batch, ms, h, *tArgs)
	case *roachpb.ResolveIntentRequest:
		resp := reply.(*roachpb.ResolveIntentResponse)
		*resp, err = r.ResolveIntent(ctx, batch, ms, h, *tArgs)
	case *roachpb.ResolveIntentRangeRequest:
		resp := reply.(*roachpb.ResolveIntentRangeResponse)
		*resp, err = r.ResolveIntentRange(ctx, batch, ms, h, *tArgs)
	case *roachpb.MergeRequest:
		resp := reply.(*roachpb.MergeResponse)
		*resp, err = r.Merge(ctx, batch, ms, h, *tArgs)
	case *roachpb.TruncateLogRequest:
		resp := reply.(*roachpb.TruncateLogResponse)
		*resp, trigger, err = r.TruncateLog(ctx, batch, ms, h, *tArgs)
	case *roachpb.RequestLeaseRequest:
		resp := reply.(*roachpb.RequestLeaseResponse)
		*resp, trigger, err = r.RequestLease(ctx, batch, ms, h, *tArgs)
	case *roachpb.TransferLeaseRequest:
		resp := reply.(*roachpb.RequestLeaseResponse)
		*resp, trigger, err = r.TransferLease(ctx, batch, ms, h, *tArgs)
	case *roachpb.ComputeChecksumRequest:
		resp := reply.(*roachpb.ComputeChecksumResponse)
		*resp, trigger, err = r.ComputeChecksum(ctx, batch, ms, h, *tArgs)
	case *roachpb.VerifyChecksumRequest:
		resp := reply.(*roachpb.VerifyChecksumResponse)
		*resp, trigger, err = r.VerifyChecksum(ctx, batch, ms, h, *tArgs)
	case *roachpb.ChangeFrozenRequest:
		resp := reply.(*roachpb.ChangeFrozenResponse)
		*resp, trigger, err = r.ChangeFrozen(ctx, batch, ms, h, *tArgs)
	default:
		err = errors.Errorf("unrecognized command %s", args.Method())
	}

	if log.V(2) {
		log.Infof(ctx, "%s: executed %s command %+v: %+v, err=%s", r, args.Method(), args, reply, err)
	}

	// Create a roachpb.Error by initializing txn from the request/response header.
	var pErr *roachpb.Error
	if err != nil {
		txn := reply.Header().Txn
		if txn == nil {
			txn = h.Txn
		}
		pErr = roachpb.NewErrorWithTxn(err, txn)
	}

	return trigger, pErr
}

func intentsToTrigger(intents []roachpb.Intent, args roachpb.Request) *PostCommitTrigger {
	if len(intents) > 0 {
		return &PostCommitTrigger{intents: []intentsWithArg{{args: args, intents: intents}}}
	}
	return nil
}

// Get returns the value for a specified key.
func (r *Replica) Get(
	ctx context.Context, batch engine.ReadWriter, h roachpb.Header, args roachpb.GetRequest,
) (roachpb.GetResponse, *PostCommitTrigger, error) {
	var reply roachpb.GetResponse

	val, intents, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	reply.Value = val
	return reply, intentsToTrigger(intents, &args), err
}

// Put sets the value for a specified key.
func (r *Replica) Put(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.PutRequest,
) (roachpb.PutResponse, error) {
	var reply roachpb.PutResponse
	ts := hlc.ZeroTimestamp
	if !args.Inline {
		ts = h.Timestamp
	}
	if h.DistinctSpans {
		if b, ok := batch.(engine.Batch); ok {
			// Use the distinct batch for both blind and normal ops so that we don't
			// accidentally flush mutations to make them visible to the distinct
			// batch.
			batch = b.Distinct()
			defer batch.Close()
		}
	}
	if args.Blind {
		return reply, engine.MVCCBlindPut(ctx, batch, ms, args.Key, ts, args.Value, h.Txn)
	}
	return reply, engine.MVCCPut(ctx, batch, ms, args.Key, ts, args.Value, h.Txn)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Replica) ConditionalPut(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.ConditionalPutRequest,
) (roachpb.ConditionalPutResponse, error) {
	var reply roachpb.ConditionalPutResponse

	if h.DistinctSpans {
		if b, ok := batch.(engine.Batch); ok {
			// Use the distinct batch for both blind and normal ops so that we don't
			// accidentally flush mutations to make them visible to the distinct
			// batch.
			batch = b.Distinct()
			defer batch.Close()
		}
	}
	if args.Blind {
		return reply, engine.MVCCBlindConditionalPut(ctx, batch, ms, args.Key, h.Timestamp, args.Value, args.ExpValue, h.Txn)
	}
	return reply, engine.MVCCConditionalPut(ctx, batch, ms, args.Key, h.Timestamp, args.Value, args.ExpValue, h.Txn)
}

// InitPut sets the value for a specified key only if it doesn't exist. It
// returns an error if the key exists with an existing value that is different
// from the value provided.
func (r *Replica) InitPut(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.InitPutRequest,
) (roachpb.InitPutResponse, error) {
	var reply roachpb.InitPutResponse

	return reply, engine.MVCCInitPut(ctx, batch, ms, args.Key, h.Timestamp, args.Value, h.Txn)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Replica) Increment(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.IncrementRequest,
) (roachpb.IncrementResponse, error) {
	var reply roachpb.IncrementResponse

	newVal, err := engine.MVCCIncrement(ctx, batch, ms, args.Key, h.Timestamp, h.Txn, args.Increment)
	reply.NewValue = newVal
	return reply, err
}

// Delete deletes the key and value specified by key.
func (r *Replica) Delete(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.DeleteRequest,
) (roachpb.DeleteResponse, error) {
	var reply roachpb.DeleteResponse

	return reply, engine.MVCCDelete(ctx, batch, ms, args.Key, h.Timestamp, h.Txn)
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Replica) DeleteRange(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.DeleteRangeRequest,
) (roachpb.DeleteRangeResponse, error) {
	var reply roachpb.DeleteRangeResponse
	// TODO(vivek): replace MaxInt64 with a specific bound passed in.
	deleted, err := engine.MVCCDeleteRange(ctx, batch, ms, args.Key, args.EndKey, math.MaxInt64, h.Timestamp, h.Txn, args.ReturnKeys)
	if err == nil {
		reply.Keys = deleted
		// DeleteRange requires that we retry on push to avoid the lost delete range anomaly.
		if h.Txn != nil {
			clonedTxn := h.Txn.Clone()
			clonedTxn.RetryOnPush = true
			reply.Txn = &clonedTxn
		}
	}
	return reply, err
}

// Scan scans the key range specified by start key through end key in ascending order up to some
// maximum number of results. maxKeys stores the number of scan results remaining for this
// batch (MaxInt64 for no limit).
func (r *Replica) Scan(
	ctx context.Context,
	batch engine.ReadWriter,
	h roachpb.Header,
	maxKeys int64,
	args roachpb.ScanRequest,
) (roachpb.ScanResponse, *PostCommitTrigger, error) {
	rows, intents, err := engine.MVCCScan(ctx, batch, args.Key, args.EndKey, maxKeys, h.Timestamp,
		h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	return roachpb.ScanResponse{Rows: rows}, intentsToTrigger(intents, &args), err
}

// ReverseScan scans the key range specified by start key through end key in descending order up to
// some maximum number of results. maxKeys stores the number of scan results remaining for
// this batch (MaxInt64 for no limit).
func (r *Replica) ReverseScan(
	ctx context.Context,
	batch engine.ReadWriter,
	h roachpb.Header,
	maxKeys int64,
	args roachpb.ReverseScanRequest,
) (roachpb.ReverseScanResponse, *PostCommitTrigger, error) {
	rows, intents, err := engine.MVCCReverseScan(ctx, batch, args.Key, args.EndKey, maxKeys,
		h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	return roachpb.ReverseScanResponse{Rows: rows}, intentsToTrigger(intents, &args), err
}

func verifyTransaction(h roachpb.Header, args roachpb.Request) error {
	if h.Txn == nil {
		return errors.Errorf("no transaction specified to %s", args.Method())
	}
	if !bytes.Equal(args.Header().Key, h.Txn.Key) {
		return errors.Errorf("request key %s should match txn key %s", args.Header().Key, h.Txn.Key)
	}
	return nil
}

// BeginTransaction writes the initial transaction record. Fails in
// the event that a transaction record is already written. This may
// occur if a transaction is started with a batch containing writes
// to different ranges, and the range containing the txn record fails
// to receive the write batch before a heartbeat or txn push is
// performed first and aborts the transaction.
func (r *Replica) BeginTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.BeginTransactionRequest,
) (roachpb.BeginTransactionResponse, error) {
	var reply roachpb.BeginTransactionResponse

	if err := verifyTransaction(h, &args); err != nil {
		return reply, err
	}
	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)
	clonedTxn := h.Txn.Clone()
	reply.Txn = &clonedTxn

	// Verify transaction does not already exist.
	txn := roachpb.Transaction{}
	ok, err := engine.MVCCGetProto(ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txn)
	if err != nil {
		return reply, err
	}
	if ok {
		switch txn.Status {
		case roachpb.ABORTED:
			// Check whether someone has come in ahead and already aborted the
			// txn.
			return reply, roachpb.NewTransactionAbortedError()

		case roachpb.PENDING:
			if h.Txn.Epoch > txn.Epoch {
				// On a transaction retry there will be an extant txn record
				// but this run should have an upgraded epoch. The extant txn
				// record may have been pushed or otherwise updated, so update
				// this command's txn and rewrite the record.
				reply.Txn.Update(&txn)
			} else {
				return reply, roachpb.NewTransactionStatusError(
					fmt.Sprintf("BeginTransaction can't overwrite %s", txn))
			}

		case roachpb.COMMITTED:
			return reply, roachpb.NewTransactionStatusError(
				fmt.Sprintf("BeginTransaction can't overwrite %s", txn),
			)

		default:
			return reply, roachpb.NewTransactionStatusError(
				fmt.Sprintf("bad txn state: %s", txn),
			)
		}
	}

	// Write the txn record.
	reply.Txn.Writing = true
	return reply, engine.MVCCPutProto(ctx, batch, ms, key, hlc.ZeroTimestamp, nil, reply.Txn)
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter. Rolling back
// an already rolled-back txn is ok.
func (r *Replica) EndTransaction(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.EndTransactionRequest,
) (roachpb.EndTransactionResponse, *PostCommitTrigger, error) {
	var reply roachpb.EndTransactionResponse

	if err := verifyTransaction(h, &args); err != nil {
		return reply, nil, err
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	// Fetch existing transaction.
	reply.Txn = &roachpb.Transaction{}
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.ZeroTimestamp, true, nil, reply.Txn,
	); err != nil {
		return reply, nil, err
	} else if !ok {
		// Return a fresh empty reply because there's an empty Transaction
		// proto in our existing one.
		return roachpb.EndTransactionResponse{},
			nil, roachpb.NewTransactionStatusError("does not exist")
	}

	// Verify that we can either commit it or abort it (according
	// to args.Commit), and also that the Timestamp and Epoch have
	// not suffered regression.
	switch reply.Txn.Status {
	case roachpb.COMMITTED:
		return reply, nil, roachpb.NewTransactionStatusError("already committed")

	case roachpb.ABORTED:
		if !args.Commit {
			// The transaction has already been aborted by other.
			// Do not return TransactionAbortedError since the client anyway
			// wanted to abort the transaction.
			externalIntents := r.resolveLocalIntents(ctx, batch, ms, args, reply.Txn)
			if err := updateTxnWithExternalIntents(
				ctx, batch, ms, args, reply.Txn, externalIntents,
			); err != nil {
				return reply, nil, err
			}
			return reply, intentsToTrigger(externalIntents, &args), nil
		}
		// If the transaction was previously aborted by a concurrent
		// writer's push, any intents written are still open. It's only now
		// that we know them, so we return them all for asynchronous
		// resolution (we're currently not able to write on error, but
		// see #1989).
		return reply,
			intentsToTrigger(roachpb.AsIntents(args.IntentSpans, reply.Txn), &args),
			roachpb.NewTransactionAbortedError()

	case roachpb.PENDING:
		if h.Txn.Epoch < reply.Txn.Epoch {
			// TODO(tschottdorf): this leaves the Txn record (and more
			// importantly, intents) dangling; we can't currently write on
			// error. Would panic, but that makes TestEndTransactionWithErrors
			// awkward.
			return reply, nil, roachpb.NewTransactionStatusError(
				fmt.Sprintf("epoch regression: %d", h.Txn.Epoch),
			)
		} else if h.Txn.Epoch == reply.Txn.Epoch && reply.Txn.Timestamp.Less(h.Txn.OrigTimestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the original transaction timestamp.

			// TODO(tschottdorf): see above comment on epoch regression.
			return reply, nil, roachpb.NewTransactionStatusError(
				fmt.Sprintf("timestamp regression: %s", h.Txn.OrigTimestamp),
			)
		}

	default:
		return reply, nil, roachpb.NewTransactionStatusError(
			fmt.Sprintf("bad txn status: %s", reply.Txn),
		)
	}

	// Take max of requested epoch and existing epoch. The requester
	// may have incremented the epoch on retries.
	if reply.Txn.Epoch < h.Txn.Epoch {
		reply.Txn.Epoch = h.Txn.Epoch
	}
	// Take max of requested priority and existing priority. This isn't
	// terribly useful, but we do it for completeness.
	if reply.Txn.Priority < h.Txn.Priority {
		reply.Txn.Priority = h.Txn.Priority
	}

	// Take max of supplied txn's timestamp and persisted txn's
	// timestamp. It may have been pushed by another transaction.
	// Note that we do not use the batch request timestamp, which for
	// a transaction is always set to the txn's original timestamp.
	reply.Txn.Timestamp.Forward(h.Txn.Timestamp)

	if isEndTransactionExceedingDeadline(reply.Txn.Timestamp, args) {
		reply.Txn.Status = roachpb.ABORTED
		// FIXME(#3037):
		// If the deadline has lapsed, return all the intents for
		// resolution. Unfortunately, since we're (a) returning an error,
		// and (b) not able to write on error (see #1989), we can't write
		// ABORTED into the master transaction record, which remains
		// PENDING, and that's pretty bad.
		return reply,
			intentsToTrigger(roachpb.AsIntents(args.IntentSpans, reply.Txn), &args),
			roachpb.NewTransactionAbortedError()
	}

	// Set transaction status to COMMITTED or ABORTED as per the
	// args.Commit parameter.
	if args.Commit {
		if isEndTransactionTriggeringRetryError(h.Txn, reply.Txn) {
			return reply, nil, roachpb.NewTransactionRetryError()
		}
		reply.Txn.Status = roachpb.COMMITTED
	} else {
		reply.Txn.Status = roachpb.ABORTED
	}

	externalIntents := r.resolveLocalIntents(ctx, batch, ms, args, reply.Txn)
	if err := updateTxnWithExternalIntents(ctx, batch, ms, args, reply.Txn, externalIntents); err != nil {
		return reply, nil, err
	}

	// Run triggers if successfully committed.
	var trigger *PostCommitTrigger
	if reply.Txn.Status == roachpb.COMMITTED {
		var err error
		if trigger, err = r.runCommitTrigger(ctx, batch.(engine.Batch), ms, args, reply.Txn); err != nil {
			// TODO(tschottdorf): should an error here always amount to a
			// ReplicaCorruptionError?
			log.Error(ctx, errors.Wrapf(err, "%s: commit trigger", r))
			return reply, nil, err
		}
	}

	// Note: there's no need to clear the abort cache state if we've
	// successfully finalized a transaction, as there's no way in
	// which an abort cache entry could have been written (the txn would
	// already have been in state=ABORTED).
	//
	// Summary of transaction replay protection after EndTransaction:
	// When a transactional write gets replayed over its own resolved
	// intents, the write will succeed but only as an intent with a
	// newer timestamp (with a WriteTooOldError). However, the replayed
	// intent cannot be resolved by a subsequent replay of this
	// EndTransaction call because the txn timestamp will be too
	// old. Replays which include a BeginTransaction never succeed
	// because EndTransaction inserts in the write timestamp cache,
	// forcing the BeginTransaction to fail with a transaction retry
	// error. If the replay didn't include a BeginTransaction, any push
	// will immediately succeed as a missing txn record on push sets the
	// transaction to aborted. In both cases, the txn will be GC'd on
	// the slow path.
	trigger = updateTrigger(trigger, intentsToTrigger(externalIntents, &args))
	return reply, trigger, nil
}

// isEndTransactionExceedingDeadline returns true if the transaction
// exceeded its deadline.
func isEndTransactionExceedingDeadline(
	t hlc.Timestamp,
	args roachpb.EndTransactionRequest,
) bool {
	return args.Deadline != nil && args.Deadline.Less(t)
}

// isEndTransactionTriggeringRetryError returns true if the
// EndTransactionRequest cannot be committed and needs to return a
// TransactionRetryError.
func isEndTransactionTriggeringRetryError(headerTxn, currentTxn *roachpb.Transaction) bool {
	// If we saw any WriteTooOldErrors, we must restart to avoid lost
	// update anomalies.
	if headerTxn.WriteTooOld {
		return true
	}

	isTxnPushed := !currentTxn.Timestamp.Equal(headerTxn.OrigTimestamp)

	// If pushing requires a retry and the transaction was pushed, retry.
	if headerTxn.RetryOnPush && isTxnPushed {
		return true
	}

	// If the isolation level is SERIALIZABLE, return a transaction
	// retry error if the commit timestamp isn't equal to the txn
	// timestamp.
	if headerTxn.Isolation == enginepb.SERIALIZABLE && isTxnPushed {
		return true
	}

	return false
}

// resolveLocalIntents synchronously resolves any intents that are
// local to this range in the same batch. The remainder are collected
// and returned so that they can be handed off to asynchronous
// processing.
func (r *Replica) resolveLocalIntents(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
) []roachpb.Intent {
	desc := r.Desc()
	var preMergeDesc *roachpb.RangeDescriptor
	if mergeTrigger := args.InternalCommitTrigger.GetMergeTrigger(); mergeTrigger != nil {
		// If this is a merge, then use the post-merge descriptor to determine
		// which intents are local (note that for a split, we want to use the
		// pre-split one instead because it's larger).
		preMergeDesc = desc
		desc = &mergeTrigger.LeftDesc
	}

	iterAndBuf := engine.GetIterAndBuf(batch)
	defer iterAndBuf.Cleanup()

	var externalIntents []roachpb.Intent
	for _, span := range args.IntentSpans {
		if err := func() error {
			intent := roachpb.Intent{Span: span, Txn: txn.TxnMeta, Status: txn.Status}
			if len(span.EndKey) == 0 {
				// For single-key intents, do a KeyAddress-aware check of
				// whether it's contained in our Range.
				if !containsKey(*desc, span.Key) {
					externalIntents = append(externalIntents, intent)
					return nil
				}
				resolveMS := ms
				if preMergeDesc != nil && !containsKey(*preMergeDesc, span.Key) {
					// If this transaction included a merge and the intents
					// are from the subsumed range, ignore the intent resolution
					// stats, as they will already be accounted for during the
					// merge trigger.
					resolveMS = nil
				}
				return engine.MVCCResolveWriteIntentUsingIter(ctx, batch, iterAndBuf, resolveMS, intent)
			}
			// For intent ranges, cut into parts inside and outside our key
			// range. Resolve locally inside, delegate the rest. In particular,
			// an intent range for range-local data is correctly considered local.
			inSpan, outSpans := intersectSpan(span, *desc)
			for _, span := range outSpans {
				outIntent := intent
				outIntent.Span = span
				externalIntents = append(externalIntents, outIntent)
			}
			if inSpan != nil {
				intent.Span = *inSpan
				_, err := engine.MVCCResolveWriteIntentRangeUsingIter(ctx, batch, iterAndBuf, ms, intent, math.MaxInt64)
				return err
			}
			return nil
		}(); err != nil {
			// TODO(tschottdorf): any legitimate reason for this to happen?
			// Figure that out and if not, should still be ReplicaCorruption
			// and not a panic.
			panic(fmt.Sprintf("error resolving intent at %s on end transaction [%s]: %s", span, txn.Status, err))
		}
	}
	return externalIntents
}

// updateTxnWithExternalIntents persists the transaction record with
// updated status (& possibly timestamp). If we've already resolved
// all intents locally, we actually delete the record right away - no
// use in keeping it around.
func updateTxnWithExternalIntents(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	args roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
	externalIntents []roachpb.Intent,
) error {
	key := keys.TransactionKey(txn.Key, txn.ID)
	if txnAutoGC && len(externalIntents) == 0 {
		if log.V(2) {
			log.Infof(ctx, "auto-gc'ed %s (%d intents)", txn.ID.Short(), len(args.IntentSpans))
		}
		return engine.MVCCDelete(ctx, batch, ms, key, hlc.ZeroTimestamp, nil /* txn */)
	}
	txn.Intents = make([]roachpb.Span, len(externalIntents))
	for i := range externalIntents {
		txn.Intents[i] = externalIntents[i].Span
	}
	return engine.MVCCPutProto(ctx, batch, ms, key, hlc.ZeroTimestamp, nil /* txn */, txn)
}

// intersectSpan takes an intent and a descriptor. It then splits the
// intent's range into up to three pieces: A first piece which is contained in
// the Range, and a slice of up to two further intents which are outside of the
// key range. An intent for which [Key, EndKey) is empty does not result in any
// intents; thus intersectIntent only applies to intent ranges.
// A range-local intent range is never split: It's returned as either
// belonging to or outside of the descriptor's key range, and passing an intent
// which begins range-local but ends non-local results in a panic.
// TODO(tschottdorf) move to proto, make more gen-purpose - kv.truncate does
// some similar things.
func intersectSpan(
	span roachpb.Span,
	desc roachpb.RangeDescriptor,
) (middle *roachpb.Span, outside []roachpb.Span) {
	start, end := desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey()
	if len(span.EndKey) == 0 {
		outside = append(outside, span)
		return
	}
	if bytes.Compare(span.Key, keys.LocalRangeMax) < 0 {
		if bytes.Compare(span.EndKey, keys.LocalRangeMax) >= 0 {
			panic("a local intent range may not have a non-local portion")
		}
		if containsKeyRange(desc, span.Key, span.EndKey) {
			return &span, nil
		}
		return nil, append(outside, span)
	}
	// From now on, we're dealing with plain old key ranges - no more local
	// addressing.
	if bytes.Compare(span.Key, start) < 0 {
		// Intent spans a part to the left of [start, end).
		iCopy := span
		if bytes.Compare(start, span.EndKey) < 0 {
			iCopy.EndKey = start
		}
		span.Key = iCopy.EndKey
		outside = append(outside, iCopy)
	}
	if bytes.Compare(span.Key, span.EndKey) < 0 && bytes.Compare(end, span.EndKey) < 0 {
		// Intent spans a part to the right of [start, end).
		iCopy := span
		if bytes.Compare(iCopy.Key, end) < 0 {
			iCopy.Key = end
		}
		span.EndKey = iCopy.Key
		outside = append(outside, iCopy)
	}
	if bytes.Compare(span.Key, span.EndKey) < 0 && bytes.Compare(span.Key, start) >= 0 && bytes.Compare(end, span.EndKey) >= 0 {
		middle = &span
	}
	return
}

func (r *Replica) runCommitTrigger(
	ctx context.Context,
	batch engine.Batch,
	ms *enginepb.MVCCStats,
	args roachpb.EndTransactionRequest,
	txn *roachpb.Transaction,
) (*PostCommitTrigger, error) {
	var trigger *PostCommitTrigger
	ct := args.InternalCommitTrigger

	if err := func() error {
		if ct.GetSplitTrigger() != nil {
			var err error
			var postSplit *PostCommitTrigger
			if *ms, postSplit, err = r.splitTrigger(
				ctx, batch, *ms, ct.SplitTrigger, txn.Timestamp,
			); err != nil {
				return err
			}
			trigger = updateTrigger(trigger, postSplit)
		}
		if ct.GetMergeTrigger() != nil {
			postMerge, err := r.mergeTrigger(ctx, batch, ms, ct.MergeTrigger, txn.Timestamp)
			if err != nil {
				return err
			}
			trigger = updateTrigger(trigger, postMerge)
		}
		if crt := ct.GetChangeReplicasTrigger(); crt != nil {
			trigger = updateTrigger(trigger, r.changeReplicasTrigger(ctx, batch, crt))
		}
		if ct.GetModifiedSpanTrigger() != nil {
			if ct.ModifiedSpanTrigger.SystemConfigSpan {
				// Check if we need to gossip the system config.
				// NOTE: System config gossiping can only execute correctly if
				// the transaction record is located on the range that contains
				// the system span. If a transaction is created which modifies
				// both system *and* non-system data, it should be ensured that
				// the transaction record itself is on the system span. This can
				// be done by making sure a system key is the first key touched
				// in the transaction.
				if !r.ContainsKey(keys.SystemConfigSpan.Key) {
					log.Errorf(ctx, "System configuration span was modified, but the "+
						"modification trigger is executing on a non-system range. "+
						"Configuration changes will not be gossiped.")
				} else {
					trigger = updateTrigger(trigger, &PostCommitTrigger{
						maybeGossipSystemConfig: true,
					})
				}
			}
		}
		return nil
	}(); err != nil {
		return nil, err
	}
	return trigger, nil
}

// RangeLookup is used to look up RangeDescriptors - a RangeDescriptor
// is a metadata structure which describes the key range and replica locations
// of a distinct range in the cluster.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix prepended. The Range Metadata
// Key for an ordinary key can be generated with the `keys.RangeMetaKey(key)`
// function. The RangeDescriptor for the range which contains a given key can be
// retrieved by generating its Range Metadata Key and dispatching it to
// RangeLookup.
//
// Note that the Range Metadata Key sent to RangeLookup is NOT the key
// at which the desired RangeDescriptor is stored. Instead, this method returns
// the RangeDescriptor stored at the _lowest_ existing key which is _greater_
// than the given key. The returned RangeDescriptor will thus contain the
// ordinary key which was originally used to generate the Range Metadata Key
// sent to RangeLookup.
//
// The "Range Metadata Key" for a range is built by appending the end key of
// the range to the respective meta prefix.
//
// Lookups for range metadata keys usually want to read inconsistently, but
// some callers need a consistent result; both are supported.
//
// This method has an important optimization in the inconsistent case: instead
// of just returning the request RangeDescriptor, it also returns a slice of
// additional range descriptors immediately consecutive to the desired
// RangeDescriptor. This is intended to serve as a sort of caching pre-fetch,
// so that the requesting nodes can aggressively cache RangeDescriptors which
// are likely to be desired by their current workload. The Reverse flag
// specifies whether descriptors are prefetched in descending or ascending
// order.
func (r *Replica) RangeLookup(
	ctx context.Context,
	batch engine.ReadWriter,
	h roachpb.Header,
	args roachpb.RangeLookupRequest,
) (roachpb.RangeLookupResponse, *PostCommitTrigger, error) {
	var reply roachpb.RangeLookupResponse
	ts := h.Timestamp // all we're going to use from the header.
	key, err := keys.Addr(args.Key)
	if err != nil {
		return reply, nil, err
	}
	if !key.Equal(args.Key) {
		return reply, nil, errors.Errorf("illegal lookup of range-local key")
	}

	rangeCount := int64(args.MaxRanges)
	if rangeCount < 1 {
		return reply, nil, errors.Errorf("Range lookup specified invalid maximum range count %d: must be > 0", rangeCount)
	}
	consistent := h.ReadConsistency != roachpb.INCONSISTENT
	if consistent && args.ConsiderIntents {
		return reply, nil, errors.Errorf("can not read consistently and special-case intents")
	}
	if args.ConsiderIntents {
		// Disable prefetching; the caller only cares about a single intent,
		// and the code below simplifies considerably.
		rangeCount = 1
	}

	var checkAndUnmarshal func(roachpb.Value) (*roachpb.RangeDescriptor, error)

	var kvs []roachpb.KeyValue // kv descriptor pairs in scan order
	var intents []roachpb.Intent
	if !args.Reverse {
		// If scanning forward, there's no special "checking": Just decode the
		// descriptor and return it.
		checkAndUnmarshal = func(v roachpb.Value) (*roachpb.RangeDescriptor, error) {
			var rd roachpb.RangeDescriptor
			if err := v.GetProto(&rd); err != nil {
				return nil, err
			}
			return &rd, nil
		}

		// We want to search for the metadata key greater than
		// args.Key. Scan for both the requested key and the keys immediately
		// afterwards, up to MaxRanges.
		startKey, endKey, err := keys.MetaScanBounds(key)
		if err != nil {
			return reply, nil, err
		}

		// Scan for descriptors.
		kvs, intents, err = engine.MVCCScan(ctx, batch, startKey, endKey, rangeCount,
			ts, consistent, h.Txn)
		if err != nil {
			// An error here is likely a WriteIntentError when reading consistently.
			return reply, nil, err
		}
	} else {
		// Use MVCCScan to get the first range. There are three cases:
		// 1. args.Key is not an endpoint of the range and
		// 2a. The args.Key is the start/end key of the range.
		// 2b. Even worse, the body of args.Key is roachpb.KeyMax.
		// In the first case, we need use the MVCCScan() to get the first
		// range descriptor, because ReverseScan can't do the work. If we
		// have ranges [a,c) and [c,f) and the reverse scan request's key
		// range is [b,d), then d.Next() is less than "f", and so the meta
		// row {f->[c,f)} would be ignored by MVCCReverseScan. In case 2a,
		// the range descriptor received by MVCCScan will be filtered before
		// results are returned: With ranges [c,f) and [f,z), reverse scan
		// on [d,f) receives the descriptor {z->[f,z)}, which is discarded
		// below since it's not being asked for. Finally, in case 2b, we
		// don't even attempt the forward scan because it's neither defined
		// nor required.
		// Note that Meta1KeyMax is admissible: it means we're looking for
		// the range descriptor that houses Meta2KeyMax, and a forward scan
		// handles it correctly.
		// In this case, checkAndUnmarshal is more complicated: It needs
		// to weed out descriptors from the forward scan above, which could
		// return a result or an intent we're not supposed to return.
		checkAndUnmarshal = func(v roachpb.Value) (*roachpb.RangeDescriptor, error) {
			var r roachpb.RangeDescriptor
			if err := v.GetProto(&r); err != nil {
				return nil, err
			}
			startKeyAddr, err := keys.Addr(keys.RangeMetaKey(r.StartKey))
			if err != nil {
				return nil, err
			}
			if !startKeyAddr.Less(key) {
				// This is the case in which we've picked up an extra descriptor
				// we don't want.
				return nil, nil
			}
			// We actually want this descriptor.
			return &r, nil
		}

		if key.Less(roachpb.RKey(keys.Meta2KeyMax)) {
			startKey, endKey, err := keys.MetaScanBounds(key)
			if err != nil {
				return reply, nil, err
			}

			kvs, intents, err = engine.MVCCScan(ctx, batch, startKey, endKey, 1,
				ts, consistent, h.Txn)
			if err != nil {
				return reply, nil, err
			}

		}
		// We want to search for the metadata key just less or equal to
		// args.Key. Scan in reverse order for both the requested key and the
		// keys immediately backwards, up to MaxRanges.
		startKey, endKey, err := keys.MetaReverseScanBounds(key)
		if err != nil {
			return reply, nil, err
		}
		// Reverse scan for descriptors.
		revKvs, revIntents, err := engine.MVCCReverseScan(ctx, batch, startKey, endKey, rangeCount,
			ts, consistent, h.Txn)
		if err != nil {
			// An error here is likely a WriteIntentError when reading consistently.
			return reply, nil, err
		}

		// Merge the results, the total ranges may be bigger than rangeCount.
		kvs = append(kvs, revKvs...)
		intents = append(intents, revIntents...)
	}

	// Decode all scanned range descriptors which haven't been unmarshaled yet.
	for i, kv := range kvs {
		// TODO(tschottdorf) Candidate for a ReplicaCorruptionError.
		rd, err := checkAndUnmarshal(kv.Value)
		if err != nil {
			return reply, nil, err
		}
		if rd != nil {
			// Add the first valid descriptor to the desired range descriptor
			// list in the response, add all others to the prefetched list.
			if i == 0 || len(reply.Ranges) == 0 {
				reply.Ranges = append(reply.Ranges, *rd)
			} else {
				reply.PrefetchedRanges = append(reply.PrefetchedRanges, *rd)
			}
		}
	}

	if args.ConsiderIntents && len(intents) > 0 {
		// NOTE (subtle): dangling intents on meta records are peculiar: It's not
		// clear whether the intent or the previous value point to the correct
		// location of the Range. It gets even more complicated when there are
		// split-related intents or a txn record co-located with a replica
		// involved in the split. Since we cannot know the correct answer, we
		// reply with both the pre- and post- transaction values when the
		// ConsiderIntents flag is set.
		//
		// This does not count against a maximum range count because they are
		// possible versions of the same descriptor. In other words, both the
		// current live descriptor and a potentially valid descriptor from
		// observed intents could be returned when MaxRanges is set to 1 and
		// the ConsiderIntents flag is set.
		for _, intent := range intents {
			val, _, err := engine.MVCCGetAsTxn(ctx, batch, intent.Key, intent.Txn.Timestamp, intent.Txn)
			if err != nil {
				return reply, nil, err
			}

			if val == nil {
				// Intent is a deletion.
				continue
			}
			rd, err := checkAndUnmarshal(*val)
			if err != nil {
				return reply, nil, err
			}
			// If this is a descriptor we're allowed to return, add that
			// to the lookup response slice and stop searching in intents.
			if rd != nil {
				reply.Ranges = append(reply.Ranges, *rd)
				break
			}
		}
	}

	if len(reply.Ranges) == 0 {
		// No matching results were returned from the scan. This should
		// never happen with the above logic.
		panic(fmt.Sprintf("RangeLookup dispatched to correct range, but no matching RangeDescriptor was found: %s", args.Key))
	} else if preCount := int64(len(reply.PrefetchedRanges)); 1+preCount > rangeCount {
		// We've possibly picked up an extra descriptor if we're in reverse
		// mode due to the initial forward scan.
		//
		// Here, we only count the desired range descriptors as a single
		// descriptor against the rangeCount limit, even if multiple versions
		// of the same descriptor were found in intents. In practice, we should
		// only get multiple desired range descriptors when prefetching is disabled
		// anyway (see above), so this should never actually matter.
		reply.PrefetchedRanges = reply.PrefetchedRanges[:rangeCount-1]
	}

	return reply, intentsToTrigger(intents, &args), nil
}

// HeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func (r *Replica) HeartbeatTxn(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.HeartbeatTxnRequest,
) (roachpb.HeartbeatTxnResponse, error) {
	var reply roachpb.HeartbeatTxnResponse

	if err := verifyTransaction(h, &args); err != nil {
		return reply, err
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	var txn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(ctx, batch, key, hlc.ZeroTimestamp, true, nil, &txn); err != nil {
		return reply, err
	} else if !ok {
		// If no existing transaction record was found, skip heartbeat.
		// This could mean the heartbeat is a delayed relic or it could
		// mean that the BeginTransaction call was delayed. In either
		// case, there's no reason to persist a new transaction record.
		return reply, errors.Errorf("heartbeat for transaction %s failed; record not present", h.Txn)
	}

	if txn.Status == roachpb.PENDING {
		if txn.LastHeartbeat == nil {
			txn.LastHeartbeat = &hlc.Timestamp{}
		}
		txn.LastHeartbeat.Forward(args.Now)
		if err := engine.MVCCPutProto(ctx, batch, ms, key, hlc.ZeroTimestamp, nil, &txn); err != nil {
			return reply, err
		}
	}

	reply.Txn = &txn
	return reply, nil
}

// GC iterates through the list of keys to garbage collect
// specified in the arguments. MVCCGarbageCollect is invoked on each
// listed key along with the expiration timestamp. The GC metadata
// specified in the args is persisted after GC.
func (r *Replica) GC(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.GCRequest,
) (roachpb.GCResponse, *PostCommitTrigger, error) {
	// All keys must be inside the current replica range. Keys outside
	// of this range in the GC request are dropped silently, which is
	// safe because they can simply be re-collected later on the correct
	// replica. Discrepancies here can arise from race conditions during
	// range splitting.
	keys := make([]roachpb.GCRequest_GCKey, 0, len(args.Keys))
	for _, k := range args.Keys {
		if r.ContainsKey(k.Key) {
			keys = append(keys, k)
		}
	}

	var reply roachpb.GCResponse
	// Garbage collect the specified keys by expiration timestamps.
	err := engine.MVCCGarbageCollect(ctx, batch, ms, keys, h.Timestamp)
	if err != nil {
		return reply, nil, err
	}

	r.mu.Lock()
	newThreshold := r.mu.state.GCThreshold
	newThreshold.Forward(args.Threshold)
	r.mu.Unlock()

	trigger := &PostCommitTrigger{
		gcThreshold: &newThreshold,
	}
	return reply, trigger, setGCThreshold(ctx, batch, ms, r.Desc().RangeID, &newThreshold)
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
// so that the updated timestamp populates the abort cache, allowing the GC
// queue to purge entries for which the transaction coordinator must have found
// out via its heartbeats that the transaction has failed.
func (r *Replica) PushTxn(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.PushTxnRequest,
) (roachpb.PushTxnResponse, error) {
	var reply roachpb.PushTxnResponse

	if h.Txn != nil {
		return reply, errTransactionUnsupported
	}
	if args.Now.Equal(hlc.ZeroTimestamp) {
		return reply, errors.Errorf("the field Now must be provided")
	}

	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		return reply, errors.Errorf("request key %s should match pushee's txn key %s", args.Key, args.PusheeTxn.Key)
	}
	key := keys.TransactionKey(args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction; if missing, we're allowed to abort.
	existTxn := &roachpb.Transaction{}
	ok, err := engine.MVCCGetProto(ctx, batch, key, hlc.ZeroTimestamp,
		true /* consistent */, nil /* txn */, existTxn)
	if err != nil {
		return reply, err
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
		// If getting an update for a transaction record which doesn't yet
		// exist, return empty Pushee.
		if args.PushType == roachpb.PUSH_QUERY {
			return reply, nil
		}
		// The transaction doesn't exist on disk; we're allowed to abort it.
		// TODO(tschottdorf): especially for SNAPSHOT transactions, there's
		// something to win here by not aborting, but instead pushing the
		// timestamp. For SERIALIZABLE it's less important, but still better
		// to have them restart than abort. See #3344.
		// TODO(tschottdorf): double-check for problems emanating from
		// using a trivial Transaction proto here. Maybe some fields ought
		// to receive dummy values.
		reply.PusheeTxn.TxnMeta = args.PusheeTxn
		reply.PusheeTxn.Timestamp = args.Now // see method comment
		reply.PusheeTxn.Status = roachpb.ABORTED
		return reply, engine.MVCCPutProto(ctx, batch, ms, key, hlc.ZeroTimestamp, nil, &reply.PusheeTxn)
	}
	// Start with the persisted transaction record as final transaction.
	reply.PusheeTxn = existTxn.Clone()
	// The pusher might be aware of a newer version of the pushee.
	reply.PusheeTxn.Timestamp.Forward(args.PusheeTxn.Timestamp)
	if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
		reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status != roachpb.PENDING {
		// Trivial noop.
		return reply, nil
	}

	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if args.PushType == roachpb.PUSH_TIMESTAMP && args.PushTo.Less(reply.PusheeTxn.Timestamp) {
		// Trivial noop.
		return reply, nil
	}

	// If getting an update for a transaction record, return now.
	if args.PushType == roachpb.PUSH_QUERY {
		return reply, nil
	}

	priority := args.PusherTxn.Priority

	var pusherWins bool
	var reason string

	switch {
	case reply.PusheeTxn.LastActive().Less(args.Now.Add(-2*base.DefaultHeartbeatInterval.Nanoseconds(), 0)):
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
	case reply.PusheeTxn.Priority != priority:
		reason = "priority"
		pusherWins = reply.PusheeTxn.Priority < priority
	case args.PusherTxn.ID == nil:
		reason = "equal priorities; pusher not transactional"
		pusherWins = false
	default:
		reason = "equal priorities; greater ID wins"
		pusherWins = bytes.Compare(reply.PusheeTxn.ID.GetBytes(),
			args.PusherTxn.ID.GetBytes()) < 0
	}

	if log.V(1) && reason != "" {
		s := "pushed"
		if !pusherWins {
			s = "failed to push"
		}
		log.Infof(ctx, "%s: %s "+s+" %s: %s", r, args.PusherTxn.ID.Short(), reply.PusheeTxn.ID.Short(), reason)
	}

	if !pusherWins {
		err := roachpb.NewTransactionPushError(reply.PusheeTxn)
		if log.V(1) {
			log.Infof(ctx, "%s: %v", r, err)
		}
		return reply, err
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(priority - 1)

	// If aborting transaction, set new status and return success.
	if args.PushType == roachpb.PUSH_ABORT {
		reply.PusheeTxn.Status = roachpb.ABORTED
		// Forward the timestamp to accommodate abort cache GC. See method
		// comment for details.
		reply.PusheeTxn.Timestamp.Forward(reply.PusheeTxn.LastActive())
	} else if args.PushType == roachpb.PUSH_TIMESTAMP {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PusheeTxn.Timestamp = args.PushTo
		reply.PusheeTxn.Timestamp.Logical++
	}

	// Persist the pushed transaction using zero timestamp for inline value.
	if err := engine.MVCCPutProto(ctx, batch, ms, key, hlc.ZeroTimestamp, nil, &reply.PusheeTxn); err != nil {
		return reply, err
	}
	return reply, nil
}

// setAbortCache clears any abort cache entry if poison is false.
// Otherwise, if poison is true, creates an entry for this transaction
// in the abort cache to prevent future reads or writes from
// spuriously succeeding on this range.
func (r *Replica) setAbortCache(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txn enginepb.TxnMeta,
	poison bool,
) error {
	if !poison {
		return r.abortCache.Del(ctx, batch, ms, txn.ID)
	}
	entry := roachpb.AbortCacheEntry{
		Key:       txn.Key,
		Timestamp: txn.Timestamp,
		Priority:  txn.Priority,
	}
	return r.abortCache.Put(ctx, batch, ms, txn.ID, &entry)
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func (r *Replica) ResolveIntent(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.ResolveIntentRequest,
) (roachpb.ResolveIntentResponse, error) {
	var reply roachpb.ResolveIntentResponse
	if h.Txn != nil {
		return reply, errTransactionUnsupported
	}

	intent := roachpb.Intent{
		Span:   args.Span,
		Txn:    args.IntentTxn,
		Status: args.Status,
	}
	if err := engine.MVCCResolveWriteIntent(ctx, batch, ms, intent); err != nil {
		return reply, err
	}
	if intent.Status == roachpb.ABORTED {
		return reply, r.setAbortCache(ctx, batch, ms, args.IntentTxn, args.Poison)
	}
	return reply, nil
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func (r *Replica) ResolveIntentRange(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.ResolveIntentRangeRequest,
) (roachpb.ResolveIntentRangeResponse, error) {
	var reply roachpb.ResolveIntentRangeResponse
	if h.Txn != nil {
		return reply, errTransactionUnsupported
	}

	intent := roachpb.Intent{
		Span:   args.Span,
		Txn:    args.IntentTxn,
		Status: args.Status,
	}

	if _, err := engine.MVCCResolveWriteIntentRange(ctx, batch, ms, intent, math.MaxInt64); err != nil {
		return reply, err
	}
	if intent.Status == roachpb.ABORTED {
		return reply, r.setAbortCache(ctx, batch, ms, args.IntentTxn, args.Poison)
	}
	return reply, nil
}

// Merge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used
// by CockroachDB for the efficient accumulation of certain
// values. Due to the difficulty of making these operations
// transactional, merges are not currently exposed directly to
// clients. Merged values are explicitly not MVCC data.
func (r *Replica) Merge(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.MergeRequest,
) (roachpb.MergeResponse, error) {
	var reply roachpb.MergeResponse

	return reply, engine.MVCCMerge(ctx, batch, ms, args.Key, h.Timestamp, args.Value)
}

// TruncateLog discards a prefix of the raft log. Truncating part of a log that
// has already been truncated has no effect. If this range is not the one
// specified within the request body, the request will also be ignored.
func (r *Replica) TruncateLog(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.TruncateLogRequest,
) (roachpb.TruncateLogResponse, *PostCommitTrigger, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var reply roachpb.TruncateLogResponse

	// After a merge, it's possible that this request was sent to the wrong
	// range based on the start key. This will cancel the request if this is not
	// the range specified in the request body.
	if r.RangeID != args.RangeID {
		log.Infof(ctx, "%s: attempting to truncate raft logs for another range %d. Normally this is due to a merge and can be ignored.",
			r, args.RangeID)
		return reply, nil, nil
	}

	// Have we already truncated this log? If so, just return without an error.
	firstIndex, err := r.FirstIndex()
	if err != nil {
		return reply, nil, err
	}

	if firstIndex >= args.Index {
		if log.V(3) {
			log.Infof(ctx, "%s: attempting to truncate previously truncated raft log. FirstIndex:%d, TruncateFrom:%d",
				r, firstIndex, args.Index)
		}
		return reply, nil, nil
	}

	// args.Index is the first index to keep.
	term, err := r.Term(args.Index - 1)
	if err != nil {
		return reply, nil, err
	}
	start := keys.RaftLogKey(r.RangeID, 0)
	end := keys.RaftLogKey(r.RangeID, args.Index)
	var diff enginepb.MVCCStats
	// Passing zero timestamp to MVCCDeleteRange is equivalent to a ranged clear
	// but it also computes stats.
	if _, err := engine.MVCCDeleteRange(ctx, batch, &diff, start, end, math.MaxInt64, /* max */
		hlc.ZeroTimestamp, nil /* txn */, false /* returnKeys */); err != nil {
		return reply, nil, err
	}
	raftLogSize := r.mu.raftLogSize + diff.SysBytes
	// Check raftLogSize since it isn't persisted between server restarts.
	if raftLogSize < 0 {
		raftLogSize = 0
	}

	tState := &roachpb.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}

	trigger := &PostCommitTrigger{
		truncatedState: tState,
		raftLogSize:    &raftLogSize,
	}
	return reply, trigger, engine.MVCCPutProto(ctx, batch, ms, keys.RaftTruncatedStateKey(r.RangeID), hlc.ZeroTimestamp, nil, tState)
}

func newFailedLeaseTrigger() *PostCommitTrigger {
	return &PostCommitTrigger{leaseMetricsResult: new(bool)}
}

// RequestLease sets the range lease for this range. The command fails
// only if the desired start timestamp collides with a previous lease.
// Otherwise, the start timestamp is wound back to right after the expiration
// of the previous lease (or zero). If this range replica is already the lease
// holder, the expiration will be extended or shortened as indicated. For a new
// lease, all duties required of the range lease holder are commenced, including
// clearing the command queue and timestamp cache.
func (r *Replica) RequestLease(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.RequestLeaseRequest,
) (roachpb.RequestLeaseResponse, *PostCommitTrigger, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.
	r.mu.Lock()
	defer r.mu.Unlock()

	prevLease := r.mu.state.Lease
	rErr := &roachpb.LeaseRejectedError{
		Existing:  *prevLease,
		Requested: args.Lease,
	}

	// MIGRATION(tschottdorf): needed to apply Raft commands which got proposed
	// before the StartStasis field was introduced.
	if args.Lease.StartStasis.Equal(hlc.ZeroTimestamp) {
		args.Lease.StartStasis = args.Lease.Expiration
	}

	isExtension := prevLease.Replica.StoreID == args.Lease.Replica.StoreID
	effectiveStart := args.Lease.Start

	// Wind the start timestamp back as far towards the previous lease as we
	// can. That'll make sure that when multiple leases are requested out of
	// order at the same replica (after all, they use the request timestamp,
	// which isn't straight out of our local clock), they all succeed unless
	// they have a "real" issue with a previous lease. Example: Assuming no
	// previous lease, one request for [5, 15) followed by one for [0, 15)
	// would fail without this optimization. With it, the first request
	// effectively gets the lease for [0, 15), which the second one can commit
	// again (even extending your own lease is possible; see below).
	//
	// If this is our lease (or no prior lease exists), we effectively absorb
	// the old lease. This allows multiple requests from the same replica to
	// merge without ticking away from the minimal common start timestamp. It
	// also has the positive side-effect of fixing #3561, which was caused by
	// the absence of replay protection.
	if prevLease.Replica.StoreID == 0 || isExtension {
		effectiveStart.Backward(prevLease.Start)
	} else {
		effectiveStart.Backward(prevLease.Expiration.Next())
	}

	if isExtension {
		if effectiveStart.Less(prevLease.Start) {
			rErr.Message = "extension moved start timestamp backwards"
			return roachpb.RequestLeaseResponse{}, newFailedLeaseTrigger(), rErr
		}
		args.Lease.Expiration.Forward(prevLease.Expiration)
	} else if effectiveStart.Less(prevLease.Expiration) {
		rErr.Message = "requested lease overlaps previous lease"
		return roachpb.RequestLeaseResponse{}, newFailedLeaseTrigger(), rErr
	}
	args.Lease.Start = effectiveStart
	return r.applyNewLeaseLocked(ctx, batch, ms, args.Lease)
}

// TransferLease sets the lease holder for the range.
// Unlike with RequestLease(), the new lease is allowed to overlap the old one,
// the contract being that the transfer must have been initiated by the (soon
// ex-) lease holder which must have dropped all of its lease holder powers
// before proposing.
func (r *Replica) TransferLease(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.TransferLeaseRequest,
) (roachpb.RequestLeaseResponse, *PostCommitTrigger, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.
	r.mu.Lock()
	defer r.mu.Unlock()
	if log.V(2) {
		prevLease := r.mu.state.Lease
		log.Infof(ctx, "[%s] lease transfer: prev lease: %+v, new lease: %+v "+
			"old expiration: %s, new start: %s",
			r, prevLease, args.Lease, prevLease.Expiration, args.Lease.Start)
	}
	return r.applyNewLeaseLocked(ctx, batch, ms, args.Lease)
}

// applyNewLeaseLocked checks that the lease contains a valid interval and that
// the new lease holder is still a member of the replica set, and then proceeds
// to write the new lease to the batch, emitting an appropriate trigger.

// The new lease might be a lease for a range that didn't previously have an
// active lease, might be an extension or a lease transfer.
//
// r.mu needs to be locked.
//
// TODO(tschottdorf): refactoring what's returned from the trigger here makes
// sense to minimize the amount of code intolerant of rolling updates.
func (r *Replica) applyNewLeaseLocked(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	lease roachpb.Lease,
) (roachpb.RequestLeaseResponse, *PostCommitTrigger, error) {
	// When returning an error from this method, must always return
	// a newFailedLeaseTrigger() to satisfy stats.

	prevLease := r.mu.state.Lease
	// Ensure Start < StartStasis <= Expiration.
	if !lease.Start.Less(lease.StartStasis) ||
		lease.Expiration.Less(lease.StartStasis) {
		// This amounts to a bug.
		return roachpb.RequestLeaseResponse{}, newFailedLeaseTrigger(),
			&roachpb.LeaseRejectedError{
				Existing:  *prevLease,
				Requested: lease,
				Message: fmt.Sprintf("illegal lease interval: [%s, %s, %s]",
					lease.Start, lease.StartStasis, lease.Expiration),
			}
	}

	// Verify that requesting replica is part of the current replica set.
	if _, ok := r.mu.state.Desc.GetReplicaDescriptor(lease.Replica.StoreID); !ok {
		return roachpb.RequestLeaseResponse{}, newFailedLeaseTrigger(),
			&roachpb.LeaseRejectedError{
				Existing:  *prevLease,
				Requested: lease,
				Message:   "replica not found",
			}
	}

	var reply roachpb.RequestLeaseResponse
	// Store the lease to disk & in-memory.
	if err := setLease(ctx, batch, ms, r.RangeID, &lease); err != nil {
		return reply, newFailedLeaseTrigger(), err
	}

	t := true
	trigger := &PostCommitTrigger{
		// If we didn't block concurrent reads here, there'd be a chance that
		// reads could sneak in on a new lease holder between setting the lease
		// and updating the low water mark. This in itself isn't a consistency
		// violation, but it's a bit suspicious and did make
		// TestRangeTransferLease flaky. We err on the side of caution for now.
		//
		// TODO(tschottdorf): Could only do this on transfers, or not at all.
		// Need to think through potential consequences.
		noConcurrentReads:  true,
		lease:              &lease,
		leaseMetricsResult: &t,
		// TODO(tschottdorf): having traced the origin of this call back to
		// rev 6281926, it seems that we should only be doing this when the
		// lease holder has changed. However, it's likely not a big deal to
		// do it always.
		maybeGossipSystemConfig: true,
	}

	return reply, trigger, nil
}

// CheckConsistency runs a consistency check on the range. It first applies
// a ComputeChecksum command on the range. It then applies a VerifyChecksum
// command passing along a locally computed checksum for the range.
func (r *Replica) CheckConsistency(
	args roachpb.CheckConsistencyRequest,
	desc *roachpb.RangeDescriptor,
) (roachpb.CheckConsistencyResponse, *roachpb.Error) {
	ctx := context.TODO()
	key := desc.StartKey.AsRawKey()
	endKey := desc.EndKey.AsRawKey()
	id := uuid.MakeV4()
	// Send a ComputeChecksum to all the replicas of the range.
	start := timeutil.Now()
	{
		var ba roachpb.BatchRequest
		ba.RangeID = r.Desc().RangeID
		checkArgs := &roachpb.ComputeChecksumRequest{
			Span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			Version:    replicaChecksumVersion,
			ChecksumID: id,
			Snapshot:   args.WithDiff,
		}
		ba.Add(checkArgs)
		ba.Timestamp = r.store.Clock().Now()
		_, pErr := r.Send(ctx, ba)
		if pErr != nil {
			return roachpb.CheckConsistencyResponse{}, pErr
		}
	}

	// Get local checksum. This might involving waiting for it.
	c, ok := r.getChecksum(ctx, id)
	if !ok || c.checksum == nil {
		return roachpb.CheckConsistencyResponse{}, roachpb.NewErrorf("unable to compute checksum for range [%v, %v]", key, endKey)
	}

	// Wait for a bit to improve the probability that all
	// the replicas have computed their checksum. We do this
	// because VerifyChecksum blocks on every replica until the
	// computed checksum is available.
	computeChecksumDuration := timeutil.Since(start)
	time.Sleep(computeChecksumDuration)

	// Send a VerifyChecksum to all the replicas of the range.
	{
		var ba roachpb.BatchRequest
		ba.RangeID = r.Desc().RangeID
		checkArgs := &roachpb.VerifyChecksumRequest{
			Span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
			Version:    replicaChecksumVersion,
			ChecksumID: id,
			Checksum:   c.checksum,
			Snapshot:   c.snapshot,
		}
		ba.Add(checkArgs)
		ba.Timestamp = r.store.Clock().Now()
		_, pErr := r.Send(ctx, ba)
		if pErr != nil {
			return roachpb.CheckConsistencyResponse{}, pErr
		}
	}

	return roachpb.CheckConsistencyResponse{}, nil
}

const (
	replicaChecksumVersion    = 1
	replicaChecksumGCInterval = time.Hour
)

// getChecksum waits for the result of ComputeChecksum and returns it.
// It returns false if there is no checksum being computed for the id,
// or it has already been GCed.
func (r *Replica) getChecksum(
	ctx context.Context,
	id uuid.UUID,
) (replicaChecksum, bool) {
	r.mu.Lock()
	c, ok := r.mu.checksums[id]
	r.mu.Unlock()
	if !ok {
		return replicaChecksum{}, false
	}
	// Wait
	now := timeutil.Now()
	<-c.notify
	if log.V(1) {
		log.Infof(ctx, "%s: waited for compute checksum for %s", r, timeutil.Since(now))
	}
	r.mu.Lock()
	c, ok = r.mu.checksums[id]
	r.mu.Unlock()
	return c, ok
}

// computeChecksumDone adds the computed checksum, sets a deadline for GCing the
// checksum, and sends out a notification.
func (r *Replica) computeChecksumDone(
	ctx context.Context,
	id uuid.UUID,
	sha []byte,
	snapshot *roachpb.RaftSnapshotData,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.mu.checksums[id]; ok {
		c.checksum = sha
		c.gcTimestamp = timeutil.Now().Add(replicaChecksumGCInterval)
		c.snapshot = snapshot
		r.mu.checksums[id] = c
		// Notify
		close(c.notify)
	} else {
		// ComputeChecksum adds an entry into the map, and the entry can
		// only be GCed once the gcTimestamp is set above. Something
		// really bad happened.
		log.Errorf(ctx, "%s: no checksum for id = %v", r, id)
	}
}

// ComputeChecksum starts the process of computing a checksum on the
// replica at a particular snapshot. The checksum is later verified
// through the VerifyChecksum request.
func (r *Replica) ComputeChecksum(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.ComputeChecksumRequest,
) (roachpb.ComputeChecksumResponse, *PostCommitTrigger, error) {
	if args.Version != replicaChecksumVersion {
		log.Errorf(ctx, "%s: Incompatible versions: e=%d, v=%d", r, replicaChecksumVersion, args.Version)
		return roachpb.ComputeChecksumResponse{}, nil, nil
	}
	return roachpb.ComputeChecksumResponse{}, &PostCommitTrigger{computeChecksum: &args}, nil
}

// sha512 computes the SHA512 hash of all the replica data at the snapshot.
// It will dump all the k:v data into snapshot if it is provided.
func (r *Replica) sha512(
	desc roachpb.RangeDescriptor,
	snap engine.Reader,
	snapshot *roachpb.RaftSnapshotData,
) ([]byte, error) {
	hasher := sha512.New()
	// Iterate over all the data in the range.
	iter := NewReplicaDataIterator(&desc, snap, true /* replicatedOnly */)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if snapshot != nil {
			// Add the k:v into the debug message.
			snapshot.KV = append(snapshot.KV, roachpb.RaftSnapshotData_KeyValue{Key: key.Key, Value: value, Timestamp: key.Timestamp})
		}

		// Encode the length of the key and value.
		if err := binary.Write(hasher, binary.LittleEndian, int64(len(key.Key))); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, int64(len(value))); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(key.Key); err != nil {
			return nil, err
		}
		timestamp, err := protoutil.Marshal(&key.Timestamp)
		if err != nil {
			return nil, err
		}
		if _, err := hasher.Write(timestamp); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(value); err != nil {
			return nil, err
		}
	}
	sha := make([]byte, 0, sha512.Size)
	return hasher.Sum(sha), nil
}

// VerifyChecksum verifies the checksum that was computed through a
// ComputeChecksum request. This command is marked as IsWrite so that
// it executes on every replica, but it actually doesn't modify the
// persistent state on the replica.
//
// Raft commands need to consistently execute on all replicas. An error
// seen on a particular replica should be returned here only if it is
// guaranteed to be seen on other replicas. In other words, a command needs
// to be consistent both in success and failure.
func (r *Replica) VerifyChecksum(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.VerifyChecksumRequest,
) (roachpb.VerifyChecksumResponse, *PostCommitTrigger, error) {
	if args.Version != replicaChecksumVersion {
		log.Errorf(ctx, "%s: consistency check skipped: incompatible versions: e=%d, v=%d",
			r, replicaChecksumVersion, args.Version)
		// Return success because version incompatibility might only
		// be seen on this replica.
		return roachpb.VerifyChecksumResponse{}, nil, nil
	}
	return roachpb.VerifyChecksumResponse{}, &PostCommitTrigger{verifyChecksum: &args}, nil
}

// ChangeFrozen freezes or unfreezes the Replica idempotently.
func (r *Replica) ChangeFrozen(
	ctx context.Context,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	h roachpb.Header,
	args roachpb.ChangeFrozenRequest,
) (roachpb.ChangeFrozenResponse, *PostCommitTrigger, error) {
	var resp roachpb.ChangeFrozenResponse
	resp.MinStartKey = roachpb.RKeyMax
	curStart, err := keys.Addr(args.Key)
	if err != nil {
		return resp, nil, err
	}
	if !bytes.Equal(curStart, args.Key) {
		return resp, nil, errors.Errorf("unsupported range-local key")
	}

	desc := r.Desc()

	frozen, err := loadFrozenStatus(ctx, batch, desc.RangeID)
	if err != nil || frozen == args.Frozen {
		// Something went wrong or we're already in the right frozen state. In
		// the latter case, we avoid writing the "same thing" because "we"
		// might actually not be the same version of the code (picture a couple
		// of freeze requests lined up, but not all of them applied between
		// version changes).
		return resp, nil, err
	}

	if args.MustVersion == "" {
		return resp, nil, errors.Errorf("empty version tag")
	} else if bi := build.GetInfo(); !frozen && args.Frozen && args.MustVersion != bi.Tag {
		// Some earlier version tried to freeze but we never applied it until
		// someone restarted this node with another version. No bueno - have to
		// assume that integrity has already been compromised.
		// Note that we have extra hooks upstream which delay returning success
		// to the caller until it's reasonable to assume that all Replicas have
		// applied the freeze.
		// This is a classical candidate for returning replica corruption, but
		// we don't do it (yet); for now we'll assume that the update steps
		// are carried out in correct order.
		log.Warningf(ctx, "%s: freeze %s issued from %s is applied by %s",
			r, desc, args.MustVersion, bi)
	}

	// Generally, we want to act only if the request hits the Range's StartKey.
	// The one case in which that behaves unexpectedly is if we're the first
	// range, which has StartKey equal to KeyMin, but the lowest curStart which
	// is feasible is LocalMax.
	if !desc.StartKey.Less(curStart) {
		resp.RangesAffected++
	} else if locMax, err := keys.Addr(keys.LocalMax); err != nil {
		return resp, nil, err
	} else if !locMax.Less(curStart) {
		resp.RangesAffected++
	}

	// Note down the Stores on which this request ran, even if the Range was
	// not affected.
	resp.Stores = make(map[roachpb.StoreID]roachpb.NodeID, len(desc.Replicas))
	for i := range desc.Replicas {
		resp.Stores[desc.Replicas[i].StoreID] = desc.Replicas[i].NodeID
	}

	if resp.RangesAffected == 0 {
		return resp, nil, nil
	}

	resp.MinStartKey = desc.StartKey

	if err := setFrozenStatus(ctx, batch, ms, r.Desc().RangeID, args.Frozen); err != nil {
		return roachpb.ChangeFrozenResponse{}, nil, err
	}

	trigger := &PostCommitTrigger{
		frozen: &args.Frozen,
	}
	return resp, trigger, nil
}

// ReplicaSnapshotDiff is a part of a []ReplicaSnapshotDiff which represents a diff between
// two replica snapshots. For now it's only a diff between their KV pairs.
type ReplicaSnapshotDiff struct {
	// LeaseHolder is set to true of this k:v pair is only present on the lease
	// holder.
	LeaseHolder bool
	Key         roachpb.Key
	Timestamp   hlc.Timestamp
	Value       []byte
}

// diffs the two k:v dumps between the lease holder and the replica.
func diffRange(l, r *roachpb.RaftSnapshotData) []ReplicaSnapshotDiff {
	if l == nil || r == nil {
		return nil
	}
	var diff []ReplicaSnapshotDiff
	i, j := 0, 0
	for {
		var e, v roachpb.RaftSnapshotData_KeyValue
		if i < len(l.KV) {
			e = l.KV[i]
		}
		if j < len(r.KV) {
			v = r.KV[j]
		}

		addLeader := func() {
			diff = append(diff, ReplicaSnapshotDiff{LeaseHolder: true, Key: e.Key, Timestamp: e.Timestamp, Value: e.Value})
			i++
		}
		addReplica := func() {
			diff = append(diff, ReplicaSnapshotDiff{LeaseHolder: false, Key: v.Key, Timestamp: v.Timestamp, Value: v.Value})
			j++
		}

		// Compare keys.
		var comp int
		// Check if it has finished traversing over all the lease holder keys.
		if e.Key == nil {
			if v.Key == nil {
				// Done traversing over all the replica keys. Done!
				break
			} else {
				comp = 1
			}
		} else {
			// Check if it has finished traversing over all the replica keys.
			if v.Key == nil {
				comp = -1
			} else {
				// Both lease holder and replica keys exist. Compare them.
				comp = bytes.Compare(e.Key, v.Key)
			}
		}
		switch comp {
		case -1:
			addLeader()

		case 0:
			if !e.Timestamp.Equal(v.Timestamp) {
				if v.Timestamp.Less(e.Timestamp) {
					addLeader()
				} else {
					addReplica()
				}
			} else if !bytes.Equal(e.Value, v.Value) {
				addLeader()
				addReplica()
			} else {
				// No diff; skip.
				i++
				j++
			}

		case 1:
			addReplica()

		}
	}
	return diff
}

// AdminSplit divides the range into into two ranges, using either
// args.SplitKey (if provided) or an internally computed key that aims
// to roughly equipartition the range by size. The split is done
// inside of a distributed txn which writes updated left and new right
// hand side range descriptors, and updates the range addressing
// metadata. The handover of responsibility for the reassigned key
// range is carried out seamlessly through a split trigger carried out
// as part of the commit of that transaction.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. An
// operation which might split a range should obtain a copy of the range's
// current descriptor before making the decision to split. If the decision is
// affirmative the descriptor is passed to AdminSplit, which performs a
// Conditional Put on the RangeDescriptor to ensure that no other operation has
// modified the range in the time the decision was being made.
// TODO(tschottdorf): should assert that split key is not a local key.
//
// See the comment on splitTrigger for details on the complexities.
func (r *Replica) AdminSplit(
	ctx context.Context, args roachpb.AdminSplitRequest, desc *roachpb.RangeDescriptor,
) (roachpb.AdminSplitResponse, *roachpb.Error) {
	var reply roachpb.AdminSplitResponse

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	log.Trace(ctx, "split begins")
	var splitKey roachpb.RKey
	{
		foundSplitKey := args.SplitKey
		if len(foundSplitKey) == 0 {
			snap := r.store.NewSnapshot()
			defer snap.Close()
			var err error
			targetSize := r.GetMaxBytes() / 2
			foundSplitKey, err = engine.MVCCFindSplitKey(ctx, snap, desc.RangeID, desc.StartKey, desc.EndKey, targetSize, nil /* logFn */)
			if err != nil {
				return reply, roachpb.NewErrorf("unable to determine split key: %s", err)
			}
		} else if !r.ContainsKey(foundSplitKey) {
			return reply, roachpb.NewError(roachpb.NewRangeKeyMismatchError(args.SplitKey, args.SplitKey, desc))
		}

		foundSplitKey, err := keys.EnsureSafeSplitKey(foundSplitKey)
		if err != nil {
			return reply, roachpb.NewErrorf("cannot split range at key %s: %v",
				args.SplitKey, err)
		}

		splitKey, err = keys.Addr(foundSplitKey)
		if err != nil {
			return reply, roachpb.NewError(err)
		}
		if !splitKey.Equal(foundSplitKey) {
			return reply, roachpb.NewErrorf("cannot split range at range-local key %s", splitKey)
		}
		if !engine.IsValidSplitKey(foundSplitKey) {
			return reply, roachpb.NewErrorf("cannot split range at key %s", splitKey)
		}
	}

	// First verify this condition so that it will not return
	// roachpb.NewRangeKeyMismatchError if splitKey equals to desc.EndKey,
	// otherwise it will cause infinite retry loop.
	if desc.StartKey.Equal(splitKey) || desc.EndKey.Equal(splitKey) {
		return reply, roachpb.NewErrorf("range is already split at key %s", splitKey)
	}
	log.Trace(ctx, "found split key")

	// Create right hand side range descriptor with the newly-allocated Range ID.
	rightDesc, err := r.store.NewRangeDescriptor(splitKey, desc.EndKey, desc.Replicas)
	if err != nil {
		return reply, roachpb.NewErrorf("unable to allocate right hand side range descriptor: %s", err)
	}

	// Init updated version of existing range descriptor.
	leftDesc := *desc
	leftDesc.EndKey = splitKey

	log.Infof(ctx, "%s: initiating a split of this range at key %s", r, splitKey)

	if err := r.store.DB().Txn(func(txn *client.Txn) error {
		log.Trace(ctx, "split closure begins")
		defer log.Trace(ctx, "split closure ends")
		// Update existing range descriptor for left hand side of
		// split. Note that we mutate the descriptor for the left hand
		// side of the split first to locate the txn record there.
		b := &client.Batch{}
		leftDescKey := keys.RangeDescriptorKey(leftDesc.StartKey)
		if err := updateRangeDescriptor(b, leftDescKey, desc, &leftDesc); err != nil {
			return err
		}
		// Create range descriptor for right hand side of the split.
		rightDescKey := keys.RangeDescriptorKey(rightDesc.StartKey)
		if err := updateRangeDescriptor(b, rightDescKey, nil, rightDesc); err != nil {
			return err
		}
		// Update range descriptor addressing record(s).
		if err := splitRangeAddressing(b, rightDesc, &leftDesc); err != nil {
			return err
		}
		if err := txn.Run(b); err != nil {
			if _, ok := err.(*roachpb.ConditionFailedError); ok {
				return errors.Errorf("conflict updating range descriptors")
			}
			return err
		}
		// Log the split into the range event log.
		if err := r.store.logSplit(txn, leftDesc, *rightDesc); err != nil {
			return err
		}
		b = &client.Batch{}
		// End the transaction manually, instead of letting RunTransaction
		// loop do it, in order to provide a split trigger.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				SplitTrigger: &roachpb.SplitTrigger{
					LeftDesc:  leftDesc,
					RightDesc: *rightDesc,
				},
			},
		})
		log.Trace(ctx, "attempting commit")
		return txn.Run(b)
	}); err != nil {
		return reply, roachpb.NewErrorf("split at key %s failed: %s", splitKey, err)
	}

	return reply, nil
}

// splitTrigger is called on a successful commit of a transaction
// containing an AdminSplit operation. It copies the abort cache for
// the new range and recomputes stats for both the existing, left hand
// side (LHS) range and the right hand side (RHS) range. For
// performance it only computes the stats for the original range (the
// left hand side) and infers the RHS stats by subtracting from the
// original stats. We compute the LHS stats because the split key
// computation ensures that we do not create large LHS
// ranges. However, this optimization is only possible if the stats
// are fully accurate. If they contain estimates, stats for both the
// LHS and RHS are computed.
//
// Splits are complicated. A split is initiated when a replica receives an
// AdminSplit request. Note that this request (and other "admin" requests)
// differs from normal requests in that it doesn't go through Raft but instead
// allows the lease holder Replica to act as the orchestrator for the
// distributed transaction that performs the split. As such, this request is
// only executed on the lease holder replica and the request is redirected to
// the lease holder if the recipient is a follower.
//
// Splits do not require the lease for correctness (which is good, because we
// only check that the lease is held at the beginning of the operation, and
// have no way to ensure that it is continually held until the end). Followers
// could perform splits too, and the only downside would be that if two splits
// were attempted concurrently (or a split and a ChangeReplicas), one would
// fail. The lease is used to designate one replica for this role and avoid
// wasting time on splits that may fail.
//
// The processing of splits is divided into two phases. The first phase occurs
// in Replica.AdminSplit. In that phase, the split-point is computed, and a
// transaction is started which updates both the LHS and RHS range descriptors
// and the meta range addressing information. (If we're splitting a meta2 range
// we'll be updating the meta1 addressing, otherwise we'll be updating the
// meta2 addressing). That transaction includes a special SplitTrigger flag on
// the EndTransaction request. Like all transactions, the requests within the
// transaction are replicated via Raft, including the EndTransaction request.
//
// The second phase of split processing occurs when each replica for the range
// encounters the SplitTrigger. Processing of the SplitTrigger happens below,
// in Replica.splitTrigger. The processing of the SplitTrigger occurs in two
// stages. The first stage operates within the context of an engine.Batch and
// updates all of the on-disk state for the old and new ranges atomically. The
// second stage is invoked when the batch commits and updates the in-memory
// state, creating the new replica in memory and populating its timestamp cache
// and registering it with the store.
//
// There is lots of subtlety here. The easy scenario is that all of the
// replicas process the SplitTrigger before processing any Raft message for RHS
// (right hand side) of the newly split range. Something like:
//
//         Node A             Node B             Node C
//     ----------------------------------------------------
// range 1   |                  |                  |
//           |                  |                  |
//      SplitTrigger            |                  |
//           |             SplitTrigger            |
//           |                  |             SplitTrigger
//           |                  |                  |
//     ----------------------------------------------------
// split finished on A, B and C |                  |
//           |                  |                  |
// range 2   |                  |                  |
//           | ---- MsgVote --> |                  |
//           | ---------------------- MsgVote ---> |
//
// But that ideal ordering is not guaranteed. The split is "finished" when two
// of the replicas have appended the end-txn request containing the
// SplitTrigger to their Raft log. The following scenario is possible:
//
//         Node A             Node B             Node C
//     ----------------------------------------------------
// range 1   |                  |                  |
//           |                  |                  |
//      SplitTrigger            |                  |
//           |             SplitTrigger            |
//           |                  |                  |
//     ----------------------------------------------------
// split finished on A and B    |                  |
//           |                  |                  |
// range 2   |                  |                  |
//           | ---- MsgVote --> |                  |
//           | --------------------- MsgVote ---> ???
//           |                  |                  |
//           |                  |             SplitTrigger
//
// In this scenario, C will create range 2 upon reception of the MsgVote from
// A, though locally that span of keys is still part of range 1. This is
// possible because at the Raft level ranges are identified by integer IDs and
// it isn't until C receives a snapshot of range 2 from the leader that it
// discovers the span of keys it covers. In order to prevent C from fully
// initializing range 2 in this instance, we prohibit applying a snapshot to a
// range if the snapshot overlaps another range. See Store.canApplySnapshot.
//
// But while a snapshot may not have been applied at C, an uninitialized
// Replica was created. An uninitialized Replica is one which belongs to a Raft
// group but for which the range descriptor has not been received. This Replica
// will have participated in the Raft elections. When we're creating the new
// Replica below we take control of this uninitialized Replica and stop it from
// responding to Raft messages by marking it "destroyed". Note that we use the
// Replica.mu.destroyed field for this, but we don't do everything that
// Replica.Destroy does (so we should probably rename that field in light of
// its new uses). In particular we don't touch any data on disk or leave a
// tombstone. This is especially important because leaving a tombstone would
// prevent the legitimate recreation of this replica.
//
// There is subtle synchronization here that is currently controlled by the
// Store.processRaft goroutine. In particular, the serial execution of
// Replica.handleRaftReady by Store.processRaft ensures that an uninitialized
// RHS won't be concurrently executing in Replica.handleRaftReady because we're
// currently running on that goroutine (i.e. Replica.splitTrigger is called on
// the processRaft goroutine).
//
// TODO(peter): The above synchronization needs to be fixed. Using a single
// goroutine for executing Replica.handleRaftReady is undesirable from a
// performance perspective. Likely we will have to add a mutex to Replica to
// protect handleRaftReady and to grab that mutex below when marking the
// uninitialized Replica as "destroyed". Hopefully we'll also be able to remove
// Store.processRaftMu.
//
// Note that in this more complex scenario, A (which performed the SplitTrigger
// first) will create the associated Raft group for range 2 and start
// campaigning immediately. It is possible for B to receive MsgVote requests
// before it has applied the SplitTrigger as well. Both B and C will vote for A
// (and preserve the records of that vote in their HardState). It is critically
// important for Raft correctness that we do not lose the records of these
// votes. After electing A the Raft leader for range 2, A will then attempt to
// send a snapshot to B and C and we'll fall into the situation above where a
// snapshot is received for a range before it has finished splitting from its
// sibling and is thus rejected. An interesting subtlety here: A will send a
// snapshot to B and C because when range 2 is initialized we were careful set
// synthesize its HardState to set its Raft log index to 10. If we had instead
// used log index 0, Raft would have believed the group to be empty, but the
// RHS has something. Using a non-zero initial log index causes Raft to believe
// that there is a discarded prefix to the log and will thus send a snapshot to
// followers.
//
// A final point of clarification: when we split a range we're splitting the
// data the range contains. But we're not forking or splitting the associated
// Raft group. Instead, we're creating a new Raft group to control the RHS of
// the split. That Raft group is starting from an empty Raft log (positioned at
// log entry 10) and a snapshot of the RHS of the split range.
//
// After the split trigger returns, the on-disk state of the right-hand side
// will be suitable for instantiating the right hand side Replica, and
// a suitable trigger is returned, along with the updated stats which represent
// the LHS delta caused by the split (i.e. all writes in the current batch
// which went to the left-hand side, minus the kv pairs which moved to the
// RHS).
//
// These stats are suitable for returning up the callstack like those for
// regular commands; the corresponding delta for the RHS is part of the
// returned trigger and is handled by the Store.
func (r *Replica) splitTrigger(
	ctx context.Context,
	batch engine.Batch,
	bothDeltaMS enginepb.MVCCStats, // stats for batch so far
	split *roachpb.SplitTrigger,
	ts hlc.Timestamp,
) (
	enginepb.MVCCStats,
	*PostCommitTrigger,
	error,
) {
	// TODO(tschottdorf): should have an incoming context from the corresponding
	// EndTransaction, but the plumbing has not been done yet.
	sp := r.store.Tracer().StartSpan("split")
	defer sp.Finish()
	desc := r.Desc()
	if !bytes.Equal(desc.StartKey, split.LeftDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, split.RightDesc.EndKey) {
		return enginepb.MVCCStats{}, nil, errors.Errorf("range does not match splits: (%s-%s) + (%s-%s) != %s",
			split.LeftDesc.StartKey, split.LeftDesc.EndKey,
			split.RightDesc.StartKey, split.RightDesc.EndKey, r)
	}

	// Preserve stats for pre-split range, excluding the current batch.
	origBothMS := r.GetMVCCStats()

	// TODO(d4l3k): we should check which side of the split is smaller
	// and compute stats for it instead of having a constraint that the
	// left hand side is smaller.

	// Compute (absolute) stats for LHS range. This means that no more writes
	// to the LHS must happen below this point.
	leftMS, err := ComputeStatsForRange(&split.LeftDesc, batch, ts.WallTime)
	if err != nil {
		return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to compute stats for LHS range after split")
	}
	log.Trace(ctx, "computed stats for left hand side range")

	// Copy the last replica GC and verification timestamps. These
	// values are unreplicated, which is why the MVCC stats are set to
	// nil on calls to MVCCPutProto.
	replicaGCTS, err := r.getLastReplicaGCTimestamp()
	if err != nil {
		return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to fetch last replica GC timestamp")
	}
	if err := engine.MVCCPutProto(ctx, batch, nil, keys.RangeLastReplicaGCTimestampKey(split.RightDesc.RangeID), hlc.ZeroTimestamp, nil, &replicaGCTS); err != nil {
		return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to copy last replica GC timestamp")
	}
	verifyTS, err := r.getLastVerificationTimestamp()
	if err != nil {
		return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to fetch last verification timestamp")
	}
	if err := engine.MVCCPutProto(ctx, batch, nil, keys.RangeLastVerificationTimestampKey(split.RightDesc.RangeID), hlc.ZeroTimestamp, nil, &verifyTS); err != nil {
		return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to copy last verification timestamp")
	}

	// Initialize the RHS range's abort cache by copying the LHS's.
	seqCount, err := r.abortCache.CopyInto(batch, &bothDeltaMS, split.RightDesc.RangeID)
	if err != nil {
		// TODO(tschottdorf): ReplicaCorruptionError.
		return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to copy abort cache to RHS split range")
	}
	log.Tracef(ctx, "copied abort cache (%d entries)", seqCount)

	// Initialize the right-hand lease to be the same as the left-hand lease.
	// This looks like an innocuous performance improvement, but it's more than
	// that - it ensures that we properly initialize the timestamp cache, which
	// is only populated on the lease holder, from that of the original Range.
	// We found out about a regression here the hard way in #7899. Prior to
	// this block, the following could happen:
	// - a client reads key 'd', leaving an entry in the timestamp cache on the
	//   lease holder of [a,e) at the time, node one.
	// - the range [a,e) splits at key 'c'. [c,e) starts out without a lease.
	// - the replicas of [a,e) on nodes one and two both process the split
	//   trigger and thus copy their timestamp caches to the new right-hand side
	//   Replica. However, only node one's timestamp cache contains information
	//   about the read of key 'd' in the first place.
	// - node two becomes the lease holder for [c,e). Its timestamp cache does
	//   know about the read at 'd' which happened at the beginning.
	// - node two can illegally propose a write to 'd' at a lower timestamp.
	{
		leftLease, err := loadLease(ctx, r.store.Engine(), r.RangeID)
		if err != nil {
			return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to load lease")
		}

		replica, found := split.RightDesc.GetReplicaDescriptor(leftLease.Replica.StoreID)
		if !found {
			return enginepb.MVCCStats{}, nil, errors.Errorf(
				"pre-split lease holder %+v not found in post-split descriptor %+v",
				leftLease.Replica, split.RightDesc,
			)
		}
		rightLease := leftLease
		rightLease.Replica = replica
		if err := setLease(
			ctx, batch, &bothDeltaMS, split.RightDesc.RangeID, rightLease,
		); err != nil {
			return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to seed right-hand side lease")
		}
	}

	// Compute (absolute) stats for RHS range.
	var rightMS enginepb.MVCCStats
	if origBothMS.ContainsEstimates || bothDeltaMS.ContainsEstimates {
		// Because either the original stats or the delta stats contain
		// estimate values, we cannot perform arithmetic to determine the
		// new range's stats. Instead, we must recompute by iterating
		// over the keys and counting.
		rightMS, err = ComputeStatsForRange(&split.RightDesc, batch, ts.WallTime)
		if err != nil {
			return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to compute stats for RHS range after split")
		}
	} else {
		// Because neither the original stats or the delta stats contain
		// estimate values, we can safely perform arithmetic to determine the
		// new range's stats. The calculation looks like:
		//   rhs_ms = orig_both_ms - orig_left_ms + right_delta_ms
		//          = orig_both_ms - left_ms + left_delta_ms + right_delta_ms
		//          = orig_both_ms - left_ms + delta_ms
		// where the following extra helper variables are used:
		// - orig_left_ms: the left-hand side key range, before the split
		// - (left|right)_delta_ms: the contributions to bothDeltaMS in this batch,
		//   itemized by the side of the split.
		//
		// Note that the result of that computation never has ContainsEstimates
		// set due to none of the inputs having it.

		// Start with the full stats before the split.
		rightMS = origBothMS
		// Remove stats from the left side of the split, at the same time adding
		// the batch contributions for the right-hand side.
		rightMS.Subtract(leftMS)
		rightMS.Add(bothDeltaMS)
	}

	// Now that we've computed the stats for the RHS so far, we persist them.
	// This looks a bit more complicated than it really is: updating the stats
	// also changes the stats, and we write not only the stats but a complete
	// initial state. Additionally, since bothDeltaMS is tracking writes to
	// both sides, we need to update it as well.
	{
		preRightMS := rightMS // for bothDeltaMS

		// Account for MVCCStats' own contribution to the RHS range's statistics.
		if err := engine.AccountForSelf(&rightMS, split.RightDesc.RangeID); err != nil {
			return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to account for enginepb.MVCCStats's own stats impact")
		}

		// TODO(tschottdorf): Writing the initial state is subtle since this
		// also seeds the Raft group. We are writing to the right hand side's
		// Raft group state in this batch. Between committing and telling the
		// Store, we could race with an uninitialized version of our new
		// Replica which might have been created by an incoming message from
		// another node which already processed the split. We rely on
		// synchronization provided at the Store level to avoid this. See #7860
		// and for history #7600. Note also that it is crucial that
		// writeInitialState *absorbs* an existing HardState (which might
		// contain a cast vote).
		rightMS, err = writeInitialState(ctx, batch, rightMS, split.RightDesc)
		if err != nil {
			return enginepb.MVCCStats{}, nil, errors.Wrap(err, "unable to write initial state")
		}
		bothDeltaMS.Subtract(preRightMS)
		bothDeltaMS.Add(rightMS)
	}

	// Compute how much data the left-hand side has shed by splitting.
	// We've already recomputed that in absolute terms, so all we need to do is
	// to turn it into a delta so the upstream machinery can digest it.
	leftDeltaMS := leftMS                  // start with new left-hand side absolute stats
	leftDeltaMS.Subtract(r.GetMVCCStats()) // subtract pre-split absolute stats
	leftDeltaMS.ContainsEstimates = false  // if there were any, recomputation removed them

	// Perform a similar computation for the right hand side. The difference
	// is that there isn't yet a Replica which could apply these stats, so
	// they will go into the trigger to make the Store (which keeps running
	// counters) aware.
	rightDeltaMS := bothDeltaMS
	rightDeltaMS.Subtract(leftDeltaMS)

	trigger := &PostCommitTrigger{
		// This makes sure that no reads are happening in parallel; see #3148.
		noConcurrentReads: true,
		split: &postCommitSplit{
			SplitTrigger: *split,
			RightDeltaMS: rightDeltaMS,
		},
	}
	return leftDeltaMS, trigger, nil
}

// AdminMerge extends this range to subsume the range that comes next
// in the key space. The merge is performed inside of a distributed
// transaction which writes the left hand side range descriptor (the
// subsuming range) and deletes the range descriptor for the right
// hand side range (the subsumed range). It also updates the range
// addressing metadata. The handover of responsibility for the
// reassigned key range is carried out seamlessly through a merge
// trigger carried out as part of the commit of that transaction.  A
// merge requires that the two ranges are collocated on the same set
// of replicas.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
func (r *Replica) AdminMerge(
	ctx context.Context, args roachpb.AdminMergeRequest, origLeftDesc *roachpb.RangeDescriptor,
) (roachpb.AdminMergeResponse, *roachpb.Error) {
	var reply roachpb.AdminMergeResponse

	if origLeftDesc.EndKey.Equal(roachpb.RKeyMax) {
		// Merging the final range doesn't make sense.
		return reply, roachpb.NewErrorf("cannot merge final range")
	}

	updatedLeftDesc := *origLeftDesc

	// Lookup right hand side range (subsumed). This really belongs
	// inside the transaction for consistency, but it is important (for
	// transaction record placement) that the first action inside the
	// transaction is the conditional put to change the left hand side's
	// descriptor end key. We look up the descriptor here only to get
	// the new end key and then repeat the lookup inside the
	// transaction.
	{
		rightRng := r.store.LookupReplica(origLeftDesc.EndKey, nil)
		if rightRng == nil {
			return reply, roachpb.NewErrorf("ranges not collocated")
		}

		updatedLeftDesc.EndKey = rightRng.Desc().EndKey
		log.Infof(ctx, "%s: initiating a merge of %s into this range", r, rightRng)
	}

	if err := r.store.DB().Txn(func(txn *client.Txn) error {
		log.Trace(ctx, "merge closure begins")
		// Update the range descriptor for the receiving range.
		{
			b := &client.Batch{}
			leftDescKey := keys.RangeDescriptorKey(updatedLeftDesc.StartKey)
			if err := updateRangeDescriptor(b, leftDescKey, origLeftDesc, &updatedLeftDesc); err != nil {
				return err
			}
			// Commit this batch on its own to ensure that the transaction record
			// is created in the right place (our triggers rely on this).
			log.Trace(ctx, "updating left descriptor")
			if err := txn.Run(b); err != nil {
				return err
			}
		}

		// Do a consistent read of the right hand side's range descriptor.
		rightDescKey := keys.RangeDescriptorKey(origLeftDesc.EndKey)
		var rightDesc roachpb.RangeDescriptor
		if err := txn.GetProto(rightDescKey, &rightDesc); err != nil {
			return err
		}

		// Verify that the two ranges are mergeable.
		if !bytes.Equal(origLeftDesc.EndKey, rightDesc.StartKey) {
			// Should never happen, but just in case.
			return errors.Errorf("ranges are not adjacent; %s != %s", origLeftDesc.EndKey, rightDesc.StartKey)
		}
		if !bytes.Equal(rightDesc.EndKey, updatedLeftDesc.EndKey) {
			// This merge raced with a split of the right-hand range.
			// TODO(bdarnell): needs a test.
			return errors.Errorf("range changed during merge; %s != %s", rightDesc.EndKey, updatedLeftDesc.EndKey)
		}
		if !replicaSetsEqual(origLeftDesc.Replicas, rightDesc.Replicas) {
			return errors.Errorf("ranges not collocated")
		}

		b := &client.Batch{}

		// Remove the range descriptor for the deleted range.
		b.Del(rightDescKey)

		if err := mergeRangeAddressing(b, origLeftDesc, &updatedLeftDesc); err != nil {
			return err
		}
		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		b.AddRawRequest(&roachpb.EndTransactionRequest{
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				MergeTrigger: &roachpb.MergeTrigger{
					LeftDesc:  updatedLeftDesc,
					RightDesc: rightDesc,
				},
			},
		})
		log.Trace(ctx, "attempting commit")
		return txn.Run(b)
	}); err != nil {
		return reply, roachpb.NewErrorf("merge of range into %d failed: %s", origLeftDesc.RangeID, err)
	}

	return reply, nil
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
//
// TODO(tschottdorf): give mergeTrigger more idiomatic stats computation as
// in splitTrigger.
func (r *Replica) mergeTrigger(
	ctx context.Context,
	batch engine.Batch,
	ms *enginepb.MVCCStats,
	merge *roachpb.MergeTrigger,
	ts hlc.Timestamp,
) (*PostCommitTrigger, error) {
	desc := r.Desc()
	if !bytes.Equal(desc.StartKey, merge.LeftDesc.StartKey) {
		return nil, errors.Errorf("LHS range start keys do not match: %s != %s",
			desc.StartKey, merge.LeftDesc.StartKey)
	}

	if !desc.EndKey.Less(merge.LeftDesc.EndKey) {
		return nil, errors.Errorf("original LHS end key is not less than the post merge end key: %s >= %s",
			desc.EndKey, merge.LeftDesc.EndKey)
	}

	rightRangeID := merge.RightDesc.RangeID
	if rightRangeID <= 0 {
		return nil, errors.Errorf("RHS range ID must be provided: %d", rightRangeID)
	}

	// Compute stats for premerged range, including current transaction.
	var mergedMS = r.GetMVCCStats()
	mergedMS.Add(*ms)
	// We will recompute the stats below and update the state, so when the
	// batch commits it has already taken ms into account.
	*ms = enginepb.MVCCStats{}

	// Add in stats for right hand side of merge, excluding system-local
	// stats, which will need to be recomputed.
	var rightMS enginepb.MVCCStats
	if err := engine.MVCCGetRangeStats(ctx, batch, rightRangeID, &rightMS); err != nil {
		return nil, err
	}
	rightMS.SysBytes, rightMS.SysCount = 0, 0
	mergedMS.Add(rightMS)

	// Copy the RHS range's abort cache to the new LHS one.
	_, err := r.abortCache.CopyFrom(ctx, batch, &mergedMS, rightRangeID)
	if err != nil {
		return nil, errors.Errorf("unable to copy abort cache to new split range: %s", err)
	}

	// Remove the RHS range's metadata. Note that we don't need to
	// keep track of stats here, because we already set the right range's
	// system-local stats contribution to 0.
	localRangeIDKeyPrefix := keys.MakeRangeIDPrefix(rightRangeID)
	if _, err := engine.MVCCDeleteRange(ctx, batch, nil, localRangeIDKeyPrefix, localRangeIDKeyPrefix.PrefixEnd(), math.MaxInt64, hlc.ZeroTimestamp, nil, false); err != nil {
		return nil, errors.Errorf("cannot remove range metadata %s", err)
	}

	// Add in the stats for the RHS range's range keys.
	iter := batch.NewIterator(false)
	defer iter.Close()
	localRangeKeyStart := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(merge.RightDesc.StartKey))
	localRangeKeyEnd := engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(merge.RightDesc.EndKey))
	msRange, err := iter.ComputeStats(localRangeKeyStart, localRangeKeyEnd, ts.WallTime)
	if err != nil {
		return nil, errors.Errorf("unable to compute RHS range's local stats: %s", err)
	}
	mergedMS.Add(msRange)

	// Set stats for updated range.
	if err := setMVCCStats(ctx, batch, r.RangeID, mergedMS); err != nil {
		return nil, errors.Errorf("unable to write MVCC stats: %s", err)
	}

	// Clear the timestamp cache. In case both the LHS and RHS replicas
	// held their respective range leases, we could merge the timestamp
	// caches for efficiency. But it's unlikely and not worth the extra
	// logic and potential for error.

	*ms = r.GetMVCCStats()
	mergedMS.Subtract(r.GetMVCCStats())
	*ms = mergedMS

	r.mu.Lock()
	r.mu.tsCache.Clear(r.store.Clock())
	r.mu.Unlock()

	trigger := &PostCommitTrigger{
		// This makes sure that no reads are happening in parallel; see #3148.
		noConcurrentReads: true,
		merge: &postCommitMerge{
			MergeTrigger: *merge,
		},
	}
	return trigger, nil
}

func (r *Replica) changeReplicasTrigger(
	ctx context.Context,
	batch engine.Batch,
	change *roachpb.ChangeReplicasTrigger,
) *PostCommitTrigger {
	var trigger *PostCommitTrigger
	// If we're removing the current replica, add it to the range GC queue.
	if change.ChangeType == roachpb.REMOVE_REPLICA && r.store.StoreID() == change.Replica.StoreID {
		// This wants to run as late as possible, maximizing the chances
		// that the other nodes have finished this command as well (since
		// processing the removal from the queue looks up the Range at the
		// lease holder, being too early here turns this into a no-op).
		trigger = updateTrigger(trigger, &PostCommitTrigger{
			addToReplicaGCQueue: true,
		})
	} else {
		// After a successful replica addition or removal check to see if the
		// range needs to be split. Splitting usually takes precedence over
		// replication via configuration of the split and replicate queues, but
		// if the split occurs concurrently with the replicas change the split
		// can fail and won't retry until the next scanner cycle. Re-queuing
		// the replica here removes that latency.
		trigger = updateTrigger(trigger, &PostCommitTrigger{
			maybeAddToSplitQueue: true,
		})
	}

	// Gossip the first range whenever the range descriptor changes. We also
	// gossip the first range whenever the lease holder changes, but that might
	// not have occurred if a replica was being added or the non-lease-holder
	// replica was being removed. Note that we attempt the gossiping even from
	// the removed replica in case it was the lease-holder and it is still
	// holding the lease.
	if r.IsFirstRange() {
		trigger = updateTrigger(trigger, &PostCommitTrigger{
			gossipFirstRange: true,
		})
	}

	cpy := *r.Desc()
	cpy.Replicas = change.UpdatedReplicas
	cpy.NextReplicaID = change.NextReplicaID
	trigger = updateTrigger(trigger, &PostCommitTrigger{
		desc: &cpy,
	})

	return trigger
}

// ChangeReplicas adds or removes a replica of a range. The change is performed
// in a distributed transaction and takes effect when that transaction is committed.
// When removing a replica, only the NodeID and StoreID fields of the Replica are used.
//
// The supplied RangeDescriptor is used as a form of optimistic lock. See the
// comment of "AdminSplit" for more information on this pattern.
//
// Changing the replicas for a range is complicated. A change is initiated by
// the "replicate" queue when it encounters a range which has too many
// replicas, too few replicas or requires rebalancing. Addition and removal of
// a replica is divided into four phases. The first phase, which occurs in
// Replica.ChangeReplicas, is performed via a distributed transaction which
// updates the range descriptor and the meta range addressing information. This
// transaction includes a special ChangeReplicasTrigger on the EndTransaction
// request. A ConditionalPut of the RangeDescriptor implements the optimistic
// lock on the RangeDescriptor mentioned previously. Like all transactions, the
// requests within the transaction are replicated via Raft, including the
// EndTransaction request.
//
// The second phase of processing occurs when the batch containing the
// EndTransaction is proposed to raft. This proposing occurs on whatever
// replica received the batch, usually, but not always the range lease
// holder. defaultProposeRaftCommandLocked notices that the EndTransaction
// contains a ChangeReplicasTrigger and proposes a ConfChange to Raft (via
// raft.RawNode.ProposeConfChange).
//
// The ConfChange is propagated to all of the replicas similar to a normal Raft
// command, though additional processing is done inside of Raft. A Replica
// encounters the ConfChange in Replica.handleRaftReady and executes it using
// raft.RawNode.ApplyConfChange. If a new replica was added the Raft leader
// will start sending it heartbeat messages and attempting to bring it up to
// date. If a replica was removed, it is at this point that the Raft leader
// will stop communicating with it.
//
// The fourth phase of change replicas occurs when each replica for the range
// encounters the ChangeReplicasTrigger when applying the EndTransaction
// request. The replica will update its local range descriptor so as to contain
// the new set of replicas. If the replica is the one that is being removed, it
// will queue itself for removal with replicaGCQueue.
//
// Note that a removed replica may not see the EndTransaction containing the
// ChangeReplicasTrigger. The ConfChange operation will be applied as soon as a
// quorum of nodes have committed it. If the removed replica is down or the
// message is dropped for some reason the removed replica will not be
// notified. The replica GC queue will eventually discover and cleanup this
// state.
//
// When a new replica is added, it will have to catch up to the state of the
// other replicas. The Raft leader automatically handles this by either sending
// the new replica Raft log entries to apply, or by generating and sending a
// snapshot. See Replica.Snapshot and Replica.Entries.
//
// Note that Replica.ChangeReplicas returns when the distributed transaction
// has been committed to a quorum of replicas in the range. The actual
// replication of data occurs asynchronously via a snapshot or application of
// Raft log entries. This is important for the replicate queue to be aware
// of. A node can process hundreds or thousands of ChangeReplicas operations
// per second even though the actual replication of data proceeds at a much
// slower base. In order to avoid having this background replication overwhelm
// the system, replication is throttled via a reservation system. When
// allocating a new replica for a range, the replicate queue reserves space for
// that replica on the target store via a ReservationRequest. (See
// StorePool.reserve). The reservation is fulfilled when the snapshot is
// applied.
//
// TODO(peter): There is a rare scenario in which a replica can be brought up
// to date via Raft log replay. In this scenario, the reservation will be left
// dangling until it expires. See #7849.
//
// TODO(peter): Describe preemptive snapshots. Preemptive snapshots are needed
// for the replicate queue to function properly. Currently the replicate queue
// will fire off as many replica additions as possible until it starts getting
// reservations denied at which point it will ignore the replica until the next
// scanner cycle.
func (r *Replica) ChangeReplicas(
	ctx context.Context,
	changeType roachpb.ReplicaChangeType,
	repDesc roachpb.ReplicaDescriptor,
	desc *roachpb.RangeDescriptor,
) error {

	repDescIdx := -1  // tracks NodeID && StoreID
	nodeUsed := false // tracks NodeID only
	for i, existingRep := range desc.Replicas {
		nodeUsedByExistingRep := existingRep.NodeID == repDesc.NodeID
		nodeUsed = nodeUsed || nodeUsedByExistingRep

		if nodeUsedByExistingRep && existingRep.StoreID == repDesc.StoreID {
			repDescIdx = i
			repDesc.ReplicaID = existingRep.ReplicaID
			break
		}
	}

	rangeID := desc.RangeID
	updatedDesc := *desc
	updatedDesc.Replicas = append([]roachpb.ReplicaDescriptor(nil), desc.Replicas...)

	switch changeType {
	case roachpb.ADD_REPLICA:
		// If the replica exists on the remote node, no matter in which store,
		// abort the replica add.
		if nodeUsed {
			return errors.Errorf("adding replica %v which is already present in range %d", repDesc, rangeID)
		}

		log.Trace(ctx, "requesting reservation")
		// Before we try to add a new replica, we first need to secure a
		// reservation for the replica on the receiving store.
		if err := r.store.allocator.storePool.reserve(
			r.store.Ident,
			repDesc.StoreID,
			rangeID,
			r.GetMVCCStats().Total(),
		); err != nil {
			return errors.Wrapf(err, "change replicas of range %d failed", rangeID)
		}
		log.Trace(ctx, "reservation granted")

		// Send a pre-emptive snapshot. Note that the replica to which this
		// snapshot is addressed has not yet had its replica ID initialized; this
		// is intentional, and serves to avoid the following race with the replica
		// GC queue:
		//
		// - snapshot received, a replica is lazily created with the "real" replica ID
		// - the replica is eligible for GC because it is not yet a member of the range
		// - GC queue runs, creating a raft tombstone with the replica's ID
		// - the replica is added to the range
		// - lazy creation of the replica fails due to the raft tombstone
		//
		// Instead, the replica GC queue will create a tombstone with replica ID
		// zero, which is never legitimately used, and thus never interferes with
		// raft operations. Racing with the replica GC queue can still partially
		// negate the benefits of pre-emptive snapshots, but that is a recoverable
		// degradation, not a catastrophic failure.
		snap, err := r.GetSnapshot(ctx)
		log.Trace(ctx, "generated snapshot")
		if err != nil {
			return errors.Wrapf(err, "change replicas of range %d failed", rangeID)
		}

		fromRepDesc, err := r.GetReplicaDescriptor()
		if err != nil {
			return errors.Wrapf(err, "change replicas of range %d failed", rangeID)
		}

		if repDesc.ReplicaID != 0 {
			return errors.Errorf(
				"must not specify a ReplicaID (%d) for new Replica",
				repDesc.ReplicaID,
			)
		}
		r.raftSender.SendAsync(&RaftMessageRequest{
			RangeID:     r.RangeID,
			FromReplica: fromRepDesc,
			ToReplica:   repDesc,
			Message: raftpb.Message{
				Type:     raftpb.MsgSnap,
				To:       0, // special cased ReplicaID for preemptive snapshots
				From:     uint64(fromRepDesc.ReplicaID),
				Term:     snap.Metadata.Term,
				Snapshot: snap,
			},
		})

		repDesc.ReplicaID = updatedDesc.NextReplicaID
		updatedDesc.NextReplicaID++
		updatedDesc.Replicas = append(updatedDesc.Replicas, repDesc)
	case roachpb.REMOVE_REPLICA:
		// If that exact node-store combination does not have the replica,
		// abort the removal.
		if repDescIdx == -1 {
			return errors.Errorf("removing replica %v which is not present in range %d", repDesc, rangeID)
		}
		updatedDesc.Replicas[repDescIdx] = updatedDesc.Replicas[len(updatedDesc.Replicas)-1]
		updatedDesc.Replicas = updatedDesc.Replicas[:len(updatedDesc.Replicas)-1]
	}

	descKey := keys.RangeDescriptorKey(desc.StartKey)

	if err := r.store.DB().Txn(func(txn *client.Txn) error {
		log.Trace(ctx, "attempting txn")
		txn.Proto.Name = replicaChangeTxnName
		// TODO(tschottdorf): oldDesc is used for sanity checks related to #7224.
		// Remove when that has been solved. The failure mode is likely based on
		// prior divergence of the Replica (in which case the check below does not
		// fire because everything reads from the local, diverged, set of data),
		// so we don't expect to see this fail in practice ever.
		oldDesc := new(roachpb.RangeDescriptor)
		if err := txn.GetProto(descKey, oldDesc); err != nil {
			return err
		}
		log.Infof(ctx, "%s: change replicas of %d: read existing descriptor %+v", r, rangeID, oldDesc)

		{
			b := txn.NewBatch()

			// Important: the range descriptor must be the first thing touched in the transaction
			// so the transaction record is co-located with the range being modified.
			if err := updateRangeDescriptor(b, descKey, desc, &updatedDesc); err != nil {
				return err
			}

			// Update range descriptor addressing record(s).
			if err := updateRangeAddressing(b, &updatedDesc); err != nil {
				return err
			}

			// Run transaction up to this point.
			if err := txn.Run(b); err != nil {
				return err
			}
		}

		// Log replica change into range event log.
		if err := r.store.logChange(txn, changeType, repDesc, updatedDesc); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a commit trigger.
		{
			b := txn.NewBatch()
			b.AddRawRequest(&roachpb.EndTransactionRequest{
				Commit: true,
				InternalCommitTrigger: &roachpb.InternalCommitTrigger{
					ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
						ChangeType:      changeType,
						Replica:         repDesc,
						UpdatedReplicas: updatedDesc.Replicas,
						NextReplicaID:   updatedDesc.NextReplicaID,
					},
				},
			})
			if err := txn.Run(b); err != nil {
				log.Trace(ctx, err.Error())
				return err
			}
		}

		if oldDesc.RangeID != 0 && !reflect.DeepEqual(oldDesc, desc) {
			// We read the previous value, it wasn't what we supposedly used in
			// the CPut, but we still overwrote in the CPut above.
			panic(fmt.Sprintf("committed replica change, but oldDesc != assumedOldDesc:\n%+v\n%+v\nnew desc:\n%+v",
				oldDesc, desc, updatedDesc))
		}
		return nil
	}); err != nil {
		log.Trace(ctx, err.Error())
		return errors.Wrapf(err, "change replicas of range %d failed", rangeID)
	}
	log.Trace(ctx, "txn complete")
	return nil
}

// replicaSetsEqual is used in AdminMerge to ensure that the ranges are
// all collocate on the same set of replicas.
func replicaSetsEqual(a, b []roachpb.ReplicaDescriptor) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[roachpb.StoreID]int)
	for _, replica := range a {
		set[replica.StoreID]++
	}

	for _, replica := range b {
		set[replica.StoreID]--
	}

	for _, value := range set {
		if value != 0 {
			return false
		}
	}

	return true
}

// updateRangeDescriptor adds a ConditionalPut on the range descriptor. The
// conditional put verifies that changes to the range descriptor are made in a
// well-defined order, preventing a scenario where a wayward replica which is
// no longer part of the original Raft group comes back online to form a
// splinter group with a node which was also a former replica, and hijacks the
// range descriptor. This is a last line of defense; other mechanisms should
// prevent rogue replicas from getting this far (see #768).
//
// Note that in addition to using this method to update the on-disk range
// descriptor, a CommitTrigger must be used to update the in-memory
// descriptor; it will not automatically be copied from newDesc.
// TODO(bdarnell): store the entire RangeDescriptor in the CommitTrigger
// and load it automatically instead of reconstructing individual
// changes.
func updateRangeDescriptor(
	b *client.Batch,
	descKey roachpb.Key,
	oldDesc,
	newDesc *roachpb.RangeDescriptor,
) error {
	if err := newDesc.Validate(); err != nil {
		return err
	}
	// This is subtle: []byte(nil) != interface{}(nil). A []byte(nil) refers to
	// an empty value. An interface{}(nil) refers to a non-existent value. So
	// we're careful to construct an interface{}(nil) when oldDesc is nil.
	var oldValue interface{}
	if oldDesc != nil {
		oldBytes, err := protoutil.Marshal(oldDesc)
		if err != nil {
			return err
		}
		oldValue = oldBytes
	}
	newValue, err := protoutil.Marshal(newDesc)
	if err != nil {
		return err
	}
	b.CPut(descKey, newValue, oldValue)
	return nil
}
