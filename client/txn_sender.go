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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package client

import (
	"sync"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// A txnSender proxies requests to the underlying KVSender, automatically
// beginning a transaction and then propagating timestamps and txn
// changes to all commands. On receipt of TransactionRetryError,
// the transaction epoch is incremented and error passed to caller.
// On receipt of TransactionAbortedError, the transaction is re-
// created and error passed to caller.
//
// txnSender is thread safe and safely accommodates concurrent
// invocations of Send().
type txnSender struct {
	sender KVSender
	clock  Clock
	TransactionOptions
	txnEnd bool // True if EndTransaction was invoked internally

	sync.Mutex // Protects variables below.
	timestamp  proto.Timestamp
	txn        *proto.Transaction
}

// newTxnSender returns a new instance of txnSender which wraps a
// KV,Sender and uses the supplied transaction options.
func newTxnSender(sender KVSender, clock Clock, opts *TransactionOptions) *txnSender {
	return &txnSender{
		sender:             sender,
		clock:              clock,
		TransactionOptions: *opts,
	}
}

// Send proxies requests to wrapped kv.KVSender instance, taking care
// to maintain correct Cockroach transactional semantics. The details
// include:
//
// - Begin transaction with first key
// - Propagate response timestamps to subsequent requests
// - Set client command IDs on read-write commands
// - Increment epoch -or- abort on TransactionRetryError
// - Restart transaction on TransactionAbortedError
// - Restart transaction on WriteTooOldError
// - Retry commands (with possible backoff) on WriteIntentErrors
//
// If limits for backoff / retry are enabled through the options and
// reached during transaction execution, TransactionRetryError will be
// returned.
func (tdb *txnSender) Send(call *Call) {
	tdb.Lock()
	// If the transaction hasn't yet been created, create now, using
	// this command's key as the base key.
	if tdb.txn == nil {
		btReply := &proto.BeginTransactionResponse{}
		btCall := &Call{
			Method: proto.BeginTransaction,
			Args: &proto.BeginTransactionRequest{
				RequestHeader: proto.RequestHeader{
					Key:          call.Args.Header().Key,
					User:         tdb.User,
					UserPriority: gogoproto.Int32(tdb.UserPriority),
				},
				Name:      tdb.Name,
				Isolation: tdb.Isolation,
			},
			Reply: btReply,
		}
		tdb.sender.Send(btCall)
		if err := btCall.Reply.Header().GoError(); err != nil {
			call.Reply.Header().SetGoError(err)
			return
		}
		if btReply.Txn == nil {
			call.Reply.Header().SetGoError(util.Errorf("begin transaction returned Txn=nil"))
			return
		}
		tdb.txn = btReply.Txn
		tdb.timestamp = tdb.txn.Timestamp
	}
	if call.Method == proto.EndTransaction || call.Method == proto.InternalEndTxn {
		// For EndTransaction, make sure key is set to txn ID.
		call.Args.Header().Key = tdb.txn.ID
	} else if !proto.IsTransactional(call.Method) {
		tdb.sender.Send(call)
		tdb.Unlock()
		return
	}
	// Set Args.Timestamp & Args.Txn to reflect current values.
	txnCopy := *tdb.txn
	call.Args.Header().User = tdb.User
	call.Args.Header().Timestamp = tdb.timestamp
	call.Args.Header().Txn = &txnCopy
	tdb.Unlock()

	// Backoff and retry loop for handling errors.
	retryOpts := TxnRetryOptions
	retryOpts.Tag = call.Method
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// Reset client command ID (if applicable) on every retry at this
		// level--retries due to network timeouts or disconnects are
		// handled by lower-level KVSender implementation(s).
		call.resetClientCmdID(tdb.clock)

		// Send call through wrapped sender.
		tdb.sender.Send(call)
		tdb.Lock()
		defer tdb.Unlock()

		// Update max timestamp using the response header's timestamp.
		if tdb.timestamp.Less(call.Reply.Header().Timestamp) {
			tdb.timestamp = call.Reply.Header().Timestamp
		}
		if log.V(1) && call.Reply.Header().GoError() != nil {
			log.Infof("command %s %+v failed: %s", call.Method, call.Args, call.Reply.Header().GoError())
		}
		// Take action on various errors.
		switch t := call.Reply.Header().GoError().(type) {
		case *proto.ReadWithinUncertaintyIntervalError:
			// If the reader encountered a newer write within the uncertainty
			// interval, move the timestamp forward, just past that write or
			// up to MaxTimestamp, whichever comes first.
			var candidateTS proto.Timestamp
			if t.ExistingTimestamp.Less(tdb.txn.MaxTimestamp) {
				candidateTS = t.ExistingTimestamp
				candidateTS.Logical++
			} else {
				candidateTS = tdb.txn.MaxTimestamp
			}
			// Only change the timestamp if we're moving it forward.
			if tdb.timestamp.Less(candidateTS) {
				tdb.timestamp = candidateTS
			}
			tdb.txn.Restart(false /* !Abort */, tdb.UserPriority, tdb.txn.Priority, tdb.timestamp)
		case *proto.TransactionAbortedError:
			// Increase timestamp if applicable.
			if tdb.timestamp.Less(t.Txn.Timestamp) {
				tdb.timestamp = t.Txn.Timestamp
			}
			tdb.txn.Restart(true /* Abort */, tdb.UserPriority, t.Txn.Priority, tdb.timestamp)
		case *proto.TransactionPushError:
			// Increase timestamp if applicable.
			if tdb.timestamp.Less(t.PusheeTxn.Timestamp) {
				tdb.timestamp = t.PusheeTxn.Timestamp
				tdb.timestamp.Logical++ // ensure this txn's timestamp > other txn
			}
			tdb.txn.Restart(false /* !Abort */, tdb.UserPriority, t.Txn.Priority-1, tdb.timestamp)
		case *proto.TransactionRetryError:
			// Increase timestamp if applicable.
			if tdb.timestamp.Less(t.Txn.Timestamp) {
				tdb.timestamp = t.Txn.Timestamp
			}
			tdb.txn.Restart(false /* !Abort */, tdb.UserPriority, t.Txn.Priority, tdb.timestamp)
		case *proto.WriteTooOldError:
			// If write is too old, update the timestamp and immediately retry.
			if tdb.timestamp.Less(t.ExistingTimestamp) {
				tdb.timestamp = t.ExistingTimestamp
				tdb.timestamp.Logical++
			}
			// Update the header so we use the newer timestamp on retry within
			// this backoff loop.
			call.Args.Header().Timestamp = tdb.timestamp
			return util.RetryReset, nil
		case *proto.WriteIntentError:
			// If write intent error is resolved, exit retry/backoff loop to
			// immediately retry.
			if t.Resolved {
				return util.RetryReset, nil
			}
			// Otherwise, update this txn's priority and timestamp to reflect
			// the unresolved intent.
			tdb.txn.UpgradePriority(t.Txn.Priority - 1)
			if tdb.timestamp.Less(t.Txn.Timestamp) {
				tdb.timestamp = t.Txn.Timestamp
			}
			// Update the header so we use the newer timestamp on retry within
			// this backoff loop.
			call.Args.Header().Timestamp = tdb.timestamp
			// Backoff on unresolvable intent and retry command.
			// Make sure to upgrade our priority to the conflicting txn's - 1.
			return util.RetryContinue, nil
		case nil:
			if call.Method == proto.EndTransaction || call.Method == proto.InternalEndTxn {
				tdb.txnEnd = true // set this txn as having been ended
			}
		}
		return util.RetryBreak, nil
	})

	if _, ok := err.(*util.RetryMaxAttemptsError); ok {
		tdb.txn.Restart(false /* !Abort */, tdb.UserPriority, tdb.txn.Priority, tdb.timestamp)
		call.Reply.Header().SetGoError(proto.NewTransactionRetryError(tdb.txn))
	}
}

// Close is a noop for the txnSender.
func (tdb *txnSender) Close() {
}
