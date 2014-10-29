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
	wrapped KVSender
	clock   Clock
	*TransactionOptions
	txnEnd bool // True if EndTransaction was invoked internally

	sync.Mutex // Protects variables below.
	timestamp  proto.Timestamp
	txn        *proto.Transaction
}

// newTxnSender returns a new instance of txnSender which wraps a
// KVSender and uses the supplied transaction options.
func newTxnSender(wrapped KVSender, clock Clock, opts *TransactionOptions) *txnSender {
	return &txnSender{
		wrapped:            wrapped,
		clock:              clock,
		TransactionOptions: opts,
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
func (ts *txnSender) Send(call *Call) {
	ts.Lock()
	// If the transaction hasn't yet been created, create now, using
	// this command's key as the base key.
	if ts.txn == nil {
		btReply := &proto.BeginTransactionResponse{}
		btCall := &Call{
			Method: proto.BeginTransaction,
			Args: &proto.BeginTransactionRequest{
				RequestHeader: proto.RequestHeader{
					Key:          call.Args.Header().Key,
					User:         ts.User,
					UserPriority: gogoproto.Int32(ts.UserPriority),
				},
				Name:      ts.Name,
				Isolation: ts.Isolation,
			},
			Reply: btReply,
		}
		ts.wrapped.Send(btCall)
		if err := btCall.Reply.Header().GoError(); err != nil {
			call.Reply.Header().SetGoError(err)
			return
		}
		if btReply.Txn == nil {
			call.Reply.Header().SetGoError(util.Errorf("begin transaction returned Txn=nil"))
			return
		}
		ts.txn = btReply.Txn
		ts.timestamp = ts.txn.Timestamp
	}
	if call.Method == proto.EndTransaction || call.Method == proto.InternalEndTxn {
		// For EndTransaction, make sure key is set to txn ID.
		call.Args.Header().Key = ts.txn.ID
	} else if !proto.IsTransactional(call.Method) {
		call.Reply.Header().SetGoError(util.Errorf("cannot invoke %s command within a transaction", call.Method))
		ts.Unlock()
		return
	}
	// Set Args.Timestamp & Args.Txn to reflect current values.
	txnCopy := *ts.txn
	call.Args.Header().User = ts.User
	call.Args.Header().Timestamp = ts.timestamp
	call.Args.Header().Txn = &txnCopy
	ts.Unlock()

	// Backoff and retry loop for handling errors.
	var retryOpts util.RetryOptions = TxnRetryOptions
	retryOpts.Tag = call.Method
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// Reset client command ID (if applicable) on every retry at this
		// level--retries due to network timeouts or disconnects are
		// handled by lower-level KVSender implementation(s).
		call.resetClientCmdID(ts.clock)

		// Send call through wrapped sender.
		ts.wrapped.Send(call)
		ts.Lock()
		defer ts.Unlock()

		// Update max timestamp using the response header's timestamp.
		if ts.timestamp.Less(call.Reply.Header().Timestamp) {
			ts.timestamp = call.Reply.Header().Timestamp
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
			if t.ExistingTimestamp.Less(ts.txn.MaxTimestamp) {
				candidateTS = t.ExistingTimestamp
				candidateTS.Logical++
			} else {
				candidateTS = ts.txn.MaxTimestamp
			}
			// Only change the timestamp if we're moving it forward.
			if ts.timestamp.Less(candidateTS) {
				ts.timestamp = candidateTS
			}
			ts.txn.Restart(false /* !Abort */, ts.UserPriority, ts.txn.Priority, ts.timestamp)
		case *proto.TransactionAbortedError:
			// Increase timestamp if applicable.
			if ts.timestamp.Less(t.Txn.Timestamp) {
				ts.timestamp = t.Txn.Timestamp
			}
			ts.txn.Restart(true /* Abort */, ts.UserPriority, t.Txn.Priority, ts.timestamp)
		case *proto.TransactionPushError:
			// Increase timestamp if applicable.
			if ts.timestamp.Less(t.PusheeTxn.Timestamp) {
				ts.timestamp = t.PusheeTxn.Timestamp
				ts.timestamp.Logical++ // ensure this txn's timestamp > other txn
			}
			ts.txn.Restart(false /* !Abort */, ts.UserPriority, t.PusheeTxn.Priority-1, ts.timestamp)
		case *proto.TransactionRetryError:
			// Increase timestamp if applicable.
			if ts.timestamp.Less(t.Txn.Timestamp) {
				ts.timestamp = t.Txn.Timestamp
			}
			ts.txn.Restart(false /* !Abort */, ts.UserPriority, t.Txn.Priority, ts.timestamp)
		case *proto.WriteTooOldError:
			// If write is too old, update the timestamp and immediately retry.
			if ts.timestamp.Less(t.ExistingTimestamp) {
				ts.timestamp = t.ExistingTimestamp
				ts.timestamp.Logical++
			}
			// Update the header so we use the newer timestamp on retry within
			// this backoff loop.
			call.Args.Header().Timestamp = ts.timestamp
			return util.RetryReset, nil
		case *proto.WriteIntentError:
			// If write intent error is resolved, exit retry/backoff loop to
			// immediately retry.
			if t.Resolved {
				return util.RetryReset, nil
			}
			// Otherwise, update this txn's priority and timestamp to reflect
			// the unresolved intent.
			ts.txn.UpgradePriority(t.Txn.Priority - 1)
			if ts.timestamp.Less(t.Txn.Timestamp) {
				ts.timestamp = t.Txn.Timestamp
				ts.timestamp.Logical++
			}
			// Update the header so we use the newer timestamp on retry within
			// this backoff loop.
			call.Args.Header().Timestamp = ts.timestamp
			// Backoff on unresolvable intent and retry command.
			// Make sure to upgrade our priority to the conflicting txn's - 1.
			return util.RetryContinue, nil
		case nil:
			if call.Method == proto.EndTransaction || call.Method == proto.InternalEndTxn {
				ts.txnEnd = true // set this txn as having been ended
			}
		}
		return util.RetryBreak, nil
	})

	if _, ok := err.(*util.RetryMaxAttemptsError); ok {
		ts.txn.Restart(false /* !Abort */, ts.UserPriority, ts.txn.Priority, ts.timestamp)
		call.Reply.Header().SetGoError(proto.NewTransactionRetryError(ts.txn))
	}
}

// Close is a noop for the txnSender.
func (ts *txnSender) Close() {
}
