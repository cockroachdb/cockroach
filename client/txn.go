// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"golang.org/x/net/context"
)

var (
	// DefaultTxnRetryOptions are the standard retry options used
	// for transactions.
	// This is exported for testing purposes only.
	DefaultTxnRetryOptions = retry.Options{
		Backoff:     50 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
		Constant:    2,
		MaxAttempts: 0, // retry indefinitely
		UseV1Info:   true,
	}
)

// txnSender implements the Sender interface and is used to hide the Send
// method from the txn interface.
type txnSender struct {
	*txn
}

func (ts *txnSender) Send(ctx context.Context, call Call) {
	// Send call through wrapped sender.
	call.Args.Header().Txn = &ts.txn.txn
	ts.wrapped.Send(ctx, call)
	ts.txn.txn.Update(call.Reply.Header().Txn)

	if err, ok := call.Reply.Header().GoError().(*proto.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		ts.txn.txn = proto.Transaction{
			Name:      ts.txn.txn.Name,
			Isolation: ts.txn.txn.Isolation,
			Priority:  err.Txn.Priority, // acts as a minimum priority on restart
		}
	}
}

// txn provides serial access to a KV store via Run and parallel
// access via Prepare and Flush. On receipt of
// TransactionRestartError, the transaction epoch is incremented and
// error passed to caller. On receipt of TransactionAbortedError, the
// transaction is re-created and the error passed to caller.
//
// A txn instance is not thread safe.
type txn struct {
	kv           kv
	wrapped      Sender
	txn          proto.Transaction
	txnSender    txnSender
	haveTxnWrite bool // True if there were transactional writes
	haveEndTxn   bool // True if there was an explicit EndTransaction
}

func (t *txn) init(kv *kv) {
	t.kv = *kv
	t.wrapped = kv.Sender
	t.txnSender.txn = t
	t.kv.Sender = &t.txnSender
}

func (t *txn) exec(retryable func(txn *txn) error) error {
	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := t.kv.txnRetryOptions
	retryOpts.Tag = t.txn.Name
	err := retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		t.haveTxnWrite, t.haveEndTxn = false, false // always reset before [re]starting txn
		err := retryable(t)
		if err == nil {
			if !t.haveEndTxn && t.haveTxnWrite {
				// If there were no errors running retryable, commit the txn. This
				// may block waiting for outstanding writes to complete in case
				// retryable didn't -- we need the most recent of all response
				// timestamps in order to commit.
				etArgs := &proto.EndTransactionRequest{Commit: true}
				etReply := &proto.EndTransactionResponse{}
				err = t.send(Call{Args: etArgs, Reply: etReply})
			}
		}
		if restartErr, ok := err.(proto.TransactionRestartError); ok {
			if restartErr.CanRestartTransaction() == proto.TransactionRestart_IMMEDIATE {
				return retry.Reset, nil
			} else if restartErr.CanRestartTransaction() == proto.TransactionRestart_BACKOFF {
				return retry.Continue, nil
			}
			// By default, fall through and return Break.
		}
		return retry.Break, err
	})
	if err != nil && t.haveTxnWrite {
		if replyErr := t.send(Call{
			Args:  &proto.EndTransactionRequest{Commit: false},
			Reply: &proto.EndTransactionResponse{},
		}); replyErr != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
		return err
	}
	return err
}

// send runs the specified calls synchronously in a single batch and
// returns any errors.
func (t *txn) send(calls ...Call) error {
	if len(calls) == 0 {
		return nil
	}
	t.updateState(calls)
	return t.kv.send(calls...)
}

func (t *txn) updateState(calls []Call) {
	for _, c := range calls {
		if b, ok := c.Args.(*proto.BatchRequest); ok {
			for _, br := range b.Requests {
				t.updateStateForRequest(br.GetValue().(proto.Request))
			}
			continue
		}
		t.updateStateForRequest(c.Args)
	}
}

func (t *txn) updateStateForRequest(r proto.Request) {
	if !t.haveTxnWrite {
		t.haveTxnWrite = proto.IsTransactionWrite(r)
	} else if _, ok := r.(*proto.EndTransactionRequest); ok {
		t.haveEndTxn = true
	}
}
