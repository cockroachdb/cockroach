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
// Author: Peter Mattis (peter.mattis@gmail.com)

package client

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"golang.org/x/net/context"
)

type txnSender struct {
	*Txn
}

func (ts *txnSender) Send(ctx context.Context, call Call) {
	// Send call through wrapped sender.
	call.Args.Header().Txn = &ts.txn
	ts.wrapped.Send(ctx, call)
	ts.txn.Update(call.Reply.Header().Txn)

	if err, ok := call.Reply.Header().GoError().(*proto.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		ts.txn = proto.Transaction{
			Name:      ts.txn.Name,
			Isolation: ts.txn.Isolation,
			Priority:  err.Txn.Priority, // acts as a minimum priority on restart
		}
	}
}

// Txn provides serial access to a KV store via Run and parallel
// access via Prepare and Flush. On receipt of
// TransactionRestartError, the transaction epoch is incremented and
// error passed to caller. On receipt of TransactionAbortedError, the
// transaction is re-created and the error passed to caller.
//
// A Txn instance is not thread safe.
type Txn struct {
	kv           KV
	wrapped      KVSender
	txn          proto.Transaction
	txnSender    txnSender
	prepared     []Callable
	haveTxnWrite bool // True if there were transactional writes
	haveEndTxn   bool // True if there was an explicit EndTransaction
}

var defaultTxnOpts = TransactionOptions{}

func newTxn(kv *KV, opts *TransactionOptions) *Txn {
	if opts == nil {
		opts = &defaultTxnOpts
	}

	t := &Txn{
		kv:      *kv,
		wrapped: kv.Sender,
		txn: proto.Transaction{
			Name:      opts.Name,
			Isolation: opts.Isolation,
		},
	}
	t.txnSender.Txn = t
	t.kv.Sender = &t.txnSender
	if opts != &defaultTxnOpts {
		t.kv.UserPriority = opts.UserPriority
	}
	return t
}

func (t *Txn) exec(retryable func(txn *Txn) error) error {
	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := t.kv.TxnRetryOptions
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
				// Prepare and flush for end txn in order to execute entire txn in
				// a single round trip if possible.
				t.Prepare(Call{Args: etArgs, Reply: etReply})
			}
			err = t.Flush()
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
		if replyErr := t.Run(Call{
			Args:  &proto.EndTransactionRequest{Commit: false},
			Reply: &proto.EndTransactionResponse{},
		}); replyErr != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
		return err
	}
	return err
}

// Run runs the specified calls synchronously in a single batch and
// returns any errors.
func (t *Txn) Run(calls ...Callable) error {
	if len(calls) == 0 {
		return nil
	}
	if len(t.prepared) > 0 || len(calls) > 1 {
		t.Prepare(calls...)
		return t.Flush()
	}
	t.updateState(calls)
	return t.kv.Run(calls...)
}

// Prepare accepts a KV API call, specified by arguments and a reply
// struct. The call will be buffered locally until the first call to
// Flush(), at which time it will be sent for execution as part of a
// batch call. Using Prepare/Flush parallelizes queries and updates
// and should be used where possible for efficiency.
//
// For clients using an HTTP sender, Prepare/Flush allows multiple
// commands to be sent over the same connection. Prepare/Flush can
// dramatically improve efficiency by compressing multiple writes into
// a single atomic update in the event that the writes are to keys
// within a single range.
//
// TODO(pmattis): Can Prepare/Flush be replaced with a Batch struct?
// Doing so could potentially make the Txn interface more symmetric
// with the KV interface, but potentially removes the optimization to
// send the EndTransaction in the same batch as the final set of
// prepared calls.
func (t *Txn) Prepare(callables ...Callable) {
	t.updateState(callables)
	for _, c := range callables {
		c.Call().resetClientCmdID(t.kv.clock)
	}
	t.prepared = append(t.prepared, callables...)
}

// Flush sends all previously prepared calls, buffered by invocations
// of Prepare(). The calls are organized into a single batch command
// and sent together. Flush returns nil if all prepared calls are
// executed successfully. Otherwise, Flush returns the first error,
// where calls are executed in the order in which they were prepared.
// After Flush returns, all prepared reply structs will be valid.
func (t *Txn) Flush() error {
	calls := t.prepared
	t.prepared = nil
	if len(calls) == 0 {
		return nil
	}
	return t.kv.Run(calls...)
}

func (t *Txn) updateState(callables []Callable) {
	for _, callable := range callables {
		c := callable.Call()
		if b, ok := c.Args.(*proto.BatchRequest); ok {
			for _, br := range b.Requests {
				t.updateStateForRequest(br.GetValue().(proto.Request))
			}
			continue
		}
		t.updateStateForRequest(c.Args)
	}
}

func (t *Txn) updateStateForRequest(r proto.Request) {
	if !t.haveTxnWrite {
		t.haveTxnWrite = proto.IsTransactionWrite(r)
	} else if _, ok := r.(*proto.EndTransactionRequest); ok {
		t.haveEndTxn = true
	}
}
