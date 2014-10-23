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

package kv

import (
	"bytes"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Default retry options for transactions.
var txnRetryOpts = util.RetryOptions{
	Backoff:     50 * time.Millisecond,
	MaxBackoff:  5 * time.Second,
	Constant:    2,
	MaxAttempts: 0, // retry indefinitely
}

// A txnDB proxies requests to the underlying DB, automatically
// beginning a transaction and then propagating timestamps and txn
// changes to all commands. On receipt of TransactionRetryError,
// the transaction epoch is incremented and error passed to caller.
// On receipt of TransactionAbortedError, the transaction is re-
// created and error passed to caller.
type txnDB struct {
	*DB
	storage.TransactionOptions
	txnEnd bool // True if EndTransaction was invoked internally
	wg     sync.WaitGroup

	sync.Mutex // Protects variables below.
	timestamp  proto.Timestamp
	txn        *proto.Transaction
}

// runTransaction runs retryable in a transactional context.
// See comments in DB.RunTransaction.
func runTransaction(db *DB, opts *storage.TransactionOptions, retryable func(db storage.DB) error) error {
	// Create new transaction DB.
	tdb := &txnDB{
		DB:                 NewDB(db.kv, db.clock),
		TransactionOptions: *opts,
	}
	tdb.DB.BaseDB.Executor = tdb.executeCmd

	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := txnRetryOpts
	if opts.Retry != nil {
		retryOpts = *opts.Retry
	}
	retryOpts.Tag = opts.Name
	if err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		tdb.txnEnd = false // always reset before [re]starting txn
		err := retryable(tdb)
		if err == nil && !tdb.txnEnd {
			// If there were no errors running retryable, commit the txn. This
			// may block waiting for outstanding writes to complete in case
			// retryable didn't -- we need the most recent of all response
			// timestamps in order to commit.
			if etReply := <-tdb.EndTransaction(&proto.EndTransactionRequest{Commit: true}); etReply.GoError() != nil {
				err = etReply.GoError()
			} else {
				return util.RetryBreak, nil
			}
		}
		switch t := err.(type) {
		case *proto.TransactionRetryError:
			// If !Backoff, return RetryReset for an immediate retry (as in
			// the case of an SSI txn whose timestamp was pushed).
			if !t.Backoff {
				return util.RetryReset, nil
			}
			// Otherwise, allow backoff/retry.
			return util.RetryContinue, nil
		case *proto.TransactionAbortedError:
			// If the transaction was aborted, the txnDB will have created
			// a new txn. We allow backoff/retry in this case.
			// TODO(spencer): need to call coordinator.EndTxn on this.
			return util.RetryContinue, nil
		case *proto.WriteTooOldError:
			// Retry immediately on write-too-old.
			return util.RetryReset, nil
		case *proto.ReadWithinUncertaintyIntervalError:
			// Retry immediately.
			return util.RetryReset, nil
		default:
			// For all other cases, finish retry loop, returning possible error.
			return util.RetryBreak, t
		}
	}); err != nil && !tdb.txnEnd {
		if etReply := <-tdb.EndTransaction(&proto.EndTransactionRequest{Commit: false}); etReply.GoError() != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", etReply.GoError(), err)
		}
		return err
	}
	return nil
}

// executeCmd proxies requests to tdb.db, taking care to:
//
// - Begin transaction with first key
// - Propagate response timestamps to subsequent requests
// - Set client command IDs on read-write commands
// - Retry commands on WriteIntentErrors
// - Increment epoch on TransactionRetryError
// - Re-create transaction on TransactionAbortedError
func (tdb *txnDB) executeCmd(method string, args proto.Request, replyChan interface{}) {
	tdb.Lock()
	// If the transaction hasn't yet been created, create now, using
	// this command's key as the base key.
	if tdb.txn == nil {
		// Use Key.Address() for base key or else the txn record isn't
		// guaranteed to be located on the same range as the first key.
		tdb.txn = storage.NewTransaction(tdb.Name, engine.Key(args.Header().Key).Address(),
			tdb.UserPriority, tdb.Isolation, tdb.clock)
		tdb.timestamp = tdb.txn.Timestamp
	}
	if method == storage.EndTransaction || method == storage.InternalEndTxn {
		// For EndTransaction, make sure key is set to txn ID.
		args.Header().Key = engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, tdb.txn.ID)
	} else if !isTransactional(method) {
		tdb.DB.executeCmd(method, args, replyChan)
		tdb.Unlock()
		return
	}
	// Set args.Timestamp & args.Txn to reflect current values.
	txnCopy := *tdb.txn
	args.Header().User = tdb.User
	args.Header().Timestamp = tdb.timestamp
	args.Header().Txn = &txnCopy
	tdb.wg.Add(1)
	tdb.Unlock()

	// Backoff and retry on write intent errors.
	retryOpts := txnRetryOpts
	if tdb.Retry != nil {
		retryOpts = *tdb.Retry
	}
	retryOpts.Tag = method
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// On mutating commands, set a client command ID. This prevents
		// mutations from being run multiple times on RPC retries.
		if storage.IsReadWrite(method) {
			args.Header().CmdID = proto.ClientCmdID{
				WallTime: tdb.clock.Now().WallTime,
				Random:   rand.Int63(),
			}
		}

		// Proxy command through to the wrapped DB. Create an intercept
		// channel so we can examine the reply before passing it on to
		// client.
		interceptChan := reflect.MakeChan(reflect.TypeOf(replyChan), 1)
		tdb.DB.executeCmd(method, args, interceptChan.Interface())
		recvVal, ok := interceptChan.Recv()
		if !ok {
			log.Fatalf("intercept channel closed on request %+v", args)
		}
		reply := recvVal.Interface().(proto.Response)

		tdb.Lock()
		defer tdb.Unlock()

		// Update max timestamp using the response header's timestamp.
		if tdb.timestamp.Less(reply.Header().Timestamp) {
			tdb.timestamp = reply.Header().Timestamp
		}
		if log.V(1) && reply.Header().GoError() != nil {
			log.Infof("command %s %+v failed: %s", method, args, reply.Header().GoError())
		}
		// Take action on various errors.
		switch t := reply.Header().GoError().(type) {
		case *proto.TransactionRetryError:
			// Increase timestamp if applicable.
			if tdb.timestamp.Less(t.Txn.Timestamp) {
				tdb.timestamp = t.Txn.Timestamp
				if !bytes.Equal(tdb.txn.ID, t.Txn.ID) {
					tdb.timestamp.Logical++ // ensure this txn's timestamp > other txn
				}
			}
			tdb.txn.Restart(false /* !Abort */, tdb.UserPriority, t.Txn.Priority-1, tdb.timestamp)
		case *proto.TransactionAbortedError:
			// Increase timestamp if applicable.
			if tdb.timestamp.Less(t.Txn.Timestamp) {
				tdb.timestamp = t.Txn.Timestamp
			}
			tdb.txn.Restart(true /* Abort */, tdb.UserPriority, t.Txn.Priority, tdb.timestamp)
		case *proto.WriteTooOldError:
			// If write is too old, update the timestamp and restart the
			// transaction. For write too old errors, we use the timestamp
			// in the error.
			if tdb.timestamp.Less(t.ExistingTimestamp) {
				tdb.timestamp = t.ExistingTimestamp
				tdb.timestamp.Logical++
			}
			tdb.txn.Restart(true /* Abort */, tdb.UserPriority, tdb.txn.Priority, tdb.timestamp)
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
			args.Header().Timestamp = tdb.timestamp
			// Backoff on unresolvable intent and retry command.
			// Make sure to upgrade our priority to the conflicting txn's - 1.
			return util.RetryContinue, nil
		case nil:
			if method == storage.EndTransaction || method == storage.InternalEndTxn {
				tdb.txnEnd = true // set this txn as having been ended
			}
		}
		proto.SendReply(replyChan, reply)
		return util.RetryBreak, nil
	})

	if _, ok := err.(*util.RetryMaxAttemptsError); ok {
		tdb.txn.Restart(false /* !Abort */, tdb.UserPriority, tdb.txn.Priority, tdb.timestamp)
		sendError(proto.NewTransactionRetryError(tdb.txn, false /* !Backoff */), replyChan)
	}

	tdb.wg.Done()
}

func sendError(err error, replyChan interface{}) {
	reply := proto.NewReply(replyChan)
	reply.Header().SetGoError(err)
	proto.SendReply(replyChan, reply)
}
