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
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// Default constants for timeouts.
const (
	retryBackoff    = 150 * time.Second
	maxRetryBackoff = 5 * time.Second
)

// A txnKV proxies requests to the underlying KV, automatically
// beginning a transaction and then propagating timestamps and txn
// changes to all commands. On receipt of TransactionRetryError,
// the transaction epoch is incremented and error passed to caller.
// On receipt of TransactionAbortedError, the transaction is re-
// created and error passed to caller.
type txnKV struct {
	kv           kv.KV
	clock        *hlc.Clock
	userPriority int32
	isolation    proto.IsolationType
	timestamp    proto.Timestamp
	txn          *proto.Transaction
}

func newTxnDB(db *DB, userPriority int32, isolation proto.IsolationType) *DB {
	txnKV := &txnKV{
		kv:           db.kv,
		userPriority: userPriority,
		isolation:    isolation,
	}
	return NewDB(txnKV, db.clock)
}

// ExecuteCmd proxies requests to tkv.db, taking care to:
//
// - Begin transaction with first key
// - Propagate response timestamps to subsequent requests
// - Set client command IDs on read-write commands
// - Retry commands on WriteIntentErrors
// - Increment epoch on TransactionRetryError
// - Re-create transaction on TransactionAbortedError
//
// TODO(spencer): still need to make this thread-safe and create
// a wait group for eventual commit to ensure all outstanding cmds
// have completed.
func (tkv *txnKV) ExecuteCmd(method string, args proto.Request, replyChan interface{}) {
	if !isTransactional(method) {
		tkv.sendError(replyChan, util.Errorf("method %q cannot be invoked through transactional DB", method))
	}
	// If the transaction hasn't yet been created, create now, using
	// this command's key as the base key.
	if tkv.txn == nil {
		tkv.txn = storage.NewTransaction(args.Header().Key, tkv.userPriority, tkv.isolation, tkv.clock)
		tkv.timestamp = tkv.txn.Timestamp
	}
	// Set args.Timestamp & args.Txn to reflect current values.
	args.Header().Timestamp = tkv.timestamp
	args.Header().Txn = tkv.txn

	// Proxy command through to the wrapped KV.
	const retryOpts = util.RetryOptions{
		Tag:         fmt.Sprintf("retrying cmd %s on write intent error", method),
		Backoff:     retryBackoff,
		MaxBackoff:  maxRetryBackoff,
		Constant:    2,
		MaxAttempts: 0, // retry indefinitely
	}
	for {
		// Backoff and retry on write intent errors.
		if err := util.RetryWithBackoff(retryOpts, func() (bool, error) {
			// On mutating commands, set a client command ID. This prevents
			// mutations from being incorrectly applied multiple times on
			// RPC retries.
			if !storage.IsReadOnly(method) {
				args.Header().ClientCmdID = proto.ClientCmdID{
					WallTime: tkv.clock.Now().WallTime,
					Random:   util.CachedRand.Int32(),
				}
			}

			// Create an intercept channel so we can examine the reply before passing it on to client.
			interceptChan := reflect.MakeChan(reflect.TypeOf(replyChan).Elem().Elem(), 1)
			tkv.kv.ExecuteCmd(method, args, interceptChan.Interface())
			reply := interceptChan.Recv().Interface().(proto.Response)

			switch t := reply.Header().GoError().(type) {
			case *proto.TransactionRetryError:
				// On retry, increment epoch & set timestamp to max of
				// txn record and current header timestamp.
				tkv.txn = t.Txn
				tkv.txn.Epoch++
				if args.Header().Timestamp > tkv.txn.Timestamp {
					tkv.txn.Timestamp = args.Header().Timestamp
				}
				tkv.timestamp = tkv.txn.Timestamp
			case *proto.TransactionAbortedError:
				// On aborted, create a new transaction.
				tkv.txn = storage.NewTransaction(args.Header().Key, tkv.userPriority, tkv.isolation, tkv.clock)
				tkv.txn.UpgradePriority(t.Txn.Priority)
				tkv.timestamp = tkv.txn.Timestamp
			case *proto.WriteIntentError:
				// If write intent error is resolved, exit retry/backoff loop to
				// immediately retry.
				if wiErr.Resolved {
					return true, wiErr
				}
				// Otherwise, backoff on unresolvable intent and retry command.
				// Make sure to update our priority to the conflicting txn's - 1.
				tkv.txn.UpgradePriority(t.Txn.Priority - 1)
				return false, nil
			}
			// If command succeeded or received any other error, update
			// our timestamp and send the reply on the channel.
			tkv.timestamp = reply.Header().Timestamp
			reflect.ValueOf(replyChan).Send(reflect.ValueOf(reply))
			return true, nil
		}); err == nil {
			return
		}
	}
}

// Close proxies through to wrapped KV.
func (tkv *txnKV) Close() {
	tkv.kv.Close()
}

func (tkv *txnKV) sendError(replyChan interface{}, err error) {
	reply := reflect.New(reflect.TypeOf(replyChan).Elem().Elem()).Interface().(proto.Response)
	reply.Header().SetGoError(err)
	reflect.ValueOf(replyChan).Send(reflect.ValueOf(reply))
}
