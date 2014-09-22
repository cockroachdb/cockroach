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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/kv"
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

// A txnDB embeds a kv.DB struct.
type txnDB struct {
	*DB
	tkv *txnKV
}

// newTxnDB creates a new txnDB using a txnKV transactional key-value
// implementation.
func newTxnDB(db *DB, user string, userPriority int32, isolation proto.IsolationType) *txnDB {
	txnKV := &txnKV{
		kv:           db.kv,
		user:         user,
		userPriority: userPriority,
		isolation:    isolation,
	}
	return &txnDB{
		DB:  NewDB(txnKV, db.clock),
		tkv: txnKV,
	}
}

// Abort invokes txnKV.Abort().
func (tdb *txnDB) Abort() error {
	return tdb.tkv.endTransaction(tdb.DB, false)
}

// Commit invokes txnKV.Commit().
func (tdb *txnDB) Commit() error {
	return tdb.tkv.endTransaction(tdb.DB, true)
}

// A txnKV proxies requests to the underlying KV, automatically
// beginning a transaction and then propagating timestamps and txn
// changes to all commands. On receipt of TransactionRetryError,
// the transaction epoch is incremented and error passed to caller.
// On receipt of TransactionAbortedError, the transaction is re-
// created and error passed to caller.
type txnKV struct {
	kv           kv.KV
	clock        *hlc.Clock
	user         string
	userPriority int32
	isolation    proto.IsolationType

	mu        sync.Mutex // Protects timestamp & txn...
	wg        sync.WaitGroup
	timestamp proto.Timestamp
	txn       *proto.Transaction
	done      bool
}

// endTransaction executes an EndTransaction command to either commit
// or abort the transactional session. All outstanding commands are
// first awaited to ensure the EndTransaction is correctly invoked
// with the most recent commit timestamp.
func (tkv *txnKV) endTransaction(db *DB, commit bool) error {
	//  First, disallow any further commands.
	tkv.mu.Lock()
	tkv.done = true
	tkv.mu.Unlock()

	// Wait for all outstanding commands to complete. This gives
	// us an accurate final timestamp.
	tkv.wg.Wait()

	tkv.mu.Lock()
	defer tkv.mu.Unlock()

	reply <- db.EndTransaction(&proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:       tkv.txn.ID,
			User:      tkv.user,
			Timestamp: tkv.timestamp,
			Txn:       tkv.txn,
		},
		Commit: commit,
	})
	return reply.Header().GoError()
}

// ExecuteCmd proxies requests to tkv.db, taking care to:
//
// - Begin transaction with first key
// - Propagate response timestamps to subsequent requests
// - Set client command IDs on read-write commands
// - Retry commands on WriteIntentErrors
// - Increment epoch on TransactionRetryError
// - Re-create transaction on TransactionAbortedError
func (tkv *txnKV) ExecuteCmd(method string, args proto.Request, replyChan interface{}) {
	tkv.mu.Lock()
	if tkv.done {
		tkv.sendError(replyChan, util.Errorf("transactional session no longer active"))
		return
	}
	if !isTransactional(method) {
		tkv.sendError(replyChan, util.Errorf("method %q cannot be invoked through transactional DB", method))
		return
	}
	// If the transaction hasn't yet been created, create now, using
	// this command's key as the base key.
	if tkv.txn == nil {
		tkv.txn = storage.NewTransaction(args.Header().Key, tkv.userPriority, tkv.isolation, tkv.clock)
		tkv.timestamp = tkv.txn.Timestamp
	}
	// Set args.Timestamp & args.Txn to reflect current values.
	args.Header().User = tkv.user
	args.Header().Timestamp = tkv.timestamp
	args.Header().Txn = tkv.txn
	tkv.wg.Add(1)
	tkv.mu.Unlock()

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
			// mutations from being run multiple times on RPC retries.
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

			tkv.mu.Lock()
			defer tkv.mu.Unlock()

			switch t := reply.Header().GoError().(type) {
			case *proto.TransactionRetryError:
				// On retry, increment epoch & set timestamp to max of
				// txn record and current header timestamp.
				tkv.txn = t.Txn
				tkv.txn.Epoch++
				if tkv.txn.timestamp.Less(args.Header().Timestamp) {
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
				// Make sure to upgrade our priority to the conflicting txn's - 1.
				tkv.txn.UpgradePriority(t.Txn.Priority - 1)
				return false, nil
			}
			// If command succeeded or received any other error, update
			// our timestamp and send the reply on the channel.
			if tkv.timestamp.Less(reply.Header().Timestamp) {
				tkv.timestamp = reply.Header().Timestamp
			}
			reflect.ValueOf(replyChan).Send(reflect.ValueOf(reply))
			return true, nil
		}); err == nil {
			tkv.wg.Done()
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
