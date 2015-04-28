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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TransactionOptions are parameters for use with KV.RunTransaction.
type TransactionOptions struct {
	Name         string // Concise desc of txn for debugging
	Isolation    proto.IsolationType
	UserPriority int32
}

// KVSender is an interface for sending a request to a Key-Value
// database backend.
type KVSender interface {
	// Send invokes the Call.Method with Call.Args and sets the result
	// in Call.Reply.
	Send(Call)
}

// KVSenderFunc is an adapter to allow the use of ordinary functions
// as KVSenders.
type KVSenderFunc func(Call)

// Send calls f(c).
func (f KVSenderFunc) Send(c Call) {
	f(c)
}

// A Clock is an interface which provides the current time.
type Clock interface {
	// Now returns nanoseconds since the Jan 1, 1970 GMT.
	Now() int64
}

// KV provides access to a KV store. A KV instance is safe for
// concurrent use by multiple goroutines.
type KV struct {
	// User is the default user to set on API calls. If User is set to
	// non-empty in call arguments, this value is ignored.
	User string
	// UserPriority is the default user priority to set on API calls. If
	// UserPriority is set non-zero in call arguments, this value is
	// ignored.
	UserPriority    int32
	TxnRetryOptions util.RetryOptions
	Sender          KVSender
	clock           Clock
}

// NewKV creates a new instance of KV using the specified sender. To
// create a transactional client, the KV struct should be manually
// initialized in order to utilize a txnSender. Clock is used to
// formulate client command IDs, which provide idempotency on API
// calls and defaults to the system clock.
// implementation.
func NewKV(ctx *Context, sender KVSender) *KV {
	if ctx == nil {
		ctx = NewContext()
	}
	return &KV{
		Sender:          sender,
		User:            ctx.User,
		UserPriority:    ctx.UserPriority,
		TxnRetryOptions: ctx.TxnRetryOptions,
		clock:           ctx.Clock,
	}
}

// Run runs the specified calls synchronously in a single batch and
// returns any errors.
func (kv *KV) Run(calls ...Call) (err error) {
	if len(calls) == 0 {
		return nil
	}

	// First check if any call contains an error. This allows the
	// generation of a Call to create an error that is reported
	// here. See PutProto for an example.
	for _, call := range calls {
		if call.Err != nil {
			return call.Err
		}
	}

	if len(calls) == 1 {
		c := calls[0]
		if c.Args.Header().User == "" {
			c.Args.Header().User = kv.User
		}
		if c.Args.Header().UserPriority == nil && kv.UserPriority != 0 {
			c.Args.Header().UserPriority = gogoproto.Int32(kv.UserPriority)
		}
		c.resetClientCmdID(kv.clock)
		kv.Sender.Send(c)
		err = c.Reply.Header().GoError()
		if err != nil {
			log.Infof("failed %s: %s", c.Method(), err)
		}
		return
	}

	replies := make([]proto.Response, 0, len(calls))
	bArgs, bReply := &proto.BatchRequest{}, &proto.BatchResponse{}
	for _, call := range calls {
		bArgs.Add(call.Args)
		replies = append(replies, call.Reply)
	}
	err = kv.Run(Call{Args: bArgs, Reply: bReply})

	// Recover from protobuf merge panics.
	defer func() {
		if r := recover(); r != nil {
			// Take care to log merge error and to return it if no error has
			// already been set.
			mergeErr := util.Errorf("unable to merge response: %s", r)
			log.Error(mergeErr)
			if err == nil {
				err = mergeErr
			}
		}
	}()

	// Transfer individual responses from batch response to prepared replies.
	for i, reply := range bReply.Responses {
		replies[i].Reset()
		gogoproto.Merge(replies[i], reply.GetValue().(gogoproto.Message))
	}
	return
}

// RunTransaction executes retryable in the context of a distributed
// transaction. The transaction is automatically aborted if retryable
// returns any error aside from recoverable internal errors, and is
// automatically committed otherwise. retryable should have no side
// effects which could cause problems in the event it must be run more
// than once. The opts struct contains transaction settings.
func (kv *KV) RunTransaction(opts *TransactionOptions, retryable func(txn *Txn) error) error {
	txn := newTxn(kv, opts)
	return txn.exec(retryable)
}
