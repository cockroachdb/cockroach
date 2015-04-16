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
	Send(*Call)
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

	sender KVSender
	clock  Clock
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
		sender:          sender,
		User:            ctx.User,
		UserPriority:    ctx.UserPriority,
		TxnRetryOptions: ctx.TxnRetryOptions,
		clock:           ctx.Clock,
	}
}

// Sender returns the sender supplied to NewKV, unless wrapped by a
// transactional sender, in which case returns the unwrapped sender.
func (kv *KV) Sender() KVSender {
	switch t := kv.sender.(type) {
	case *txnSender:
		return t.wrapped
	default:
		return t
	}
}

// Run runs the specified calls synchronously in a single batch and
// returns any errors.
func (kv *KV) Run(calls ...*Call) (err error) {
	if len(calls) == 0 {
		return nil
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
		kv.sender.Send(c)
		err = c.Reply.Header().GoError()
		if err != nil {
			log.Infof("failed %s: %s", c.Method, err)
		}
		return
	}

	replies := make([]proto.Response, 0, len(calls))
	bArgs, bReply := &proto.BatchRequest{}, &proto.BatchResponse{}
	for _, call := range calls {
		bArgs.Add(call.Args)
		replies = append(replies, call.Reply)
	}
	err = kv.Run(&Call{bArgs, bReply})

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
//
// Calling RunTransaction on the transactional KV client which is
// supplied to the retryable function is an error.
func (kv *KV) RunTransaction(opts *TransactionOptions, retryable func(txn *Txn) error) error {
	if _, ok := kv.sender.(*txnSender); ok {
		return util.Errorf("cannot invoke RunTransaction on an already-transactional client")
	}

	// Create a new KV for the transaction using a transactional KV sender.
	if opts == nil {
		opts = &defaultTxnOpts
	}
	txnSender := newTxnSender(kv.Sender(), opts)
	txn := &Txn{}
	txn.kv = *kv
	txn.kv.sender = txnSender
	if opts != &defaultTxnOpts {
		txn.kv.UserPriority = opts.UserPriority
	}

	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := kv.TxnRetryOptions
	retryOpts.Tag = opts.Name
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		txnSender.txnEnd = false // always reset before [re]starting txn
		err := retryable(txn)
		if err == nil && !txnSender.txnEnd {
			// If there were no errors running retryable, commit the txn. This
			// may block waiting for outstanding writes to complete in case
			// retryable didn't -- we need the most recent of all response
			// timestamps in order to commit.
			etArgs := &proto.EndTransactionRequest{Commit: true}
			etReply := &proto.EndTransactionResponse{}
			// Prepare and flush for end txn in order to execute entire txn in
			// a single round trip if possible.
			txn.Prepare(&Call{etArgs, etReply})
			err = txn.Flush()
		}
		if restartErr, ok := err.(proto.TransactionRestartError); ok {
			if restartErr.CanRestartTransaction() == proto.TransactionRestart_IMMEDIATE {
				return util.RetryReset, nil
			} else if restartErr.CanRestartTransaction() == proto.TransactionRestart_BACKOFF {
				return util.RetryContinue, nil
			}
			// By default, fall through and return RetryBreak.
		}
		return util.RetryBreak, err
	})
	if err != nil && !txnSender.txnEnd {
		etArgs := &proto.EndTransactionRequest{Commit: false}
		etReply := &proto.EndTransactionResponse{}
		txn.Run(&Call{etArgs, etReply})
		if etReply.Header().GoError() != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", etReply.Header().GoError(), err)
		}
		return err
	}
	return nil
}

// Get fetches the value at the specified key. Returns true on success
// or false if the key was not found. The timestamp of the write is
// returned as the second return value. The first result parameter is
// "ok": true if a value was found for the requested key; false
// otherwise. An error is returned on error fetching from underlying
// storage or deserializing value.
func (kv *KV) Get(key proto.Key) (bool, []byte, proto.Timestamp, error) {
	value, err := kv.getInternal(key)
	if err != nil || value == nil {
		return false, nil, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, nil, proto.Timestamp{}, util.Errorf("unexpected integer value at key %s: %+v", key, value)
	}
	return true, value.Bytes, *value.Timestamp, nil
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. See comments for GetI for details on
// return values.
func (kv *KV) GetProto(key proto.Key, msg gogoproto.Message) (bool, proto.Timestamp, error) {
	value, err := kv.getInternal(key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gogoproto.Unmarshal(value.Bytes, msg); err != nil {
		return true, *value.Timestamp, err
	}
	return true, *value.Timestamp, nil
}

// getInternal fetches the requested key and returns the value.
func (kv *KV) getInternal(key proto.Key) (*proto.Value, error) {
	reply := &proto.GetResponse{}
	if err := kv.Run(GetCall(key, reply)); err != nil {
		return nil, err
	}
	if reply.Value != nil {
		return reply.Value, reply.Value.Verify(key)
	}
	return nil, nil
}

// Put writes the specified byte slice value to key.
func (kv *KV) Put(key proto.Key, value []byte) error {
	return kv.Run(PutCall(key, value, nil))
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg.
func (kv *KV) PutProto(key proto.Key, msg gogoproto.Message) error {
	call, err := PutProtoCall(key, msg, nil)
	if err != nil {
		return err
	}
	return kv.Run(call)
}

// putInternal writes the specified value to key.
func (kv *KV) putInternal(key proto.Key, value proto.Value) error {
	value.InitChecksum(key)
	req := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key},
		Value:         value,
	}
	return kv.Run(&Call{req, &proto.PutResponse{}})
}
