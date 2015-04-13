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
	"bytes"
	"encoding/gob"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// TransactionOptions are parameters for use with KV.RunTransaction.
type TransactionOptions struct {
	Name      string // Concise desc of txn for debugging
	Isolation proto.IsolationType
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

// KV provides serial access to a KV store via Call and parallel
// access via Prepare and Flush. A KV instance is not thread safe.
type KV struct {
	// User is the default user to set on API calls. If User is set to
	// non-empty in call arguments, this value is ignored.
	User string
	// UserPriority is the default user priority to set on API calls. If
	// UserPriority is set non-zero in call arguments, this value is
	// ignored.
	UserPriority    int32
	TxnRetryOptions util.RetryOptions

	sender   KVSender
	clock    Clock
	prepared []*Call
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

// Context returns a new Context that represents the current configuration.
func (kv *KV) Context() *Context {
	return &Context{
		User:            kv.User,
		UserPriority:    kv.UserPriority,
		TxnRetryOptions: kv.TxnRetryOptions,
		Clock:           kv.clock,
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

// Call invokes the KV command synchronously and returns the response
// and error, if applicable. If preceeding calls have been made to
// Prepare() without a call to Flush(), this call is prepared and
// then all prepared calls are flushed.
func (kv *KV) Call(method string, args proto.Request, reply proto.Response) error {
	if len(kv.prepared) > 0 {
		kv.Prepare(method, args, reply)
		return kv.Flush()
	}
	if args.Header().User == "" {
		args.Header().User = kv.User
	}
	if args.Header().UserPriority == nil && kv.UserPriority != 0 {
		args.Header().UserPriority = gogoproto.Int32(kv.UserPriority)
	}
	call := &Call{
		Method: method,
		Args:   args,
		Reply:  reply,
	}
	call.resetClientCmdID(kv.clock)
	kv.sender.Send(call)
	err := call.Reply.Header().GoError()
	if err != nil {
		log.Infof("failed %s: %s", call.Method, err)
	}
	return err
}

// Prepare accepts a KV API call, specified by method name, arguments
// and a reply struct. The call will be buffered locally until the
// first call to Flush(), at which time it will be sent for execution
// as part of a batch call. Using Prepare/Flush parallelizes queries
// and updates and should be used where possible for efficiency.
//
// For clients using an HTTP sender, Prepare/Flush allows multiple
// commands to be sent over the same connection. For transactional
// clients, Prepare/Flush can dramatically improve efficiency by
// compressing multiple writes into a single atomic update in the
// event that the writes are to keys within a single range. However,
// using Prepare/Flush alone will not guarantee atomicity. Clients
// must use a transaction for that purpose.
//
// The supplied reply struct will not be valid until after a call
// to Flush().
func (kv *KV) Prepare(method string, args proto.Request, reply proto.Response) {
	call := &Call{
		Method: method,
		Args:   args,
		Reply:  reply,
	}
	call.resetClientCmdID(kv.clock)
	kv.prepared = append(kv.prepared, call)
}

// Flush sends all previously prepared calls, buffered by invocations
// of Prepare(). The calls are organized into a single batch command
// and sent together. Flush returns nil if all prepared calls are
// executed successfully. Otherwise, Flush returns the first error,
// where calls are executed in the order in which they were prepared.
// After Flush returns, all prepared reply structs will be valid.
func (kv *KV) Flush() (err error) {
	if len(kv.prepared) == 0 {
		return
	} else if len(kv.prepared) == 1 {
		call := kv.prepared[0]
		kv.prepared = []*Call{}
		err = kv.Call(call.Method, call.Args, call.Reply)
		return
	}
	replies := make([]proto.Response, 0, len(kv.prepared))
	bArgs, bReply := &proto.BatchRequest{}, &proto.BatchResponse{}
	for _, call := range kv.prepared {
		bArgs.Add(call.Args)
		replies = append(replies, call.Reply)
	}
	kv.prepared = []*Call{}
	err = kv.Call(proto.Batch, bArgs, bReply)

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
func (kv *KV) RunTransaction(opts *TransactionOptions, retryable func(txn *KV) error) error {
	if _, ok := kv.sender.(*txnSender); ok {
		return util.Errorf("cannot invoke RunTransaction on an already-transactional client")
	}

	// Create a new KV for the transaction using a transactional KV sender.
	txnSender := newTxnSender(kv.Sender(), opts)
	curCtx := kv.Context()
	txnKV := NewKV(curCtx, txnSender)

	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := kv.TxnRetryOptions
	retryOpts.Tag = opts.Name
	if err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		txnSender.txnEnd = false // always reset before [re]starting txn
		err := retryable(txnKV)
		if err == nil && !txnSender.txnEnd {
			// If there were no errors running retryable, commit the txn. This
			// may block waiting for outstanding writes to complete in case
			// retryable didn't -- we need the most recent of all response
			// timestamps in order to commit.
			etArgs := &proto.EndTransactionRequest{Commit: true}
			etReply := &proto.EndTransactionResponse{}
			// Prepare and flush for end txn in order to execute entire txn in
			// a single round trip if possible.
			txnKV.Prepare(proto.EndTransaction, etArgs, etReply)
			err = txnKV.Flush()
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
	}); err != nil && !txnSender.txnEnd {
		etArgs := &proto.EndTransactionRequest{Commit: false}
		etReply := &proto.EndTransactionResponse{}
		txnKV.Call(proto.EndTransaction, etArgs, etReply)
		if etReply.Header().GoError() != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", etReply.Header().GoError(), err)
		}
		return err
	}
	return nil
}

// GetI fetches the value at the specified key and gob-deserializes it
// into "value". Returns true on success or false if the key was not
// found. The timestamp of the write is returned as the second return
// value. The first result parameter is "ok": true if a value was
// found for the requested key; false otherwise. An error is returned
// on error fetching from underlying storage or deserializing value.
func (kv *KV) GetI(key proto.Key, iface interface{}) (bool, proto.Timestamp, error) {
	value, err := kv.getInternal(key)
	if err != nil || value == nil {
		return false, proto.Timestamp{}, err
	}
	if value.Integer != nil {
		return false, proto.Timestamp{}, util.Errorf("unexpected integer value at key %q: %+v", key, value)
	}
	if err := gob.NewDecoder(bytes.NewBuffer(value.Bytes)).Decode(iface); err != nil {
		return true, *value.Timestamp, err
	}
	return true, *value.Timestamp, nil
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
	if err := kv.Call(proto.Get, &proto.GetRequest{
		RequestHeader: proto.RequestHeader{Key: key},
	}, reply); err != nil {
		return nil, err
	}
	if reply.Value != nil {
		return reply.Value, reply.Value.Verify(key)
	}
	return nil, nil
}

// PutI sets the given key to the gob-serialized byte string of value.
func (kv *KV) PutI(key proto.Key, iface interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(iface); err != nil {
		return err
	}
	return kv.putInternal(key, proto.Value{Bytes: buf.Bytes()})
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg.
func (kv *KV) PutProto(key proto.Key, msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	return kv.putInternal(key, proto.Value{Bytes: data})
}

// putInternal writes the specified value to key.
func (kv *KV) putInternal(key proto.Key, value proto.Value) error {
	value.InitChecksum(key)
	return kv.Call(proto.Put, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key},
		Value:         value,
	}, &proto.PutResponse{})
}

// PreparePutProto sets the given key to the protobuf-serialized byte
// string of msg. The resulting Put call is buffered and will not be
// sent until a subsequent call to Flush. Returns marshalling errors
// if encountered.
func (kv *KV) PreparePutProto(key proto.Key, msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	value := proto.Value{Bytes: data}
	value.InitChecksum(key)
	kv.Prepare(proto.Put, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key},
		Value:         value,
	}, &proto.PutResponse{})
	return nil
}
