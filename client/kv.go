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
	"crypto/tls"
	"encoding/gob"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// TxnRetryOptions sets the retry options for handling write conflicts.
var TxnRetryOptions = util.RetryOptions{
	Backoff:     50 * time.Millisecond,
	MaxBackoff:  5 * time.Second,
	Constant:    2,
	MaxAttempts: 0, // retry indefinitely
}

// TransactionOptions are parameters for use with KV.RunTransaction.
type TransactionOptions struct {
	Name         string // Concise desc of txn for debugging
	User         string
	UserPriority int32
	Isolation    proto.IsolationType
}

// KVSender is an interface for sending a request to a Key-Value
// database backend.
type KVSender interface {
	// Send invokes the Call.Method with Call.Args and sets the result
	// in Call.Reply.
	Send(*Call)
	// Close frees up resources in use by the sender.
	Close()
}

// A Clock is an interface which provides the current time.
type Clock interface {
	// Now returns nanoseconds since the Jan 1, 1970 GMT.
	Now() int64
}

// KV provides access to a KV store via Call() and Prepare() /
// Flush().
type KV struct {
	Clock  Clock    // Used to formulate client command IDs
	Sender KVSender // The KVSender instance
	User   string   // If blank, relies on certificate for user
	inTxn  bool     // Is this KV transactional?
}

// NewKV creates a new instance of KV which connects to the specified
// Cockroach node at address (in host:port format), using the provided
// tls config.
func NewKV(server string, tlsConfig *tls.Config) *KV {
	return &KV{
		Sender: newHTTPSender(server, tlsConfig),
	}
}

// Call invokes the KV command synchronously and returns the response
// and error, if applicable.
func (kv *KV) Call(method string, args proto.Request, reply proto.Response) error {
	call := &Call{
		Method: method,
		Args:   args,
		Reply:  reply,
	}
	if !kv.inTxn {
		kv.runSingleCall(call)
	} else {
		kv.Sender.Send(call)
	}
	return call.Reply.Header().GoError()
}

// TODO(spencer): implement Prepare.

// TODO(spencer): implement Flush.

// runSingleCall executes the supplied call with necessary retry logic
// to account for concurrency errors such as resolved/unresolved write
// intents and transaction retry errors.
func (kv *KV) runSingleCall(call *Call) {
	// Zero timestamp on any read-write call.
	if proto.IsReadWrite(call.Method) {
		call.Args.Header().Timestamp = proto.Timestamp{}
	}
	retryOpts := TxnRetryOptions
	retryOpts.Tag = call.Method
	if err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// Reset client command ID (if applicable) on every retry at this
		// level--retries due to network timeouts or disconnects are
		// handled at lower levels by the KVSender implementation(s).
		call.resetClientCmdID(kv.Clock)

		// Send the call.
		kv.Sender.Send(call)

		if call.Reply.Header().Error != nil {
			log.Infof("failed %s: %s", call.Method, call.Reply.Header().GoError())
		}
		switch t := call.Reply.Header().GoError().(type) {
		case *proto.TransactionPushError:
			// Backoff on failure to push conflicting txn; on a single call,
			// this means we encountered a write intent but were unable to
			// push the transaction.
			return util.RetryContinue, nil
		case *proto.WriteTooOldError:
			// Retry immediately on write-too-old.
			return util.RetryReset, nil
		case *proto.WriteIntentError:
			// Backoff if necessary; otherwise reset for immediate retry (intent was pushed)
			if t.Resolved {
				return util.RetryReset, nil
			}
			return util.RetryContinue, nil
		}
		// For all other cases, break out of retry loop.
		return util.RetryBreak, nil
	}); err != nil {
		call.Reply.Header().SetGoError(err)
	}
}

// RunTransaction executes retryable in the context of a distributed
// transaction. The transaction is automatically aborted if retryable
// returns any error aside from recoverable internal errors, and is
// automatically committed otherwise. retryable should have no side
// effects which could cause problems in the event it must be run more
// than once. The opts struct contains transaction settings.
func (kv *KV) RunTransaction(opts *TransactionOptions, retryable func(kv *KV) error) error {
	// Create a new KV for the transaction using a transactional KV sender.
	txnSender := newTxnSender(kv.Sender, kv.Clock, opts)
	txnKV := &KV{
		Sender: txnSender,
		inTxn:  true,
	}
	defer txnKV.Close()

	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := TxnRetryOptions
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
			txnKV.Call(proto.EndTransaction, etArgs, etReply)
			err = etReply.Header().GoError()
		}
		switch t := err.(type) {
		case *proto.ReadWithinUncertaintyIntervalError:
			// Retry immediately on read within uncertainty interval.
			return util.RetryReset, nil
		case *proto.TransactionAbortedError:
			// If the transaction was aborted, the txnSender will have created
			// a new txn. We allow backoff/retry in this case.
			return util.RetryContinue, nil
		case *proto.TransactionPushError:
			// Backoff and retry on failure to push a conflicting transaction.
			return util.RetryContinue, nil
		case *proto.TransactionRetryError:
			// Return RetryReset for an immediate retry (as in the case of
			// an SSI txn whose timestamp was pushed).
			return util.RetryReset, nil
		default:
			// For all other cases, finish retry loop, returning possible error.
			return util.RetryBreak, t
		}
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
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: kv.User,
		},
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
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: kv.User,
		},
		Value: value,
	}, &proto.PutResponse{})
}

// Close closes the KV client and its sender.
func (kv *KV) Close() {
	kv.Sender.Close()
}
