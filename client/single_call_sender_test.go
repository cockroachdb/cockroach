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
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
)

func TestSingleCallSenderSend(t *testing.T) {
	ts := newSingleCallSender(newTestSender(nil), nil)
	reply := &proto.PutResponse{}
	ts.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
	if reply.GoError() != nil {
		t.Errorf("unexpected error: %s", reply.GoError())
	}
}

// TestSingleCallSenderZeroTimestap verifies the timestamp is always
// set to zero for read/write (mutating) calls.
func TestSingleCallSenderZeroTimestamp(t *testing.T) {
	scs := newSingleCallSender(newTestSender(func(call *Call) {
		if proto.IsReadWrite(call.Method) {
			if !call.Args.Header().Timestamp.Equal(proto.MinTimestamp) {
				t.Errorf("%s: expected zero timestamp for method", call.Method)
			}
		} else {
			if call.Args.Header().Timestamp.Equal(proto.MinTimestamp) {
				t.Errorf("%s: expected non-zero timestamp for method", call.Method)
			}
		}
	}), nil)
	for method := range proto.AllMethods {
		args, reply, err := proto.CreateArgsAndReply(method)
		if err != nil {
			t.Errorf("%s: unexpected error creating args and reply: %s", method, err)
		}
		args.Header().Timestamp = makeTS(10, 10)
		scs.Send(&Call{Method: method, Args: args, Reply: reply})
	}
}

// TestSingleCallSenderErrorsw verifies retry on some errors and
// immediately propagation on others.
func TestSingleCallSenderTransactionPushError(t *testing.T) {
	TxnRetryOptions.Backoff = 1 * time.Millisecond

	testCases := []struct {
		err   error
		retry bool // If false, the error will immediately propagate
	}{
		{&proto.TransactionPushError{}, true},
		{&proto.WriteTooOldError{}, true},
		{&proto.WriteIntentError{}, true},
		{&proto.GenericError{}, false},
		{&proto.RangeNotFoundError{}, false},
		{&proto.RangeKeyMismatchError{}, false},
		{&proto.ReadWithinUncertaintyIntervalError{}, false},
		{&proto.TransactionAbortedError{}, false},
		{&proto.TransactionRetryError{}, false},
		{&proto.TransactionStatusError{}, false},
	}

	for i, test := range testCases {
		count := 0
		scs := newSingleCallSender(newTestSender(func(call *Call) {
			count++
			if count == 1 {
				call.Reply.Header().SetGoError(test.err)
			}
		}), nil)
		reply := &proto.PutResponse{}
		scs.Send(&Call{Method: proto.Put, Args: testPutReq, Reply: reply})
		if test.retry {
			if count != 2 {
				t.Errorf("%d: expected one retry; got %d", i, count)
			}
			if reply.GoError() != nil {
				t.Errorf("%d: expected success on retry; got %S", i, reply.GoError())
			}
		} else {
			if count != 1 {
				t.Errorf("%d: expected no retries; got %d", i, count)
			}
			if reflect.TypeOf(reply.GoError()) != reflect.TypeOf(test.err) {
				t.Errorf("%d: expected error of type %T; got %T", i, test.err, reply.GoError())
			}
		}
	}
}
