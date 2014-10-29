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
)

// A singleCallSender wraps a sender and provides retry logic for
// conflicts in a non-transactional context.
type singleCallSender struct {
	wrapped KVSender
	clock   Clock
}

// newSingleCallSender returns a new instance of singleCallSender
// wrapping the supplied sender. The clock is used to create client
// command IDs.
func newSingleCallSender(wrapped KVSender, clock Clock) *singleCallSender {
	return &singleCallSender{
		wrapped: wrapped,
		clock:   clock,
	}
}

// Send implements the KVSender interface. It executes the supplied
// call with necessary retry logic to account for concurrency errors
// such as resolved/unresolved write intents and transaction push errors.
func (s *singleCallSender) Send(call *Call) {
	// Zero timestamp on any read-write call.
	if proto.IsReadWrite(call.Method) {
		call.Args.Header().Timestamp = proto.Timestamp{}
	}
	var retryOpts util.RetryOptions = TxnRetryOptions
	retryOpts.Tag = call.Method
	if err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		// Reset client command ID (if applicable) on every retry at this
		// level--retries due to network timeouts or disconnects are
		// handled at lower levels by the KVSender implementation(s).
		call.resetClientCmdID(s.clock)

		// Send the call.
		s.wrapped.Send(call)

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

// Close implements the KVSender interface. It invokes Close on the
// wrapped sender.
func (s *singleCallSender) Close() {
	s.wrapped.Close()
}
