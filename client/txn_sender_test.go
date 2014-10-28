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

import "testing"

// TestTxnSenderBeginTxn verifies a BeginTransaction is issued as soon
// as a request is received using the request key as base key.
func TestTxnSenderBeginTxn(t *testing.T) {
}

// TestTxnSenderEndTxn verifies that request key is set to the txn ID
// on a call to EndTransaction.
func TestTxnSenderEndTxn(t *testing.T) {
	// verify endTxn is true.
}

// TestTxnSenderNonTransactional verifies that non-transactional requests
// are passed directly through to the wrapped sender.
func TestTxnSenderNonTransactional(t *testing.T) {
}

// TestTxnSenderRequestHeader verifies request header (including
// generated client command ID) is set.
func TestTxnSenderRequestHeader(t *testing.T) {
}

// TestTxnSenderReadWithinUncertaintyIntervalError .
func TestTxnSenderReadWithinUncertaintyIntervalError(t *testing.T) {
}

// TestTxnSenderTransactionAbortedError .
func TestTxnSenderTransactionAbortedError(t *testing.T) {
}

// TestTxnSenderTransactionPushError .
func TestTxnSenderTransactionPushError(t *testing.T) {
}

// TestTxnSenderTransactionRetryError .
func TestTxnSenderTransactionRetryError(t *testing.T) {
}

// TestTxnSenderWriteTooOldError .
func TestTxnSenderWriteTooOldError(t *testing.T) {
}

// TestTxnSenderWriteIntentError verifies that the send is retried
// on write intent errors.
func TestTxnSenderWriteIntentError(t *testing.T) {
}
