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

package proto

import "fmt"

// Error implements the Go error interface.
func (ge *GenericError) Error() string {
	return ge.Message
}

// CanRetry implements the util/Retryable interface.
func (ge *GenericError) CanRetry() bool {
	return ge.Retryable
}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("range not leader; leader is %+v", e.Leader)
}

// NewRangeNotFoundError initializes a new RangeNotFoundError.
func NewRangeNotFoundError(rid int64) *RangeNotFoundError {
	return &RangeNotFoundError{
		RangeID: rid,
	}
}

// Error formats error.
func (e *RangeNotFoundError) Error() string {
	return fmt.Sprintf("range %d was not found", e.RangeID)
}

// CanRetry indicates whether or not this RangeNotFoundError can be retried.
func (e *RangeNotFoundError) CanRetry() bool {
	return true
}

// NewRangeKeyMismatchError initializes a new RangeKeyMismatchError.
func NewRangeKeyMismatchError(start, end []byte, desc *RangeDescriptor) *RangeKeyMismatchError {
	return &RangeKeyMismatchError{
		RequestStartKey: start,
		RequestEndKey:   end,
		Range:           desc,
	}
}

// Error formats error.
func (e *RangeKeyMismatchError) Error() string {
	if e.Range != nil {
		return fmt.Sprintf("key range %q-%q outside of bounds of range %q-%q",
			string(e.RequestStartKey), string(e.RequestEndKey),
			string(e.Range.StartKey), string(e.Range.EndKey))
	}
	return fmt.Sprintf("key range %q-%q could not be located within a range on store",
		string(e.RequestStartKey), string(e.RequestEndKey))
}

// CanRetry indicates whether or not this RangeKeyMismatchError can be retried.
func (e *RangeKeyMismatchError) CanRetry() bool {
	return true
}

// NewTransactionAbortedError initializes a new TransactionAbortedError.
func NewTransactionAbortedError(txn *Transaction) *TransactionAbortedError {
	return &TransactionAbortedError{Txn: *txn}
}

// Error formats error.
func (e *TransactionAbortedError) Error() string {
	return fmt.Sprintf("txn %s: aborted", e.Txn)
}

// NewTransactionRetryError initializes a new TransactionRetryError.
func NewTransactionRetryError(txn *Transaction, backoff bool) *TransactionRetryError {
	return &TransactionRetryError{Txn: *txn, Backoff: backoff}
}

// Error formats error.
func (e *TransactionRetryError) Error() string {
	return fmt.Sprintf("retry txn: %s, backoff? %t", e.Txn, e.Backoff)
}

// NewTransactionStatusError initializes a new TransactionStatusError.
func NewTransactionStatusError(txn *Transaction, msg string) *TransactionStatusError {
	return &TransactionStatusError{
		Txn: *txn,
		Msg: msg,
	}
}

// Error formats error.
func (e *TransactionStatusError) Error() string {
	return fmt.Sprintf("txn %s: %s", e.Txn, e.Msg)
}

// Error formats error.
func (e *WriteIntentError) Error() string {
	return fmt.Sprintf("conflicting write intent at key %q from transaction %s: resolved? %t", e.Key, e.Txn, e.Resolved)
}

// Error formats error.
func (e *WriteTooOldError) Error() string {
	return fmt.Sprintf("failed write with timestamp %s < %s", e.Timestamp, e.ExistingTimestamp)
}
