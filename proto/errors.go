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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/util/retry"
)

// ResponseWithError is a tuple of a BatchResponse and an error. It is used to
// pass around a BatchResponse with its associated error where that
// entanglement is necessary (e.g. channels, methods that need to return
// another error in addition to this one).
type ResponseWithError struct {
	Reply *BatchResponse
	Err   error
}

// ErrorUnexpectedlySet is the string we panic on when we assert
// `ResponseHeader.Error == nil` before calling
// `ResponseHeader.SetGoError()`.
const ErrorUnexpectedlySet = "error is unexpectedly set"

// TransactionRestartError is an interface implemented by errors that cause
// a transaction to be restarted.
type TransactionRestartError interface {
	CanRestartTransaction() TransactionRestart
	// Optionally, a transaction that should be used
	// for an update before retrying.
	Transaction() *Transaction
}

// Error implements the Go error interface.
func (e *Error) Error() string {
	return e.Message
}

// CanRetry implements the retry.Retryable interface.
func (e *Error) CanRetry() bool {
	return e.Retryable
}

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *Error) CanRestartTransaction() TransactionRestart {
	return e.TransactionRestart
}

// Transaction implements the TransactionRestartError interface.
func (e *Error) Transaction() *Transaction {
	detail := e.GetDetail()
	if detail != nil {
		if txnErr, ok := detail.GetValue().(TransactionRestartError); ok {
			return txnErr.Transaction()
		}
	}
	return nil
}

// SetResponseGoError sets Error using err.
func (e *Error) SetResponseGoError(err error) {
	e.Message = err.Error()
	if r, ok := err.(retry.Retryable); ok {
		e.Retryable = r.CanRetry()
	}
	if r, ok := err.(TransactionRestartError); ok {
		e.TransactionRestart = r.CanRestartTransaction()
	}
	// If the specific error type exists in the detail union, set it.
	detail := &ErrorDetail{}
	if detail.SetValue(err) {
		e.Detail = detail
	}
}

// Error formats error.
func (e *NodeUnavailableError) Error() string {
	return "node unavailable; try another peer"
}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("range %d: replica %s not leader; leader is %s", e.RangeID, e.Replica, e.Leader)
}

// Error formats error.
func (e *LeaseRejectedError) Error() string {
	return fmt.Sprintf("cannot replace lease %s with %s", e.Existing, e.Requested)
}

// CanRetry indicates that this error can not be retried; it should never
// make it back to the client anyways.
func (e *LeaseRejectedError) CanRetry() bool {
	return false
}

// NewRangeNotFoundError initializes a new RangeNotFoundError.
func NewRangeNotFoundError(rangeID RangeID) *RangeNotFoundError {
	return &RangeNotFoundError{
		RangeID: rangeID,
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
func NewRangeKeyMismatchError(start, end Key, desc *RangeDescriptor) *RangeKeyMismatchError {
	return &RangeKeyMismatchError{
		RequestStartKey: start,
		RequestEndKey:   end,
		Range:           desc,
	}
}

// Error formats error.
func (e *RangeKeyMismatchError) Error() string {
	if e.Range != nil {
		return fmt.Sprintf("key range %s-%s outside of bounds of range %s-%s",
			e.RequestStartKey, e.RequestEndKey, e.Range.StartKey, e.Range.EndKey)
	}
	return fmt.Sprintf("key range %s-%s could not be located within a range on store", e.RequestStartKey, e.RequestEndKey)
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
	return fmt.Sprintf("txn aborted %s", e.Txn)
}

var _ TransactionRestartError = &TransactionAbortedError{}

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *TransactionAbortedError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
}

// Transaction implements TransactionRestartError. It returns nil.
func (*TransactionAbortedError) Transaction() *Transaction {
	return nil
}

// NewTransactionPushError initializes a new TransactionPushError.
// Txn is the transaction which will be retried.
func NewTransactionPushError(txn, pusheeTxn *Transaction) *TransactionPushError {
	return &TransactionPushError{Txn: txn, PusheeTxn: *pusheeTxn}
}

// Error formats error.
func (e *TransactionPushError) Error() string {
	if e.Txn == nil {
		return fmt.Sprintf("failed to push %s", e.PusheeTxn)
	}
	return fmt.Sprintf("txn %s failed to push %s", e.Txn, e.PusheeTxn)
}

var _ TransactionRestartError = &TransactionPushError{}

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *TransactionPushError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
}

// Transaction implements the TransactionRestartError interface.
func (*TransactionPushError) Transaction() *Transaction {
	return nil // pusher's txn doesn't change on a Push.
}

// NewTransactionRetryError initializes a new TransactionRetryError.
// Txn is the transaction which will be retried.
func NewTransactionRetryError(txn *Transaction) *TransactionRetryError {
	return &TransactionRetryError{Txn: *txn}
}

// Error formats error.
func (e *TransactionRetryError) Error() string {
	return fmt.Sprintf("retry txn %s", e.Txn)
}

var _ TransactionRestartError = &TransactionRetryError{}

// CanRestartTransaction implements the TransactionRestartError interface.
func (*TransactionRetryError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Transaction implements the TransactionRestartError interface.
func (e *TransactionRetryError) Transaction() *Transaction {
	return &e.Txn
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
	var keys []Key
	for _, intent := range e.Intents {
		keys = append(keys, intent.Key)
	}
	return fmt.Sprintf("conflicting intents on %v: resolved? %t", keys, e.Resolved)
}

// Error formats error.
func (e *WriteTooOldError) Error() string {
	return fmt.Sprintf("write too old: timestamp %s <= %s", e.Timestamp, e.ExistingTimestamp)
}

// Error formats error.
func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return fmt.Sprintf("read at time %s encountered previous write with future timestamp %s within uncertainty interval", e.Timestamp, e.ExistingTimestamp)
}

var _ TransactionRestartError = &ReadWithinUncertaintyIntervalError{}

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *ReadWithinUncertaintyIntervalError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Transaction implements the TransactionRestartError interface.
func (e *ReadWithinUncertaintyIntervalError) Transaction() *Transaction {
	return &e.Txn
}

// Error formats error.
func (e *OpRequiresTxnError) Error() string {
	return "the operation requires transactional context"
}

// Error formats error.
func (e *ConditionFailedError) Error() string {
	return fmt.Sprintf("unexpected value: %s", e.ActualValue)
}
