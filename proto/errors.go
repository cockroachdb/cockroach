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
	"errors"
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

func (e Error) getDetail() error {
	if e.Detail == nil {
		return nil
	}
	return e.Detail.GetValue().(error)
}

// NewError creates an Error from the given error.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	e := &Error{}
	e.SetResponseGoError(err)
	return e
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	return e.GoError().Error()
}

// GoError returns the non-nil error from the proto.Error union.
func (e *Error) GoError() error {
	if e == nil {
		return nil
	}
	if e.Detail == nil {
		return errors.New(e.Message)
	}
	err := e.getDetail()
	if err == nil {
		// Unknown error detail; return the generic error.
		return errors.New(e.Message)
	}
	// Make sure that the flags in the generic portion of the error
	// match the methods of the specific error type.
	if e.Retryable {
		if r, ok := err.(retry.Retryable); !ok || !r.CanRetry() {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to be retryable", err))
		}
	}
	if r, ok := err.(TransactionRestartError); ok {
		if r.CanRestartTransaction() != e.TransactionRestart {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to have restart mode %v",
				err, e.TransactionRestart))
		}
	} else {
		// Error type doesn't implement TransactionRestartError, so expect it to have the default.
		if e.TransactionRestart != TransactionRestart_ABORT {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to have restart mode ABORT", err))
		}
	}
	return err
}

// SetResponseGoError sets Error using err.
func (e *Error) SetResponseGoError(err error) {
	if e.Message != "" {
		panic("cannot re-use proto.Error")
	}
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

// Error formats error.
func (s SendError) Error() string {
	return "failed to send RPC: " + s.Message
}

// CanRetry implements the Retryable interface.
func (s SendError) CanRetry() bool { return s.Retryable }

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
