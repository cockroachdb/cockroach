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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package roachpb

import (
	"fmt"

	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/retry"
)

// ResponseWithError is a tuple of a BatchResponse and an error. It is used to
// pass around a BatchResponse with its associated error where that
// entanglement is necessary (e.g. channels, methods that need to return
// another error in addition to this one).
type ResponseWithError struct {
	Reply *BatchResponse
	Err   *Error
}

// ErrorUnexpectedlySet creates a string to panic with when a response (typically
// a roachpb.BatchResponse) unexpectedly has Error set in its response header.
func ErrorUnexpectedlySet(culprit, response interface{}) string {
	return fmt.Sprintf("error is unexpectedly set, culprit is %T:\n%+v", culprit, response)
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
	if intErr, ok := err.(*InternalError); ok {
		*e = *(*Error)(intErr)
	} else {
		e.SetGoError(err)
	}
	return e
}

// NewUErrorf creates an Error from the given error message. Used
// for user-facing errors.
func NewUErrorf(format string, a ...interface{}) *Error {
	return NewError(fmt.Errorf(format, a...))
}

// NewErrorf creates an Error from the given error message. It is a
// passthrough to fmt.Errorf, with an additional prefix containing the
// filename and line number.
func NewErrorf(format string, a ...interface{}) *Error {
	// Cannot use util.Errorf here due to cyclic dependency.
	file, line, _ := caller.Lookup(2)
	s := fmt.Sprintf("%s:%d: ", file, line)
	return NewError(fmt.Errorf(s+format, a...))
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	return e.Message
}

// InternalError is an error used for internal system errors.
type InternalError Error

// Error implements error.
func (e *InternalError) Error() string {
	return (*Error)(e).String()
}

// CanRetry implements the retry.Retryable interface.
func (e *InternalError) CanRetry() bool {
	return e.Retryable
}

// CanRetry implements the retry.Retryable interface.
func (e *Error) CanRetry() bool {
	return e.Retryable
}

// GoError returns the non-nil error from the roachpb.Error union.
func (e *Error) GoError() error {
	if e == nil {
		return nil
	}
	if e.Detail == nil {
		return (*InternalError)(e)
	}
	err := e.getDetail()
	if err == nil {
		// Unknown error detail; return the generic error.
		return (*InternalError)(e)
	}
	// Make sure that the flags in the generic portion of the error
	// match the methods of the specific error type.
	if e.Retryable {
		if r, ok := err.(retry.Retryable); !ok || !r.CanRetry() {
			panic(fmt.Sprintf("inconsistent error proto; expected %T to be retryable", err))
		}
	}

	return err
}

// SetGoError sets Error using err.
func (e *Error) SetGoError(err error) {
	if e.Message != "" {
		panic("cannot re-use roachpb.Error")
	}
	e.Message = err.Error()
	if r, ok := err.(retry.Retryable); ok {
		e.Retryable = r.CanRetry()
	}

	// If the specific error type exists in the detail union, set it.
	detail := &ErrorDetail{}
	if detail.SetValue(err) {
		e.Detail = detail
	}

	// TODO(kaneda): Find a better way to set TransactionRestart.
	switch err.(type) {
	case *TransactionAbortedError, *TransactionPushError:
		e.TransactionRestart = TransactionRestart_BACKOFF
	case *TransactionRetryError, *ReadWithinUncertaintyIntervalError:
		e.TransactionRestart = TransactionRestart_IMMEDIATE
	}
}

// SetErrorIndex sets the index of the error.
func (e *Error) SetErrorIndex(index int32) {
	e.Index = &ErrPosition{Index: index}
}

// Error formats error.
func (*NodeUnavailableError) Error() string {
	return "node unavailable; try another peer"
}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("range %d: replica %s not leader; leader is %s", e.RangeID, e.Replica, e.Leader)
}

// Error formats error.
func (e *LeaseRejectedError) Error() string {
	return fmt.Sprintf("cannot replace lease %s with %s: %s", e.Existing, e.Requested, e.Message)
}

// CanRetry indicates that this error can not be retried; it should never
// make it back to the client anyways.
func (*LeaseRejectedError) CanRetry() bool {
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
func (*RangeNotFoundError) CanRetry() bool {
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
func (*RangeKeyMismatchError) CanRetry() bool {
	return true
}

// NewTransactionAbortedError initializes a new TransactionAbortedError. It
// creates a copy of the given Transaction.
func NewTransactionAbortedError(txn *Transaction) *TransactionAbortedError {
	return &TransactionAbortedError{Txn: *txn.Clone()}
}

// Error formats error.
func (e *TransactionAbortedError) Error() string {
	return fmt.Sprintf("txn aborted %s", e.Txn)
}

// NewTransactionPushError initializes a new TransactionPushError.
// Txn is the transaction which will be retried. Both arguments are copied.
// Transactions.
func NewTransactionPushError(txn, pusheeTxn Transaction) *TransactionPushError {
	err := &TransactionPushError{PusheeTxn: *pusheeTxn.Clone()}
	if len(txn.ID) != 0 {
		// When the pusher is non-transactional, txn will be
		// empty but for the priority. In that case, ignore it
		// here.
		err.Txn = txn.Clone()
	}
	return err
}

// Error formats error.
func (e *TransactionPushError) Error() string {
	if e.Txn == nil {
		return fmt.Sprintf("failed to push %s", e.PusheeTxn)
	}
	return fmt.Sprintf("txn %s failed to push %s", e.Txn, e.PusheeTxn)
}

// NewTransactionRetryError initializes a new TransactionRetryError.
// Txn is the transaction which will be retried (a copy is taken).
func NewTransactionRetryError(txn *Transaction) *TransactionRetryError {
	return &TransactionRetryError{Txn: *txn.Clone()}
}

// Error formats error.
func (e *TransactionRetryError) Error() string {
	return fmt.Sprintf("retry txn %s", e.Txn)
}

// NewTransactionStatusError initializes a new TransactionStatusError from
// the given Transaction (which is copied) and a message.
func NewTransactionStatusError(txn Transaction, msg string) *TransactionStatusError {
	return &TransactionStatusError{Txn: *txn.Clone(), Msg: msg}
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

// Error formats error.
func (*OpRequiresTxnError) Error() string {
	return "the operation requires transactional context"
}

// Error formats error.
func (e *ConditionFailedError) Error() string {
	return fmt.Sprintf("unexpected value: %s", e.ActualValue)
}

// Error formats error.
func (*RaftGroupDeletedError) Error() string {
	return "raft group deleted"
}

func (e *ReplicaCorruptionError) Error() string {
	return fmt.Sprintf("replica corruption (processed=%t): %s", e.Processed, e.ErrorMsg)
}
