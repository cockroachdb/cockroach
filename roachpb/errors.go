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
	"errors"
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

// transactionRestartError is an interface implemented by errors that cause
// a transaction to be restarted.
type transactionRestartError interface {
	canRestartTransaction() TransactionRestart
}

// GetDetail returns an error detail associated with the error.
func (e *Error) GetDetail() ErrorDetailInterface {
	if e == nil {
		return nil
	}
	if e.Detail == nil {
		// Unknown error detail; return the generic error.
		return (*internalError)(e)
	}

	if err, ok := e.Detail.GetValue().(ErrorDetailInterface); ok {
		return err
	}
	// Unknown error detail; return the generic error.
	return (*internalError)(e)
}

// NewError creates an Error from the given error.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	e := &Error{}
	if intErr, ok := err.(*internalError); ok {
		*e = *(*Error)(intErr)
	} else {
		e.setGoError(err)
	}
	return e
}

// NewErrorWithTxn creates an Error from the given error and a transaction.
func NewErrorWithTxn(err error, txn *Transaction) *Error {
	e := NewError(err)
	e.SetTxn(txn)
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
	file, line, _ := caller.Lookup(1)
	s := fmt.Sprintf("%s:%d: ", file, line)
	return NewError(fmt.Errorf(s+format, a...))
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	return e.Message
}

type internalError Error

// Error implements error.
func (e *internalError) Error() string {
	return (*Error)(e).String()
}

// message returns an error message.
func (e *internalError) message(pErr *Error) string {
	return (*Error)(e).String()
}

// CanRetry implements the retry.Retryable interface.
func (e *internalError) CanRetry() bool {
	return e.Retryable
}

// canRestartTransaction implements the transactionRestartError interface.
func (e *internalError) canRestartTransaction() TransactionRestart {
	return e.TransactionRestart
}

var _ ErrorDetailInterface = &internalError{}

// ErrorDetailInterface is an interface for each error detail.
type ErrorDetailInterface interface {
	// message returns an error message.
	message(pErr *Error) string
}

// CanRetry implements the retry.Retryable interface.
func (e *Error) CanRetry() bool {
	return e.Retryable
}

// GoError returns a Go error converted from Error.
func (e *Error) GoError() error {
	if e == nil {
		return nil
	}
	return errors.New(e.Message)
}

// setGoError sets Error using err.
func (e *Error) setGoError(err error) {
	if e.Message != "" {
		panic("cannot re-use roachpb.Error")
	}
	if sErr, ok := err.(ErrorDetailInterface); ok {
		e.Message = sErr.message(e)
	} else {
		e.Message = err.Error()
	}
	if r, ok := err.(retry.Retryable); ok {
		e.Retryable = r.CanRetry()
	}
	var isTxnError bool
	if r, ok := err.(transactionRestartError); ok {
		isTxnError = true
		e.TransactionRestart = r.canRestartTransaction()
	}
	// If the specific error type exists in the detail union, set it.
	detail := &ErrorDetail{}
	if detail.SetValue(err) {
		e.Detail = detail
	} else if _, isInternalError := err.(*internalError); !isInternalError && isTxnError {
		panic(fmt.Sprintf("transactionRestartError %T must be an ErrorDetail", err))
	}
}

// SetTxn sets the txn and resets the error message.
// TODO(kaneda): Unexpose this method and make callers use NewErrorWithTxn.
func (e *Error) SetTxn(txn *Transaction) {
	e.UnexposedTxn = txn.Clone()
	if e.Detail != nil {
		if sErr, ok := e.Detail.GetValue().(ErrorDetailInterface); ok {
			// Refresh the message as the txn is updated.
			e.Message = sErr.message(e)
		}
	}
}

// GetTxn returns the txn.
func (e *Error) GetTxn() *Transaction {
	return e.UnexposedTxn
}

// SetErrorIndex sets the index of the error.
func (e *Error) SetErrorIndex(index int32) {
	e.Index = &ErrPosition{Index: index}
}

// Error formats error.
func (e *NodeUnavailableError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (*NodeUnavailableError) message(pErr *Error) string {
	return "node unavailable; try another peer"
}

var _ ErrorDetailInterface = &NodeUnavailableError{}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *NotLeaderError) message(pErr *Error) string {
	return fmt.Sprintf("range %d: replica %s not leader; leader is %s", e.RangeID, e.Replica, e.Leader)
}

var _ ErrorDetailInterface = &NotLeaderError{}

// Error formats error.
func (e *LeaseRejectedError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *LeaseRejectedError) message(pErr *Error) string {
	return fmt.Sprintf("cannot replace lease %s with %s: %s", e.Existing, e.Requested, e.Message)
}

// CanRetry indicates that this error can not be retried; it should never
// make it back to the client anyways.
func (*LeaseRejectedError) CanRetry() bool {
	return false
}

var _ ErrorDetailInterface = &LeaseRejectedError{}

// Error formats error.
func (s SendError) Error() string {
	return s.message(nil)
}

// message returns an error message.
func (s *SendError) message(pErr *Error) string {
	return "failed to send RPC: " + s.Message
}

// CanRetry implements the Retryable interface.
func (s SendError) CanRetry() bool { return s.Retryable }

var _ ErrorDetailInterface = &SendError{}

// NewRangeNotFoundError initializes a new RangeNotFoundError.
func NewRangeNotFoundError(rangeID RangeID) *RangeNotFoundError {
	return &RangeNotFoundError{
		RangeID: rangeID,
	}
}

// Error formats error.
func (e *RangeNotFoundError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *RangeNotFoundError) message(pErr *Error) string {
	return fmt.Sprintf("range %d was not found", e.RangeID)
}

// CanRetry indicates whether or not this RangeNotFoundError can be retried.
func (*RangeNotFoundError) CanRetry() bool {
	return true
}

var _ ErrorDetailInterface = &RangeNotFoundError{}

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
	return e.message(nil)
}

// message returns an error message.
func (e *RangeKeyMismatchError) message(pErr *Error) string {
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

var _ ErrorDetailInterface = &RangeNotFoundError{}

// Error formats error.
func (e *TransactionAbortedError) Error() string {
	return fmt.Sprintf("txn aborted")
}

// message returns an error message.
func (e *TransactionAbortedError) message(pErr *Error) string {
	return fmt.Sprintf("txn aborted %s", pErr.UnexposedTxn)
}

var _ ErrorDetailInterface = &TransactionAbortedError{}
var _ transactionRestartError = &TransactionAbortedError{}

// canRestartTransaction implements the transactionRestartError interface.
func (*TransactionAbortedError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
}

// NewTransactionAbortedError initializes a new TransactionAbortedError.
func NewTransactionAbortedError() *TransactionAbortedError {
	return &TransactionAbortedError{}
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
	return e.message(nil)
}

// message returns an error message.
func (e *TransactionPushError) message(pErr *Error) string {
	if e.Txn == nil {
		return fmt.Sprintf("failed to push %s", e.PusheeTxn)
	}
	return fmt.Sprintf("txn %s failed to push %s", e.Txn, e.PusheeTxn)
}

var _ ErrorDetailInterface = &TransactionPushError{}
var _ transactionRestartError = &TransactionPushError{}

// canRestartTransaction implements the transactionRestartError interface.
func (*TransactionPushError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
}

// NewTransactionRetryError initializes a new TransactionRetryError.
func NewTransactionRetryError() *TransactionRetryError {
	return &TransactionRetryError{}
}

// Error formats error.
// TODO(kaneda): Delete this method once we fully unimplement error for every
// error detail.
func (e *TransactionRetryError) Error() string {
	return fmt.Sprintf("retry txn")
}

// message returns an error message.
func (e *TransactionRetryError) message(pErr *Error) string {
	return fmt.Sprintf("retry txn %s", pErr.UnexposedTxn)
}

var _ ErrorDetailInterface = &TransactionRetryError{}
var _ transactionRestartError = &TransactionRetryError{}

// canRestartTransaction implements the transactionRestartError interface.
func (*TransactionRetryError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// NewTransactionStatusError initializes a new TransactionStatusError from
// the given message.
func NewTransactionStatusError(msg string) *TransactionStatusError {
	return &TransactionStatusError{Msg: msg}
}

// Error formats error.
func (e *TransactionStatusError) Error() string {
	return e.Msg
}

// message returns an error message.
func (e *TransactionStatusError) message(pErr *Error) string {
	return fmt.Sprintf("txn %s: %s", pErr.UnexposedTxn, e.Msg)
}

var _ ErrorDetailInterface = &TransactionStatusError{}

// Error formats error.
func (e *WriteIntentError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *WriteIntentError) message(pErr *Error) string {
	var keys []Key
	for _, intent := range e.Intents {
		keys = append(keys, intent.Key)
	}
	return fmt.Sprintf("conflicting intents on %v: resolved? %t", keys, e.Resolved)
}

var _ ErrorDetailInterface = &WriteIntentError{}

// Error formats error.
func (e *WriteTooOldError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *WriteTooOldError) message(pErr *Error) string {
	return fmt.Sprintf("write too old: timestamp %s <= %s", e.Timestamp, e.ExistingTimestamp)
}

var _ ErrorDetailInterface = &WriteTooOldError{}

// Error formats error.
func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *ReadWithinUncertaintyIntervalError) message(pErr *Error) string {
	return fmt.Sprintf("read at time %s encountered previous write with future timestamp %s within uncertainty interval", e.Timestamp, e.ExistingTimestamp)
}

var _ ErrorDetailInterface = &ReadWithinUncertaintyIntervalError{}
var _ transactionRestartError = &ReadWithinUncertaintyIntervalError{}

// canRestartTransaction implements the transactionRestartError interface.
func (*ReadWithinUncertaintyIntervalError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Error formats error.
func (e *OpRequiresTxnError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *OpRequiresTxnError) message(pErr *Error) string {
	return "the operation requires transactional context"
}

var _ ErrorDetailInterface = &OpRequiresTxnError{}

// Error formats error.
func (e *ConditionFailedError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *ConditionFailedError) message(pErr *Error) string {
	return fmt.Sprintf("unexpected value: %s", e.ActualValue)
}

var _ ErrorDetailInterface = &ConditionFailedError{}

// Error formats error.
func (e *RaftGroupDeletedError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (*RaftGroupDeletedError) message(pErr *Error) string {
	return "raft group deleted"
}

var _ ErrorDetailInterface = &RaftGroupDeletedError{}

// Error formats error.
func (e *ReplicaCorruptionError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (e *ReplicaCorruptionError) message(pErr *Error) string {
	return fmt.Sprintf("replica corruption (processed=%t): %s", e.Processed, e.ErrorMsg)
}

var _ ErrorDetailInterface = &ReplicaCorruptionError{}

// Error formats error.
func (e *LeaseVersionChangedError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (*LeaseVersionChangedError) message(pErr *Error) string {
	return "lease version changed"
}

var _ ErrorDetailInterface = &LeaseVersionChangedError{}

// Error formats error.
func (e *DidntUpdateDescriptorError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (*DidntUpdateDescriptorError) message(pErr *Error) string {
	return "didn't update the table descriptor"
}

var _ ErrorDetailInterface = &DidntUpdateDescriptorError{}

// Error formats error.
func (e *SqlTransactionAbortedError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (*SqlTransactionAbortedError) message(pErr *Error) string {
	return "current transaction is aborted, commands ignored until end of transaction block"
}

var _ ErrorDetailInterface = &SqlTransactionAbortedError{}

// Error formats error.
func (e *ExistingSchemaChangeLeaseError) Error() string {
	return e.message(nil)
}

// message returns an error message.
func (*ExistingSchemaChangeLeaseError) message(pErr *Error) string {
	return "an outstanding schema change lease exists"
}

var _ ErrorDetailInterface = &ExistingSchemaChangeLeaseError{}
