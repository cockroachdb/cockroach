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
	"github.com/cockroachdb/cockroach/util/uuid"
)

// RetryableTxnError represent a retryable transaction error - the transaction
// that caused it should be re-run.
type RetryableTxnError struct {
	message string
	TxnID   *uuid.UUID
	// TODO(spencer): Get rid of BACKOFF retries. Note that we don't propagate
	// the backoff hint to the client anyway. See #5249
	Backoff bool

	// The error that this RetryableTxnError wraps. Useful for tests that want to
	// assert that they got the expected error.
	Cause ErrorDetailInterface

	// TODO(andrei): These are here temporarily for facilitation converting
	// RetryableTxnError to pErr. Get rid of it afterwards.
	Transaction *Transaction
	CauseProto  *ErrorDetail
}

func (e *RetryableTxnError) Error() string {
	return e.message
}

var _ error = &RetryableTxnError{}

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
	} else if retErr, ok := err.(*RetryableTxnError); ok {
		// TODO(andrei): constructing an Error from a RetryableError is only needed
		// in Store.Send(), which runs "internal batches" for pushing other
		// transactions. It can get a RetryableError which it needs to marhall to
		// the caller as a pErr. This needs to go away by moving away from using the
		// external client interface for running these push batches.
		// It's also needed while the transition from pErr to error is not complete,
		// because there are some places where we go back and forth between the two
		// types.
		// TODO(andrei): at the very least move this to a separate function that's
		// called only from Store.Send() after the sql layer is free of pErr.
		e.Message = retErr.message
		if retErr.Backoff {
			e.TransactionRestart = TransactionRestart_BACKOFF
		} else {
			e.TransactionRestart = TransactionRestart_IMMEDIATE
		}
		e.SetTxn(retErr.Transaction)
		if retErr.CauseProto == nil {
			panic("RetryableTxnProto without a cause")
		}
		e.Detail = retErr.CauseProto
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
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

type internalError Error

func (e *internalError) Error() string {
	return (*Error)(e).String()
}

func (e *internalError) message(_ *Error) string {
	return (*Error)(e).String()
}

// CanRetry implements the retry.Retryable interface.
func (e *internalError) CanRetry() bool {
	return e.Retryable
}

func (e *internalError) canRestartTransaction() TransactionRestart {
	return e.TransactionRestart
}

var _ ErrorDetailInterface = &internalError{}

// ErrorDetailInterface is an interface for each error detail.
type ErrorDetailInterface interface {
	error
	// message returns an error message.
	message(*Error) string
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
	if e.TransactionRestart != TransactionRestart_NONE {
		backoff := e.TransactionRestart == TransactionRestart_BACKOFF
		var txnID *uuid.UUID
		if e.GetTxn() != nil {
			txnID = e.GetTxn().ID
		}
		return &RetryableTxnError{
			message:     e.Message,
			TxnID:       txnID,
			Transaction: e.GetTxn(),
			Cause:       e.GetDetail(),
			CauseProto:  e.Detail,
			Backoff:     backoff,
		}
	}
	detail := e.GetDetail()
	return detail
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
	e.UnexposedTxn = txn
	if txn != nil {
		txnClone := txn.Clone()
		e.UnexposedTxn = &txnClone
	}
	if e.Detail != nil {
		if sErr, ok := e.Detail.GetValue().(ErrorDetailInterface); ok {
			// Refresh the message as the txn is updated.
			e.Message = sErr.message(e)
		}
	}
}

// GetTxn returns the txn.
func (e *Error) GetTxn() *Transaction {
	if e == nil {
		return nil
	}
	return e.UnexposedTxn
}

// SetErrorIndex sets the index of the error.
func (e *Error) SetErrorIndex(index int32) {
	e.Index = &ErrPosition{Index: index}
}

func (e *ErrorWithPGCode) Error() string {
	return e.message(nil)
}

func (e *ErrorWithPGCode) message(_ *Error) string {
	return e.Message
}

var _ ErrorDetailInterface = &ErrorWithPGCode{}

func (e *NodeUnavailableError) Error() string {
	return e.message(nil)
}

func (*NodeUnavailableError) message(_ *Error) string {
	return "node unavailable; try another peer"
}

var _ ErrorDetailInterface = &NodeUnavailableError{}

func (e *NotLeaderError) Error() string {
	return e.message(nil)
}

func (e *NotLeaderError) message(_ *Error) string {
	return fmt.Sprintf("range %d: replica %s not leader; leader is %s", e.RangeID, e.Replica, e.Leader)
}

var _ ErrorDetailInterface = &NotLeaderError{}

func (e *LeaseRejectedError) Error() string {
	return e.message(nil)
}

func (e *LeaseRejectedError) message(_ *Error) string {
	return fmt.Sprintf("cannot replace lease %s with %s: %s", e.Existing, e.Requested, e.Message)
}

// CanRetry indicates that this error can not be retried; it should never
// make it back to the client anyways.
func (*LeaseRejectedError) CanRetry() bool {
	return false
}

var _ ErrorDetailInterface = &LeaseRejectedError{}

// NewSendError creates a SendError. canRetry should be true in most cases; the
// only non-retryable SendErrors are for things like malformed (and not merely
// unresolvable) addresses.
func NewSendError(msg string, canRetry bool) *SendError {
	return &SendError{Message: msg, Retryable: canRetry}
}

func (s SendError) Error() string {
	return s.message(nil)
}

func (s *SendError) message(_ *Error) string {
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

func (e *RangeNotFoundError) Error() string {
	return e.message(nil)
}

func (e *RangeNotFoundError) message(_ *Error) string {
	return fmt.Sprintf("range %d was not found", e.RangeID)
}

// CanRetry indicates whether or not this RangeNotFoundError can be retried.
func (*RangeNotFoundError) CanRetry() bool {
	return true
}

var _ ErrorDetailInterface = &RangeNotFoundError{}

// NewRangeKeyMismatchError initializes a new RangeKeyMismatchError.
func NewRangeKeyMismatchError(start, end Key, desc *RangeDescriptor) *RangeKeyMismatchError {
	if desc != nil && !desc.IsInitialized() {
		// We must never send uninitialized ranges back to the client (nil
		// is fine) guard against regressions of #6027.
		panic("descriptor is not initialized")
	}
	return &RangeKeyMismatchError{
		RequestStartKey: start,
		RequestEndKey:   end,
		MismatchedRange: desc,
	}
}

func (e *RangeKeyMismatchError) Error() string {
	return e.message(nil)
}

func (e *RangeKeyMismatchError) message(_ *Error) string {
	if e.MismatchedRange != nil {
		return fmt.Sprintf("key range %s-%s outside of bounds of range %s-%s",
			e.RequestStartKey, e.RequestEndKey, e.MismatchedRange.StartKey, e.MismatchedRange.EndKey)
	}
	return fmt.Sprintf("key range %s-%s could not be located within a range on store", e.RequestStartKey, e.RequestEndKey)
}

// CanRetry indicates whether or not this RangeKeyMismatchError can be retried.
func (*RangeKeyMismatchError) CanRetry() bool {
	return true
}

var _ ErrorDetailInterface = &RangeKeyMismatchError{}

// NewRangeFrozenError initializes a new RangeFrozenError.
func NewRangeFrozenError(desc RangeDescriptor) *RangeFrozenError {
	return &RangeFrozenError{Desc: desc}
}

func (e *RangeFrozenError) Error() string {
	return fmt.Sprintf("range is frozen: %s", e.Desc)
}

func (e *RangeFrozenError) message(_ *Error) string {
	return e.Error()
}

var _ ErrorDetailInterface = &RangeFrozenError{}

func (e *TransactionAbortedError) Error() string {
	return "txn aborted"
}

func (e *TransactionAbortedError) message(pErr *Error) string {
	return fmt.Sprintf("txn aborted %s", pErr.GetTxn())
}

func (*TransactionAbortedError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
}

var _ ErrorDetailInterface = &TransactionAbortedError{}
var _ transactionRestartError = &TransactionAbortedError{}

// NewTransactionAbortedError initializes a new TransactionAbortedError.
func NewTransactionAbortedError() *TransactionAbortedError {
	return &TransactionAbortedError{}
}

// NewTransactionPushError initializes a new TransactionPushError.
// The argument is copied.
func NewTransactionPushError(pusheeTxn Transaction) *TransactionPushError {
	// Note: this error will cause a txn restart. The error that the client
	// receives contains a txn that might have a modified priority.
	return &TransactionPushError{PusheeTxn: pusheeTxn.Clone()}
}

func (e *TransactionPushError) Error() string {
	return e.message(nil)
}

func (e *TransactionPushError) message(pErr *Error) string {
	if pErr.GetTxn() == nil {
		return fmt.Sprintf("failed to push %s", e.PusheeTxn)
	}
	return fmt.Sprintf("txn %s failed to push %s", pErr.GetTxn(), e.PusheeTxn)
}

var _ ErrorDetailInterface = &TransactionPushError{}
var _ transactionRestartError = &TransactionPushError{}

func (*TransactionPushError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
}

// NewTransactionRetryError initializes a new TransactionRetryError.
func NewTransactionRetryError() *TransactionRetryError {
	return &TransactionRetryError{}
}

// TODO(kaneda): Delete this method once we fully unimplement error for every
// error detail.
func (e *TransactionRetryError) Error() string {
	return fmt.Sprintf("retry txn")
}

func (e *TransactionRetryError) message(pErr *Error) string {
	return fmt.Sprintf("retry txn %s", pErr.GetTxn())
}

var _ ErrorDetailInterface = &TransactionRetryError{}
var _ transactionRestartError = &TransactionRetryError{}

func (*TransactionRetryError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// NewTransactionReplayError initializes a new TransactionReplayError.
func NewTransactionReplayError() *TransactionReplayError {
	return &TransactionReplayError{}
}

func (e *TransactionReplayError) Error() string {
	return fmt.Sprintf("replay txn")
}

func (e *TransactionReplayError) message(pErr *Error) string {
	return fmt.Sprintf("replay txn %s", pErr.GetTxn())
}

var _ ErrorDetailInterface = &TransactionReplayError{}

// NewTransactionStatusError initializes a new TransactionStatusError from
// the given message.
func NewTransactionStatusError(msg string) *TransactionStatusError {
	return &TransactionStatusError{Msg: msg}
}

func (e *TransactionStatusError) Error() string {
	return e.Msg
}

func (e *TransactionStatusError) message(pErr *Error) string {
	return fmt.Sprintf("txn %s: %s", pErr.GetTxn(), e.Msg)
}

var _ ErrorDetailInterface = &TransactionStatusError{}

func (e *WriteIntentError) Error() string {
	return e.message(nil)
}

func (e *WriteIntentError) message(_ *Error) string {
	var keys []Key
	for _, intent := range e.Intents {
		keys = append(keys, intent.Key)
	}
	return fmt.Sprintf("conflicting intents on %v: resolved? %t", keys, e.Resolved)
}

var _ ErrorDetailInterface = &WriteIntentError{}

func (e *WriteTooOldError) Error() string {
	return e.message(nil)
}

func (e *WriteTooOldError) message(_ *Error) string {
	return fmt.Sprintf("write at timestamp %s too old; wrote at %s", e.Timestamp, e.ActualTimestamp)
}

var _ ErrorDetailInterface = &WriteTooOldError{}
var _ transactionRestartError = &WriteTooOldError{}

func (*WriteTooOldError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// NewReadWithinUncertaintyIntervalError creates a new uncertainty retry error.
// The read and existing timestamps are purely informational and used for
// formatting the error message.
func NewReadWithinUncertaintyIntervalError(readTS, existingTS Timestamp) *ReadWithinUncertaintyIntervalError {
	return &ReadWithinUncertaintyIntervalError{
		ReadTimestamp:     readTS,
		ExistingTimestamp: existingTS,
	}
}

func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return e.message(nil)
}

func (e *ReadWithinUncertaintyIntervalError) message(_ *Error) string {
	return fmt.Sprintf("read at time %s encountered previous write with future timestamp %s within uncertainty interval", e.ReadTimestamp, e.ExistingTimestamp)
}

var _ ErrorDetailInterface = &ReadWithinUncertaintyIntervalError{}
var _ transactionRestartError = &ReadWithinUncertaintyIntervalError{}

func (*ReadWithinUncertaintyIntervalError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

func (e *OpRequiresTxnError) Error() string {
	return e.message(nil)
}

func (e *OpRequiresTxnError) message(_ *Error) string {
	return "the operation requires transactional context"
}

var _ ErrorDetailInterface = &OpRequiresTxnError{}

func (e *ConditionFailedError) Error() string {
	return e.message(nil)
}

func (e *ConditionFailedError) message(_ *Error) string {
	return fmt.Sprintf("unexpected value: %s", e.ActualValue)
}

var _ ErrorDetailInterface = &ConditionFailedError{}

func (e *RaftGroupDeletedError) Error() string {
	return e.message(nil)
}

func (*RaftGroupDeletedError) message(_ *Error) string {
	return "raft group deleted"
}

var _ ErrorDetailInterface = &RaftGroupDeletedError{}

func (e *ReplicaCorruptionError) Error() string {
	return e.message(nil)
}

func (e *ReplicaCorruptionError) message(_ *Error) string {
	msg := fmt.Sprintf("replica corruption (processed=%t)", e.Processed)
	if e.ErrorMsg != "" {
		msg += ": " + e.ErrorMsg
	}
	return msg
}

var _ ErrorDetailInterface = &ReplicaCorruptionError{}

func (e *LeaseVersionChangedError) Error() string {
	return e.message(nil)
}

func (*LeaseVersionChangedError) message(_ *Error) string {
	return "lease version changed"
}

var _ ErrorDetailInterface = &LeaseVersionChangedError{}

func (e *DidntUpdateDescriptorError) Error() string {
	return e.message(nil)
}

func (*DidntUpdateDescriptorError) message(_ *Error) string {
	return "didn't update the table descriptor"
}

var _ ErrorDetailInterface = &DidntUpdateDescriptorError{}

func (e *ExistingSchemaChangeLeaseError) Error() string {
	return e.message(nil)
}

func (*ExistingSchemaChangeLeaseError) message(_ *Error) string {
	return "an outstanding schema change lease exists"
}

var _ ErrorDetailInterface = &ExistingSchemaChangeLeaseError{}

func (e *DescriptorDeletedError) Error() string {
	return e.message(nil)
}

func (*DescriptorDeletedError) message(_ *Error) string {
	return "descriptor deleted"
}

var _ ErrorDetailInterface = &DescriptorDeletedError{}

func (e *DescriptorNotFoundError) Error() string {
	return e.message(nil)
}

func (e *DescriptorNotFoundError) message(_ *Error) string {
	return fmt.Sprintf("descriptor ID %d not found", e.DescriptorId)
}

var _ ErrorDetailInterface = &DescriptorNotFoundError{}
