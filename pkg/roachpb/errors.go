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

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// RetryableTxnError represents a retryable transaction error - the transaction
// that caused it should be re-run.
type RetryableTxnError struct {
	message string
	TxnID   *uuid.UUID

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

// NewRetryableTxnError creates a shim RetryableTxnError that
// reports the given cause when converted to String(). This can be
// used to fake/force a retry at the SQL layer.
//
// txnID is the id of the transaction that this error is supposed to cause a
// retry for. Can be nil, in which case it will cause retries for transactions
// that don't have an ID set.
// TODO(andrei): this function should really take a transaction as an argument.
// The caller (crdb_internal.force_retry) should be given access to the current
// transaction through the EvalContext.
func NewRetryableTxnError(cause string, txnID *uuid.UUID) *RetryableTxnError {
	return &RetryableTxnError{
		message: cause,
		TxnID:   txnID,
	}
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
	} else if _, ok := err.(*RetryableTxnError); ok {
		// This shouldn't happen; RetryableTxnError should never be converted back
		// into a pErr because it might lead to the wrong transaction being retried.
		// If this conversation were attempted, it'd be a sign of a pErr having been
		// converted to an error which is now being converted back to a pErr.
		panic("RetryableTxnError being converted back to pErr.")
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

// NewErrorf creates an Error from the given error message. It is a
// passthrough to fmt.Errorf, with an additional prefix containing the
// filename and line number.
func NewErrorf(format string, a ...interface{}) *Error {
	// Cannot use errors.Errorf here due to cyclic dependency.
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

// GoError returns a Go error converted from Error.
func (e *Error) GoError() error {
	if e == nil {
		return nil
	}
	if e.TransactionRestart != TransactionRestart_NONE {
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
		}
	}
	return e.GetDetail()
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

// UpdateTxn updates the txn.
func (e *Error) UpdateTxn(o *Transaction) {
	if e == nil {
		return
	}
	if e.UnexposedTxn == nil {
		e.UnexposedTxn = o
	} else {
		e.UnexposedTxn.Update(o)
	}
}

// SetErrorIndex sets the index of the error.
func (e *Error) SetErrorIndex(index int32) {
	e.Index = &ErrPosition{Index: index}
}

func (e *NodeUnavailableError) Error() string {
	return e.message(nil)
}

func (*NodeUnavailableError) message(_ *Error) string {
	return "node unavailable; try another peer"
}

var _ ErrorDetailInterface = &NodeUnavailableError{}

func (e *NotLeaseHolderError) Error() string {
	return e.message(nil)
}

func (e *NotLeaseHolderError) message(_ *Error) string {
	const prefix = "[NotLeaseHolderError] "
	if e.CustomMsg != "" {
		return prefix + e.CustomMsg
	}
	if e.LeaseHolder == nil {
		return fmt.Sprintf("%srange %d: replica %s not lease holder; lease holder unknown", prefix, e.RangeID, e.Replica)
	} else if e.Lease != nil {
		return fmt.Sprintf("%srange %d: replica %s not lease holder; current lease is %s", prefix, e.RangeID, e.Replica, e.Lease)
	}
	return fmt.Sprintf("%srange %d: replica %s not lease holder; replica %s is", prefix, e.RangeID, e.Replica, *e.LeaseHolder)
}

var _ ErrorDetailInterface = &NotLeaseHolderError{}

func (e *LeaseRejectedError) Error() string {
	return e.message(nil)
}

func (e *LeaseRejectedError) message(_ *Error) string {
	return fmt.Sprintf("cannot replace lease %s with %s: %s", e.Existing, e.Requested, e.Message)
}

var _ ErrorDetailInterface = &LeaseRejectedError{}

// NewSendError creates a SendError.
func NewSendError(msg string) *SendError {
	return &SendError{Message: msg}
}

func (s SendError) Error() string {
	return s.message(nil)
}

func (s *SendError) message(_ *Error) string {
	return "failed to send RPC: " + s.Message
}

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

var _ ErrorDetailInterface = &RangeNotFoundError{}

// NewRangeKeyMismatchError initializes a new RangeKeyMismatchError.
func NewRangeKeyMismatchError(start, end Key, desc *RangeDescriptor) *RangeKeyMismatchError {
	if desc != nil && !desc.IsInitialized() {
		// We must never send uninitialized ranges back to the client (nil
		// is fine) guard against regressions of #6027.
		panic(fmt.Sprintf("descriptor is not initialized: %+v", desc))
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

var _ ErrorDetailInterface = &RangeKeyMismatchError{}

// NewAmbiguousResultError initializes a new AmbiguousResultError with
// an explanatory message.
func NewAmbiguousResultError(msg string) *AmbiguousResultError {
	return &AmbiguousResultError{Message: msg}
}

func (e *AmbiguousResultError) Error() string {
	return e.message(nil)
}

func (e *AmbiguousResultError) message(_ *Error) string {
	return fmt.Sprintf("result is ambiguous (%s)", e.Message)
}

var _ ErrorDetailInterface = &AmbiguousResultError{}

func (e *TransactionAbortedError) Error() string {
	return "txn aborted"
}

func (e *TransactionAbortedError) message(pErr *Error) string {
	return fmt.Sprintf("txn aborted %s", pErr.GetTxn())
}

func (*TransactionAbortedError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
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
	return TransactionRestart_IMMEDIATE
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
	return fmt.Sprintf("conflicting intents on %v", keys)
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
func NewReadWithinUncertaintyIntervalError(
	readTS, existingTS hlc.Timestamp,
) *ReadWithinUncertaintyIntervalError {
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

// NewReplicaTooOldError initializes a new ReplicaTooOldError.
func NewReplicaTooOldError(replicaID ReplicaID) *ReplicaTooOldError {
	return &ReplicaTooOldError{
		ReplicaID: replicaID,
	}
}

func (e *ReplicaTooOldError) Error() string {
	return e.message(nil)
}

func (*ReplicaTooOldError) message(_ *Error) string {
	return "sender replica too old, discarding message"
}

var _ ErrorDetailInterface = &ReplicaTooOldError{}

// NewStoreNotFoundError initializes a new StoreNotFoundError.
func NewStoreNotFoundError(storeID StoreID) *StoreNotFoundError {
	return &StoreNotFoundError{
		StoreID: storeID,
	}
}

func (e *StoreNotFoundError) Error() string {
	return e.message(nil)
}

func (e *StoreNotFoundError) message(_ *Error) string {
	return fmt.Sprintf("store %d was not found", e.StoreID)
}

var _ ErrorDetailInterface = &StoreNotFoundError{}
