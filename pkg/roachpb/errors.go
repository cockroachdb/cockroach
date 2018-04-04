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

package roachpb

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func (e *UnhandledRetryableError) Error() string {
	return e.PErr.Message
}

var _ error = &UnhandledRetryableError{}

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
//
// txn is cloned before being stored in Error.
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
		return &UnhandledRetryableError{
			PErr: *e,
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

// SetTxn sets the txn and resets the error message. txn is cloned before being
// stored in the Error.
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
		return fmt.Sprintf("%sr%d: replica %s not lease holder; lease holder unknown", prefix, e.RangeID, e.Replica)
	} else if e.Lease != nil {
		return fmt.Sprintf("%sr%d: replica %s not lease holder; current lease is %s", prefix, e.RangeID, e.Replica, e.Lease)
	}
	return fmt.Sprintf("%sr%d: replica %s not lease holder; replica %s is", prefix, e.RangeID, e.Replica, *e.LeaseHolder)
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
	return fmt.Sprintf("r%d was not found", e.RangeID)
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
	return "TransactionAbortedError: txn aborted"
}

func (e *TransactionAbortedError) message(pErr *Error) string {
	return fmt.Sprintf("TransactionAbortedError: txn aborted %s", pErr.GetTxn())
}

func (*TransactionAbortedError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &TransactionAbortedError{}
var _ transactionRestartError = &TransactionAbortedError{}

func (e *HandledRetryableTxnError) Error() string {
	return e.message(nil)
}

func (e *HandledRetryableTxnError) message(_ *Error) string {
	return fmt.Sprintf("HandledRetryableTxnError: %s", e.Msg)
}

var _ ErrorDetailInterface = &HandledRetryableTxnError{}

// NewTransactionAbortedError initializes a new TransactionAbortedError.
func NewTransactionAbortedError() *TransactionAbortedError {
	return &TransactionAbortedError{}
}

// NewHandledRetryableTxnError initializes a new HandledRetryableTxnError.
//
// txnID is the ID of the transaction being restarted.
// txn is the transaction that the client should use for the next attempts.
func NewHandledRetryableTxnError(
	msg string, txnID uuid.UUID, txn Transaction,
) *HandledRetryableTxnError {
	return &HandledRetryableTxnError{Msg: msg, TxnID: txnID, Transaction: txn}
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
func NewTransactionRetryError(reason TransactionRetryReason) *TransactionRetryError {
	return &TransactionRetryError{
		Reason: reason,
	}
}

func (e *TransactionRetryError) Error() string {
	return fmt.Sprintf("TransactionRetryError: retry txn (%s)", e.Reason)
}

func (e *TransactionRetryError) message(pErr *Error) string {
	return fmt.Sprintf("%s: %s", e.Error(), pErr.GetTxn())
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
	return &TransactionStatusError{
		Msg:    msg,
		Reason: TransactionStatusError_REASON_UNKNOWN,
	}
}

// NewTransactionNotFoundStatusError initializes a new TransactionStatusError with
// a REASON_TXN_NOT_FOUND reason.
func NewTransactionNotFoundStatusError() *TransactionStatusError {
	return &TransactionStatusError{
		Msg:    "txn record not found",
		Reason: TransactionStatusError_REASON_TXN_NOT_FOUND,
	}
}

func (e *TransactionStatusError) Error() string {
	return fmt.Sprintf("TransactionStatusError: %s (%s)", e.Msg, e.Reason)
}

func (e *TransactionStatusError) message(pErr *Error) string {
	return fmt.Sprintf("%s: %s", e.Error(), pErr.GetTxn())
}

var _ ErrorDetailInterface = &TransactionStatusError{}

func (e *WriteIntentError) Error() string {
	return e.message(nil)
}

func (e *WriteIntentError) message(_ *Error) string {
	var buf bytes.Buffer
	buf.WriteString("conflicting intents on ")

	// If we have a lot of intents, we only want to show the first and the last.
	const maxBegin = 5
	const maxEnd = 5
	var begin, end []Intent
	if len(e.Intents) <= maxBegin+maxEnd {
		begin = e.Intents
	} else {
		begin = e.Intents[0:maxBegin]
		end = e.Intents[len(e.Intents)-maxEnd : len(e.Intents)]
	}

	for i := range begin {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(begin[i].Key.String())
	}
	if end != nil {
		buf.WriteString(" ... ")
		for i := range end {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(end[i].Key.String())
		}
	}
	return buf.String()
}

var _ ErrorDetailInterface = &WriteIntentError{}

func (e *WriteTooOldError) Error() string {
	return e.message(nil)
}

func (e *WriteTooOldError) message(_ *Error) string {
	return fmt.Sprintf("WriteTooOldError: write at timestamp %s too old; wrote at %s",
		e.Timestamp, e.ActualTimestamp)
}

var _ ErrorDetailInterface = &WriteTooOldError{}
var _ transactionRestartError = &WriteTooOldError{}

func (*WriteTooOldError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// NewReadWithinUncertaintyIntervalError creates a new uncertainty retry error.
// The read and existing timestamps as well as the txn are purely informational
// and used for formatting the error message.
func NewReadWithinUncertaintyIntervalError(
	readTS, existingTS hlc.Timestamp, txn *Transaction,
) *ReadWithinUncertaintyIntervalError {
	rwue := &ReadWithinUncertaintyIntervalError{
		ReadTimestamp:     readTS,
		ExistingTimestamp: existingTS,
	}
	if txn != nil {
		maxTS := txn.MaxTimestamp
		rwue.MaxTimestamp = &maxTS
		rwue.ObservedTimestamps = append([]ObservedTimestamp(nil), txn.ObservedTimestamps...)
	}
	return rwue
}

func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return e.message(nil)
}

func (e *ReadWithinUncertaintyIntervalError) message(_ *Error) string {
	return fmt.Sprintf("ReadWithinUncertaintyIntervalError: read at time %s encountered "+
		"previous write with future timestamp %s within uncertainty interval `t <= %v`; "+
		"observed timestamps: %v",
		e.ReadTimestamp, e.ExistingTimestamp, e.MaxTimestamp, observedTimestampSlice(e.ObservedTimestamps))
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

func (e *TxnPrevAttemptError) Error() string {
	return e.message(nil)
}

func (*TxnPrevAttemptError) message(_ *Error) string {
	return "response meant for previous incarnation of transaction"
}

var _ ErrorDetailInterface = &TxnPrevAttemptError{}

func (e *IntegerOverflowError) Error() string {
	return e.message(nil)
}

func (e *IntegerOverflowError) message(_ *Error) string {
	return fmt.Sprintf(
		"key %s with value %d incremented by %d results in overflow",
		e.Key, e.CurrentValue, e.IncrementValue)
}

var _ ErrorDetailInterface = &IntegerOverflowError{}

func (e *UnsupportedRequestError) Error() string {
	return e.message(nil)
}

func (e *UnsupportedRequestError) message(_ *Error) string {
	return "unsupported request"
}

var _ ErrorDetailInterface = &UnsupportedRequestError{}

func (e *MixedSuccessError) Error() string {
	return e.message(nil)
}

func (e *MixedSuccessError) message(_ *Error) string {
	return "the batch experienced mixed success and failure"
}

var _ ErrorDetailInterface = &MixedSuccessError{}

func (e *BatchTimestampBeforeGCError) Error() string {
	return e.message(nil)
}

func (e *BatchTimestampBeforeGCError) message(_ *Error) string {
	return fmt.Sprintf("batch timestamp %v must be after GC threshold %v", e.Timestamp, e.Threshold)
}

var _ ErrorDetailInterface = &BatchTimestampBeforeGCError{}
