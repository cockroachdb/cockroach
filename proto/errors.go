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

// TransactionRestartError is an interface implemented by errors that cause
// a transaction to be restarted.
type TransactionRestartError interface {
	CanRestartTransaction() TransactionRestart
}

// Error implements the Go error interface.
func (e *Error) Error() string {
	return e.Message
}

// CanRetry implements the util/Retryable interface.
func (e *Error) CanRetry() bool {
	return e.Retryable
}

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *Error) CanRestartTransaction() TransactionRestart {
	return e.TransactionRestart
}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("replica %s not leader; leader is %s", e.Replica, e.Leader)
}

// Error formats error.
func (e *LeaseRejectedError) Error() string {
	return fmt.Sprintf("cannot replace lease %s with %s", e.Existing, e.Requested)
}

// NewRangeNotFoundError initializes a new RangeNotFoundError.
func NewRangeNotFoundError(raftID int64) *RangeNotFoundError {
	return &RangeNotFoundError{
		RaftID: raftID,
	}
}

// Error formats error.
func (e *RangeNotFoundError) Error() string {
	return fmt.Sprintf("range %d was not found", e.RaftID)
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

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *TransactionAbortedError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
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

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *TransactionPushError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_BACKOFF
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

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *TransactionRetryError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
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
	return fmt.Sprintf("conflicting write intents %+v: resolved? %t", e.Intents, e.Resolved)
}

// Error formats error.
func (e *WriteTooOldError) Error() string {
	return fmt.Sprintf("write too old: timestamp %s <= %s", e.Timestamp, e.ExistingTimestamp)
}

// Error formats error.
func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return fmt.Sprintf("read at time %s encountered previous write with future timestamp %s within uncertainty interval", e.Timestamp, e.ExistingTimestamp)
}

// CanRestartTransaction implements the TransactionRestartError interface.
func (e *ReadWithinUncertaintyIntervalError) CanRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Error formats error.
func (e *OpRequiresTxnError) Error() string {
	return "the operation requires transactional context"
}

// Error formats error.
func (e *ConditionFailedError) Error() string {
	return fmt.Sprintf("unexpected value: %s", e.ActualValue)
}
