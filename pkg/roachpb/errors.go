// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ClientVisibleRetryError is to be implemented by errors visible by
// layers above and that can be handled by retrying the transaction.
type ClientVisibleRetryError interface {
	ClientVisibleRetryError()
}

// ClientVisibleAmbiguousError is to be implemented by errors visible
// by layers above and that indicate uncertainty.
type ClientVisibleAmbiguousError interface {
	ClientVisibleAmbiguousError()
}

func (e *UnhandledRetryableError) Error() string {
	return e.PErr.Message
}

var _ error = &UnhandledRetryableError{}

// transactionRestartError is an interface implemented by errors that cause
// a transaction to be restarted.
type transactionRestartError interface {
	canRestartTransaction() TransactionRestart
}

// ErrorUnexpectedlySet creates a string to panic with when a response (typically
// a roachpb.BatchResponse) unexpectedly has Error set in its response header.
func ErrorUnexpectedlySet(culprit, response interface{}) string {
	return fmt.Sprintf("error is unexpectedly set, culprit is %T:\n%+v", culprit, response)
}

// ErrorPriority is used to rank errors such that the "best" one is chosen to be
// presented as the batch result when a batch is split up and observes multiple
// errors. Higher values correspond to higher priorities.
type ErrorPriority int

const (
	_ ErrorPriority = iota
	// ErrorScoreTxnRestart indicates that the transaction should be restarted
	// with an incremented epoch.
	ErrorScoreTxnRestart

	// ErrorScoreUnambiguousError is used for errors which are known to return a
	// transaction reflecting the highest timestamp of any intent that was
	// written. We allow the transaction to continue after such errors; we also
	// allow RollbackToSavepoint() to be called after such errors. In particular,
	// this is useful for SQL which wants to allow rolling back to a savepoint
	// after ConditionFailedErrors (uniqueness violations). With continuing after
	// errors its important for the coordinator to track the timestamp at which
	// intents might have been written.
	//
	// Note that all the lower scores also are unambiguous in this sense, so this
	// score can be seen as an upper-bound for unambiguous errors.
	ErrorScoreUnambiguousError

	// ErrorScoreNonRetriable indicates that the transaction performed an
	// operation that does not warrant a retry. The error should be propagated to
	// the client and the transaction should terminate immediately.
	ErrorScoreNonRetriable

	// ErrorScoreTxnAbort indicates that the transaction is aborted. The
	// operation can only try again under the purview of a new transaction.
	//
	// This error has the highest priority because, as far as KV is concerned, a
	// TransactionAbortedError is impossible to recover from (whereas
	// non-retriable errors could conceivably be recovered if the client wanted to
	// ignore them). Also, the TxnCoordSender likes to assume that a
	// TransactionAbortedError is the only way it finds about an aborted
	// transaction, and so it benefits from all other errors being merged into a
	// TransactionAbortedError instead of the other way around.
	ErrorScoreTxnAbort
)

// ErrPriority computes the priority of the given error.
func ErrPriority(err error) ErrorPriority {
	if err == nil {
		return 0
	}
	switch v := err.(type) {
	case *UnhandledRetryableError:
		if _, ok := v.PErr.GetDetail().(*TransactionAbortedError); ok {
			return ErrorScoreTxnAbort
		}
		return ErrorScoreTxnRestart
	case *TransactionRetryWithProtoRefreshError:
		if v.PrevTxnAborted() {
			return ErrorScoreTxnAbort
		}
		return ErrorScoreTxnRestart
	case *ConditionFailedError:
		// We particularly care about returning the low ErrorScoreUnambiguousError
		// because we don't want to transition a transaction that encounters
		// ConditionFailedError to an error state. More specifically, we want to
		// allow rollbacks to savepoint after a ConditionFailedError.
		return ErrorScoreUnambiguousError
	}
	return ErrorScoreNonRetriable
}

// NewError creates an Error from the given error.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	e := &Error{}
	e.SetDetail(err)
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
	err := errors.Newf(format, a...)
	file, line, _ := caller.Lookup(1)
	err = errors.Wrapf(err, "%s:%d", file, line)
	return NewError(err)
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

// SetDetail sets the error detail for the error. The argument cannot be nil.
func (e *Error) SetDetail(err error) {
	if err == nil {
		panic("nil err argument")
	}
	if intErr, ok := err.(*internalError); ok {
		*e = *(*Error)(intErr)
	} else {
		if sErr, ok := err.(ErrorDetailInterface); ok {
			e.Message = sErr.message(e)
		} else {
			e.Message = err.Error()
		}
		if r, ok := err.(transactionRestartError); ok {
			e.TransactionRestart = r.canRestartTransaction()
		} else {
			e.TransactionRestart = TransactionRestart_NONE
		}
		// If the specific error type exists in the detail union, set it.
		if !e.Detail.SetInner(err) {
			_, isInternalError := err.(*internalError)
			if !isInternalError && e.TransactionRestart != TransactionRestart_NONE {
				panic(fmt.Sprintf("transactionRestartError %T must be an ErrorDetail", err))
			}
		}
		e.checkTxnStatusValid()
	}
}

// GetDetail returns an error detail associated with the error.
func (e *Error) GetDetail() ErrorDetailInterface {
	if e == nil {
		return nil
	}
	if err, ok := e.Detail.GetInner().(ErrorDetailInterface); ok {
		return err
	}
	// Unknown error detail; return the generic error.
	return (*internalError)(e)
}

// SetTxn sets the error transaction and resets the error message.
// The argument is cloned before being stored in the Error.
func (e *Error) SetTxn(txn *Transaction) {
	e.UnexposedTxn = nil
	e.UpdateTxn(txn)
}

// UpdateTxn updates the error transaction and resets the error message.
// The argument is cloned before being stored in the Error.
func (e *Error) UpdateTxn(o *Transaction) {
	if o == nil {
		return
	}
	if e.UnexposedTxn == nil {
		e.UnexposedTxn = o.Clone()
	} else {
		e.UnexposedTxn.Update(o)
	}
	if sErr, ok := e.Detail.GetInner().(ErrorDetailInterface); ok {
		// Refresh the message as the txn is updated.
		e.Message = sErr.message(e)
	}
	e.checkTxnStatusValid()
}

// checkTxnStatusValid verifies that the transaction status is in-sync with the
// error detail.
func (e *Error) checkTxnStatusValid() {
	txn := e.UnexposedTxn
	err := e.Detail.GetInner()
	if txn == nil {
		return
	}
	if e.TransactionRestart == TransactionRestart_NONE {
		return
	}
	if errors.HasType(err, (*TransactionAbortedError)(nil)) {
		return
	}
	if txn.Status.IsFinalized() {
		log.Fatalf(context.TODO(), "transaction unexpectedly finalized in (%T): %v", err, e)
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
	var buf strings.Builder
	buf.WriteString(prefix)
	fmt.Fprintf(&buf, "r%d: ", e.RangeID)
	if e.Replica != (ReplicaDescriptor{}) {
		fmt.Fprintf(&buf, "replica %s not lease holder; ", e.Replica)
	} else {
		fmt.Fprint(&buf, "replica not lease holder; ")
	}
	if e.LeaseHolder == nil {
		fmt.Fprint(&buf, "lease holder unknown")
	} else if e.Lease != nil {
		fmt.Fprintf(&buf, "current lease is %s", e.Lease)
	} else {
		fmt.Fprintf(&buf, "replica %s is", *e.LeaseHolder)
	}
	return buf.String()
}

var _ ErrorDetailInterface = &NotLeaseHolderError{}

func (e *LeaseRejectedError) Error() string {
	return e.message(nil)
}

func (e *LeaseRejectedError) message(_ *Error) string {
	return fmt.Sprintf("cannot replace lease %s with %s: %s", e.Existing, e.Requested, e.Message)
}

var _ ErrorDetailInterface = &LeaseRejectedError{}

// NewRangeNotFoundError initializes a new RangeNotFoundError for the given RangeID and, optionally,
// a StoreID.
func NewRangeNotFoundError(rangeID RangeID, storeID StoreID) *RangeNotFoundError {
	return &RangeNotFoundError{
		RangeID: rangeID,
		StoreID: storeID,
	}
}

func (e *RangeNotFoundError) Error() string {
	return e.message(nil)
}

func (e *RangeNotFoundError) message(_ *Error) string {
	msg := fmt.Sprintf("r%d was not found", e.RangeID)
	if e.StoreID != 0 {
		msg += fmt.Sprintf(" on s%d", e.StoreID)
	}
	return msg
}

var _ ErrorDetailInterface = &RangeNotFoundError{}

// NewRangeKeyMismatchError initializes a new RangeKeyMismatchError.
//
// desc and lease represent info about the range that the request was
// erroneously routed to. lease can be nil. If it's not nil but the leaseholder
// is not part of desc, it is ignored. This allows callers to read the
// descriptor and lease non-atomically without worrying about incoherence.
//
// Note that more range info is commonly added to the error after the error is
// created.
func NewRangeKeyMismatchError(
	ctx context.Context, start, end Key, desc *RangeDescriptor, lease *Lease,
) *RangeKeyMismatchError {
	if desc == nil {
		panic("NewRangeKeyMismatchError with nil descriptor")
	}
	if !desc.IsInitialized() {
		// We must never send uninitialized ranges back to the client guard against
		// regressions of #6027.
		panic(fmt.Sprintf("descriptor is not initialized: %+v", desc))
	}
	var l Lease
	if lease != nil {
		// We ignore leases that are not part of the descriptor.
		_, ok := desc.GetReplicaDescriptorByID(lease.Replica.ReplicaID)
		if ok {
			l = *lease
		}
	}
	e := &RangeKeyMismatchError{
		RequestStartKey:           start,
		RequestEndKey:             end,
		DeprecatedMismatchedRange: *desc,
	}
	// More ranges are sometimes added to rangesInternal later.
	e.AppendRangeInfo(ctx, *desc, l)
	return e
}

func (e *RangeKeyMismatchError) Error() string {
	return e.message(nil)
}

func (e *RangeKeyMismatchError) message(_ *Error) string {
	desc := &e.Ranges()[0].Desc
	return fmt.Sprintf("key range %s-%s outside of bounds of range %s-%s; suggested ranges: %s",
		e.RequestStartKey, e.RequestEndKey, desc.StartKey, desc.EndKey, e.Ranges())
}

// Ranges returns the range info for the range that the request was erroneously
// routed to. It deals with legacy errors coming from 20.1 nodes by returning
// empty lease for the respective descriptors.
//
// At least one RangeInfo is returned.
func (e *RangeKeyMismatchError) Ranges() []RangeInfo {
	if len(e.rangesInternal) != 0 {
		return e.rangesInternal
	}
	// Fallback for 20.1 errors. Remove in 20.3.
	ranges := []RangeInfo{{Desc: e.DeprecatedMismatchedRange}}
	if e.DeprecatedSuggestedRange != nil {
		ranges = append(ranges, RangeInfo{Desc: *e.DeprecatedSuggestedRange})
	}
	return ranges
}

// AppendRangeInfo appends info about one range to the set returned to the
// kvclient.
//
// l can be empty. Otherwise, the leaseholder is asserted to be a replica in
// desc.
func (e *RangeKeyMismatchError) AppendRangeInfo(
	ctx context.Context, desc RangeDescriptor, l Lease,
) {
	if !l.Empty() {
		if _, ok := desc.GetReplicaDescriptorByID(l.Replica.ReplicaID); !ok {
			log.Fatalf(ctx, "lease names missing replica; lease: %s, desc: %s", l, desc)
		}
	}
	e.rangesInternal = append(e.rangesInternal, RangeInfo{
		Desc:  desc,
		Lease: l,
	})
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

// ClientVisibleAmbiguousError implements the ClientVisibleAmbiguousError interface.
func (e *AmbiguousResultError) ClientVisibleAmbiguousError() {}

var _ ErrorDetailInterface = &AmbiguousResultError{}
var _ ClientVisibleAmbiguousError = &AmbiguousResultError{}

func (e *TransactionAbortedError) Error() string {
	return fmt.Sprintf("TransactionAbortedError(%s)", e.Reason)
}

func (e *TransactionAbortedError) message(pErr *Error) string {
	return fmt.Sprintf("TransactionAbortedError(%s): %s", e.Reason, pErr.GetTxn())
}

func (*TransactionAbortedError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &TransactionAbortedError{}
var _ transactionRestartError = &TransactionAbortedError{}

// ClientVisibleRetryError implements the ClientVisibleRetryError interface.
func (e *TransactionRetryWithProtoRefreshError) ClientVisibleRetryError() {}

func (e *TransactionRetryWithProtoRefreshError) Error() string {
	return e.message(nil)
}

func (e *TransactionRetryWithProtoRefreshError) message(_ *Error) string {
	return fmt.Sprintf("TransactionRetryWithProtoRefreshError: %s", e.Msg)
}

var _ ClientVisibleRetryError = &TransactionRetryWithProtoRefreshError{}
var _ ErrorDetailInterface = &TransactionRetryWithProtoRefreshError{}

// NewTransactionAbortedError initializes a new TransactionAbortedError.
func NewTransactionAbortedError(reason TransactionAbortedReason) *TransactionAbortedError {
	return &TransactionAbortedError{
		Reason: reason,
	}
}

// NewTransactionRetryWithProtoRefreshError initializes a new TransactionRetryWithProtoRefreshError.
//
// txnID is the ID of the transaction being restarted.
// txn is the transaction that the client should use for the next attempts.
func NewTransactionRetryWithProtoRefreshError(
	msg string, txnID uuid.UUID, txn Transaction,
) *TransactionRetryWithProtoRefreshError {
	return &TransactionRetryWithProtoRefreshError{
		Msg:         msg,
		TxnID:       txnID,
		Transaction: txn,
	}
}

// PrevTxnAborted returns true if this error originated from a
// TransactionAbortedError. If true, the client will need to create a new
// transaction, as opposed to continuing with the existing one at a bumped
// epoch.
func (e *TransactionRetryWithProtoRefreshError) PrevTxnAborted() bool {
	return !e.TxnID.Equal(e.Transaction.ID)
}

// NewTransactionPushError initializes a new TransactionPushError.
func NewTransactionPushError(pusheeTxn Transaction) *TransactionPushError {
	// Note: this error will cause a txn restart. The error that the client
	// receives contains a txn that might have a modified priority.
	return &TransactionPushError{PusheeTxn: pusheeTxn}
}

func (e *TransactionPushError) Error() string {
	return e.message(nil)
}

func (e *TransactionPushError) message(pErr *Error) string {
	s := fmt.Sprintf("failed to push %s", e.PusheeTxn)
	if pErr.GetTxn() == nil {
		return s
	}
	return fmt.Sprintf("txn %s %s", pErr.GetTxn(), s)
}

func (*TransactionPushError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &TransactionPushError{}
var _ transactionRestartError = &TransactionPushError{}

// NewTransactionRetryError initializes a new TransactionRetryError.
func NewTransactionRetryError(
	reason TransactionRetryReason, extraMsg string,
) *TransactionRetryError {
	return &TransactionRetryError{
		Reason:   reason,
		ExtraMsg: extraMsg,
	}
}

func (e *TransactionRetryError) Error() string {
	msg := ""
	if e.ExtraMsg != "" {
		msg = " - " + e.ExtraMsg
	}
	return fmt.Sprintf("TransactionRetryError: retry txn (%s%s)", e.Reason, msg)
}

func (e *TransactionRetryError) message(pErr *Error) string {
	return fmt.Sprintf("%s: %s", e.Error(), pErr.GetTxn())
}

func (*TransactionRetryError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &TransactionRetryError{}
var _ transactionRestartError = &TransactionRetryError{}

// NewTransactionStatusError initializes a new TransactionStatusError from
// the given message.
func NewTransactionStatusError(msg string) *TransactionStatusError {
	return &TransactionStatusError{
		Msg:    msg,
		Reason: TransactionStatusError_REASON_UNKNOWN,
	}
}

// NewTransactionCommittedStatusError initializes a new TransactionStatusError
// with a REASON_TXN_COMMITTED.
func NewTransactionCommittedStatusError() *TransactionStatusError {
	return &TransactionStatusError{
		Msg:    "already committed",
		Reason: TransactionStatusError_REASON_TXN_COMMITTED,
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

// NewWriteTooOldError creates a new write too old error. The function accepts
// the timestamp of the operation that hit the error, along with the timestamp
// immediately after the existing write which had a higher timestamp and which
// caused the error.
func NewWriteTooOldError(operationTS, actualTS hlc.Timestamp) *WriteTooOldError {
	return &WriteTooOldError{
		Timestamp:       operationTS,
		ActualTimestamp: actualTS,
	}
}

func (e *WriteTooOldError) Error() string {
	return e.message(nil)
}

func (e *WriteTooOldError) message(_ *Error) string {
	return fmt.Sprintf("WriteTooOldError: write at timestamp %s too old; wrote at %s",
		e.Timestamp, e.ActualTimestamp)
}

func (*WriteTooOldError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &WriteTooOldError{}
var _ transactionRestartError = &WriteTooOldError{}

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
		rwue.ObservedTimestamps = txn.ObservedTimestamps
	}
	return rwue
}

func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return e.message(nil)
}

func (e *ReadWithinUncertaintyIntervalError) message(_ *Error) string {
	var ts strings.Builder
	ts.WriteByte('[')
	for i, ot := range observedTimestampSlice(e.ObservedTimestamps) {
		if i > 0 {
			ts.WriteByte(' ')
		}
		fmt.Fprintf(&ts, "{%d %v}", ot.NodeID, ot.Timestamp)
	}
	ts.WriteByte(']')

	return fmt.Sprintf("ReadWithinUncertaintyIntervalError: read at time %s encountered "+
		"previous write with future timestamp %s within uncertainty interval `t <= %v`; "+
		"observed timestamps: %s",
		e.ReadTimestamp, e.ExistingTimestamp, e.MaxTimestamp, ts.String())
}

func (*ReadWithinUncertaintyIntervalError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &ReadWithinUncertaintyIntervalError{}
var _ transactionRestartError = &ReadWithinUncertaintyIntervalError{}

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

// NewReplicaCorruptionError creates a new error indicating a corrupt replica.
// The supplied error is used to provide additional detail in the error message.
func NewReplicaCorruptionError(err error) *ReplicaCorruptionError {
	return &ReplicaCorruptionError{ErrorMsg: err.Error()}
}

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

func (e *TxnAlreadyEncounteredErrorError) Error() string {
	return e.message(nil)
}

func (e *TxnAlreadyEncounteredErrorError) message(_ *Error) string {
	return fmt.Sprintf(
		"txn already encountered an error; cannot be used anymore (previous err: %s)",
		e.PrevError,
	)
}

var _ ErrorDetailInterface = &TxnAlreadyEncounteredErrorError{}

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

func (e *BatchTimestampBeforeGCError) Error() string {
	return e.message(nil)
}

func (e *BatchTimestampBeforeGCError) message(_ *Error) string {
	return fmt.Sprintf("batch timestamp %v must be after replica GC threshold %v", e.Timestamp, e.Threshold)
}

var _ ErrorDetailInterface = &BatchTimestampBeforeGCError{}

// NewIntentMissingError creates a new IntentMissingError.
func NewIntentMissingError(key Key, wrongIntent *Intent) *IntentMissingError {
	return &IntentMissingError{
		Key:         key,
		WrongIntent: wrongIntent,
	}
}

func (e *IntentMissingError) Error() string {
	return e.message(nil)
}

func (e *IntentMissingError) message(_ *Error) string {
	var detail string
	if e.WrongIntent != nil {
		detail = fmt.Sprintf("; found intent %v at key instead", e.WrongIntent)
	}
	return fmt.Sprintf("intent missing%s", detail)
}

func (*IntentMissingError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &IntentMissingError{}
var _ transactionRestartError = &IntentMissingError{}

func (e *MergeInProgressError) Error() string {
	return e.message(nil)
}

func (e *MergeInProgressError) message(_ *Error) string {
	return "merge in progress"
}

var _ ErrorDetailInterface = &MergeInProgressError{}

// NewRangeFeedRetryError initializes a new RangeFeedRetryError.
func NewRangeFeedRetryError(reason RangeFeedRetryError_Reason) *RangeFeedRetryError {
	return &RangeFeedRetryError{
		Reason: reason,
	}
}

func (e *RangeFeedRetryError) Error() string {
	return e.message(nil)
}

func (e *RangeFeedRetryError) message(pErr *Error) string {
	return fmt.Sprintf("retry rangefeed (%s)", e.Reason)
}

var _ ErrorDetailInterface = &RangeFeedRetryError{}

// NewIndeterminateCommitError initializes a new IndeterminateCommitError.
func NewIndeterminateCommitError(txn Transaction) *IndeterminateCommitError {
	return &IndeterminateCommitError{StagingTxn: txn}
}

func (e *IndeterminateCommitError) Error() string {
	return e.message(nil)
}

func (e *IndeterminateCommitError) message(pErr *Error) string {
	s := fmt.Sprintf("found txn in indeterminate STAGING state %s", e.StagingTxn)
	if pErr.GetTxn() == nil {
		return s
	}
	return fmt.Sprintf("txn %s %s", pErr.GetTxn(), s)
}

var _ ErrorDetailInterface = &IndeterminateCommitError{}

// IsRangeNotFoundError returns true if err contains a *RangeNotFoundError.
func IsRangeNotFoundError(err error) bool {
	return errors.HasType(err, (*RangeNotFoundError)(nil))
}
