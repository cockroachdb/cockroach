// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	_ "github.com/cockroachdb/errors/extgrpc" // register EncodeError support for gRPC Status
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/proto"
)

// Printer is an interface that lets us use what's common between the
// errors.Printer interface and redact.SafePrinter so we can write functions
// that both SafeFormatError and SafeFormat can share.
type Printer interface {
	// Print appends args to the message output.
	Print(args ...interface{})

	// Printf writes a formatted string.
	Printf(format string, args ...interface{})
}

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
	return e.String()
}

var _ error = &UnhandledRetryableError{}

func (e *UnhandledRetryableError) SafeFormatError(p errors.Printer) (next error) {
	p.Print(e.PErr)
	return nil
}

// SafeFormat implements redact.SafeFormatter.
func (e *UnhandledRetryableError) SafeFormat(s redact.SafePrinter, r rune) {
	e.PErr.SafeFormat(s, r)
}

func (e *UnhandledRetryableError) String() string {
	return redact.StringWithoutMarkers(e)
}

// transactionRestartError is an interface implemented by errors that cause
// a transaction to be restarted.
type transactionRestartError interface {
	canRestartTransaction() TransactionRestart
}

// ErrorUnexpectedlySet creates a string to panic with when a response (typically
// a kvpb.BatchResponse) unexpectedly has Error set in its response header.
func ErrorUnexpectedlySet(culprit, response interface{}) error {
	return errors.AssertionFailedf("error is unexpectedly set, culprit is %T:\n%+v", culprit, response)
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
	// after ConditionFailedErrors (uniqueness violations) and LockConflictError
	// (lock not available errors). With continuing after errors it's important
	// for the coordinator to track the timestamp at which intents might have been
	// written.
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
	// TODO(tbg): this method could take an `*Error` if it weren't for SQL
	// propagating these as an `error`. See `DistSQLReceiver.Push`.
	var detail ErrorDetailInterface
	switch tErr := err.(type) {
	case nil:
		return 0
	case ErrorDetailInterface:
		detail = tErr
	case *internalError:
		detail = (*Error)(tErr).GetDetail()
	case *UnhandledRetryableError:
		if _, ok := tErr.PErr.GetDetail().(*TransactionAbortedError); ok {
			return ErrorScoreTxnAbort
		}
		return ErrorScoreTxnRestart
	}

	switch v := detail.(type) {
	case *TransactionRetryWithProtoRefreshError:
		if v.PrevTxnAborted() {
			return ErrorScoreTxnAbort
		}
		return ErrorScoreTxnRestart
	case *ConditionFailedError, *WriteIntentError:
		// We particularly care about returning the low ErrorScoreUnambiguousError
		// because we don't want to transition a transaction that encounters a
		// ConditionFailedError or a WriteIntentError to an error state. More
		// specifically, we want to allow rollbacks to savepoint after one of these
		// errors.
		return ErrorScoreUnambiguousError
	}
	return ErrorScoreNonRetriable
}

// NewError creates an Error from the given error.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	e := &Error{
		EncodedError: errors.EncodeError(context.Background(), err),
	}
	return e
}

// NewErrorWithTxn creates an Error from the given error and a transaction.
//
// txn is cloned before being stored in Error.
func NewErrorWithTxn(err error, txn *roachpb.Transaction) *Error {
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

// SafeFormat implements redact.SafeFormatter.
func (e *Error) SafeFormat(s redact.SafePrinter, _ rune) {
	if e == nil {
		s.Print(nil)
		return
	}

	s.Print(errors.DecodeError(context.Background(), e.EncodedError))

	if txn := e.GetTxn(); txn != nil {
		s.SafeString(": ")
		s.Print(txn)
	}
}

func (e *Error) SafeFormatError(p errors.Printer) (next error) {
	if e == nil {
		p.Print(nil)
		return
	}

	p.Print(errors.DecodeError(context.Background(), e.EncodedError))

	if txn := e.GetTxn(); txn != nil {
		p.Printf(": %v", txn)
	}
	return nil
}

// String implements fmt.Stringer.
func (e *Error) String() string {
	return redact.StringWithoutMarkers(e)
}

// TransactionRestart returns the TransactionRestart for this Error.
func (e *Error) TransactionRestart() TransactionRestart {
	if e.EncodedError.IsSet() {
		var iface transactionRestartError
		if errors.As(errors.DecodeError(context.Background(), e.EncodedError), &iface) {
			return iface.canRestartTransaction()
		}
	}
	return TransactionRestart_NONE
}

type internalError Error

func (e *internalError) Error() string {
	return (*Error)(e).String()
}

// ErrorDetailInterface is an interface for each error detail.
// These must not be implemented by anything other than our protobuf-backed error details
// as we rely on a 1:1 correspondence between the interface and what can be stored via
// `Error.DeprecatedSetDetail`.
type ErrorDetailInterface interface {
	error
	protoutil.Message
	// Type returns the error's type.
	Type() ErrorDetailType
}

// ErrorDetailType identifies the type of KV error.
type ErrorDetailType int

// This lists all ErrorDetail types. The numeric values in this list are used to
// identify corresponding timeseries.
//
//go:generate stringer -type=ErrorDetailType
const (
	NotLeaseHolderErrType                   ErrorDetailType = 1
	RangeNotFoundErrType                    ErrorDetailType = 2
	RangeKeyMismatchErrType                 ErrorDetailType = 3
	ReadWithinUncertaintyIntervalErrType    ErrorDetailType = 4
	TransactionAbortedErrType               ErrorDetailType = 5
	TransactionPushErrType                  ErrorDetailType = 6
	TransactionRetryErrType                 ErrorDetailType = 7
	TransactionStatusErrType                ErrorDetailType = 8
	WriteIntentErrType                      ErrorDetailType = 9
	WriteTooOldErrType                      ErrorDetailType = 10
	OpRequiresTxnErrType                    ErrorDetailType = 11
	ConditionFailedErrType                  ErrorDetailType = 12
	LeaseRejectedErrType                    ErrorDetailType = 13
	NodeUnavailableErrType                  ErrorDetailType = 14
	RaftGroupDeletedErrType                 ErrorDetailType = 16
	ReplicaCorruptionErrType                ErrorDetailType = 17
	ReplicaTooOldErrType                    ErrorDetailType = 18
	AmbiguousResultErrType                  ErrorDetailType = 26
	StoreNotFoundErrType                    ErrorDetailType = 27
	TransactionRetryWithProtoRefreshErrType ErrorDetailType = 28
	IntegerOverflowErrType                  ErrorDetailType = 31
	UnsupportedRequestErrType               ErrorDetailType = 32
	BatchTimestampBeforeGCErrType           ErrorDetailType = 34
	TxnAlreadyEncounteredErrType            ErrorDetailType = 35
	IntentMissingErrType                    ErrorDetailType = 36
	MergeInProgressErrType                  ErrorDetailType = 37
	RangeFeedRetryErrType                   ErrorDetailType = 38
	IndeterminateCommitErrType              ErrorDetailType = 39
	InvalidLeaseErrType                     ErrorDetailType = 40
	OptimisticEvalConflictsErrType          ErrorDetailType = 41
	MinTimestampBoundUnsatisfiableErrType   ErrorDetailType = 42
	RefreshFailedErrType                    ErrorDetailType = 43
	MVCCHistoryMutationErrType              ErrorDetailType = 44
	LockConflictErrType                     ErrorDetailType = 45
	ReplicaUnavailableErrType               ErrorDetailType = 46
	ProxyFailedErrType                      ErrorDetailType = 47
	// When adding new error types, don't forget to update NumErrors below.

	// CommunicationErrType indicates a gRPC error; this is not an ErrorDetail.
	// The value 22 is chosen because it's reserved in the errors proto.
	CommunicationErrType ErrorDetailType = 22
	// InternalErrType indicates a pErr that doesn't contain a recognized error
	// detail. The value 25 is chosen because it's reserved in the errors proto.
	InternalErrType ErrorDetailType = 25

	NumErrors int = 48
)

// Register the migration of all errors that used to be in the roachpb package
// and are now in the kv/kvpb package.
func init() {
	roachpbPath := reflect.TypeOf(roachpb.Key("")).PkgPath()
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.UnhandledRetryableError", &UnhandledRetryableError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.internalError", &internalError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.NotLeaseHolderError", &NotLeaseHolderError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.RangeNotFoundError", &RangeNotFoundError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.RangeKeyMismatchError", &RangeKeyMismatchError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.ReadWithinUncertaintyIntervalError", &ReadWithinUncertaintyIntervalError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.TransactionAbortedError", &TransactionAbortedError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.TransactionPushError", &TransactionPushError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.TransactionRetryError", &TransactionRetryError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.TransactionStatusError", &TransactionStatusError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.WriteIntentError", &WriteIntentError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.WriteTooOldError", &WriteTooOldError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.OpRequiresTxnError", &OpRequiresTxnError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.ConditionFailedError", &ConditionFailedError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.LeaseRejectedError", &LeaseRejectedError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.NodeUnavailableError", &NodeUnavailableError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.RaftGroupDeletedError", &RaftGroupDeletedError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.ReplicaCorruptionError", &ReplicaCorruptionError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.ReplicaTooOldError", &ReplicaTooOldError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.StoreNotFoundError", &StoreNotFoundError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.TransactionRetryWithProtoRefreshError", &TransactionRetryWithProtoRefreshError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.IntegerOverflowError", &IntegerOverflowError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.UnsupportedRequestError", &UnsupportedRequestError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.BatchTimestampBeforeGCError", &BatchTimestampBeforeGCError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.TxnAlreadyEncounteredErrorError", &TxnAlreadyEncounteredErrorError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.IntentMissingError", &IntentMissingError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.MergeInProgressError", &MergeInProgressError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.RangeFeedRetryError", &RangeFeedRetryError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.IndeterminateCommitError", &IndeterminateCommitError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.InvalidLeaseError", &InvalidLeaseError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.OptimisticEvalConflictsError", &OptimisticEvalConflictsError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.MinTimestampBoundUnsatisfiableError", &MinTimestampBoundUnsatisfiableError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.RefreshFailedError", &RefreshFailedError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.MVCCHistoryMutationError", &MVCCHistoryMutationError{})
	errors.RegisterTypeMigration(roachpbPath, "*roachpb.InsufficientSpaceError", &InsufficientSpaceError{})
}

// GoError returns a Go error converted from Error. If the error is a transaction
// retry error, it returns the error itself wrapped in an UnhandledRetryableError.
// Otherwise, if an error detail is present, is is returned (i.e. the result will
// match GetDetail()). Otherwise, returns the error itself masqueraded as an `error`.
func (e *Error) GoError() error {
	if e == nil {
		return nil
	}
	if e.EncodedError.IsSet() {
		err := errors.DecodeError(context.Background(), e.EncodedError)
		var iface transactionRestartError
		if errors.As(err, &iface) {
			if txnRestart := iface.canRestartTransaction(); txnRestart != TransactionRestart_NONE {
				// TODO(tbg): revisit this unintuitive error wrapping here and see if
				// a better solution can be found.
				return &UnhandledRetryableError{
					PErr: *e,
				}
			}
		}
		return err
	}

	// Everything below is legacy behavior that can be deleted in 21.2.
	if e.TransactionRestart() != TransactionRestart_NONE {
		return &UnhandledRetryableError{
			PErr: *e,
		}
	}
	if detail := e.GetDetail(); detail != nil {
		return detail
	}
	return (*internalError)(e)
}

// GetDetail returns an error detail associated with the error, or nil otherwise.
func (e *Error) GetDetail() ErrorDetailInterface {
	if e == nil || !e.EncodedError.IsSet() {
		return nil
	}
	var detail ErrorDetailInterface
	errors.As(errors.DecodeError(context.Background(), e.EncodedError), &detail)
	return detail
}

// SetTxn sets the error transaction and resets the error message.
// The argument is cloned before being stored in the Error.
func (e *Error) SetTxn(txn *roachpb.Transaction) {
	e.UnexposedTxn = nil
	e.UpdateTxn(txn)
}

// UpdateTxn updates the error transaction and resets the error message.
// The argument is cloned before being stored in the Error.
func (e *Error) UpdateTxn(o *roachpb.Transaction) {
	if o == nil {
		return
	}
	if e.UnexposedTxn == nil {
		e.UnexposedTxn = o.Clone()
	} else {
		e.UnexposedTxn.Update(o)
	}
	e.checkTxnStatusValid()
}

// checkTxnStatusValid verifies that the transaction status is in-sync with the
// error detail.
func (e *Error) checkTxnStatusValid() {
	txn := e.UnexposedTxn
	err := e.GetDetail()
	if txn == nil {
		return
	}
	if errors.HasType(err, (*TransactionAbortedError)(nil)) {
		return
	}
	if e.TransactionRestart() == TransactionRestart_NONE {
		return
	}
	if txn.Status.IsFinalized() {
		log.Fatalf(context.TODO(), "transaction unexpectedly finalized in (%T): %v", err, e)
	}
}

// GetTxn returns the txn.
func (e *Error) GetTxn() *roachpb.Transaction {
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
	return redact.Sprint(e).StripMarkers()
}

func (e *NodeUnavailableError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("node unavailable; try another peer")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *NodeUnavailableError) Type() ErrorDetailType {
	return NodeUnavailableErrType
}

var _ ErrorDetailInterface = &NodeUnavailableError{}

func (e *NotLeaseHolderError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

// Type is part of the ErrorDetailInterface.
func (e *NotLeaseHolderError) Type() ErrorDetailType {
	return NotLeaseHolderErrType
}

func (e *NotLeaseHolderError) printError(s Printer) {
	s.Printf("[NotLeaseHolderError] ")
	if e.CustomMsg != "" {
		s.Print(e.CustomMsg)
		s.Printf("; ")
	}
	s.Printf("r%d: ", e.RangeID)
	if e.Replica != (roachpb.ReplicaDescriptor{}) {
		s.Printf("replica %s not lease holder; ", e.Replica)
	} else {
		s.Printf("replica not lease holder; ")
	}
	if e.Lease != nil {
		s.Printf("current lease is %s", e.Lease)
	} else {
		s.Printf("lease holder unknown")
	}
}

func (e *NotLeaseHolderError) SafeFormatError(p errors.Printer) (next error) {
	e.printError(p)
	return nil
}

var _ ErrorDetailInterface = &NotLeaseHolderError{}

// Type is part of the ErrorDetailInterface.
func (e *LeaseRejectedError) Type() ErrorDetailType {
	return LeaseRejectedErrType
}

func (e *LeaseRejectedError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *LeaseRejectedError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("cannot replace lease %s with %s: %s", e.Existing, e.Requested, e.Message)
	return nil
}

var _ ErrorDetailInterface = &LeaseRejectedError{}

// NewRangeNotFoundError initializes a new RangeNotFoundError for the given RangeID and, optionally,
// a StoreID.
func NewRangeNotFoundError(rangeID roachpb.RangeID, storeID roachpb.StoreID) *RangeNotFoundError {
	return &RangeNotFoundError{
		RangeID: rangeID,
		StoreID: storeID,
	}
}

func (e *RangeNotFoundError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *RangeNotFoundError) printError(s Printer) {
	s.Printf("r%d was not found", e.RangeID)
	if e.StoreID != 0 {
		s.Printf(" on s%d", e.StoreID)
	}
}

func (e *RangeNotFoundError) SafeFormatError(p errors.Printer) (next error) {
	e.printError(p)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *RangeNotFoundError) Type() ErrorDetailType {
	return RangeNotFoundErrType
}

var _ ErrorDetailInterface = &RangeNotFoundError{}

// IsRangeNotFoundError returns true if err contains a *RangeNotFoundError.
func IsRangeNotFoundError(err error) bool {
	return errors.HasType(err, (*RangeNotFoundError)(nil))
}

// NewRangeKeyMismatchErrorWithCTPolicy initializes a new RangeKeyMismatchError.
// identical to NewRangeKeyMismatchError, with the given ClosedTimestampPolicy.
func NewRangeKeyMismatchErrorWithCTPolicy(
	ctx context.Context,
	start, end roachpb.Key,
	desc *roachpb.RangeDescriptor,
	lease *roachpb.Lease,
	ctPolicy roachpb.RangeClosedTimestampPolicy,
) *RangeKeyMismatchError {
	if desc == nil {
		panic("NewRangeKeyMismatchError with nil descriptor")
	}
	if !desc.IsInitialized() {
		// We must never send uninitialized ranges back to the client guard against
		// regressions of #6027.
		panic(fmt.Sprintf("descriptor is not initialized: %+v", desc))
	}
	var l roachpb.Lease
	if lease != nil {
		// We ignore leases that are not part of the descriptor.
		_, ok := desc.GetReplicaDescriptorByID(lease.Replica.ReplicaID)
		if ok {
			l = *lease
		}
	}
	e := &RangeKeyMismatchError{
		RequestStartKey: start,
		RequestEndKey:   end,
	}
	ri := roachpb.RangeInfo{
		Desc:                  *desc,
		Lease:                 l,
		ClosedTimestampPolicy: ctPolicy,
	}
	// More ranges are sometimes added to rangesInternal later.
	e.AppendRangeInfo(ctx, ri)
	return e
}

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
	ctx context.Context, start, end roachpb.Key, desc *roachpb.RangeDescriptor, lease *roachpb.Lease,
) *RangeKeyMismatchError {
	return NewRangeKeyMismatchErrorWithCTPolicy(ctx,
		start,
		end,
		desc,
		lease,
		roachpb.LAG_BY_CLUSTER_SETTING, /* default closed timestsamp policy*/
	)
}

func (e *RangeKeyMismatchError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *RangeKeyMismatchError) printError(s Printer) {
	mr, err := e.MismatchedRange()
	if err != nil {
		s.Print(err)
	}
	s.Printf("key range %s-%s outside of bounds of range %s-%s; suggested ranges: %s",
		e.RequestStartKey, e.RequestEndKey, mr.Desc.StartKey, mr.Desc.EndKey, e.Ranges)
}

func (e *RangeKeyMismatchError) SafeFormatError(p errors.Printer) (next error) {
	e.printError(p)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *RangeKeyMismatchError) Type() ErrorDetailType {
	return RangeKeyMismatchErrType
}

// MismatchedRange returns the range info for the range that the request was
// erroneously routed to, or an error if the Ranges slice is empty.
func (e *RangeKeyMismatchError) MismatchedRange() (roachpb.RangeInfo, error) {
	if len(e.Ranges) == 0 {
		return roachpb.RangeInfo{}, errors.AssertionFailedf(
			"RangeKeyMismatchError (key range %s-%s) with empty RangeInfo slice", e.RequestStartKey, e.RequestEndKey,
		)
	}
	return e.Ranges[0], nil
}

// AppendRangeInfo appends info about a group of ranges to the set returned to the
// kvclient.
//
// l can be empty. Otherwise, the leaseholder is asserted to be a replica in
// desc.
func (e *RangeKeyMismatchError) AppendRangeInfo(ctx context.Context, ris ...roachpb.RangeInfo) {
	for _, ri := range ris {
		if !ri.Lease.Empty() {
			if _, ok := ri.Desc.GetReplicaDescriptorByID(ri.Lease.Replica.ReplicaID); !ok {
				log.Fatalf(ctx, "lease names missing replica; lease: %s, desc: %s", ri.Lease, ri.Desc)
			}
		}
		e.Ranges = append(e.Ranges, ri)
	}
}

var _ ErrorDetailInterface = &RangeKeyMismatchError{}

// ClientVisibleAmbiguousError implements the ClientVisibleAmbiguousError interface.
func (e *AmbiguousResultError) ClientVisibleAmbiguousError() {}

var _ ErrorDetailInterface = &AmbiguousResultError{}
var _ ClientVisibleAmbiguousError = &AmbiguousResultError{}

func (e *TransactionAbortedError) Error() string {
	return fmt.Sprintf("TransactionAbortedError(%s)", e.Reason)
}

func (*TransactionAbortedError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Type is part of the ErrorDetailInterface.
func (e *TransactionAbortedError) Type() ErrorDetailType {
	return TransactionAbortedErrType
}

var _ ErrorDetailInterface = &TransactionAbortedError{}
var _ transactionRestartError = &TransactionAbortedError{}

// ClientVisibleRetryError implements the ClientVisibleRetryError interface.
func (e *TransactionRetryWithProtoRefreshError) ClientVisibleRetryError() {}

func (e *TransactionRetryWithProtoRefreshError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

// Type is part of the ErrorDetailInterface.
func (e *TransactionRetryWithProtoRefreshError) Type() ErrorDetailType {
	return TransactionRetryWithProtoRefreshErrType
}

var _ ClientVisibleRetryError = &TransactionRetryWithProtoRefreshError{}
var _ ErrorDetailInterface = &TransactionRetryWithProtoRefreshError{}

// NewTransactionAbortedError initializes a new TransactionAbortedError.
func NewTransactionAbortedError(reason TransactionAbortedReason) *TransactionAbortedError {
	return &TransactionAbortedError{
		Reason: reason,
	}
}

func (e *TransactionAbortedError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("TransactionAbortedError(%s)", redact.SafeString(TransactionAbortedReason_name[int32(e.Reason)]))
	return nil
}

type retryErrOptions struct {
	conflictingTxn *enginepb.TxnMeta
}

// RetryErrOption is used to annotate optional fields in retry related errors.
type RetryErrOption interface {
	apply(*retryErrOptions)
}

type retryErrOptionFunc func(*retryErrOptions)

func (f retryErrOptionFunc) apply(o *retryErrOptions) {
	f(o)
}

// WithConflictingTxn is used to annotate a retry error with the conflicting
// transaction metadata (optional). This is only used for cases where a refresh
// fails with `REASON_INTENT`.
func WithConflictingTxn(txn *enginepb.TxnMeta) RetryErrOption {
	return retryErrOptionFunc(func(o *retryErrOptions) {
		o.conflictingTxn = txn
	})
}

// NewTransactionRetryWithProtoRefreshError initializes a new
// TransactionRetryWithProtoRefreshError.
//
// prevTxnID is the ID of the transaction being retried.
// prevTxnEpoch is the epoch of the transaction being retried.
// nextTxn is the transaction that the client should use for the next attempts.
//
// TODO(tbg): the message passed here is usually pErr.String(), which is a bad
// pattern (loses structure, thus redaction). We can leverage error chaining
// to improve this: wrap `pErr.GoError()` with a barrier and then with the
// TransactionRetryWithProtoRefreshError.
func NewTransactionRetryWithProtoRefreshError(
	msg redact.RedactableString,
	prevTxnID uuid.UUID,
	prevTxnEpoch enginepb.TxnEpoch,
	nextTxn roachpb.Transaction,
	opts ...RetryErrOption,
) *TransactionRetryWithProtoRefreshError {
	options := retryErrOptions{}
	for _, o := range opts {
		o.apply(&options)
	}
	return &TransactionRetryWithProtoRefreshError{
		Msg:             msg.StripMarkers(),
		MsgRedactable:   msg,
		PrevTxnID:       prevTxnID,
		PrevTxnEpoch:    prevTxnEpoch,
		NextTransaction: nextTxn,
		ConflictingTxn:  options.conflictingTxn,
	}
}

func (e *TransactionRetryWithProtoRefreshError) SafeFormatError(p errors.Printer) (next error) {
	if e.MsgRedactable != "" {
		p.Printf("TransactionRetryWithProtoRefreshError: %s", e.MsgRedactable)
	} else {
		p.Printf("TransactionRetryWithProtoRefreshError: %s", e.Msg)
	}
	return nil
}

// PrevTxnAborted returns true if this error originated from a
// TransactionAbortedError. If true, the client will need to create a new
// transaction, as opposed to continuing with the existing one at a bumped
// epoch.
func (e *TransactionRetryWithProtoRefreshError) PrevTxnAborted() bool {
	return !e.PrevTxnID.Equal(e.NextTransaction.ID)
}

// PrevTxnEpochBumped returns true if the previous transaction was not aborted
// but its epoch was bumped. In this case, the client can continue with the
// existing transaction, but must restart from the beginning because its writes
// were discarded.
//
// NOTE: the method panics if the previous transaction was aborted and the next
// transaction has a different identity. Callers must first check PrevTxnAborted.
func (e *TransactionRetryWithProtoRefreshError) PrevTxnEpochBumped() bool {
	if e.PrevTxnAborted() {
		panic("PrevTxnEpochBumped called on aborted txn")
	}
	return e.PrevTxnEpoch != e.NextTransaction.Epoch
}

// TxnMustRestartFromBeginning returns true if the previous transaction's writes
// were discarded due to the retry error, meaning that it must restart from the
// beginning.
func (e *TransactionRetryWithProtoRefreshError) TxnMustRestartFromBeginning() bool {
	return e.PrevTxnAborted() || e.PrevTxnEpochBumped()
}

// NewTransactionPushError initializes a new TransactionPushError.
func NewTransactionPushError(pusheeTxn roachpb.Transaction) *TransactionPushError {
	// Note: this error will cause a txn restart. The error that the client
	// receives contains a txn that might have a modified priority.
	return &TransactionPushError{PusheeTxn: pusheeTxn}
}

func (e *TransactionPushError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *TransactionPushError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("failed to push %v", e.PusheeTxn)
	return nil
}

func (*TransactionPushError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Type is part of the ErrorDetailInterface.
func (e *TransactionPushError) Type() ErrorDetailType {
	return TransactionPushErrType
}

var _ ErrorDetailInterface = &TransactionPushError{}
var _ transactionRestartError = &TransactionPushError{}

// NewTransactionRetryError initializes a new TransactionRetryError.
func NewTransactionRetryError(
	reason TransactionRetryReason, extraMsg redact.RedactableString, opts ...RetryErrOption,
) *TransactionRetryError {
	options := retryErrOptions{}
	for _, o := range opts {
		o.apply(&options)
	}
	return &TransactionRetryError{
		Reason:             reason,
		ExtraMsg:           extraMsg.StripMarkers(),
		ExtraMsgRedactable: extraMsg,
		ConflictingTxn:     options.conflictingTxn,
	}
}

func (e *TransactionRetryError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *TransactionRetryError) SafeFormatError(p errors.Printer) (next error) {
	var msg redact.RedactableString = ""
	if e.ExtraMsgRedactable != "" {
		msg = redact.Sprintf(" - %s", e.ExtraMsgRedactable)
	} else if e.ExtraMsg != "" {
		msg = redact.Sprintf(" - %s", e.ExtraMsg)
	}
	if e.ConflictingTxn != nil {
		msg = redact.Sprintf("%s - conflicting txn: meta={%s}", msg, e.ConflictingTxn.String())
	}
	p.Printf("TransactionRetryError: retry txn (%s%s)", redact.SafeString(TransactionRetryReason_name[int32(e.Reason)]), msg)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *TransactionRetryError) Type() ErrorDetailType {
	return TransactionRetryErrType
}

func (*TransactionRetryError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &TransactionRetryError{}
var _ transactionRestartError = &TransactionRetryError{}

// NewTransactionStatusError initializes a new TransactionStatusError with
// the given message and reason.
func NewTransactionStatusError(
	reason TransactionStatusError_Reason, msg redact.RedactableString,
) *TransactionStatusError {
	return &TransactionStatusError{
		Msg:           msg.StripMarkers(),
		MsgRedactable: msg,
		Reason:        reason,
	}
}

func (e *TransactionStatusError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

// Type is part of the ErrorDetailInterface.
func (e *TransactionStatusError) Type() ErrorDetailType {
	return TransactionStatusErrType
}

func (e *TransactionStatusError) SafeFormatError(p errors.Printer) (next error) {
	if e.MsgRedactable != "" {
		p.Printf("TransactionStatusError: %s (%s)", e.MsgRedactable, redact.Safe(e.Reason))
	} else {
		p.Printf("TransactionStatusError: %s (%s)", e.Msg, redact.Safe(e.Reason))
	}
	return nil
}

var _ ErrorDetailInterface = &TransactionStatusError{}

func (e *LockConflictError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *LockConflictError) SafeFormatError(p errors.Printer) (next error) {
	e.printError(p)
	return nil
}

func (e *LockConflictError) printError(buf Printer) {
	printConflictingLocks(buf, e.Locks)
}

// Type is part of the ErrorDetailInterface.
func (e *LockConflictError) Type() ErrorDetailType {
	return LockConflictErrType
}

var _ ErrorDetailInterface = &LockConflictError{}

func (e *WriteIntentError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *WriteIntentError) SafeFormatError(p errors.Printer) (next error) {
	e.printError(p)
	return nil
}

func (e *WriteIntentError) printError(buf Printer) {
	printConflictingLocks(buf, e.Locks)

	switch e.Reason {
	case WriteIntentError_REASON_UNSPECIFIED:
		// Nothing to say.
	case WriteIntentError_REASON_WAIT_POLICY:
		buf.Printf(" [reason=wait_policy]")
	case WriteIntentError_REASON_LOCK_TIMEOUT:
		buf.Printf(" [reason=lock_timeout]")
	case WriteIntentError_REASON_LOCK_WAIT_QUEUE_MAX_LENGTH_EXCEEDED:
		buf.Printf(" [reason=lock_wait_queue_max_length_exceeded]")
	default:
		// Could panic, better to silently ignore in case new reasons are added.
	}
}

// Type is part of the ErrorDetailInterface.
func (e *WriteIntentError) Type() ErrorDetailType {
	return WriteIntentErrType
}

var _ ErrorDetailInterface = &WriteIntentError{}

func printConflictingLocks(buf Printer, locks []roachpb.Lock) {
	buf.Printf("conflicting locks on ")

	// If we have a lot of locks, we only want to show the first and the last.
	const maxBegin = 5
	const maxEnd = 5
	var begin, end []roachpb.Lock
	if len(locks) <= maxBegin+maxEnd {
		begin = locks
	} else {
		begin = locks[0:maxBegin]
		end = locks[len(locks)-maxEnd:]
	}

	for i := range begin {
		if i > 0 {
			buf.Printf(", ")
		}
		buf.Print(begin[i].Key)
	}
	if end != nil {
		buf.Printf(" ... ")
		for i := range end {
			if i > 0 {
				buf.Printf(", ")
			}
			buf.Print(end[i].Key)
		}
	}
}

// NewWriteTooOldError creates a new write too old error. The function accepts
// the timestamp of the operation that hit the error, along with the timestamp
// immediately after the existing write which had a higher timestamp and which
// caused the error. An optional Key parameter is accepted to denote one key
// where this error was encountered.
func NewWriteTooOldError(operationTS, actualTS hlc.Timestamp, key roachpb.Key) *WriteTooOldError {
	if len(key) > 0 {
		oldKey := key
		key = make([]byte, len(oldKey))
		copy(key, oldKey)
	}
	return &WriteTooOldError{
		Timestamp:       operationTS,
		ActualTimestamp: actualTS,
		Key:             key,
	}
}

func (e *WriteTooOldError) SafeFormatError(p errors.Printer) (next error) {
	if len(e.Key) > 0 {
		p.Printf("WriteTooOldError: write for key %s at timestamp %s too old; must write at or above %s",
			e.Key, e.Timestamp, e.ActualTimestamp)
		return nil
	}
	p.Printf("WriteTooOldError: write at timestamp %s too old; must write at or above %s",
		e.Timestamp, e.ActualTimestamp)
	return nil
}

func (e *WriteTooOldError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (*WriteTooOldError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// Type is part of the ErrorDetailInterface.
func (e *WriteTooOldError) Type() ErrorDetailType {
	return WriteTooOldErrType
}

// RetryTimestamp returns the timestamp that should be used to retry an
// operation after encountering a WriteTooOldError.
func (e *WriteTooOldError) RetryTimestamp() hlc.Timestamp {
	return e.ActualTimestamp
}

var _ ErrorDetailInterface = &WriteTooOldError{}
var _ transactionRestartError = &WriteTooOldError{}

// NewReadWithinUncertaintyIntervalError creates a new uncertainty retry error.
// The read and value timestamps as well as the txn are purely informational and
// used for formatting the error message.
func NewReadWithinUncertaintyIntervalError(
	readTS hlc.Timestamp,
	localUncertaintyLimit hlc.ClockTimestamp,
	txn *roachpb.Transaction,
	valueTS hlc.Timestamp,
	localTS hlc.ClockTimestamp,
) *ReadWithinUncertaintyIntervalError {
	var globalUncertaintyLimit hlc.Timestamp
	var observedTSs []roachpb.ObservedTimestamp
	if txn != nil {
		globalUncertaintyLimit = txn.GlobalUncertaintyLimit
		observedTSs = txn.ObservedTimestamps
	}
	return &ReadWithinUncertaintyIntervalError{
		// Information about the reader.
		ReadTimestamp:          readTS,
		LocalUncertaintyLimit:  localUncertaintyLimit,
		GlobalUncertaintyLimit: globalUncertaintyLimit,
		ObservedTimestamps:     observedTSs,
		// Information about the uncertain value.
		ValueTimestamp: valueTS,
		LocalTimestamp: localTS,
	}
}

// SafeFormat implements redact.SafeFormatter.
func (e *ReadWithinUncertaintyIntervalError) SafeFormat(s redact.SafePrinter, _ rune) {
	e.printError(s)
}

func (e *ReadWithinUncertaintyIntervalError) printError(p Printer) {
	var localTsStr redact.RedactableString
	if e.ValueTimestamp != e.LocalTimestamp.ToTimestamp() {
		localTsStr = redact.Sprintf(" (local=%s)", e.LocalTimestamp)
	}

	p.Printf("ReadWithinUncertaintyIntervalError: read at time %s encountered "+
		"previous write with future timestamp %s%s within uncertainty interval `t <= "+
		"(local=%s, global=%s)`; "+
		"observed timestamps: ",
		e.ReadTimestamp, e.ValueTimestamp, localTsStr, e.LocalUncertaintyLimit, e.GlobalUncertaintyLimit)

	p.Printf("[")
	for i, ot := range e.ObservedTimestamps {
		if i > 0 {
			p.Printf(" ")
		}
		p.Printf("{%d %s}", ot.NodeID, ot.Timestamp)
	}
	p.Printf("]")
}

func (e *ReadWithinUncertaintyIntervalError) SafeFormatError(p errors.Printer) (next error) {
	e.printError(p)
	return nil
}

func (e *ReadWithinUncertaintyIntervalError) String() string {
	return redact.StringWithoutMarkers(e)
}

func (e *ReadWithinUncertaintyIntervalError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

// Type is part of the ErrorDetailInterface.
func (e *ReadWithinUncertaintyIntervalError) Type() ErrorDetailType {
	return ReadWithinUncertaintyIntervalErrType
}

func (*ReadWithinUncertaintyIntervalError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

// RetryTimestamp returns the timestamp that should be used to retry an
// operation after encountering a ReadWithinUncertaintyIntervalError.
func (e *ReadWithinUncertaintyIntervalError) RetryTimestamp() hlc.Timestamp {
	// If the reader encountered a newer write within the uncertainty interval,
	// we advance the txn's timestamp just past the uncertain value's timestamp.
	// This ensures that we read above the uncertain value on a retry.
	ts := e.ValueTimestamp.Next()
	// In addition to advancing past the uncertainty value's timestamp, we also
	// advance the txn's timestamp up to the local uncertainty limit on the node
	// which hit the error. This ensures that no future read after the retry on
	// this node (ignoring lease complications in ComputeLocalUncertaintyLimit
	// and values with future-time timestamps) will throw an uncertainty error,
	// even when reading other keys.
	//
	// Note that if the request was not able to establish a local uncertainty
	// limit due to a missing observed timestamp (for instance, if the request
	// was evaluated on a follower replica and the txn had never visited the
	// leaseholder), then LocalUncertaintyLimit will be empty and the Forward
	// will be a no-op. In this case, we could advance all the way past the
	// global uncertainty limit, but this time would likely be in the future, so
	// this would necessitate a commit-wait period after committing.
	//
	// In general, we expect the local uncertainty limit, if set, to be above
	// the uncertainty value's timestamp. So we expect this Forward to advance
	// ts. However, this is not always the case. The one exception is if the
	// uncertain value had a future-time timestamp, so it was compared against
	// the global uncertainty limit to determine uncertainty (see IsUncertain).
	// In such cases, we're ok advancing just past the value's timestamp. Either
	// way, we won't see the same value in our uncertainty interval on a retry.
	ts.Forward(e.LocalUncertaintyLimit.ToTimestamp())
	return ts
}

var _ ErrorDetailInterface = &ReadWithinUncertaintyIntervalError{}
var _ transactionRestartError = &ReadWithinUncertaintyIntervalError{}

func (e *OpRequiresTxnError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *OpRequiresTxnError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("the operation requires transactional context")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *OpRequiresTxnError) Type() ErrorDetailType {
	return OpRequiresTxnErrType
}

var _ ErrorDetailInterface = &OpRequiresTxnError{}

func (e *ConditionFailedError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *ConditionFailedError) SafeFormatError(p errors.Printer) (next error) {
	if e.HadNewerOriginTimestamp {
		p.Printf("higher OriginTimestamp but unexpected value: %s", e.ActualValue)
	} else if e.OriginTimestampOlderThan.IsSet() {
		p.Printf("OriginTimestamp older than %s", e.OriginTimestampOlderThan)
	} else {
		p.Printf("unexpected value: %s", e.ActualValue)
	}
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *ConditionFailedError) Type() ErrorDetailType {
	return ConditionFailedErrType
}

var _ ErrorDetailInterface = &ConditionFailedError{}

func (e *RaftGroupDeletedError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *RaftGroupDeletedError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("raft group deleted")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *RaftGroupDeletedError) Type() ErrorDetailType {
	return RaftGroupDeletedErrType
}

var _ ErrorDetailInterface = &RaftGroupDeletedError{}

// NewReplicaCorruptionError creates a new error indicating a corrupt replica.
// The supplied error is used to provide additional detail in the error message.
// NB: Take caution when marking errors as replica corruption errors to be sure
// that they are actually indicative of replica corruption and should be treated
// as such; for example while in general a failures to apply a command might be,
// a timeout or context cancellation error may not be, especially if a user
// request controls that cancellation/timeout. See the helper below in
// MaybeWrapReplicaCorruptionError.
func NewReplicaCorruptionError(err error) *ReplicaCorruptionError {
	return &ReplicaCorruptionError{ErrorMsg: err.Error()}
}

// MaybeWrapReplicaCorruptionError wraps a passed error as a replica corruption
// error unless it matches the error in the passed context, which would suggest
// the whole operation was cancelled due to the latter rather than indicating a
// fault which implies replica corruption.
func MaybeWrapReplicaCorruptionError(ctx context.Context, err error) error {
	if errors.Is(err, ctx.Err()) {
		return err
	}
	return NewReplicaCorruptionError(err)
}

func (e *ReplicaCorruptionError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *ReplicaCorruptionError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("replica corruption (processed=%t)", e.Processed)
	if e.ErrorMsg != "" {
		p.Printf(": %s", e.ErrorMsg)
	}
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *ReplicaCorruptionError) Type() ErrorDetailType {
	return ReplicaCorruptionErrType
}

var _ ErrorDetailInterface = &ReplicaCorruptionError{}

// NewReplicaTooOldError initializes a new ReplicaTooOldError.
func NewReplicaTooOldError(replicaID roachpb.ReplicaID) *ReplicaTooOldError {
	return &ReplicaTooOldError{
		ReplicaID: replicaID,
	}
}

func (e *ReplicaTooOldError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *ReplicaTooOldError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("sender replica too old, discarding message")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *ReplicaTooOldError) Type() ErrorDetailType {
	return ReplicaTooOldErrType
}

var _ ErrorDetailInterface = &ReplicaTooOldError{}

// NewStoreNotFoundError initializes a new StoreNotFoundError.
func NewStoreNotFoundError(storeID roachpb.StoreID) *StoreNotFoundError {
	return &StoreNotFoundError{
		StoreID: storeID,
	}
}

func (e *StoreNotFoundError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *StoreNotFoundError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("store %d was not found", e.StoreID)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *StoreNotFoundError) Type() ErrorDetailType {
	return StoreNotFoundErrType
}

var _ ErrorDetailInterface = &StoreNotFoundError{}

func (e *TxnAlreadyEncounteredErrorError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *TxnAlreadyEncounteredErrorError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf(
		"txn already encountered an error; cannot be used anymore (previous err: %v)",
		e.PrevError,
	)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *TxnAlreadyEncounteredErrorError) Type() ErrorDetailType {
	return TxnAlreadyEncounteredErrType
}

var _ ErrorDetailInterface = &TxnAlreadyEncounteredErrorError{}

func (e *IntegerOverflowError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *IntegerOverflowError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf(
		"key %s with value %d incremented by %d results in overflow",
		e.Key, e.CurrentValue, e.IncrementValue)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *IntegerOverflowError) Type() ErrorDetailType {
	return IntegerOverflowErrType
}

var _ ErrorDetailInterface = &IntegerOverflowError{}

func (e *UnsupportedRequestError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *UnsupportedRequestError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("unsupported request")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *UnsupportedRequestError) Type() ErrorDetailType {
	return UnsupportedRequestErrType
}

var _ ErrorDetailInterface = &UnsupportedRequestError{}

func (e *BatchTimestampBeforeGCError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *BatchTimestampBeforeGCError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf(
		"batch timestamp %v must be after replica GC threshold %v (r%d: %s)",
		e.Timestamp, e.Threshold, e.RangeID,
		roachpb.RSpan{Key: []byte(e.StartKey), EndKey: []byte(e.EndKey)},
	)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *BatchTimestampBeforeGCError) Type() ErrorDetailType {
	return BatchTimestampBeforeGCErrType
}

var _ ErrorDetailInterface = &BatchTimestampBeforeGCError{}

func (e *MVCCHistoryMutationError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *MVCCHistoryMutationError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("unexpected MVCC history mutation in span %s", e.Span)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *MVCCHistoryMutationError) Type() ErrorDetailType {
	return MVCCHistoryMutationErrType
}

var _ ErrorDetailInterface = &MVCCHistoryMutationError{}

// NewIntentMissingError creates a new IntentMissingError.
func NewIntentMissingError(key roachpb.Key, wrongIntent *roachpb.Intent) *IntentMissingError {
	return &IntentMissingError{
		Key:         key,
		WrongIntent: wrongIntent.AsLockPtr(),
	}
}

func (e *IntentMissingError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *IntentMissingError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("intent missing")
	if e.WrongIntent != nil {
		p.Printf("; found intent %v at key instead", e.WrongIntent)
	}
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *IntentMissingError) Type() ErrorDetailType {
	return IntentMissingErrType
}

func (*IntentMissingError) canRestartTransaction() TransactionRestart {
	return TransactionRestart_IMMEDIATE
}

var _ ErrorDetailInterface = &IntentMissingError{}
var _ transactionRestartError = &IntentMissingError{}

func (e *MergeInProgressError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *MergeInProgressError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("merge in progress")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *MergeInProgressError) Type() ErrorDetailType {
	return MergeInProgressErrType
}

var _ ErrorDetailInterface = &MergeInProgressError{}

// NewRangeFeedRetryError initializes a new RangeFeedRetryError.
func NewRangeFeedRetryError(reason RangeFeedRetryError_Reason) *RangeFeedRetryError {
	return &RangeFeedRetryError{
		Reason: reason,
	}
}

func (e *RangeFeedRetryError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *RangeFeedRetryError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("retry rangefeed (%s)", redact.Safe(e.Reason))
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *RangeFeedRetryError) Type() ErrorDetailType {
	return RangeFeedRetryErrType
}

var _ ErrorDetailInterface = &RangeFeedRetryError{}

// NewIndeterminateCommitError initializes a new IndeterminateCommitError.
func NewIndeterminateCommitError(txn roachpb.Transaction) *IndeterminateCommitError {
	return &IndeterminateCommitError{StagingTxn: txn}
}

func (e *IndeterminateCommitError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *IndeterminateCommitError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("found txn in indeterminate STAGING state %s", e.StagingTxn)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *IndeterminateCommitError) Type() ErrorDetailType {
	return IndeterminateCommitErrType
}

var _ ErrorDetailInterface = &IndeterminateCommitError{}

func (e *InvalidLeaseError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *InvalidLeaseError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("invalid lease")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *InvalidLeaseError) Type() ErrorDetailType {
	return InvalidLeaseErrType
}

var _ ErrorDetailInterface = &InvalidLeaseError{}

// NewOptimisticEvalConflictsError initializes a new
// OptimisticEvalConflictsError.
func NewOptimisticEvalConflictsError() *OptimisticEvalConflictsError {
	return &OptimisticEvalConflictsError{}
}

func (e *OptimisticEvalConflictsError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *OptimisticEvalConflictsError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("optimistic eval encountered conflict")
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *OptimisticEvalConflictsError) Type() ErrorDetailType {
	return OptimisticEvalConflictsErrType
}

var _ ErrorDetailInterface = &OptimisticEvalConflictsError{}

// NewMinTimestampBoundUnsatisfiableError initializes a new
// MinTimestampBoundUnsatisfiableError.
func NewMinTimestampBoundUnsatisfiableError(
	minTimestampBound, resolvedTimestamp hlc.Timestamp,
) *MinTimestampBoundUnsatisfiableError {
	return &MinTimestampBoundUnsatisfiableError{
		MinTimestampBound: minTimestampBound,
		ResolvedTimestamp: resolvedTimestamp,
	}
}

func (e *MinTimestampBoundUnsatisfiableError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *MinTimestampBoundUnsatisfiableError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("bounded staleness read with minimum timestamp "+
		"bound of %s could not be satisfied by a local resolved timestamp of %s",
		e.MinTimestampBound, e.ResolvedTimestamp)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *MinTimestampBoundUnsatisfiableError) Type() ErrorDetailType {
	return MinTimestampBoundUnsatisfiableErrType
}

var _ ErrorDetailInterface = &MinTimestampBoundUnsatisfiableError{}

// NewRefreshFailedError initializes a new RefreshFailedError. reason can be 'committed value'
// or 'intent' which caused the failed refresh, key is the key that we failed
// refreshing, and ts is the timestamp of the committed value or intent that was written.
func NewRefreshFailedError(
	ctx context.Context,
	reason RefreshFailedError_Reason,
	key roachpb.Key,
	ts hlc.Timestamp,
	opts ...RetryErrOption,
) *RefreshFailedError {
	options := retryErrOptions{}
	for _, o := range opts {
		o.apply(&options)
	}
	if reason == RefreshFailedError_REASON_INTENT && options.conflictingTxn == nil {
		log.Fatal(ctx, "conflictingTxn should be set if refresh failed with REASON_INTENT")
	}
	if reason != RefreshFailedError_REASON_INTENT && options.conflictingTxn != nil {
		log.Fatal(ctx, "conflictingTxn should not be set if refresh failed without REASON_INTENT")
	}
	return &RefreshFailedError{
		Reason:         reason,
		Key:            key,
		Timestamp:      ts,
		ConflictingTxn: options.conflictingTxn,
	}
}

func (e *RefreshFailedError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *RefreshFailedError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("encountered recently written %s %s @%s", e.FailureReason(), e.Key, e.Timestamp)
	return nil
}

// FailureReason returns the failure reason as a string.
func (e *RefreshFailedError) FailureReason() redact.SafeString {
	var r redact.SafeString
	switch e.Reason {
	case RefreshFailedError_REASON_COMMITTED_VALUE:
		r = "committed value"
	case RefreshFailedError_REASON_INTENT:
		r = "intent"
	default:
		r = "UNKNOWN"
	}
	return r
}

// Type is part of the ErrorDetailInterface.
func (e *RefreshFailedError) Type() ErrorDetailType {
	return RefreshFailedErrType
}

var _ ErrorDetailInterface = &RefreshFailedError{}

func (e *InsufficientSpaceError) Error() string {
	return fmt.Sprint(e)
}

// Format implements fmt.Formatter.
func (e *InsufficientSpaceError) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

// SafeFormatError implements errors.SafeFormatter.
func (e *InsufficientSpaceError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("store %d has insufficient remaining capacity to %s (remaining: %s / %.1f%%, min required: %.1f%%)",
		e.StoreID, redact.SafeString(e.Op), humanizeutil.IBytes(e.Available), float64(e.Available)/float64(e.Capacity)*100, e.Required*100)
	return nil
}

// NewNotLeaseHolderError returns a NotLeaseHolderError initialized with the
// replica for the holder (if any) of the given lease.
//
// Note that this error can be generated on the Raft processing goroutine, so
// its output should be completely determined by its parameters.
func NewNotLeaseHolderError(
	l roachpb.Lease, proposerStoreID roachpb.StoreID, rangeDesc *roachpb.RangeDescriptor, msg string,
) *NotLeaseHolderError {
	err := &NotLeaseHolderError{
		RangeID:   rangeDesc.RangeID,
		RangeDesc: *rangeDesc,
		CustomMsg: msg,
	}
	if proposerStoreID != 0 {
		err.Replica, _ = rangeDesc.GetReplicaDescriptor(proposerStoreID)
	}
	if !l.Empty() {
		// Normally, we return the lease-holding Replica here. However, in the
		// case in which a leader removes itself, we want the followers to
		// avoid handing out a misleading clue (which in itself shouldn't be
		// overly disruptive as the lease would expire and then this method
		// shouldn't be called for it any more, but at the very least it
		// could catch tests in a loop, presumably due to manual clocks).
		_, stillMember := rangeDesc.GetReplicaDescriptor(l.Replica.StoreID)
		if stillMember {
			err.Lease = new(roachpb.Lease)
			*err.Lease = l
		}
	}
	return err
}

// NewNotLeaseHolderErrorWithSpeculativeLease returns a NotLeaseHolderError
// initialized with a speculative lease pointing to the supplied replica.
// A NotLeaseHolderError may be constructed with a speculative lease if the
// current lease is not known, but the error is being created by guessing who
// the leaseholder may be.
func NewNotLeaseHolderErrorWithSpeculativeLease(
	leaseHolder roachpb.ReplicaDescriptor,
	proposerStoreID roachpb.StoreID,
	rangeDesc *roachpb.RangeDescriptor,
	msg string,
) *NotLeaseHolderError {
	speculativeLease := roachpb.Lease{
		Replica: leaseHolder,
	}
	return NewNotLeaseHolderError(speculativeLease, proposerStoreID, rangeDesc, msg)
}

// MissingRecordError is reported when a record is missing.
type MissingRecordError struct{}

func (e *MissingRecordError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *MissingRecordError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("missing record")
	return nil
}

// DescNotFoundError is reported when a descriptor is missing.
type DescNotFoundError struct {
	id      int32
	isStore bool
}

// NewStoreDescNotFoundError initializes a new DescNotFoundError for a missing
// store descriptor.
func NewStoreDescNotFoundError(storeID roachpb.StoreID) *DescNotFoundError {
	return &DescNotFoundError{
		id:      int32(storeID),
		isStore: true,
	}
}

// NewNodeDescNotFoundError initializes a new DescNotFoundError for a missing
// node descriptor.
func NewNodeDescNotFoundError(nodeID roachpb.NodeID) *DescNotFoundError {
	return &DescNotFoundError{
		id:      int32(nodeID),
		isStore: false,
	}
}

func (e *DescNotFoundError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

func (e *DescNotFoundError) SafeFormatError(p errors.Printer) (next error) {
	s := redact.SafeString("node")
	if e.isStore {
		s = "store"
	}
	p.Printf("%s descriptor with %s ID %d was not found", s, s, e.id)
	return nil
}

// Type is part of the ErrorDetailInterface.
func (e *ReplicaUnavailableError) Type() ErrorDetailType {
	return ReplicaUnavailableErrType
}

var _ ErrorDetailInterface = &ReplicaUnavailableError{}

// Type is part of the ErrorDetailInterface.
func (e *ProxyFailedError) Type() ErrorDetailType {
	return ProxyFailedErrType
}

// Error is part of the builtin err interface
func (e *ProxyFailedError) Error() string {
	return redact.Sprint(e).StripMarkers()
}

// Format implements fmt.Formatter.
func (e *ProxyFailedError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// SafeFormatError is part of the SafeFormatter
func (e *ProxyFailedError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("proxy failed with send error")
	return nil
}

// Unwrap implements errors.Wrapper.
func (e *ProxyFailedError) Unwrap() error {
	return errors.DecodeError(context.Background(), e.Cause)
}

// NewProxyFailedError returns an ProxyFailedError wrapping (via
// errors.Wrapper) the supplied error.
func NewProxyFailedError(err error) *ProxyFailedError {
	return &ProxyFailedError{
		Cause: errors.EncodeError(context.Background(), err),
	}
}

var _ ErrorDetailInterface = &ProxyFailedError{}
var _ errors.SafeFormatter = (*ProxyFailedError)(nil)
var _ fmt.Formatter = (*ProxyFailedError)(nil)
var _ errors.Wrapper = (*ProxyFailedError)(nil)

// KeyCollisionError represents a failed attempt to ingest the same key twice.
type KeyCollisionError struct {
	Key   roachpb.Key
	Value []byte
}

// Format implements fmt.Formatter.
func (d *KeyCollisionError) Format(s fmt.State, verb rune) {
	errors.FormatError(d, s, verb)
}

func (d *KeyCollisionError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("ingested key collides with an existing one: %s", d.Key)
	return nil
}

func (d *KeyCollisionError) Error() string {
	return fmt.Sprint(d)
}

// NewKeyCollisionError constructs a KeyCollisionError, copying its input.
func NewKeyCollisionError(key roachpb.Key, value []byte) error {
	ret := &KeyCollisionError{
		Key:   key.Clone(),
		Value: slices.Clone(value),
	}
	return ret
}

func init() {
	encode := func(ctx context.Context, err error) (msgPrefix string, safeDetails []string, payload proto.Message) {
		errors.As(err, &payload) // payload = err.(proto.Message)
		return "", nil, payload
	}
	decode := func(ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload proto.Message) error {
		return payload.(*ProxyFailedError)
	}
	typeName := errors.GetTypeKey((*ProxyFailedError)(nil))
	errors.RegisterWrapperEncoder(typeName, encode)
	errors.RegisterWrapperDecoder(typeName, decode)
}

func init() {
	errors.RegisterLeafDecoder(errors.GetTypeKey((*MissingRecordError)(nil)), func(_ context.Context, _ string, _ []string, _ proto.Message) error {
		return &MissingRecordError{}
	})
	errorutilpath := reflect.TypeOf(errorutil.TempSentinel{}).PkgPath()
	errors.RegisterTypeMigration(errorutilpath, "*errorutil.descriptorNotFound", &DescNotFoundError{})
}

var _ errors.SafeFormatter = &MissingRecordError{}
var _ errors.SafeFormatter = &NotLeaseHolderError{}
var _ errors.SafeFormatter = &RangeNotFoundError{}
var _ errors.SafeFormatter = &RangeKeyMismatchError{}
var _ errors.SafeFormatter = &ReadWithinUncertaintyIntervalError{}
var _ errors.SafeFormatter = &TransactionAbortedError{}
var _ errors.SafeFormatter = &TransactionPushError{}
var _ errors.SafeFormatter = &TransactionRetryError{}
var _ errors.SafeFormatter = &TransactionStatusError{}
var _ errors.SafeFormatter = &LockConflictError{}
var _ errors.SafeFormatter = &WriteTooOldError{}
var _ errors.SafeFormatter = &OpRequiresTxnError{}
var _ errors.SafeFormatter = &ConditionFailedError{}
var _ errors.SafeFormatter = &LeaseRejectedError{}
var _ errors.SafeFormatter = &NodeUnavailableError{}
var _ errors.SafeFormatter = &RaftGroupDeletedError{}
var _ errors.SafeFormatter = &ReplicaCorruptionError{}
var _ errors.SafeFormatter = &ReplicaTooOldError{}
var _ errors.SafeFormatter = &AmbiguousResultError{}
var _ errors.SafeFormatter = &StoreNotFoundError{}
var _ errors.SafeFormatter = &TransactionRetryWithProtoRefreshError{}
var _ errors.SafeFormatter = &IntegerOverflowError{}
var _ errors.SafeFormatter = &UnsupportedRequestError{}
var _ errors.SafeFormatter = &BatchTimestampBeforeGCError{}
var _ errors.SafeFormatter = &TxnAlreadyEncounteredErrorError{}
var _ errors.SafeFormatter = &IntentMissingError{}
var _ errors.SafeFormatter = &MergeInProgressError{}
var _ errors.SafeFormatter = &RangeFeedRetryError{}
var _ errors.SafeFormatter = &IndeterminateCommitError{}
var _ errors.SafeFormatter = &InvalidLeaseError{}
var _ errors.SafeFormatter = &OptimisticEvalConflictsError{}
var _ errors.SafeFormatter = &MinTimestampBoundUnsatisfiableError{}
var _ errors.SafeFormatter = &RefreshFailedError{}
var _ errors.SafeFormatter = &MVCCHistoryMutationError{}
var _ errors.SafeFormatter = &UnhandledRetryableError{}
var _ errors.SafeFormatter = &ReplicaUnavailableError{}
var _ errors.SafeFormatter = &ProxyFailedError{}
var _ errors.SafeFormatter = &KeyCollisionError{}
