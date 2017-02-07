// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Txn is an in-progress distributed database transaction. A Txn is not safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db           DB
	Proto        roachpb.Transaction
	UserPriority roachpb.UserPriority
	Context      context.Context // must not be nil
	// systemConfigTrigger is set to true when modifying keys from the SystemConfig
	// span. This sets the SystemConfigTrigger on EndTransactionRequest.
	systemConfigTrigger bool
	// txnAnchorKey is the key at which to anchor the transaction record. If
	// unset, the first key written in the transaction will be used.
	txnAnchorKey roachpb.Key
	// commitTriggers are run upon successful commit.
	commitTriggers []func()
	// The txn has to be committed by this deadline. A nil value indicates no
	// deadline.
	deadline *hlc.Timestamp
	// see IsFinalized()
	finalized bool
}

// NewTxn returns a new txn.
func NewTxn(ctx context.Context, db DB) *Txn {
	return &Txn{
		db:      db,
		Context: ctx,
	}
}

// IsFinalized returns true if this Txn has been finalized and should therefore
// not be used for any more KV operations.
// A Txn is considered finalized if it successfully committed or if a rollback
// was attempted (successful or not).
// Note that Commit() always leaves the transaction finalized, since it attempts
// to rollback on error.
func (txn *Txn) IsFinalized() bool {
	return txn.finalized
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI.
func (txn *Txn) SetDebugName(name string) {
	txn.Proto.Name = name
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	if txn.Proto.ID == nil {
		return txn.Proto.Name
	}
	return fmt.Sprintf("%s (id: %s)", txn.Proto.Name, txn.Proto.ID)
}

// SetIsolation sets the transaction's isolation type. Transactions default to
// serializable isolation. The isolation must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetIsolation(isolation enginepb.IsolationType) error {
	if txn.Proto.Isolation == isolation {
		return nil
	}
	if txn.Proto.IsInitialized() {
		return errors.Errorf("cannot change the isolation level of a running transaction")
	}
	txn.Proto.Isolation = isolation
	return nil
}

// SetUserPriority sets the transaction's user priority. Transactions default to
// normal user priority. The user priority must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetUserPriority(userPriority roachpb.UserPriority) error {
	if txn.UserPriority == userPriority {
		return nil
	}
	if txn.Proto.IsInitialized() {
		return errors.Errorf("cannot change the user priority of a running transaction")
	}
	if userPriority < roachpb.MinUserPriority || userPriority > roachpb.MaxUserPriority {
		return errors.Errorf("the given user priority %f is out of the allowed range [%f, %d]", userPriority, roachpb.MinUserPriority, roachpb.MaxUserPriority)
	}
	txn.UserPriority = userPriority
	return nil
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority int32) {
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.UserPriority = roachpb.UserPriority(-priority)
}

// SetSystemConfigTrigger sets the system db trigger to true on this transaction.
// This will impact the EndTransactionRequest.
//
// NOTE: The system db trigger will only execute correctly if the transaction
// record is located on the range that contains the system span. If a
// transaction is created which modifies both system *and* non-system data, it
// should be ensured that the transaction record itself is on the system span.
// This can be done by making sure a system key is the first key touched in the
// transaction.
func (txn *Txn) SetSystemConfigTrigger() {
	if !txn.systemConfigTrigger {
		txn.systemConfigTrigger = true
		// The system-config trigger must be run on the system-config range which
		// means any transaction with the trigger set needs to be anchored to the
		// system-config range.
		txn.SetTxnAnchorKey(keys.SystemConfigSpan.Key)
	}
}

// SystemConfigTrigger returns the systemConfigTrigger flag.
func (txn *Txn) SystemConfigTrigger() bool {
	return txn.systemConfigTrigger
}

// SetTxnAnchorKey sets the key at which to anchor the transaction record. The
// transaction anchor key defaults to the first key written in a transaction.
func (txn *Txn) SetTxnAnchorKey(key roachpb.Key) {
	if txn.Proto.Writing {
		panic("must set txn anchor key before any txn writes")
	}
	txn.txnAnchorKey = key
}

// NewBatch creates and returns a new empty batch object for use with the Txn.
func (txn *Txn) NewBatch() *Batch {
	return &Batch{txn: txn}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error. It is not considered an error for the key to not exist.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) Get(key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.Get(key)
	return getOneRow(txn.Run(b), b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message. If the key doesn't exist, the proto will simply be reset.
//
// key can be either a byte slice or a string.
func (txn *Txn) GetProto(key interface{}, msg proto.Message) error {
	r, err := txn.Get(key)
	if err != nil {
		return err
	}
	return r.ValueProto(msg)
}

// Put sets the value for a key
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) Put(key, value interface{}) error {
	b := txn.NewBatch()
	b.Put(key, value)
	return getOneErr(txn.Run(b), b)
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue. Note that this must be an interface{}(nil), not a
// typed nil value (e.g. []byte(nil)).
//
// Returns an error if the existing value is not equal to expValue.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) CPut(key, value, expValue interface{}) error {
	b := txn.NewBatch()
	b.CPut(key, value, expValue)
	return getOneErr(txn.Run(b), b)
}

// InitPut sets the first value for a key to value. An error is reported if a
// value already exists for the key and it's not equal to the value passed in.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc). It is illegal to
// set value to nil.
func (txn *Txn) InitPut(key, value interface{}) error {
	b := txn.NewBatch()
	b.InitPut(key, value)
	return getOneErr(txn.Run(b), b)
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// The returned Result will contain a single row and Result.Err will indicate
// success or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) Inc(key interface{}, value int64) (KeyValue, error) {
	b := txn.NewBatch()
	b.Inc(key, value)
	return getOneRow(txn.Run(b), b)
}

func (txn *Txn) scan(begin, end interface{}, maxRows int64, isReverse bool) ([]KeyValue, error) {
	b := txn.NewBatch()
	if maxRows > 0 {
		b.Header.MaxSpanRequestKeys = maxRows
	}
	if !isReverse {
		b.Scan(begin, end)
	} else {
		b.ReverseScan(begin, end)
	}
	r, err := getOneResult(txn.Run(b), b)
	return r.Rows, err
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return txn.scan(begin, end, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return txn.scan(begin, end, maxRows, true)
}

// TODO(pmattis): Txn.ReverseScan is neither used or tested. Silence unused
// warning.
var _ = (*Txn)(nil).ReverseScan

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (txn *Txn) Del(keys ...interface{}) error {
	b := txn.NewBatch()
	b.Del(keys...)
	return getOneErr(txn.Run(b), b)
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned Result will contain 0 rows and Result.Err will indicate success
// or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) DelRange(begin, end interface{}) error {
	b := txn.NewBatch()
	b.DelRange(begin, end, false)
	return getOneErr(txn.Run(b), b)
}

// Run executes the operations queued up within a batch. Before executing any
// of the operations the batch is first checked to see if there were any errors
// during its construction (e.g. failure to marshal a proto message).
//
// The operations within a batch are run in parallel and the order is
// non-deterministic. It is an unspecified behavior to modify and retrieve the
// same key within a batch.
//
// Upon completion, Batch.Results will contain the results for each
// operation. The order of the results matches the order the operations were
// added to the batch.
func (txn *Txn) Run(b *Batch) error {
	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	if err := b.prepare(); err != nil {
		return err
	}
	return sendAndFill(txn.send, b)
}

func (txn *Txn) commit() error {
	err := txn.sendEndTxnReq(true /* commit */, txn.deadline)
	if err == nil {
		txn.finalized = true
		for _, t := range txn.commitTriggers {
			t()
		}
	}
	return err
}

// CleanupOnError cleans up the transaction as a result of an error.
func (txn *Txn) CleanupOnError(err error) {
	if err == nil {
		panic("no error")
	}
	if txn.Proto.Status == roachpb.PENDING {
		if replyErr := txn.Rollback(); replyErr != nil {
			log.Errorf(txn.Context, "failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
	}
}

// Commit is the same as CommitOrCleanup but will not attempt to clean
// up on failure. This can be used when the caller is prepared to do proper
// cleanup.
func (txn *Txn) Commit() error {
	return txn.commit()
}

// CommitInBatch executes the operations queued up within a batch and
// commits the transaction. Explicitly committing a transaction is
// optional, but more efficient than relying on the implicit commit
// performed when the transaction function returns without error.
// The batch must be created by this transaction.
// If the command completes successfully, the txn is considered finalized. On
// error, no attempt is made to clean up the (possibly still pending)
// transaction.
func (txn *Txn) CommitInBatch(b *Batch) error {
	if txn != b.txn {
		return errors.Errorf("a batch b can only be committed by b.txn")
	}
	b.AddRawRequest(endTxnReq(true /* commit */, txn.deadline, txn.SystemConfigTrigger()))
	err := txn.Run(b)
	if err == nil {
		txn.finalized = true
	}
	return err
}

// CommitOrCleanup sends an EndTransactionRequest with Commit=true.
// If that fails, an attempt to rollback is made.
// txn should not be used to send any more commands after this call.
func (txn *Txn) CommitOrCleanup() error {
	err := txn.commit()
	if err != nil {
		txn.CleanupOnError(err)
	}
	if !txn.IsFinalized() {
		panic("Commit() failed to move txn to a final state")
	}
	return err
}

// UpdateDeadlineMaybe sets the transactions deadline to the lower of the
// current one (if any) and the passed value.
func (txn *Txn) UpdateDeadlineMaybe(deadline hlc.Timestamp) bool {
	if txn.deadline == nil || deadline.Less(*txn.deadline) {
		txn.deadline = &deadline
		return true
	}
	return false
}

// ResetDeadline resets the deadline.
func (txn *Txn) ResetDeadline() {
	txn.deadline = nil
}

// GetDeadline returns the deadline. For testing.
func (txn *Txn) GetDeadline() *hlc.Timestamp {
	return txn.deadline
}

// Rollback sends an EndTransactionRequest with Commit=false.
// txn is considered finalized and cannot be used to send any more commands.
func (txn *Txn) Rollback() error {
	log.VEventf(txn.Context, 2, "rolling back transaction")
	err := txn.sendEndTxnReq(false /* commit */, nil)
	txn.finalized = true
	return err
}

// AddCommitTrigger adds a closure to be executed on successful commit
// of the transaction.
func (txn *Txn) AddCommitTrigger(trigger func()) {
	txn.commitTriggers = append(txn.commitTriggers, trigger)
}

func (txn *Txn) sendEndTxnReq(commit bool, deadline *hlc.Timestamp) error {
	var ba roachpb.BatchRequest
	ba.Add(endTxnReq(commit, deadline, txn.SystemConfigTrigger()))
	_, err := txn.send(ba)
	return err.GoError()
}

func endTxnReq(commit bool, deadline *hlc.Timestamp, hasTrigger bool) roachpb.Request {
	req := &roachpb.EndTransactionRequest{
		Commit:   commit,
		Deadline: deadline,
	}
	if hasTrigger {
		req.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
			ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
				SystemConfigSpan: true,
			},
		}
	}
	return req
}

// TxnExecOptions controls how Exec() runs a transaction and the corresponding
// closure.
type TxnExecOptions struct {
	// If set, the transaction is automatically aborted if the closure returns any
	// error aside from recoverable internal errors, in which case the closure is
	// retried. The retryable function should have no side effects which could
	// cause problems in the event it must be run more than once.
	// If not set, all errors cause the txn to be aborted.
	AutoRetry bool
	// If set, then the txn is automatically committed if no errors are
	// encountered. If not set, committing or leaving open the txn is the
	// responsibility of the client.
	AutoCommit bool
	// If not nil, the clock can be used to generate txn timestamps early.
	// Useful for SQL txns for ensuring that the value returned by
	// `cluster_logical_timestamp()` is consistent with the commit (serializable)
	// ordering.
	Clock *hlc.Clock
}

// AutoCommitError wraps a non-retryable error coming from auto-commit.
type AutoCommitError struct {
	cause error
}

func (e *AutoCommitError) Error() string {
	return e.cause.Error()
}

// Exec executes fn in the context of a distributed transaction.
// Execution is controlled by opt (see comments in TxnExecOptions).
//
// opt is passed to fn, and it's valid for fn to modify opt as it sees
// fit during each execution attempt.
//
// It's valid for txn to be nil (meaning the txn has already aborted) if fn
// can handle that. This is useful for continuing transactions that have been
// aborted because of an error in a previous batch of statements in the hope
// that a ROLLBACK will reset the state. Neither opt.AutoRetry not opt.AutoCommit
// can be set in this case.
//
// When this method returns, txn might be in any state; Exec does not attempt
// to clean up the transaction before returning an error. In case of
// TransactionAbortedError, txn is reset to a fresh transaction, ready to be
// used.
func (txn *Txn) Exec(opt TxnExecOptions, fn func(txn *Txn, opt *TxnExecOptions) error) (err error) {
	// Run fn in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	if txn == nil && (opt.AutoRetry || opt.AutoCommit) {
		panic("asked to retry or commit a txn that is already aborted")
	}

	for {
		if txn != nil {
			// If we're looking at a brand new transaction, then communicate
			// what should be used as initial timestamp for the KV txn created
			// by TxnCoordSender.
			if opt.Clock != nil && !txn.Proto.IsInitialized() {
				// Control the KV timestamp, such that the value returned by
				// `cluster_logical_timestamp()` is consistent with the commit
				// (serializable) ordering.
				txn.Proto.OrigTimestamp = opt.Clock.Now()
			}
		}

		err = fn(txn, &opt)

		// TODO(andrei): Until 7881 is fixed.
		if err == nil && opt.AutoCommit && txn.Proto.Status == roachpb.ABORTED {
			log.Errorf(txn.Context, "#7881: no err but aborted txn proto. opt: %+v, txn: %+v",
				opt, txn)
		}

		if err == nil && opt.AutoCommit && txn.Proto.Status == roachpb.PENDING {
			// fn succeeded, but didn't commit.
			err = txn.Commit()
			log.Eventf(txn.Context, "client.Txn did AutoCommit. err: %v\ntxn: %+v", err, txn.Proto)
			if err != nil {
				if _, retryable := err.(*roachpb.RetryableTxnError); !retryable {
					// We can't retry, so let the caller know we tried to
					// autocommit.
					err = &AutoCommitError{cause: err}
				}
			}
		}

		retErr, retryable := err.(*roachpb.RetryableTxnError)
		if retryable && !IsRetryableErrMeantForTxn(retErr, txn) {
			// Make sure the txn record that err carries is for this txn.
			// If it's not, we terminate the "retryable" character of the error. We
			// might get a RetryableTxnError if the closure ran another transaction
			// internally and let the error propagate upwards.
			return errors.Wrap(retErr, "retryable error from another txn")
		}
		_, retryable = err.(*roachpb.RetryableTxnError)
		if !opt.AutoRetry || !retryable {
			break
		}

		txn.commitTriggers = nil

		log.VEventf(txn.Context, 2, "automatically retrying transaction: %s because of error: %s",
			txn.DebugName(), err)
	}

	return err
}

// IsRetryableErrMeantForTxn returns true if err is a retryable
// error meant to restart txn.
func IsRetryableErrMeantForTxn(err *roachpb.RetryableTxnError, txn *Txn) bool {
	// Make sure the txn record that err carries is for this txn.
	// TODO(andrei): this "wrong id" detection doesn't always work after a
	// transaction's proto has been reset (when txn.Proto.ID == nil at the time of
	// this check). Figure out a more robust mechanism. We should probably move
	// the initialization of the txn ID here, in the client, from the
	// TxnCoordSender.
	return txn.Proto.ID == nil || roachpb.TxnIDEqual(err.TxnID, txn.Proto.ID)
}

// sendInternal sends the batch and updates the transaction on error. Depending
// on the error type, the transaction might be replaced by a new one.
func (txn *Txn) sendInternal(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) == 0 {
		return nil, nil
	}
	if pErr := txn.db.prepareToSend(&ba); pErr != nil {
		return nil, pErr
	}

	// Send call through the DB's sender.
	ba.Txn = &txn.Proto
	// For testing purposes, txn.UserPriority can be a negative value (see
	// MakePriority).
	if txn.UserPriority != 0 {
		ba.UserPriority = txn.UserPriority
	}

	// TODO(radu): when db.send supports a context, we can just use that (and
	// remove the prepareToSend call above).
	br, pErr := txn.db.sender.Send(txn.Context, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(txn.db.sender, br))
	}

	if br != nil && len(br.CollectedSpans) != 0 {
		if err := tracing.IngestRemoteSpans(txn.Context, br.CollectedSpans); err != nil {
			return nil, roachpb.NewErrorf("error ingesting remote spans: %s", err)
		}
	}

	// Only successful requests can carry an updated Txn in their response
	// header. Any error (e.g. a restart) can have a Txn attached to them as
	// well; those update our local state in the same way for the next attempt.
	// The exception is if our transaction was aborted and needs to restart
	// from scratch, in which case we do just that.
	if pErr == nil {
		txn.Proto.Update(br.Txn)
		return br, nil
	}

	if log.V(1) {
		log.Infof(txn.Context, "failed batch: %s", pErr)
	}

	if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
		if !IsRetryableErrMeantForTxn(
			pErr.GoError().(*roachpb.RetryableTxnError), txn) {
			// If this happens, something is wrong; we've received an error that
			// wasn't meant for this transaction. This is a sign that we either
			// somehow ran another txn inside our txn and didn't properly terminate
			// its error, or our transaction got a TransactionAbortedError (and the
			// proto was reset), was retried, and then we still somehow managed to get
			// an error meant for the previous incarnation of the transaction.
			// Letting this wrong error slip here can cause us to retry the wrong
			// transaction.
			// TODO(andrei): this "wrong id" detection doesn't always work after a
			// transaction's proto has been reset (when txn.Proto.ID == nil at the
			// time of this check). Figure out a more robust mechanism. We should
			// probably move the initialization of the txn ID here, in the client,
			// from the TxnCoordSender.
			panic(fmt.Sprintf("Got a retryable error meant for a different transaction. "+
				"txn.Proto.ID: %v, pErr.ID: %v", txn.Proto.ID, pErr.GetTxn().ID))
		}
	}

	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		txn.Proto = roachpb.Transaction{
			TxnMeta: enginepb.TxnMeta{
				Isolation: txn.Proto.Isolation,
			},
			Name: txn.Proto.Name,
		}
		// Acts as a minimum priority on restart.
		if pErr.GetTxn() != nil {
			txn.Proto.Priority = pErr.GetTxn().Priority
		}
	} else if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
		txn.Proto.Update(pErr.GetTxn())
	}
	return nil, pErr
}

// send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only or has already
// been successfully committed or aborted, a potential trailing
// EndTransaction call is silently dropped, allowing the caller to
// always commit or clean-up explicitly even when that may not be
// required (or even erroneous). Returns (nil, nil) for an empty batch.
func (txn *Txn) send(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {

	if txn.Proto.Status != roachpb.PENDING || txn.IsFinalized() {
		return nil, roachpb.NewErrorf(
			"attempting to use transaction with wrong status or finalized: %s", txn.Proto.Status)
	}

	// It doesn't make sense to use inconsistent reads in a transaction. However,
	// we still need to accept it as a parameter for this to compile.
	if ba.ReadConsistency != roachpb.CONSISTENT {
		return nil, roachpb.NewErrorf("cannot use %s ReadConsistency in txn",
			ba.ReadConsistency)
	}

	lastIndex := len(ba.Requests) - 1
	if lastIndex < 0 {
		return nil, nil
	}

	// firstWriteIndex is set to the index of the first command which is
	// a transactional write. If != -1, this indicates an intention to
	// write. This is in contrast to txn.Proto.Writing, which is set by
	// the coordinator when the first intent has been created, and which
	// lives for the life of the transaction.
	firstWriteIndex := -1
	txnAnchorKey := txn.txnAnchorKey

	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < lastIndex {
			if _, ok := args.(*roachpb.EndTransactionRequest); ok {
				return nil, roachpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if roachpb.IsTransactionWrite(args) {
			if firstWriteIndex == -1 {
				firstWriteIndex = i
			}
			if len(txnAnchorKey) == 0 {
				txnAnchorKey = args.Header().Key
			}
		}
	}

	haveTxnWrite := firstWriteIndex != -1
	endTxnRequest, haveEndTxn := ba.Requests[lastIndex].GetInner().(*roachpb.EndTransactionRequest)
	needBeginTxn := !txn.Proto.Writing && haveTxnWrite
	needEndTxn := txn.Proto.Writing || haveTxnWrite
	elideEndTxn := haveEndTxn && !needEndTxn

	// If we're not yet writing in this txn, but intend to, insert a
	// begin transaction request before the first write command.
	if needBeginTxn {
		// If the transaction already has a key (we're in a restart), make
		// sure we set the key in the begin transaction request to the original.
		bt := &roachpb.BeginTransactionRequest{
			Span: roachpb.Span{
				Key: txnAnchorKey,
			},
		}
		if txn.Proto.Key != nil {
			bt.Key = txn.Proto.Key
		}
		// Inject the new request before position firstWriteIndex, taking
		// care to avoid unnecessary allocations.
		oldRequests := ba.Requests
		ba.Requests = make([]roachpb.RequestUnion, len(ba.Requests)+1)
		copy(ba.Requests, oldRequests[:firstWriteIndex])
		ba.Requests[firstWriteIndex].MustSetInner(bt)
		copy(ba.Requests[firstWriteIndex+1:], oldRequests[firstWriteIndex:])
	}

	if elideEndTxn {
		ba.Requests = ba.Requests[:lastIndex]
	}

	br, pErr := txn.sendInternal(ba)
	if elideEndTxn && pErr == nil {
		// Check that read only transactions do not violate their deadline. This can NOT
		// happen since the txn deadline is normally updated when it is about to expire
		// or expired. We will just keep the code for safety (see TestReacquireLeaseOnRestart).
		if endTxnRequest.Deadline != nil {
			if endTxnRequest.Deadline.Less(txn.Proto.Timestamp) {
				return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &txn.Proto)
			}
		}
		// This normally happens on the server and sent back in response
		// headers, but this transaction was optimized away. The caller may
		// still inspect the transaction struct, so we manually update it
		// here to emulate a true transaction.
		if endTxnRequest.Commit {
			txn.Proto.Status = roachpb.COMMITTED
		} else {
			txn.Proto.Status = roachpb.ABORTED
		}
		txn.finalized = true
	}

	// If we inserted a begin transaction request, remove it here.
	if needBeginTxn {
		if br != nil && br.Responses != nil {
			br.Responses = append(br.Responses[:firstWriteIndex], br.Responses[firstWriteIndex+1:]...)
		}
		// Handle case where inserted begin txn confused an indexed error.
		if pErr != nil && pErr.Index != nil {
			idx := pErr.Index.Index
			if idx == int32(firstWriteIndex) {
				// An error was encountered on begin txn; disallow the indexing.
				pErr.Index = nil
			} else if idx > int32(firstWriteIndex) {
				// An error was encountered after begin txn; decrement index.
				pErr.SetErrorIndex(idx - 1)
			}
		}
	}
	return br, pErr
}
