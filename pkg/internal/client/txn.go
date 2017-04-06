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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Txn is an in-progress distributed database transaction. A Txn is safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db *DB

	// The following fields are not safe for concurrent modification.
	// They should be set before operating on the transaction.

	// commitTriggers are run upon successful commit.
	commitTriggers []func()
	// systemConfigTrigger is set to true when modifying keys from the SystemConfig
	// span. This sets the SystemConfigTrigger on EndTransactionRequest.
	systemConfigTrigger bool
	// The txn has to be committed by this deadline. A nil value indicates no
	// deadline.
	deadline *hlc.Timestamp

	// mu holds fields that need to be synchronized for concurrent request execution.
	mu struct {
		syncutil.Mutex
		Proto roachpb.Transaction
		// UserPriority is the transaction's priority.
		UserPriority roachpb.UserPriority
		// txnAnchorKey is the key at which to anchor the transaction record. If
		// unset, the first key written in the transaction will be used.
		txnAnchorKey roachpb.Key
		// writingTxnRecord is set when the Txn is in the middle of writing
		// its transaction record. It is used to assure that even in the presence
		// of concurrent requests, only one sends a BeginTxnRequest.
		writingTxnRecord bool
		// see IsFinalized()
		finalized bool
		// previousIDs holds the set of all previous IDs that the Txn's Proto has had
		// across transaction aborts. This allows us to determine if a given response
		// was meant for any incarnation of this transaction.
		previousIDs map[uuid.UUID]struct{}
		// commandCount indicates how many commands have been sent through
		// this transaction. Reset on retryable txn errors.
		commandCount int
	}
}

// NewTxn returns a new txn.
func NewTxn(db *DB) *Txn {
	return NewTxnWithProto(db, roachpb.Transaction{})
}

// NewTxnWithProto returns a new txn with the provided Transaction proto.
// This allows a client.Txn to be created with an already initialized proto.
func NewTxnWithProto(db *DB, proto roachpb.Transaction) *Txn {
	txn := &Txn{db: db}
	txn.mu.Proto = proto
	return txn
}

// CommandCount returns the count of commands executed through this txn.
// Retryable errors on the transaction will reset the count to 0.
func (txn *Txn) CommandCount() int {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.commandCount
}

// IsFinalized returns true if this Txn has been finalized and should therefore
// not be used for any more KV operations.
// A Txn is considered finalized if it successfully committed or if a rollback
// was attempted (successful or not).
// Note that Commit() always leaves the transaction finalized, since it attempts
// to rollback on error.
func (txn *Txn) IsFinalized() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.finalized
}

// SetUserPriority sets the transaction's user priority. Transactions default to
// normal user priority. The user priority must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetUserPriority(userPriority roachpb.UserPriority) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.UserPriority == userPriority {
		return nil
	}
	if txn.mu.Proto.IsInitialized() {
		return errors.Errorf("cannot change the user priority of a running transaction")
	}
	if userPriority < roachpb.MinUserPriority || userPriority > roachpb.MaxUserPriority {
		return errors.Errorf("the given user priority %f is out of the allowed range [%f, %d]",
			userPriority, roachpb.MinUserPriority, roachpb.MaxUserPriority)
	}
	txn.mu.UserPriority = userPriority
	return nil
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority int32) {
	txn.mu.Lock()
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.mu.UserPriority = roachpb.UserPriority(-priority)
	txn.mu.Unlock()
}

// UserPriority returns the transaction's user priority.
func (txn *Txn) UserPriority() roachpb.UserPriority {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.UserPriority
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI.
func (txn *Txn) SetDebugName(name string) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.Proto.Name == name {
		return
	}
	if txn.mu.Proto.IsInitialized() {
		panic("cannot change the debug name of a running transaction")
	}
	txn.mu.Proto.Name = name
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.Proto.ID == nil {
		return txn.mu.Proto.Name
	}
	return fmt.Sprintf("%s (id: %s)", txn.mu.Proto.Name, txn.mu.Proto.ID)
}

// SetIsolation sets the transaction's isolation type. Transactions default to
// serializable isolation. The isolation must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetIsolation(isolation enginepb.IsolationType) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.Proto.Isolation == isolation {
		return nil
	}
	if txn.mu.Proto.IsInitialized() {
		return errors.Errorf("cannot change the isolation level of a running transaction")
	}
	txn.mu.Proto.Isolation = isolation
	return nil
}

// Isolation returns the transaction's isolation type.
func (txn *Txn) Isolation() enginepb.IsolationType {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.Proto.Isolation
}

// OrigTimestamp returns the transaction's starting timestamp.
func (txn *Txn) OrigTimestamp() hlc.Timestamp {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.Proto.OrigTimestamp
}

// AnchorKey returns the transaction's anchor key. The caller should treat the
// returned byte slice as immutable.
func (txn *Txn) AnchorKey() []byte {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.Proto.Key
}

// SetTxnAnchorKey sets the key at which to anchor the transaction record. The
// transaction anchor key defaults to the first key written in a transaction.
func (txn *Txn) SetTxnAnchorKey(key roachpb.Key) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.Proto.Writing || txn.mu.writingTxnRecord {
		return errors.Errorf("transaction anchor key already set")
	}
	txn.mu.txnAnchorKey = key
	return nil
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
func (txn *Txn) SetSystemConfigTrigger() error {
	if !txn.systemConfigTrigger {
		txn.systemConfigTrigger = true
		// The system-config trigger must be run on the system-config range which
		// means any transaction with the trigger set needs to be anchored to the
		// system-config range.
		return txn.SetTxnAnchorKey(keys.SystemConfigSpan.Key)
	}
	return nil
}

// Proto returns the transactions underlying protocol buffer. It is not thread-safe,
// only use if you know that no requests are executing concurrently.
//
// A thread-safe alternative would be to clone the Proto under lock and return
// this clone, but we currently have no situations where this is needed.
func (txn *Txn) Proto() *roachpb.Transaction {
	return &txn.mu.Proto
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
func (txn *Txn) Get(ctx context.Context, key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.Get(key)
	return getOneRow(txn.Run(ctx, b), b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message. If the key doesn't exist, the proto will simply be reset.
//
// key can be either a byte slice or a string.
func (txn *Txn) GetProto(ctx context.Context, key interface{}, msg proto.Message) error {
	r, err := txn.Get(ctx, key)
	if err != nil {
		return err
	}
	return r.ValueProto(msg)
}

// Put sets the value for a key
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) Put(ctx context.Context, key, value interface{}) error {
	b := txn.NewBatch()
	b.Put(key, value)
	return getOneErr(txn.Run(ctx, b), b)
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
func (txn *Txn) CPut(ctx context.Context, key, value, expValue interface{}) error {
	b := txn.NewBatch()
	b.CPut(key, value, expValue)
	return getOneErr(txn.Run(ctx, b), b)
}

// InitPut sets the first value for a key to value. An error is reported if a
// value already exists for the key and it's not equal to the value passed in.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc). It is illegal to
// set value to nil.
func (txn *Txn) InitPut(ctx context.Context, key, value interface{}) error {
	b := txn.NewBatch()
	b.InitPut(key, value)
	return getOneErr(txn.Run(ctx, b), b)
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// The returned Result will contain a single row and Result.Err will indicate
// success or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) Inc(ctx context.Context, key interface{}, value int64) (KeyValue, error) {
	b := txn.NewBatch()
	b.Inc(key, value)
	return getOneRow(txn.Run(ctx, b), b)
}

func (txn *Txn) scan(
	ctx context.Context, begin, end interface{}, maxRows int64, isReverse bool,
) ([]KeyValue, error) {
	b := txn.NewBatch()
	if maxRows > 0 {
		b.Header.MaxSpanRequestKeys = maxRows
	}
	if !isReverse {
		b.Scan(begin, end)
	} else {
		b.ReverseScan(begin, end)
	}
	r, err := getOneResult(txn.Run(ctx, b), b)
	return r.Rows, err
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) Scan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements (or all results
// when zero is supplied).
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]KeyValue, error) {
	return txn.scan(ctx, begin, end, maxRows, true)
}

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (txn *Txn) Del(ctx context.Context, keys ...interface{}) error {
	b := txn.NewBatch()
	b.Del(keys...)
	return getOneErr(txn.Run(ctx, b), b)
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned Result will contain 0 rows and Result.Err will indicate success
// or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) DelRange(ctx context.Context, begin, end interface{}) error {
	b := txn.NewBatch()
	b.DelRange(begin, end, false)
	return getOneErr(txn.Run(ctx, b), b)
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
func (txn *Txn) Run(ctx context.Context, b *Batch) error {
	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	if err := b.prepare(); err != nil {
		return err
	}
	return sendAndFill(ctx, txn.send, b)
}

func (txn *Txn) commit(ctx context.Context) error {
	err := txn.sendEndTxnReq(ctx, true /* commit */, txn.deadline)
	if err == nil {
		for _, t := range txn.commitTriggers {
			t()
		}
	}
	return err
}

// CleanupOnError cleans up the transaction as a result of an error.
func (txn *Txn) CleanupOnError(ctx context.Context, err error) {
	if err == nil {
		log.Fatal(ctx, "CleanupOnError called with nil error")
	}
	// This may race with a concurrent EndTxnRequests. That's fine though because
	// we're just trying to clean up and will happily log the failed Rollback error
	// if someone beat us.
	txn.mu.Lock()
	isPending := txn.mu.Proto.Status == roachpb.PENDING
	txn.mu.Unlock()
	if isPending {
		if replyErr := txn.Rollback(ctx); replyErr != nil {
			log.Errorf(ctx, "failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
	}
}

// Commit is the same as CommitOrCleanup but will not attempt to clean
// up on failure. This can be used when the caller is prepared to do proper
// cleanup.
func (txn *Txn) Commit(ctx context.Context) error {
	return txn.commit(ctx)
}

// CommitInBatch executes the operations queued up within a batch and
// commits the transaction. Explicitly committing a transaction is
// optional, but more efficient than relying on the implicit commit
// performed when the transaction function returns without error.
// The batch must be created by this transaction.
// If the command completes successfully, the txn is considered finalized. On
// error, no attempt is made to clean up the (possibly still pending)
// transaction.
func (txn *Txn) CommitInBatch(ctx context.Context, b *Batch) error {
	if txn != b.txn {
		return errors.Errorf("a batch b can only be committed by b.txn")
	}
	b.AddRawRequest(endTxnReq(true /* commit */, txn.deadline, txn.systemConfigTrigger))
	return txn.Run(ctx, b)
}

// CommitOrCleanup sends an EndTransactionRequest with Commit=true.
// If that fails, an attempt to rollback is made.
// txn should not be used to send any more commands after this call.
func (txn *Txn) CommitOrCleanup(ctx context.Context) error {
	err := txn.commit(ctx)
	if err != nil {
		txn.CleanupOnError(ctx, err)
	}
	if !txn.IsFinalized() {
		log.Fatal(ctx, "Commit() failed to move txn to a final state")
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
func (txn *Txn) Rollback(ctx context.Context) error {
	log.VEventf(ctx, 2, "rolling back transaction")
	return txn.sendEndTxnReq(ctx, false /* commit */, nil)
}

// AddCommitTrigger adds a closure to be executed on successful commit
// of the transaction.
func (txn *Txn) AddCommitTrigger(trigger func()) {
	txn.commitTriggers = append(txn.commitTriggers, trigger)
}

func (txn *Txn) sendEndTxnReq(ctx context.Context, commit bool, deadline *hlc.Timestamp) error {
	var ba roachpb.BatchRequest
	ba.Add(endTxnReq(commit, deadline, txn.systemConfigTrigger))
	_, err := txn.send(ctx, ba)
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
	// If set, an OrigTimestamp will be assigned to the transaction as early as
	// possible, instead of when the first KV operation is performed. This allows
	// users to guarantee that the transactions timestamp is a lower bound for any
	// operation performed in Exec's closure.
	//
	// Useful for SQL txns for ensuring that the value returned by
	// `cluster_logical_timestamp()` is consistent with the commit (serializable)
	// ordering.
	AssignTimestampImmediately bool
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
// It is undefined to call Commit concurrently with any call to Exec. Since Exec
// with the AutoCommitflag is equivalent to an Exec possibly followed by a Commit,
// it must not be called concurrently with any other call to Exec or Commit.
//
// When this method returns, txn might be in any state; Exec does not attempt
// to clean up the transaction before returning an error. In case of
// TransactionAbortedError, txn is reset to a fresh transaction, ready to be
// used.
func (txn *Txn) Exec(
	ctx context.Context, opt TxnExecOptions, fn func(context.Context, *Txn, *TxnExecOptions) error,
) (err error) {
	// Run fn in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	if txn == nil && (opt.AutoRetry || opt.AutoCommit) {
		log.Fatal(ctx, "asked to retry or commit a txn that is already aborted")
	}

	for {
		if txn != nil {
			txn.mu.Lock()
			// If we're looking at a brand new transaction, then communicate
			// what should be used as initial timestamp.
			if opt.AssignTimestampImmediately && !txn.mu.Proto.IsInitialized() {
				// Control the KV timestamp, such that the value returned by
				// `cluster_logical_timestamp()` is consistent with the commit
				// (serializable) ordering.
				txn.mu.Proto.OrigTimestamp = txn.db.clock.Now()
			}
			txn.mu.Unlock()
		}

		err = fn(ctx, txn, &opt)

		if err == nil && opt.AutoCommit {
			// Copy the status out of the Proto under lock. Making decisions on
			// this later is not thread-safe, but the commutativity property of
			// transactions assure that reasoning about the situation is straightforward.
			txn.mu.Lock()
			status := txn.Proto().Status
			txn.mu.Unlock()

			switch status {
			case roachpb.ABORTED:
				// TODO(andrei): Until 7881 is fixed.
				log.Errorf(ctx, "#7881: no err but aborted txn proto. opt: %+v, txn: %+v",
					opt, txn)
			case roachpb.PENDING:
				// fn succeeded, but didn't commit.
				err = txn.Commit(ctx)
				log.Eventf(ctx, "client.Txn did AutoCommit. err: %v\ntxn: %+v", err, txn.Proto())
				if err != nil {
					if _, retryable := err.(*roachpb.RetryableTxnError); !retryable {
						// We can't retry, so let the caller know we tried to
						// autocommit.
						err = &AutoCommitError{cause: err}
					}
				}
			}
		}

		retErr, retryable := err.(*roachpb.RetryableTxnError)
		if retryable && !txn.IsRetryableErrMeantForTxn(retErr) {
			// Make sure the txn record that err carries is for this txn.
			// If it's not, we terminate the "retryable" character of the error. We
			// might get a RetryableTxnError if the closure ran another transaction
			// internally and let the error propagate upwards.
			return errors.Wrap(retErr, "retryable error from another txn")
		}
		if !opt.AutoRetry || !retryable {
			break
		}

		txn.commitTriggers = nil

		log.VEventf(ctx, 2, "automatically retrying transaction: %s because of error: %s",
			txn.DebugName(), err)
	}

	return err
}

// IsRetryableErrMeantForTxn returns true if err is a retryable
// error meant to restart this client transaction.
func (txn *Txn) IsRetryableErrMeantForTxn(err *roachpb.RetryableTxnError) bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.isRetryableErrMeantForTxnLocked(err)
}

func (txn *Txn) isRetryableErrMeantForTxnLocked(err *roachpb.RetryableTxnError) bool {
	// Make sure the txn record that err carries is for this txn.
	// First check if the error was meant for a previous incarnation
	// of the transaction.
	if err.TxnID != nil {
		if _, ok := txn.mu.previousIDs[*err.TxnID]; ok {
			return true
		}
	}
	// If not, make sure it was meant for this transaction.
	return roachpb.TxnIDEqual(err.TxnID, txn.mu.Proto.ID)
}

// send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only or has already
// been successfully committed or aborted, a potential trailing
// EndTransaction call is silently dropped, allowing the caller to
// always commit or clean-up explicitly even when that may not be
// required (or even erroneous). Returns (nil, nil) for an empty batch.
func (txn *Txn) send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
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

	firstWriteIdx, pErr := firstWriteIndex(ba)
	if pErr != nil {
		return nil, pErr
	}

	haveTxnWrite := firstWriteIdx != -1
	endTxnRequest, haveEndTxn := ba.Requests[lastIndex].GetInner().(*roachpb.EndTransactionRequest)

	var needBeginTxn, elideEndTxn bool
	lockedPrelude := func() *roachpb.Error {
		txn.mu.Lock()
		defer txn.mu.Unlock()

		if txn.mu.Proto.Status != roachpb.PENDING || txn.mu.finalized {
			return roachpb.NewErrorf(
				"attempting to use transaction with wrong status or finalized: %s %v",
				txn.mu.Proto.Status, txn.mu.finalized)
		}

		// For testing purposes, txn.UserPriority can be a negative value (see
		// roachpb.MakePriority).
		if txn.mu.UserPriority != 0 {
			ba.UserPriority = txn.mu.UserPriority
		}

		needBeginTxn = !(txn.mu.Proto.Writing || txn.mu.writingTxnRecord) && haveTxnWrite
		needEndTxn := txn.mu.Proto.Writing || txn.mu.writingTxnRecord || haveTxnWrite
		elideEndTxn = haveEndTxn && !needEndTxn

		// If we're not yet writing in this txn, but intend to, insert a
		// begin transaction request before the first write command and update
		// transaction state accordingly.
		if needBeginTxn {
			// Set txn key based on the key of the first transactional write if
			// not already set. If the transaction already has a key (we're in a
			// restart), make sure we keep the anchor key the same.
			if len(txn.mu.Proto.Key) == 0 {
				txnAnchorKey := txn.mu.txnAnchorKey
				if len(txnAnchorKey) == 0 {
					txnAnchorKey = ba.Requests[0].GetInner().Header().Key
				}
				txn.mu.Proto.Key = txnAnchorKey
			}
			// Set the key in the begin transaction request to the txn's anchor key.
			bt := &roachpb.BeginTransactionRequest{
				Span: roachpb.Span{
					Key: txn.mu.Proto.Key,
				},
			}
			// Inject the new request before position firstWriteIdx, taking
			// care to avoid unnecessary allocations.
			oldRequests := ba.Requests
			ba.Requests = make([]roachpb.RequestUnion, len(ba.Requests)+1)
			copy(ba.Requests, oldRequests[:firstWriteIdx])
			ba.Requests[firstWriteIdx].MustSetInner(bt)
			copy(ba.Requests[firstWriteIdx+1:], oldRequests[firstWriteIdx:])
			// We're going to be writing the transaction record by sending the
			// begin transaction request.
			txn.mu.writingTxnRecord = true
		}

		// Initialize an uninitialized (ID == nil) Transaction proto.
		if !txn.mu.Proto.IsInitialized() {
			// The initial timestamp may be communicated by a higher layer.
			// If so, use that. Otherwise make up a new one.
			timestamp := txn.mu.Proto.OrigTimestamp
			if timestamp == (hlc.Timestamp{}) {
				timestamp = txn.db.clock.Now()
			}
			newTxn := roachpb.NewTransaction(
				txn.mu.Proto.Name,
				txn.mu.Proto.Key,
				ba.UserPriority,
				txn.mu.Proto.Isolation,
				timestamp,
				txn.db.clock.MaxOffset().Nanoseconds(),
			)
			// Use existing priority as a minimum. This is used on transaction
			// aborts to ratchet priority when creating successor transaction.
			if newTxn.Priority < txn.mu.Proto.Priority {
				newTxn.Priority = txn.mu.Proto.Priority
			}
			txn.mu.Proto = *newTxn
		}

		if elideEndTxn {
			ba.Requests = ba.Requests[:lastIndex]
		}

		// Increment the statement count sent through this transaction.
		txn.mu.commandCount += len(ba.Requests)

		// Clone the Txn's Proto so that future modifications can be made without
		// worrying about synchronization.
		newTxn := txn.mu.Proto.Clone()
		ba.Txn = &newTxn
		return nil
	}
	if pErr := lockedPrelude(); pErr != nil {
		return nil, pErr
	}

	// Send call through the DB.
	br, pErr := txn.db.send(ctx, ba)

	// Lock for the entire response postlude.
	txn.mu.Lock()
	defer txn.mu.Unlock()

	// If we inserted a begin transaction request, remove it here. We also
	// unset the flag writingTxnRecord flag in case another ever needs to
	// be sent again (for instance, if we're aborted and need to restart).
	if needBeginTxn {
		if br != nil && br.Responses != nil {
			br.Responses = append(br.Responses[:firstWriteIdx], br.Responses[firstWriteIdx+1:]...)
		}
		// Handle case where inserted begin txn confused an indexed error.
		if pErr != nil && pErr.Index != nil {
			idx := pErr.Index.Index
			if idx == int32(firstWriteIdx) {
				// An error was encountered on begin txn; disallow the indexing.
				pErr.Index = nil
			} else if idx > int32(firstWriteIdx) {
				// An error was encountered after begin txn; decrement index.
				pErr.SetErrorIndex(idx - 1)
			}
		}

		txn.mu.writingTxnRecord = false
	}
	if haveEndTxn {
		if pErr == nil || !endTxnRequest.Commit {
			// Finalize the transaction if either we sent a successful commit
			// EndTxnRequest, or sent a rollback EndTxnRequest (regardless of
			// if it succeeded).
			txn.mu.finalized = true
		}
	}

	if pErr != nil {
		if log.V(1) {
			log.Infof(ctx, "failed batch: %s", pErr)
		}
		txn.updateStateOnErrLocked(pErr)
		return nil, pErr
	}

	if br != nil {
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(txn.db.sender, br))
		}
		if len(br.CollectedSpans) != 0 {
			if err := tracing.IngestRemoteSpans(ctx, br.CollectedSpans); err != nil {
				return nil, roachpb.NewErrorf("error ingesting remote spans: %s", err)
			}
		}

		// Only successful requests can carry an updated Txn in their response
		// header. Any error (e.g. a restart) can have a Txn attached to them as
		// well; those update our local state in the same way for the next attempt.
		// The exception is if our transaction was aborted and needs to restart
		// from scratch, in which case we do just that.
		txn.mu.Proto.Update(br.Txn)
	}

	if elideEndTxn {
		// Check that read only transactions do not violate their deadline. This can NOT
		// happen since the txn deadline is normally updated when it is about to expire
		// or expired. We will just keep the code for safety (see TestReacquireLeaseOnRestart).
		if endTxnRequest.Deadline != nil {
			if endTxnRequest.Deadline.Less(txn.mu.Proto.Timestamp) {
				return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &txn.mu.Proto)
			}
		}
		// This normally happens on the server and sent back in response
		// headers, but this transaction was optimized away. The caller may
		// still inspect the transaction struct, so we manually update it
		// here to emulate a true transaction.
		if endTxnRequest.Commit {
			txn.mu.Proto.Status = roachpb.COMMITTED
		} else {
			txn.mu.Proto.Status = roachpb.ABORTED
		}
	}
	return br, nil
}

// firstWriteIndex returns the index of the first transactional write in the
// BatchRequest. Returns -1 if the batch has not intention to write. It also
// verifies that if an EndTransactionRequest is included, then it is the last
// request in the batch.
func firstWriteIndex(ba roachpb.BatchRequest) (int, *roachpb.Error) {
	firstWriteIdx := -1
	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < len(ba.Requests)-1 /* if not last*/ {
			if _, ok := args.(*roachpb.EndTransactionRequest); ok {
				return -1, roachpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if roachpb.IsTransactionWrite(args) {
			if firstWriteIdx == -1 {
				firstWriteIdx = i
			}
		}
	}
	return firstWriteIdx, nil
}

func (txn *Txn) updateStateOnErrLocked(pErr *roachpb.Error) {
	if pErr.TransactionRestart == roachpb.TransactionRestart_NONE {
		return
	}

	retryErr := pErr.GoError().(*roachpb.RetryableTxnError)
	if !txn.isRetryableErrMeantForTxnLocked(retryErr) {
		// If this happens, something is wrong; we've received an error that
		// wasn't meant for this transaction. This is a sign that we either
		// somehow ran another txn inside our txn and didn't properly terminate
		// its error, or our transaction got a TransactionAbortedError (and the
		// proto was reset), was retried, and then we still somehow managed to get
		// an error meant for the previous incarnation of the transaction.
		// Letting this wrong error slip here can cause us to retry the wrong
		// transaction.
		panic(fmt.Sprintf("Got a retryable error meant for a different transaction. "+
			"txn.mu.Proto.ID: %v, pErr.ID: %v", txn.mu.Proto.ID, pErr.GetTxn().ID))
	}

	// Reset the statement count as this is a retryable txn error.
	txn.mu.commandCount = 0

	if roachpb.TxnIDEqual(retryErr.TxnID, txn.mu.Proto.ID) {
		// Only update the proto if the retryable error is meant for the current
		// incarnation of the transaction. In other words, only update it if
		// retryErr.TxnID is not in txn.mu.previousIDs.
		switch retryErr.Cause.(type) {
		case *roachpb.TransactionAbortedError:
			// On Abort, save the old transaction ID so that concurrent requests
			// or delayed responses that that throw errors know that these errors
			// were sent to the correct transaction, even once the proto is reset.
			if txn.mu.previousIDs == nil {
				txn.mu.previousIDs = make(map[uuid.UUID]struct{})
			}
			txn.mu.previousIDs[*txn.mu.Proto.ID] = struct{}{}

			// Next, reset the transaction proto so we start anew on restart.
			txn.mu.Proto = roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					Isolation: txn.mu.Proto.Isolation,
				},
				Name: txn.mu.Proto.Name,
			}
			// Acts as a minimum priority on restart.
			if pErr.GetTxn() != nil {
				txn.mu.Proto.Priority = pErr.GetTxn().Priority
			}
		default:
			txn.mu.Proto.Update(pErr.GetTxn())
		}
	}
}
