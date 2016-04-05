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
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/gogo/protobuf/proto"
	basictracer "github.com/opentracing/basictracer-go"
)

// DefaultTxnRetryOptions are the standard retry options used
// for transactions.
// This is exported for testing purposes only.
var DefaultTxnRetryOptions = retry.Options{
	InitialBackoff: 50 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	Multiplier:     2,
}

// txnSender implements the Sender interface and is used to keep the Send
// method out of the Txn method set.
type txnSender Txn

// Send updates the transaction on error. Depending on the error type, the
// transaction might be replaced by a new one.
func (ts *txnSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	// Send call through wrapped sender.
	ba.Txn = &ts.Proto
	// For testing purposes, ts.UserPriority can be a negative value (see
	// MakePriority).
	if ts.UserPriority != 0 {
		ba.UserPriority = ts.UserPriority
	}

	br, pErr := ts.wrapped.Send(ts.Context, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(ts.wrapped, br))
	}

	if br != nil {
		for _, encSp := range br.CollectedSpans {
			var newSp basictracer.RawSpan
			if err := tracing.DecodeRawSpan(encSp, &newSp); err != nil {
				return nil, roachpb.NewError(err)
			}
			ts.CollectedSpans = append(ts.CollectedSpans, newSp)
		}
	}
	// Only successful requests can carry an updated Txn in their response
	// header. Any error (e.g. a restart) can have a Txn attached to them as
	// well; those update our local state in the same way for the next attempt.
	// The exception is if our transaction was aborted and needs to restart
	// from scratch, in which case we do just that.
	if pErr == nil {
		ts.Proto.Update(br.Txn)
		return br, nil
	} else if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		ts.Proto = roachpb.Transaction{
			TxnMeta: roachpb.TxnMeta{
				Isolation: ts.Proto.Isolation,
			},
			Name: ts.Proto.Name,
		}
		// Acts as a minimum priority on restart.
		if pErr.GetTxn() != nil {
			ts.Proto.Priority = pErr.GetTxn().Priority
		}
	} else if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
		ts.Proto.Update(pErr.GetTxn())
	}
	return nil, pErr
}

// Txn is an in-progress distributed database transaction. A Txn is not safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db             DB
	wrapped        Sender
	Proto          roachpb.Transaction
	UserPriority   roachpb.UserPriority
	Context        context.Context // must not be nil
	CollectedSpans []basictracer.RawSpan
	// systemConfigTrigger is set to true when modifying keys from the SystemConfig
	// span. This sets the SystemConfigTrigger on EndTransactionRequest.
	systemConfigTrigger bool
	retrying            bool
	// see IsFinalized()
	finalized bool
}

// NewTxn returns a new txn.
func NewTxn(ctx context.Context, db DB) *Txn {
	txn := &Txn{
		db:      db,
		wrapped: db.sender,
		Context: ctx,
	}
	txn.db.sender = (*txnSender)(txn)
	return txn
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
// appear in log files and the web UI. Each transaction starts out with an
// automatically assigned debug name composed of the file and line number where
// the transaction was created.
func (txn *Txn) SetDebugName(name string, depth int) {
	file, line, fun := caller.Lookup(depth + 1)
	if name == "" {
		name = fun
	}
	txn.Proto.Name = file + ":" + strconv.Itoa(line) + " " + name
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	return txn.Proto.Name
}

// SetIsolation sets the transaction's isolation type. Transactions default to
// serializable isolation. The isolation must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetIsolation(isolation roachpb.IsolationType) error {
	if txn.retrying {
		if txn.Proto.Isolation != isolation {
			return util.Errorf("cannot change the isolation level of a retrying transaction")
		}
		return nil
	}
	if txn.Proto.IsInitialized() {
		return util.Errorf("cannot change the isolation level of a running transaction")
	}
	txn.Proto.Isolation = isolation
	return nil
}

// SetUserPriority sets the transaction's user priority. Transactions default to
// normal user priority. The user priority must be set before any operations are
// performed on the transaction.
func (txn *Txn) SetUserPriority(userPriority roachpb.UserPriority) error {
	if txn.retrying {
		if txn.UserPriority != userPriority {
			return util.Errorf("cannot change the user priority of a retrying transaction")
		}
		return nil
	}
	if txn.Proto.IsInitialized() {
		return util.Errorf("cannot change the user priority of a running transaction")
	}
	if userPriority < roachpb.MinUserPriority || userPriority > roachpb.MaxUserPriority {
		return util.Errorf("the given user priority %f is out of the allowed range [%f, %d]", userPriority, roachpb.MinUserPriority, roachpb.MaxUserPriority)
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
func (txn *Txn) SetSystemConfigTrigger() {
	txn.systemConfigTrigger = true
}

// SystemConfigTrigger returns the systemConfigTrigger flag.
func (txn *Txn) SystemConfigTrigger() bool {
	return txn.systemConfigTrigger
}

// NewBatch creates and returns a new empty batch object for use with the Txn.
func (txn *Txn) NewBatch() *Batch {
	return &Batch{DB: &txn.db, txn: txn}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (txn *Txn) Get(key interface{}) (KeyValue, *roachpb.Error) {
	b := txn.NewBatch()
	b.Get(key)
	return runOneRow(txn, b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message.
//
// key can be either a byte slice or a string.
func (txn *Txn) GetProto(key interface{}, msg proto.Message) *roachpb.Error {
	r, pErr := txn.Get(key)
	if pErr != nil {
		return pErr
	}
	return roachpb.NewError(r.ValueProto(msg))
}

// Put sets the value for a key
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) Put(key, value interface{}) *roachpb.Error {
	b := txn.NewBatch()
	b.Put(key, value)
	_, pErr := runOneResult(txn, b)
	return pErr
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue. Note that this must be an interface{}(nil), not a
// typed nil value (e.g. []byte(nil)).
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (txn *Txn) CPut(key, value, expValue interface{}) *roachpb.Error {
	b := txn.NewBatch()
	b.CPut(key, value, expValue)
	_, pErr := runOneResult(txn, b)
	return pErr
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// The returned Result will contain a single row and Result.Err will indicate
// success or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) Inc(key interface{}, value int64) (KeyValue, *roachpb.Error) {
	b := txn.NewBatch()
	b.Inc(key, value)
	return runOneRow(txn, b)
}

func (txn *Txn) scan(begin, end interface{}, maxRows int64, isReverse bool) ([]KeyValue, *roachpb.Error) {
	b := txn.NewBatch()
	if !isReverse {
		b.Scan(begin, end, maxRows)
	} else {
		b.ReverseScan(begin, end, maxRows)
	}
	r, pErr := runOneResult(txn, b)
	return r.Rows, pErr
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (txn *Txn) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, *roachpb.Error) {
	return txn.scan(begin, end, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (txn *Txn) ReverseScan(begin, end interface{}, maxRows int64) ([]KeyValue, *roachpb.Error) {
	return txn.scan(begin, end, maxRows, true)
}

// TODO(pmattis): Txn.ReverseScan is neither used or tested. Silence unused
// warning.
var _ = (*Txn)(nil).ReverseScan

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (txn *Txn) Del(keys ...interface{}) *roachpb.Error {
	b := txn.NewBatch()
	b.Del(keys...)
	_, pErr := runOneResult(txn, b)
	return pErr
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned Result will contain 0 rows and Result.Err will indicate success
// or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) DelRange(begin, end interface{}) *roachpb.Error {
	b := txn.NewBatch()
	b.DelRange(begin, end, false)
	_, pErr := runOneResult(txn, b)
	return pErr
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
func (txn *Txn) Run(b *Batch) *roachpb.Error {
	_, pErr := txn.RunWithResponse(b)
	return pErr
}

// RunWithResponse is a version of Run that returns the BatchResponse.
func (txn *Txn) RunWithResponse(b *Batch) (*roachpb.BatchResponse, *roachpb.Error) {
	tracing.AnnotateTrace()
	defer tracing.AnnotateTrace()

	if pErr := b.prepare(); pErr != nil {
		return nil, pErr
	}
	return sendAndFill(txn.send, b)
}

func (txn *Txn) commit(deadline *roachpb.Timestamp) *roachpb.Error {
	pErr := txn.sendEndTxnReq(true /* commit */, deadline)
	if pErr == nil {
		txn.finalized = true
	}
	return pErr
}

// CleanupOnError cleans up the transaction as a result of an error.
func (txn *Txn) CleanupOnError(pErr *roachpb.Error) {
	if pErr == nil {
		panic("no error")
	}
	if replyErr := txn.Rollback(); replyErr != nil {
		log.Errorf("failure aborting transaction: %s; abort caused by: %s", replyErr, pErr)
	}
}

// Commit is the same as CommitOrCleanup but will not attempt to clean
// up on failure. This can be used when the caller is prepared to do proper
// cleanup.
func (txn *Txn) Commit() *roachpb.Error {
	return txn.commit(nil)
}

// CommitInBatch executes the operations queued up within a batch and
// commits the transaction. Explicitly committing a transaction is
// optional, but more efficient than relying on the implicit commit
// performed when the transaction function returns without error.
// The batch must be created by this transaction.
// If the command completes successfully, the txn is considered finalized. On
// error, no attempt is made to clean up the (possibly still pending)
// transaction.
func (txn *Txn) CommitInBatch(b *Batch) *roachpb.Error {
	_, pErr := txn.CommitInBatchWithResponse(b)
	return pErr
}

// CommitInBatchWithResponse is a version of CommitInBatch that returns the
// BatchResponse.
func (txn *Txn) CommitInBatchWithResponse(b *Batch) (*roachpb.BatchResponse, *roachpb.Error) {
	if txn != b.txn {
		return nil, roachpb.NewErrorf("a batch b can only be committed by b.txn")
	}
	b.reqs = append(b.reqs, endTxnReq(true /* commit */, nil, txn.SystemConfigTrigger()))
	b.initResult(1, 0, nil)
	resp, pErr := txn.RunWithResponse(b)
	if pErr == nil {
		txn.finalized = true
	}
	return resp, pErr
}

// CommitOrCleanup sends an EndTransactionRequest with Commit=true.
// If that fails, an attempt to rollback is made.
// txn should not be used to send any more commands after this call.
func (txn *Txn) CommitOrCleanup() *roachpb.Error {
	pErr := txn.commit(nil)
	if pErr != nil {
		txn.CleanupOnError(pErr)
	}
	if !txn.IsFinalized() {
		panic("Commit() failed to move txn to a final state")
	}
	return pErr
}

// CommitBy sends an EndTransactionRequest with Commit=true and
// Deadline=deadline.
func (txn *Txn) CommitBy(deadline roachpb.Timestamp) *roachpb.Error {
	pErr := txn.commit(&deadline)
	if pErr != nil {
		txn.CleanupOnError(pErr)
	}
	return pErr
}

// Rollback sends an EndTransactionRequest with Commit=false.
// The txn's status is set to ABORTED in case of error. txn is
// considered finalized and cannot be used to send any more commands.
func (txn *Txn) Rollback() *roachpb.Error {
	err := txn.sendEndTxnReq(false /* commit */, nil)
	txn.finalized = true
	return err
}

func (txn *Txn) sendEndTxnReq(commit bool, deadline *roachpb.Timestamp) *roachpb.Error {
	_, pErr := txn.send(0, roachpb.CONSISTENT, endTxnReq(commit, deadline, txn.SystemConfigTrigger()))
	return pErr
}

func endTxnReq(commit bool, deadline *roachpb.Timestamp, hasTrigger bool) roachpb.Request {
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
	// Minimum initial timestamp, if so desired by a higher level (e.g. sql.Executor).
	MinInitialTimestamp roachpb.Timestamp
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
//
// TODO(andrei): Make Exec() return error; make fn return an error + a retriable
// bit. There's no reason to propagate roachpb.Error (protos) above this point.
func (txn *Txn) Exec(
	opt TxnExecOptions,
	fn func(txn *Txn, opt *TxnExecOptions) *roachpb.Error) *roachpb.Error {
	// Run fn in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	var pErr *roachpb.Error
	var retryOptions retry.Options
	if txn == nil && (opt.AutoRetry || opt.AutoCommit) {
		panic("asked to retry or commit a txn that is already aborted")
	}

	if opt.AutoRetry {
		retryOptions = txn.db.txnRetryOptions
	}
RetryLoop:
	for r := retry.Start(retryOptions); r.Next(); {
		if txn != nil {
			// If we're looking at a brand new transaction, then communicate
			// what should be used as initial timestamp for the KV txn created
			// by TxnCoordSender.
			if txn.Proto.OrigTimestamp == roachpb.ZeroTimestamp {
				txn.Proto.OrigTimestamp = opt.MinInitialTimestamp
			}
		}

		pErr = fn(txn, &opt)
		if txn != nil {
			txn.retrying = true
			defer func() {
				txn.retrying = false
			}()
		}
		if (pErr == nil) && opt.AutoCommit && (txn.Proto.Status == roachpb.PENDING) {
			// fn succeeded, but didn't commit.
			pErr = txn.Commit()
		}

		if pErr == nil {
			break
		}

		// Make sure the txn record that pErr carries is for this txn.
		// We check only when txn.Proto.ID has been initialized after an initial successful send.
		if pErr.GetTxn() != nil && txn.Proto.ID != nil {
			if errTxn := pErr.GetTxn(); !errTxn.Equal(&txn.Proto) {
				return roachpb.NewErrorf("mismatching transaction record in the error:\n%s\nv.s.\n%s",
					errTxn, txn.Proto)
			}
		}

		if !opt.AutoRetry {
			break RetryLoop
		}
		switch pErr.TransactionRestart {
		case roachpb.TransactionRestart_IMMEDIATE:
			r.Reset()
		case roachpb.TransactionRestart_BACKOFF:
		default:
			break RetryLoop
		}
		if log.V(2) {
			log.Infof("automatically retrying transaction: %s because of error: %s",
				txn.DebugName(), pErr)
		}
	}

	if pErr != nil {
		pErr.StripErrorTransaction()
	}
	return pErr
}

// send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only or has already
// been successfully committed or aborted, a potential trailing
// EndTransaction call is silently dropped, allowing the caller to
// always commit or clean-up explicitly even when that may not be
// required (or even erroneous).
func (txn *Txn) send(maxScanResults int64, readConsistency roachpb.ReadConsistencyType, reqs ...roachpb.Request) (
	*roachpb.BatchResponse, *roachpb.Error) {

	if txn.Proto.Status != roachpb.PENDING || txn.IsFinalized() {
		return nil, roachpb.NewErrorf(
			"attempting to use transaction with wrong status or finalized: %s", txn.Proto.Status)
	}

	// It doesn't make sense to use inconsistent reads in a transaction. However,
	// we still need to accept it as a parameter for this to compile.
	if readConsistency != roachpb.CONSISTENT {
		return nil, roachpb.NewErrorf("attempting to use %d readConsistency in a txn", readConsistency)
	}

	lastIndex := len(reqs) - 1
	if lastIndex < 0 {
		return &roachpb.BatchResponse{}, nil
	}

	// firstWriteIndex is set to the index of the first command which is
	// a transactional write. If != -1, this indicates an intention to
	// write. This is in contrast to txn.Proto.Writing, which is set by
	// the coordinator when the first intent has been created, and which
	// lives for the life of the transaction.
	firstWriteIndex := -1
	var firstWriteKey roachpb.Key

	for i, args := range reqs {
		if i < lastIndex {
			if _, ok := args.(*roachpb.EndTransactionRequest); ok {
				return nil, roachpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if roachpb.IsTransactionWrite(args) && firstWriteIndex == -1 {
			firstWriteKey = args.Header().Key
			firstWriteIndex = i
		}
	}

	haveTxnWrite := firstWriteIndex != -1
	endTxnRequest, haveEndTxn := reqs[lastIndex].(*roachpb.EndTransactionRequest)
	needBeginTxn := !txn.Proto.Writing && haveTxnWrite
	needEndTxn := txn.Proto.Writing || haveTxnWrite
	elideEndTxn := haveEndTxn && !needEndTxn

	// If we're not yet writing in this txn, but intend to, insert a
	// begin transaction request before the first write command.
	if needBeginTxn {
		bt := &roachpb.BeginTransactionRequest{
			Span: roachpb.Span{
				Key: firstWriteKey,
			},
		}
		// If the transaction already has a key (we're in a restart), make
		// sure we set the key in the begin transaction request to the original.
		if txn.Proto.Key != nil {
			bt.Key = txn.Proto.Key
		}
		reqs = append(append(append([]roachpb.Request(nil), reqs[:firstWriteIndex]...), bt), reqs[firstWriteIndex:]...)
	}

	if elideEndTxn {
		reqs = reqs[:lastIndex]
	}

	br, pErr := txn.db.send(maxScanResults, readConsistency, reqs...)
	if elideEndTxn && pErr == nil {
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
