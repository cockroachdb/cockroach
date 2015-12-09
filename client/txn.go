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
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/gogo/protobuf/proto"
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

func (ts *txnSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	// Send call through wrapped sender.

	ba.Txn = &ts.Proto
	ba.SetNewRequest()
	br, pErr := ts.wrapped.Send(ctx, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(ts.wrapped, br))
	}

	// Only successful requests can carry an updated Txn in their response
	// header. Any error (e.g. a restart) can have a Txn attached to them as
	// well; those update our local state in the same way for the next attempt.
	// The exception is if our transaction was aborted and needs to restart
	// from scratch, in which case we do just that.
	if pErr == nil {
		ts.Proto.Update(br.Txn)
		return br, nil
	} else if _, ok := pErr.GoError().(*roachpb.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		ts.Proto = roachpb.Transaction{
			Name:      ts.Proto.Name,
			Isolation: ts.Proto.Isolation,
		}
		// Acts as a minimum priority on restart.
		if pErr.Txn != nil {
			ts.Proto.Priority = pErr.Txn.Priority
		}
	} else if pErr.TransactionRestart != roachpb.TransactionRestart_ABORT {
		ts.Proto.Update(pErr.Txn)
	}
	return nil, pErr
}

// Txn is an in-progress distributed database transaction. A Txn is not safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db      DB
	wrapped Sender
	Proto   roachpb.Transaction
	// systemDBTrigger is set to true when modifying keys from the
	// SystemDB span. This sets the SystemDBTrigger on EndTransactionRequest.
	systemDBTrigger bool
}

// NewTxn returns a new txn.
func NewTxn(db DB) *Txn {
	txn := &Txn{
		db:      db,
		wrapped: db.sender,
	}
	txn.db.sender = (*txnSender)(txn)
	return txn
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
func (txn *Txn) SetIsolation(isolation roachpb.IsolationType) *roachpb.Error {
	if txn.Proto.Isolation != isolation {
		if txn.Proto.IsInitialized() {
			return roachpb.NewUErrorf("cannot change the isolation level of a running transaction")
		}
		txn.Proto.Isolation = isolation
	}
	return nil
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority int32) {
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.db.userPriority = float64(-priority)
}

// SetSystemDBTrigger sets the system db trigger to true on this transaction.
// This will impact the EndTransactionRequest.
func (txn *Txn) SetSystemDBTrigger() {
	txn.systemDBTrigger = true
}

// SystemDBTrigger returns the systemDBTrigger flag.
func (txn *Txn) SystemDBTrigger() bool {
	return txn.systemDBTrigger
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
	r, err := txn.Get(key)
	if err != nil {
		return err
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
	_, err := runOneResult(txn, b)
	return err
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
	_, err := runOneResult(txn, b)
	return err
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
	r, err := runOneResult(txn, b)
	return r.Rows, err
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

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (txn *Txn) Del(keys ...interface{}) *roachpb.Error {
	b := txn.NewBatch()
	b.Del(keys...)
	_, err := runOneResult(txn, b)
	return err
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned Result will contain 0 rows and Result.Err will indicate success
// or failure.
//
// key can be either a byte slice or a string.
func (txn *Txn) DelRange(begin, end interface{}) *roachpb.Error {
	b := txn.NewBatch()
	b.DelRange(begin, end)
	_, err := runOneResult(txn, b)
	return err
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
	_, err := txn.RunWithResponse(b)
	return err
}

// RunWithResponse is a version of Run that returns the BatchResponse.
func (txn *Txn) RunWithResponse(b *Batch) (*roachpb.BatchResponse, *roachpb.Error) {
	if err := b.prepare(); err != nil {
		return nil, err
	}
	return sendAndFill(txn.send, b)
}

func (txn *Txn) commit(deadline *roachpb.Timestamp) *roachpb.Error {
	return txn.sendEndTxnReq(true /* commit */, deadline)
}

// Cleanup cleans up the transaction as appropriate based on err.
func (txn *Txn) Cleanup(err *roachpb.Error) {
	if err != nil {
		if replyErr := txn.Rollback(); replyErr != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
	}
}

// CommitNoCleanup is the same as Commit but will not attempt to clean
// up on failure. It is exposed only for use in txn_correctness_test.go
// because those tests manipulate transaction state at a low level.
func (txn *Txn) CommitNoCleanup() *roachpb.Error {
	return txn.commit(nil)
}

// CommitInBatch executes the operations queued up within a batch and
// commits the transaction. Explicitly committing a transaction is
// optional, but more efficient than relying on the implicit commit
// performed when the transaction function returns without error.
// The batch must be created by this transaction.
func (txn *Txn) CommitInBatch(b *Batch) *roachpb.Error {
	_, err := txn.CommitInBatchWithResponse(b)
	return err
}

// CommitInBatchWithResponse is a version of CommitInBatch that returns the
// BatchResponse.
func (txn *Txn) CommitInBatchWithResponse(b *Batch) (*roachpb.BatchResponse, *roachpb.Error) {
	if txn != b.txn {
		return nil, roachpb.NewErrorf("a batch b can only be committed by b.txn")
	}
	b.reqs = append(b.reqs, endTxnReq(true /* commit */, nil, txn.SystemDBTrigger()))
	b.initResult(1, 0, nil)
	return txn.RunWithResponse(b)
}

// Commit sends an EndTransactionRequest with Commit=true.
func (txn *Txn) Commit() *roachpb.Error {
	err := txn.commit(nil)
	txn.Cleanup(err)
	return err
}

// CommitBy sends an EndTransactionRequest with Commit=true and
// Deadline=deadline.
func (txn *Txn) CommitBy(deadline roachpb.Timestamp) *roachpb.Error {
	err := txn.commit(&deadline)
	txn.Cleanup(err)
	return err
}

// Rollback sends an EndTransactionRequest with Commit=false.
func (txn *Txn) Rollback() *roachpb.Error {
	return txn.sendEndTxnReq(false /* commit */, nil)
}

func (txn *Txn) sendEndTxnReq(commit bool, deadline *roachpb.Timestamp) *roachpb.Error {
	_, err := txn.send(endTxnReq(commit, deadline, txn.SystemDBTrigger()))
	return err
}

func endTxnReq(commit bool, deadline *roachpb.Timestamp, hasTrigger bool) roachpb.Request {
	req := &roachpb.EndTransactionRequest{
		Commit:   commit,
		Deadline: deadline,
	}
	if hasTrigger {
		req.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
			ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
				SystemDBSpan: true,
			},
		}
	}
	return req
}

func (txn *Txn) exec(retryable func(txn *Txn) *roachpb.Error) *roachpb.Error {
	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	var err *roachpb.Error
	for r := retry.Start(txn.db.txnRetryOptions); r.Next(); {
		err = retryable(txn)
		if err == nil && txn.Proto.Status == roachpb.PENDING {
			// retryable succeeded, but didn't commit.
			err = txn.commit(nil)
		}

		if err != nil {
			switch err.TransactionRestart {
			case roachpb.TransactionRestart_IMMEDIATE:
				if log.V(2) {
					log.Warning(err)
				}
				r.Reset()
				continue
			case roachpb.TransactionRestart_BACKOFF:
				if log.V(2) {
					log.Warning(err)
				}
				continue
			}
			// By default, fall through and break.

		}
		break
	}
	txn.Cleanup(err)
	return err
}

// send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only or has already
// been successfully committed or aborted, a potential trailing
// EndTransaction call is silently dropped, allowing the caller to
// always commit or clean-up explicitly even when that may not be
// required (or even erroneous).
func (txn *Txn) send(reqs ...roachpb.Request) (*roachpb.BatchResponse, *roachpb.Error) {

	if txn.Proto.Status != roachpb.PENDING {
		return nil, roachpb.NewErrorf("attempting to use %s transaction", txn.Proto.Status)
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
		reqs = append(append(append([]roachpb.Request(nil), reqs[:firstWriteIndex]...), bt), reqs[firstWriteIndex:]...)
	}

	if elideEndTxn {
		reqs = reqs[:lastIndex]
	}

	br, pErr := txn.db.send(reqs...)
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
				pErr = roachpb.NewErrorf("error on begin transaction: %s", pErr)
			} else if idx > int32(firstWriteIndex) {
				// An error was encountered after begin txn; decrement index.
				pErr.SetErrorIndex(idx - 1)
			}
		}
	}
	return br, pErr
}
