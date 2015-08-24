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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	gogoproto "github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
)

var (
	// DefaultTxnRetryOptions are the standard retry options used
	// for transactions.
	// This is exported for testing purposes only.
	DefaultTxnRetryOptions = retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2,
	}
	errMultipleEndTxn = errors.New("cannot end transaction multiple times")
)

// txnSender implements the Sender interface and is used to keep the Send
// method out of the Txn method set.
type txnSender Txn

func (ts *txnSender) Send(ctx context.Context, call proto.Call) {
	// Send call through wrapped sender.
	call.Args.Header().Txn = &ts.Proto
	ts.wrapped.Send(ctx, call)
	ts.Proto.Update(call.Reply.Header().Txn)

	if err, ok := call.Reply.Header().GoError().(*proto.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		ts.Proto = proto.Transaction{
			Name:      ts.Proto.Name,
			Isolation: ts.Proto.Isolation,
			Priority:  err.Txn.Priority, // acts as a minimum priority on restart
		}
	}
}

// Txn is an in-progress distributed database transaction. A Txn is not safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db      DB
	wrapped Sender
	Proto   proto.Transaction
	// haveTxnWrite is true as soon as the current attempt contains a write
	// (prior to sending). This is in contrast to txn.Writing, which is set
	// by the coordinator when the first intent has been created, and which
	// does not reset in-between retries. As such, haveTxnWrite helps deter-
	// mine whether it makes sense to add an EndTransaction when txn.Writing
	// is (still) unset.
	haveTxnWrite bool
	haveEndTxn   bool // True if there was an explicit EndTransaction
}

func newTxn(db DB) *Txn {
	txn := &Txn{
		db:      db,
		wrapped: db.Sender,
	}
	txn.db.Sender = (*txnSender)(txn)

	// Caller's caller.
	file, line, fun := caller.Lookup(2)
	txn.Proto.Name = fmt.Sprintf("%s:%d %s", file, line, fun)
	return txn
}

// NewTxn returns a new txn.
func NewTxn(db DB) *Txn {
	return newTxn(db)
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI. Each transaction starts out with an
// automatically assigned debug name composed of the file and line number where
// the transaction was created.
func (txn *Txn) SetDebugName(name string) {
	file, line, _ := caller.Lookup(1)
	txn.Proto.Name = fmt.Sprintf("%s:%d %s", file, line, name)
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	return txn.Proto.Name
}

// SetSnapshotIsolation sets the transaction's isolation type to
// snapshot. Transactions default to serializable isolation. The
// isolation must be set before any operations are performed on the
// transaction.
//
// TODO(pmattis): This isn't tested yet but will be as part of the
// conversion of client_test.go.
func (txn *Txn) SetSnapshotIsolation() {
	// TODO(pmattis): Panic if the transaction has already had
	// operations run on it. Needs to tie into the Txn reset in case of
	// retries.
	txn.Proto.Isolation = proto.SNAPSHOT
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority int32) {
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.db.userPriority = -priority
}

// NewBatch creates and returns a new empty batch object for use with the Txn.
func (txn *Txn) NewBatch() *Batch {
	return &Batch{DB: &txn.db}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) Get(key interface{}) (KeyValue, error) {
	b := txn.NewBatch()
	b.Get(key)
	return runOneRow(txn, b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) GetProto(key interface{}, msg gogoproto.Message) error {
	r, err := txn.Get(key)
	if err != nil {
		return err
	}
	return r.ValueProto(msg)
}

// Put sets the value for a key
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (txn *Txn) Put(key, value interface{}) error {
	b := txn.NewBatch()
	b.Put(key, value)
	_, err := runOneResult(txn, b)
	return err
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (txn *Txn) CPut(key, value, expValue interface{}) error {
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
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) Inc(key interface{}, value int64) (KeyValue, error) {
	b := txn.NewBatch()
	b.Inc(key, value)
	return runOneRow(txn, b)
}

func (txn *Txn) scan(begin, end interface{}, maxRows int64, isReverse bool) ([]KeyValue, error) {
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
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return txn.scan(begin, end, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) ReverseScan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return txn.scan(begin, end, maxRows, true)
}

// Del deletes one or more keys.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) Del(keys ...interface{}) error {
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
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) DelRange(begin, end interface{}) error {
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
func (txn *Txn) Run(b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	if err := txn.send(b.calls...); err != nil {
		return err
	}
	return b.fillResults()
}

// CommitInBatch executes the operations queued up within a batch and
// commits the transaction. Explicitly committing a transaction is
// optional, but more efficient than relying on the implicit commit
// performed when the transaction function returns without error.
func (txn *Txn) CommitInBatch(b *Batch) error {
	b.calls = append(b.calls, endTxnCall(true /* commit */))
	b.initResult(1, 0, nil)
	return txn.Run(b)
}

// Commit sends an EndTransactionRequest with Commit=true.
func (txn *Txn) Commit() error {
	if txn.Proto.Writing {
		return txn.sendEndTxnCall(true /* commit */)
	}
	return nil
}

// Rollback sends an EndTransactionRequest with Commit=false.
func (txn *Txn) Rollback() error {
	var err error
	if txn.Proto.Writing {
		err = txn.sendEndTxnCall(false /* commit */)
	}
	// Explicitly set the status as ABORTED so that higher layers
	// know that this transaction has ended.
	txn.Proto.Status = proto.ABORTED
	return err
}

func (txn *Txn) sendEndTxnCall(commit bool) error {
	return txn.send(endTxnCall(commit))
}

func endTxnCall(commit bool) proto.Call {
	return proto.Call{
		Args:  &proto.EndTransactionRequest{Commit: commit},
		Reply: &proto.EndTransactionResponse{},
	}
}

func (txn *Txn) exec(retryable func(txn *Txn) error) (err error) {
	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	for r := retry.Start(txn.db.txnRetryOptions); r.Next(); {
		txn.haveTxnWrite, txn.haveEndTxn = false, false // always reset before [re]starting txn
		if err = retryable(txn); err == nil {
			if !txn.haveEndTxn {
				// If there were no errors running retryable, commit the txn. This
				// may block waiting for outstanding writes to complete in case
				// retryable didn't -- we need the most recent of all response
				// timestamps in order to commit.
				err = txn.Commit()
			}
		}
		if restartErr, ok := err.(proto.TransactionRestartError); ok {
			if log.V(2) {
				log.Warning(err)
			}
			if restartErr.CanRestartTransaction() == proto.TransactionRestart_IMMEDIATE {
				r.Reset()
				continue
			} else if restartErr.CanRestartTransaction() == proto.TransactionRestart_BACKOFF {
				continue
			}
			// By default, fall through and break.
		}
		break
	}
	if err != nil {
		// If the retry logic gave up and haveEndTxn is true, then even if we
		// tried to run EndTransaction, it must have failed (or was never run;
		// after all, it's always the last call to be executed). So we pretend
		// we never sent one (this is necessary to send another one).
		txn.haveEndTxn = false // if we sent one, it didn't succeed
		if replyErr := txn.Rollback(); replyErr != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
	}
	return
}

// send runs the specified calls synchronously in a single batch and
// returns any errors. If the transaction is read-only, a potential
// EndTransaction call at the end is trimmed.
func (txn *Txn) send(calls ...proto.Call) error {
	if len(calls) == 0 {
		return nil
	}
	if err := txn.updateState(calls); err != nil {
		return err
	}

	// If the transaction record indicates that the coordinator never wrote
	// an intent (and the client doesn't have one lined up), then there's no
	// need to send EndTransaction. If there is one anyways, cut it off.
	if txn.haveEndTxn && !(txn.Proto.Writing || txn.haveTxnWrite) {
		// There's always a call if we get here.
		lastIndex := len(calls) - 1
		if calls[lastIndex].Method() != proto.EndTransaction {
			panic("EndTransaction not sent as last call")
		}
		calls = calls[0:lastIndex]
	}
	return txn.db.send(calls...)
}

func (txn *Txn) updateState(calls []proto.Call) error {
	for _, c := range calls {
		if b, ok := c.Args.(*proto.BatchRequest); ok {
			for _, br := range b.Requests {
				if err := txn.updateStateForRequest(br.GetValue().(proto.Request)); err != nil {
					return err
				}
			}
			continue
		}
		if err := txn.updateStateForRequest(c.Args); err != nil {
			return err
		}
	}
	return nil
}

func (txn *Txn) updateStateForRequest(r proto.Request) error {
	if !txn.haveTxnWrite {
		txn.haveTxnWrite = proto.IsTransactionWrite(r)
	}
	if _, ok := r.(*proto.EndTransactionRequest); ok {
		if txn.haveEndTxn {
			return errMultipleEndTxn
		}
		txn.haveEndTxn = true
	}
	return nil
}
