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
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/cockroachdb/cockroach/proto"
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
		BackOff: backoff.ExponentialBackOff{
			Clock:           backoff.SystemClock,
			InitialInterval: 50 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			Multiplier:      2,
		},
		UseV1Info: true,
	}
)

// txnSender implements the Sender interface and is used to keep the Send
// method out of the Txn method set.
type txnSender Txn

func (ts *txnSender) Send(ctx context.Context, call Call) {
	// Send call through wrapped sender.
	call.Args.Header().Txn = &ts.txn
	ts.wrapped.Send(ctx, call)
	ts.txn.Update(call.Reply.Header().Txn)

	if err, ok := call.Reply.Header().GoError().(*proto.TransactionAbortedError); ok {
		// On Abort, reset the transaction so we start anew on restart.
		ts.txn = proto.Transaction{
			Name:      ts.txn.Name,
			Isolation: ts.txn.Isolation,
			Priority:  err.Txn.Priority, // acts as a minimum priority on restart
		}
	}
}

// Txn is an in-progress distributed database transaction. A Txn is not safe for
// concurrent use by multiple goroutines.
type Txn struct {
	db           DB
	wrapped      Sender
	txn          proto.Transaction
	haveTxnWrite bool // True if there were transactional writes
	haveEndTxn   bool // True if there was an explicit EndTransaction
}

func newTxn(db DB, depth int) *Txn {
	txn := &Txn{
		db:      db,
		wrapped: db.Sender,
	}
	txn.db.Sender = (*txnSender)(txn)

	if _, file, line, ok := runtime.Caller(depth + 1); ok {
		// TODO(pmattis): include the parent directory?
		txn.txn.Name = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	return txn
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI. Each transaction starts out with an
// automatically assigned debug name composed of the file and line number where
// the transaction was created.
func (txn *Txn) SetDebugName(name string) {
	if _, file, line, ok := runtime.Caller(1); ok {
		txn.txn.Name = fmt.Sprintf("%s:%d: %s", filepath.Base(file), line, name)
	} else {
		txn.txn.Name = name
	}
}

// DebugName returns the debug name associated with the transaction.
func (txn *Txn) DebugName() string {
	return txn.txn.Name
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
	txn.txn.Isolation = proto.SNAPSHOT
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (txn *Txn) InternalSetPriority(priority int32) {
	// The negative user priority is translated on the server into a positive,
	// non-randomized, priority for the transaction.
	txn.db.userPriority = -priority
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
	b := &Batch{}
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
	b := &Batch{}
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
	b := &Batch{}
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
	b := &Batch{}
	b.Inc(key, value)
	return runOneRow(txn, b)
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive).
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	b := &Batch{}
	b.Scan(begin, end, maxRows)
	r, err := runOneResult(txn, b)
	return r.Rows, err
}

// Del deletes one or more keys.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (txn *Txn) Del(keys ...interface{}) error {
	b := &Batch{}
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
	b := &Batch{}
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

// Commit executes the operations queued up within a batch and commits the
// transaction. Explicitly committing a transaction is optional, but more
// efficient than relying on the implicit commit performed when the transaction
// function returns without error.
func (txn *Txn) Commit(b *Batch) error {
	args := &proto.EndTransactionRequest{Commit: true}
	reply := &proto.EndTransactionResponse{}
	b.calls = append(b.calls, Call{Args: args, Reply: reply})
	b.initResult(1, 0, nil)
	return txn.Run(b)
}

func (txn *Txn) exec(retryable func(txn *Txn) error) error {
	// Run retryable in a retry loop until we encounter a success or
	// error condition this loop isn't capable of handling.
	retryOpts := txn.db.txnRetryOptions
	retryOpts.Tag = txn.txn.Name
	err := retry.WithBackoff(retryOpts, func(r *retry.Retry) error {
		txn.haveTxnWrite, txn.haveEndTxn = false, false // always reset before [re]starting txn
		err := retryable(txn)
		if err == nil {
			if !txn.haveEndTxn && txn.haveTxnWrite {
				// If there were no errors running retryable, commit the txn. This
				// may block waiting for outstanding writes to complete in case
				// retryable didn't -- we need the most recent of all response
				// timestamps in order to commit.
				etArgs := &proto.EndTransactionRequest{Commit: true}
				etReply := &proto.EndTransactionResponse{}
				err = txn.send(Call{Args: etArgs, Reply: etReply})
			}
		}
		if err == nil {
			return nil
		}
		if restartErr, ok := err.(proto.TransactionRestartError); ok {
			if restartErr.CanRestartTransaction() == proto.TransactionRestart_IMMEDIATE {
				r.Reset()
				return err
			} else if restartErr.CanRestartTransaction() == proto.TransactionRestart_BACKOFF {
				return err
			}
		}
		r.Stop()
		return err
	})
	if err != nil && txn.haveTxnWrite {
		if replyErr := txn.send(Call{
			Args:  &proto.EndTransactionRequest{Commit: false},
			Reply: &proto.EndTransactionResponse{},
		}); replyErr != nil {
			log.Errorf("failure aborting transaction: %s; abort caused by: %s", replyErr, err)
		}
		return err
	}
	return err
}

// send runs the specified calls synchronously in a single batch and
// returns any errors.
func (txn *Txn) send(calls ...Call) error {
	if len(calls) == 0 {
		return nil
	}
	txn.updateState(calls)
	return txn.db.send(calls...)
}

func (txn *Txn) updateState(calls []Call) {
	for _, c := range calls {
		if b, ok := c.Args.(*proto.BatchRequest); ok {
			for _, br := range b.Requests {
				txn.updateStateForRequest(br.GetValue().(proto.Request))
			}
			continue
		}
		txn.updateStateForRequest(c.Args)
	}
}

func (txn *Txn) updateStateForRequest(r proto.Request) {
	if !txn.haveTxnWrite {
		txn.haveTxnWrite = proto.IsTransactionWrite(r)
	} else if _, ok := r.(*proto.EndTransactionRequest); ok {
		txn.haveEndTxn = true
	}
}
