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
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/gogo/protobuf/proto"
)

// KeyValue represents a single key/value pair and corresponding
// timestamp. This is similar to roachpb.KeyValue except that the value may be
// nil.
type KeyValue struct {
	Key   roachpb.Key
	Value *roachpb.Value // Timestamp will always be zero
}

func (kv *KeyValue) String() string {
	return kv.Key.String() + "=" + kv.PrettyValue()
}

// Exists returns true iff the value exists.
func (kv *KeyValue) Exists() bool {
	return kv.Value != nil
}

// PrettyValue returns a human-readable version of the value as a string.
func (kv *KeyValue) PrettyValue() string {
	if kv.Value == nil {
		return "nil"
	}
	switch kv.Value.GetTag() {
	case roachpb.ValueType_INT:
		v, err := kv.Value.GetInt()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%d", v)
	case roachpb.ValueType_FLOAT:
		v, err := kv.Value.GetFloat()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%v", v)
	case roachpb.ValueType_BYTES:
		v, err := kv.Value.GetBytes()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%q", v)
	case roachpb.ValueType_TIME:
		v, err := kv.Value.GetTime()
		if err != nil {
			return fmt.Sprintf("%v", err)
		}
		return fmt.Sprintf("%s", v)
	}
	return fmt.Sprintf("%q", kv.Value.RawBytes)
}

// ValueBytes returns the value as a byte slice. This method will panic if the
// value's type is not a byte slice.
func (kv *KeyValue) ValueBytes() []byte {
	if kv.Value == nil {
		return nil
	}
	bytes, err := kv.Value.GetBytes()
	if err != nil {
		panic(err)
	}
	return bytes
}

// ValueInt returns the value decoded as an int64. This method will panic if
// the value cannot be decoded as an int64.
func (kv *KeyValue) ValueInt() int64 {
	if kv.Value == nil {
		return 0
	}
	i, err := kv.Value.GetInt()
	if err != nil {
		panic(err)
	}
	return i
}

// ValueProto parses the byte slice value into msg.
func (kv *KeyValue) ValueProto(msg proto.Message) error {
	if kv.Value == nil {
		msg.Reset()
		return nil
	}
	return kv.Value.GetProto(msg)
}

// Result holds the result for a single DB or Txn operation (e.g. Get, Put,
// etc).
type Result struct {
	calls int
	// Err contains any error encountered when performing the operation.
	PErr *roachpb.Error
	// Rows contains the key/value pairs for the operation. The number of rows
	// returned varies by operation. For Get, Put, CPut, Inc and Del the number
	// of rows returned is the number of keys operated on. For Scan the number of
	// rows returned is the number or rows matching the scan capped by the
	// maxRows parameter. For DelRange Rows is nil.
	Rows []KeyValue

	// Keys is set by some operations instead of returning the rows themselves.
	Keys []roachpb.Key
}

func (r Result) String() string {
	if r.PErr != nil {
		return r.PErr.String()
	}
	var buf bytes.Buffer
	for i, row := range r.Rows {
		if i > 0 {
			buf.WriteString("\n")
		}
		fmt.Fprintf(&buf, "%d: %s", i, &row)
	}
	return buf.String()
}

// DB is a database handle to a single cockroach cluster. A DB is safe for
// concurrent use by multiple goroutines.
type DB struct {
	sender Sender

	// userPriority is the default user priority to set on API calls. If
	// userPriority is set to any value except 1 in call arguments, this
	// value is ignored.
	userPriority    roachpb.UserPriority
	txnRetryOptions retry.Options
}

// GetSender returns the underlying Sender. Only exported for tests.
func (db *DB) GetSender() Sender {
	return db.sender
}

// NewDB returns a new DB.
func NewDB(sender Sender) *DB {
	return &DB{
		sender:          sender,
		userPriority:    roachpb.NormalUserPriority,
		txnRetryOptions: DefaultTxnRetryOptions,
	}
}

// NewDBWithPriority returns a new DB.
func NewDBWithPriority(sender Sender, userPriority roachpb.UserPriority) *DB {
	db := NewDB(sender)
	db.userPriority = userPriority
	return db
}

// NewBatch creates and returns a new empty batch object for use with the DB.
// TODO(tschottdorf): it appears this can be unexported.
func (db *DB) NewBatch() *Batch {
	return &Batch{DB: db}
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice or a string.
func (db *DB) Get(key interface{}) (KeyValue, *roachpb.Error) {
	b := db.NewBatch()
	b.Get(key)
	return runOneRow(db, b)
}

// GetInconsistent is Get with an inconsistent read.
func (db *DB) GetInconsistent(key interface{}) (KeyValue, *roachpb.Error) {
	b := db.NewBatch()
	b.ReadConsistency = roachpb.INCONSISTENT
	b.Get(key)
	return runOneRow(db, b)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message.
//
// key can be either a byte slice or a string.
func (db *DB) GetProto(key interface{}, msg proto.Message) *roachpb.Error {
	r, pErr := db.Get(key)
	if pErr != nil {
		return pErr
	}
	return roachpb.NewError(r.ValueProto(msg))
}

// GetProtoInconsistent is GetProto with an inconsistent read.
func (db *DB) GetProtoInconsistent(key interface{}, msg proto.Message) *roachpb.Error {
	r, pErr := db.GetInconsistent(key)
	if pErr != nil {
		return pErr
	}
	return roachpb.NewError(r.ValueProto(msg))
}

// Put sets the value for a key.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (db *DB) Put(key, value interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.Put(key, value)
	_, pErr := runOneResult(db, b)
	return pErr
}

// PutInline sets the value for a key, but does not maintain
// multi-version values. The most recent value is always overwritten.
// Inline values cannot be mutated transactionally and should be used
// with caution.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (db *DB) PutInline(key, value interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.PutInline(key, value)
	_, pErr := runOneResult(db, b)
	return pErr
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue. Note that this must be an interface{}(nil), not a
// typed nil value (e.g. []byte(nil)).
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (db *DB) CPut(key, value, expValue interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.CPut(key, value, expValue)
	_, pErr := runOneResult(db, b)
	return pErr
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// key can be either a byte slice or a string.
func (db *DB) Inc(key interface{}, value int64) (KeyValue, *roachpb.Error) {
	b := db.NewBatch()
	b.Inc(key, value)
	return runOneRow(db, b)
}

func (db *DB) scan(
	begin, end interface{},
	maxRows int64,
	isReverse bool,
	readConsistency roachpb.ReadConsistencyType,
) ([]KeyValue, *roachpb.Error) {
	b := db.NewBatch()
	b.ReadConsistency = readConsistency
	if !isReverse {
		b.Scan(begin, end, maxRows)
	} else {
		b.ReverseScan(begin, end, maxRows)
	}
	r, pErr := runOneResult(db, b)
	return r.Rows, pErr
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (db *DB) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, *roachpb.Error) {
	return db.scan(begin, end, maxRows, false, roachpb.CONSISTENT)
}

// ScanInconsistent is Scan with an inconsistent read.
func (db *DB) ScanInconsistent(begin, end interface{}, maxRows int64) ([]KeyValue, *roachpb.Error) {
	return db.scan(begin, end, maxRows, false, roachpb.INCONSISTENT)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice or a string.
func (db *DB) ReverseScan(begin, end interface{}, maxRows int64) ([]KeyValue, *roachpb.Error) {
	return db.scan(begin, end, maxRows, true, roachpb.CONSISTENT)
}

// Del deletes one or more keys.
//
// key can be either a byte slice or a string.
func (db *DB) Del(keys ...interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.Del(keys...)
	_, pErr := runOneResult(db, b)
	return pErr
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// TODO(pmattis): Perhaps the result should return which rows were deleted.
//
// key can be either a byte slice or a string.
func (db *DB) DelRange(begin, end interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.DelRange(begin, end, false)
	_, pErr := runOneResult(db, b)
	return pErr
}

// AdminMerge merges the range containing key and the subsequent
// range. After the merge operation is complete, the range containing
// key will contain all of the key/value pairs of the subsequent range
// and the subsequent range will no longer exist.
//
// key can be either a byte slice or a string.
func (db *DB) AdminMerge(key interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.adminMerge(key)
	_, pErr := runOneResult(db, b)
	return pErr
}

// AdminSplit splits the range at splitkey.
//
// key can be either a byte slice or a string.
func (db *DB) AdminSplit(splitKey interface{}) *roachpb.Error {
	b := db.NewBatch()
	b.adminSplit(splitKey)
	_, pErr := runOneResult(db, b)
	return pErr
}

// CheckConsistency runs a consistency check on all the ranges containing
// the key span. It logs a diff of all the keys that are inconsistent
// when withDiff is set to true.
func (db *DB) CheckConsistency(begin, end interface{}, withDiff bool) *roachpb.Error {
	b := db.NewBatch()
	b.CheckConsistency(begin, end, withDiff)
	_, pErr := runOneResult(db, b)
	return pErr
}

// sendAndFill is a helper which sends the given batch and fills its results,
// returning the appropriate error which is either from the first failing call,
// or an "internal" error.
func sendAndFill(send func(int64, roachpb.ReadConsistencyType, ...roachpb.Request) (*roachpb.BatchResponse, *roachpb.Error), b *Batch) (*roachpb.BatchResponse, *roachpb.Error) {
	// Errors here will be attached to the results, so we will get them from
	// the call to fillResults in the regular case in which an individual call
	// fails. But send() also returns its own errors, so there's some dancing
	// here to do because we want to run fillResults() so that the individual
	// result gets initialized with an error from the corresponding call.
	br, pErr := send(b.MaxScanResults, b.ReadConsistency, b.reqs...)
	if pErr != nil {
		// Discard errors from fillResults.
		_ = b.fillResults(nil, pErr)
		return nil, pErr
	}
	pErr = b.fillResults(br, nil)

	if pErr != nil {
		return nil, pErr
	}
	return br, nil
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
func (db *DB) Run(b *Batch) *roachpb.Error {
	_, pErr := db.RunWithResponse(b)
	return pErr
}

// RunWithResponse is a version of Run that returns the BatchResponse.
func (db *DB) RunWithResponse(b *Batch) (*roachpb.BatchResponse, *roachpb.Error) {
	if pErr := b.prepare(); pErr != nil {
		return nil, pErr
	}
	return sendAndFill(db.send, b)
}

// Txn executes retryable in the context of a distributed transaction. The
// transaction is automatically aborted if retryable returns any error aside
// from recoverable internal errors, and is automatically committed
// otherwise. The retryable function should have no side effects which could
// cause problems in the event it must be run more than once.
//
// If you need more control over how the txn is executed, check out txn.Exec().
func (db *DB) Txn(retryable func(txn *Txn) *roachpb.Error) *roachpb.Error {
	// TODO(dan): This context should, at longest, live for the lifetime of this
	// method. Add a defered cancel.
	txn := NewTxn(context.TODO(), *db)
	txn.SetDebugName("", 1)
	pErr := txn.Exec(TxnExecOptions{AutoRetry: true, AutoCommit: true},
		func(txn *Txn, _ *TxnExecOptions) *roachpb.Error {
			return retryable(txn)
		})
	if pErr != nil {
		txn.CleanupOnError(pErr)
	}
	return pErr
}

// send runs the specified calls synchronously in a single batch and returns
// any errors. Returns a nil response for empty input (no requests).
func (db *DB) send(maxScanResults int64, readConsistency roachpb.ReadConsistencyType,
	reqs ...roachpb.Request) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(reqs) == 0 {
		return nil, nil
	}

	if readConsistency == roachpb.INCONSISTENT {
		for _, req := range reqs {
			if req.Method() != roachpb.Get && req.Method() != roachpb.Scan &&
				req.Method() != roachpb.ReverseScan {
				return nil, roachpb.NewErrorf("method %s not allowed with INCONSISTENT batch", req.Method)
			}
		}
	}

	ba := roachpb.BatchRequest{}
	ba.Add(reqs...)

	ba.MaxScanResults = maxScanResults
	if db.userPriority != 1 {
		ba.UserPriority = db.userPriority
	}
	ba.ReadConsistency = readConsistency

	tracing.AnnotateTrace()

	br, pErr := db.sender.Send(context.TODO(), ba)
	if pErr != nil {
		if log.V(1) {
			log.Infof("failed batch: %s", pErr)
		}
		return nil, pErr
	}
	return br, nil
}

// Runner only exports the Run method on a batch of operations.
type Runner interface {
	Run(b *Batch) *roachpb.Error
}

func runOneResult(r Runner, b *Batch) (Result, *roachpb.Error) {
	if pErr := r.Run(b); pErr != nil {
		return Result{PErr: pErr}, pErr
	}
	res := b.Results[0]
	return res, res.PErr
}

func runOneRow(r Runner, b *Batch) (KeyValue, *roachpb.Error) {
	if pErr := r.Run(b); pErr != nil {
		return KeyValue{}, pErr
	}
	res := b.Results[0]
	return res.Rows[0], res.PErr
}
