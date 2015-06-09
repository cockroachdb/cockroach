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
	"bytes"
	"encoding"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

// KeyValue represents a single key/value pair and corresponding timestamp.
type KeyValue struct {
	Key       []byte
	Value     interface{}
	Timestamp time.Time
}

func (kv *KeyValue) String() string {
	switch t := kv.Value.(type) {
	case nil:
		return string(kv.Key) + "=nil"
	case []byte:
		return string(kv.Key) + "=" + string(t)
	case *int64:
		return string(kv.Key) + "=" + strconv.FormatInt(*t, 10)
	}
	return string(kv.Key) + fmt.Sprintf("=<ERROR:%T>", kv.Value)
}

// Exists returns true iff the value exists.
func (kv *KeyValue) Exists() bool {
	return kv.Value != nil
}

func (kv *KeyValue) setValue(v *proto.Value) {
	if v == nil {
		return
	}
	if v.Bytes != nil {
		kv.Value = v.Bytes
	} else if v.Integer != nil {
		kv.Value = v.Integer
	}
	if v.Timestamp != nil {
		kv.Timestamp = v.Timestamp.GoTime()
	}
}

func (kv *KeyValue) setTimestamp(t proto.Timestamp) {
	kv.Timestamp = t.GoTime()
}

// ValueBytes returns the value as a byte slice. This method will panic if the
// value's type is not a byte slice.
func (kv *KeyValue) ValueBytes() []byte {
	if kv.Value == nil {
		return nil
	}
	return kv.Value.([]byte)
}

// ValueInt returns the value as an int64. This method will panic if the
// value's type is not an int64.
func (kv *KeyValue) ValueInt() int64 {
	return *kv.Value.(*int64)
}

// ValueProto parses the byte slice value as a proto message.
func (kv *KeyValue) ValueProto(msg gogoproto.Message) error {
	switch val := kv.Value.(type) {
	case nil:
		msg.Reset()
		return nil
	case []byte:
		return gogoproto.Unmarshal(val, msg)
	}
	return fmt.Errorf("unable to unmarshal proto: %T", kv.Value)
}

// Result holds the result for a single DB or Tx operation (e.g. Get, Put,
// etc).
type Result struct {
	calls int
	// Err contains any error encountered when performing the operation.
	Err error
	// Rows contains the key/value pairs for the operation. The number of rows
	// returned varies by operation. For Get, Put, CPut, Inc and Del the number
	// of rows returned is the number of keys operated on. For Scan the number of
	// rows returned is the number or rows matching the scan capped by the
	// maxRows parameter. For DelRange Rows is nil.
	Rows []KeyValue
}

func (r Result) String() string {
	if r.Err != nil {
		return r.Err.Error()
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
	// B is a helper to make creating a new batch and performing an
	// operation on it easer:
	//
	//   err := db.Run(db.B.Put("a", "1").Put("b", "2"))
	B batcher
	kv
}

// Option is the signature for a function which applies an option to a DB.
type Option func(*DB)

// SenderOpt sets the sender for a DB.
func SenderOpt(sender Sender) Option {
	return func(db *DB) {
		db.kv.Sender = sender
	}
}

// TODO(pmattis): Allow setting the sender/txn retry options.

// Open creates a new database handle to the cockroach cluster specified by
// addr. The cluster is identified by a URL with the format:
//
//   [<sender>:]//[<user>@]<host>:<port>[?certs=<dir>,priority=<val>]
//
// The URL scheme (<sender>) specifies which transport to use for talking to
// the cockroach cluster. Currently allowable values are: http, https, rpc,
// rpcs. The rpc and rpcs senders use a variant of Go's builtin rpc library for
// communication with the cluster. This protocol is lower overhead and more
// efficient than http. The decision between the encrypted (https, rpcs) and
// unencrypted senders (http, rpc) depends on the settings of the cluster. A
// given cluster supports either encrypted or unencrypted traffic, but not
// both. The <sender> can be left unspecified in the URL and set by passing
// client.SenderOpt.
//
// If not specified, the <user> field defaults to "root".
//
// The certs parameter can be used to override the default directory to use for
// client certificates. In tests, the directory "test_certs" uses the embedded
// test certificates.
//
// The priority parameter can be used to override the default priority for
// operations.
func Open(addr string, opts ...Option) (*DB, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if u.User == nil {
		u.User = url.User("root")
	}

	ctx := &base.Context{}
	ctx.InitDefaults()
	q := u.Query()
	if dir := q["certs"]; len(dir) > 0 {
		ctx.Certs = dir[0]
	}

	sender, err := newSender(u, ctx)
	if err != nil {
		return nil, err
	}

	db := &DB{}
	db.kv.Sender = sender
	db.kv.user = u.User.Username()
	db.kv.txnRetryOptions = DefaultTxnRetryOptions

	if priority := q["priority"]; len(priority) > 0 {
		p, err := strconv.Atoi(priority[0])
		if err != nil {
			return nil, err
		}
		db.kv.userPriority = int32(p)
	}

	for _, opt := range opts {
		opt(db)
	}
	return db, nil
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Get(key interface{}) (KeyValue, error) {
	return runOneRow(db, db.B.Get(key))
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) GetProto(key interface{}, msg gogoproto.Message) error {
	r, err := db.Get(key)
	if err != nil {
		return err
	}
	return r.ValueProto(msg)
}

// Put sets the value for a key.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (db *DB) Put(key, value interface{}) error {
	_, err := runOneResult(db, db.B.Put(key, value))
	return err
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (db *DB) CPut(key, value, expValue interface{}) error {
	_, err := runOneResult(db, db.B.CPut(key, value, expValue))
	return err
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Inc(key interface{}, value int64) (KeyValue, error) {
	return runOneRow(db, db.B.Inc(key, value))
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive).
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	r, err := runOneResult(db, db.B.Scan(begin, end, maxRows))
	return r.Rows, err
}

// Del deletes one or more keys.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Del(keys ...interface{}) error {
	_, err := runOneResult(db, db.B.Del(keys...))
	return err
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// TODO(pmattis): Perhaps the result should return which rows were deleted.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) DelRange(begin, end interface{}) error {
	_, err := runOneResult(db, db.B.DelRange(begin, end))
	return err
}

// AdminMerge merges the range containing key and the subsequent
// range. After the merge operation is complete, the range containing
// key will contain all of the key/value pairs of the subsequent range
// and the subsequent range will no longer exist.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) AdminMerge(key interface{}) error {
	_, err := runOneResult(db, (&Batch{}).adminMerge(key))
	return err
}

// AdminSplit splits the range at splitkey.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) AdminSplit(splitKey interface{}) error {
	_, err := runOneResult(db, (&Batch{}).adminSplit(splitKey))
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
func (db *DB) Run(b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	if err := db.send(b.calls...); err != nil {
		return err
	}
	return b.fillResults()
}

// Tx executes retryable in the context of a distributed transaction. The
// transaction is automatically aborted if retryable returns any error aside
// from recoverable internal errors, and is automatically committed
// otherwise. The retryable function should have no side effects which could
// cause problems in the event it must be run more than once.
//
// TODO(pmattis): Allow transaction options to be specified.
func (db *DB) Tx(retryable func(tx *Tx) error) error {
	_, file, line, ok := runtime.Caller(1)

	tx := Tx{}
	tx.txn.init(&db.kv)
	return tx.txn.exec(func(txn *txn) error {
		if ok {
			// TODO(pmattis): include the parent directory?
			tx.txn.txn.Name = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		}
		return retryable(&tx)
	})
}

// Tx is an in-progress distributed database transaction. A Tx is not safe for
// concurrent use by multiple goroutines.
type Tx struct {
	// B is a helper to make creating a new batch and performing an
	// operation on it easer:
	//
	//   err := db.Tx(func(tx *Tx) error {
	//     return tx.Commit(tx.B.Put("a", "1").Put("b", "2"))
	//   })
	B batcher
	txn
}

// SetDebugName sets the debug name associated with the transaction which will
// appear in log files and the web UI. Each transaction starts out with an
// automatically assigned debug name composed of the file and line number where
// the transaction was created.
func (tx *Tx) SetDebugName(name string) {
	if _, file, line, ok := runtime.Caller(1); ok {
		tx.txn.txn.Name = fmt.Sprintf("%s:%d: %s", filepath.Base(file), line, name)
	} else {
		tx.txn.txn.Name = name
	}
}

// DebugName returns the debug name associated with the transaction.
func (tx *Tx) DebugName() string {
	return tx.txn.txn.Name
}

// SetSnapshotIsolation sets the transaction's isolation type to
// snapshot. Transactions default to serializable isolation. The
// isolation must be set before any operations are performed on the
// transaction.
//
// TODO(pmattis): This isn't tested yet but will be as part of the
// conversion of client_test.go.
func (tx *Tx) SetSnapshotIsolation() {
	// TODO(pmattis): Panic if the transaction has already had
	// operations run on it. Needs to tie into the Txn reset in case of
	// retries.
	tx.txn.txn.Isolation = proto.SNAPSHOT
}

// InternalSetPriority sets the transaction priority. It is intended for
// internal (testing) use only.
func (tx *Tx) InternalSetPriority(priority int32) {
	// The negative user priority translates into a positive (and known) priority
	// for the transaction.
	tx.txn.kv.userPriority = -priority
}

// Get retrieves the value for a key, returning the retrieved key/value or an
// error.
//
//   r, err := db.Get("a")
//   // string(r.Key) == "a"
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (tx *Tx) Get(key interface{}) (KeyValue, error) {
	return runOneRow(tx, tx.B.Get(key))
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (tx *Tx) GetProto(key interface{}, msg gogoproto.Message) error {
	r, err := tx.Get(key)
	if err != nil {
		return err
	}
	return r.ValueProto(msg)
}

// Put sets the value for a key
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (tx *Tx) Put(key, value interface{}) error {
	_, err := runOneResult(tx, tx.B.Put(key, value))
	return err
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (tx *Tx) CPut(key, value, expValue interface{}) error {
	_, err := runOneResult(tx, tx.B.CPut(key, value, expValue))
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
func (tx *Tx) Inc(key interface{}, value int64) (KeyValue, error) {
	return runOneRow(tx, tx.B.Inc(key, value))
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive).
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (tx *Tx) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	r, err := runOneResult(tx, tx.B.Scan(begin, end, maxRows))
	return r.Rows, err
}

// Del deletes one or more keys.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (tx *Tx) Del(keys ...interface{}) error {
	_, err := runOneResult(tx, tx.B.Del(keys...))
	return err
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// The returned Result will contain 0 rows and Result.Err will indicate success
// or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (tx *Tx) DelRange(begin, end interface{}) error {
	_, err := runOneResult(tx, tx.B.DelRange(begin, end))
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
func (tx *Tx) Run(b *Batch) error {
	if err := b.prepare(); err != nil {
		return err
	}
	if err := tx.send(b.calls...); err != nil {
		return err
	}
	return b.fillResults()
}

// Commit executes the operations queued up within a batch and commits the
// transaction. Explicitly committing a transaction is optional, but more
// efficient than relying on the implicit commit performed when the transaction
// function returns without error.
func (tx *Tx) Commit(b *Batch) error {
	args := &proto.EndTransactionRequest{Commit: true}
	reply := &proto.EndTransactionResponse{}
	b.calls = append(b.calls, Call{Args: args, Reply: reply})
	b.initResult(1, 0, nil)
	return tx.Run(b)
}

// Batch provides for the parallel execution of a number of database
// operations. Operations are added to the Batch and then the Batch is executed
// via either DB.Run, Tx.Run or Tx.Commit.
//
// TODO(pmattis): Allow a timestamp to be specified which is applied to all
// operations within the batch.
type Batch struct {
	// Results contains an entry for each operation added to the batch. The order
	// of the results matches the order the operations were added to the
	// batch. For example:
	//
	//   b := client.B.Put("a", "1").Put("b", "2")
	//   _ = db.Run(b)
	//   // string(b.Results[0].Rows[0].Key) == "a"
	//   // string(b.Results[1].Rows[0].Key) == "b"
	Results    []Result
	calls      []Call
	resultsBuf [8]Result
	rowsBuf    [8]KeyValue
	rowsIdx    int
}

func (b *Batch) prepare() error {
	for _, r := range b.Results {
		if err := r.Err; err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) initResult(calls, numRows int, err error) {
	r := Result{calls: calls, Err: err}
	if numRows > 0 {
		if b.rowsIdx+numRows <= len(b.rowsBuf) {
			r.Rows = b.rowsBuf[b.rowsIdx : b.rowsIdx+numRows]
			b.rowsIdx += numRows
		} else {
			r.Rows = make([]KeyValue, numRows)
		}
	}
	if b.Results == nil {
		b.Results = b.resultsBuf[0:0]
	}
	b.Results = append(b.Results, r)
}

func (b *Batch) fillResults() error {
	offset := 0
	for i := range b.Results {
		result := &b.Results[i]

		for k := 0; k < result.calls; k++ {
			call := b.calls[offset+k]

			if result.Err == nil {
				result.Err = call.Reply.Header().GoError()
			}

			switch t := call.Reply.(type) {
			case *proto.GetResponse:
				row := &result.Rows[k]
				row.Key = []byte(call.Args.(*proto.GetRequest).Key)
				if result.Err == nil {
					row.setValue(t.Value)
				}
			case *proto.PutResponse:
				req := call.Args.(*proto.PutRequest)
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.setValue(&req.Value)
					row.setTimestamp(t.Timestamp)
				}
			case *proto.ConditionalPutResponse:
				req := call.Args.(*proto.ConditionalPutRequest)
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.setValue(&req.Value)
					row.setTimestamp(t.Timestamp)
				}
			case *proto.IncrementResponse:
				row := &result.Rows[k]
				row.Key = []byte(call.Args.(*proto.IncrementRequest).Key)
				if result.Err == nil {
					// TODO(pmattis): Should IncrementResponse contain a
					// proto.Value so that the timestamp can be returned?
					row.Value = &t.NewValue
				}
			case *proto.ScanResponse:
				result.Rows = make([]KeyValue, len(t.Rows))
				for j, kv := range t.Rows {
					row := &result.Rows[j]
					row.Key = kv.Key
					row.setValue(&kv.Value)
				}
			case *proto.DeleteResponse:
				row := &result.Rows[k]
				row.Key = []byte(call.Args.(*proto.DeleteRequest).Key)

			case *proto.AdminMergeResponse:
			case *proto.AdminSplitResponse:
			case *proto.DeleteRangeResponse:
			case *proto.EndTransactionResponse:
			case *proto.InternalBatchResponse:
			case *proto.InternalGCResponse:
			case *proto.InternalMergeResponse:
			case *proto.InternalPushTxnResponse:
			case *proto.InternalRangeLookupResponse:
			case *proto.InternalResolveIntentResponse:
			case *proto.InternalResolveIntentRangeResponse:
				// Nothing to do for these methods as they do not generate any
				// rows. For the proto.Internal* responses the caller will have hold of
				// the response object itself.

			default:
				if result.Err == nil {
					result.Err = fmt.Errorf("unsupported reply: %T", call.Reply)
				}
			}
		}
		offset += result.calls
	}

	for i := range b.Results {
		result := &b.Results[i]
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}

// InternalAddCall adds the specified call to the batch. It is intended for
// internal use only.
func (b *Batch) InternalAddCall(call Call) {
	numRows := 0
	switch call.Args.(type) {
	case *proto.GetRequest,
		*proto.PutRequest,
		*proto.ConditionalPutRequest,
		*proto.IncrementRequest,
		*proto.DeleteRequest:
		numRows = 1
	}
	b.calls = append(b.calls, call)
	b.initResult(1 /* calls */, numRows, nil)
}

// Get retrieves the value for a key. A new result will be appended to the
// batch which will contain a single row.
//
//   r, err := db.Get("a")
//   // string(r.Rows[0].Key) == "a"
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) Get(key interface{}) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, Get(proto.Key(k)))
	b.initResult(1, 1, nil)
	return b
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message. A new result will be appended to the batch which will contain a
// single row. Note that the proto will not be decoded until after the batch is
// executed using DB.Run or Tx.Run.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) GetProto(key interface{}, msg gogoproto.Message) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, GetProto(proto.Key(k), msg))
	b.initResult(1, 1, nil)
	return b
}

// Put sets the value for a key.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (b *Batch) Put(key, value interface{}) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, Put(proto.Key(k), v))
	b.initResult(1, 1, nil)
	return b
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (b *Batch) CPut(key, value, expValue interface{}) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	ev, err := marshalValue(expValue)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, ConditionalPut(proto.Key(k), v, ev))
	b.initResult(1, 1, nil)
	return b
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) Inc(key interface{}, value int64) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return b
	}
	b.calls = append(b.calls, Increment(proto.Key(k), value))
	b.initResult(1, 1, nil)
	return b
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive).
//
// A new result will be appended to the batch which will contain up to maxRows
// rows and Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) Scan(s, e interface{}, maxRows int64) *Batch {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	b.calls = append(b.calls, Scan(proto.Key(begin), proto.Key(end), maxRows))
	b.initResult(1, 0, nil)
	return b
}

// Del deletes one or more keys.
//
// A new result will be appended to the batch and each key will have a
// corresponding row in the returned Result.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) Del(keys ...interface{}) *Batch {
	var calls []Call
	for _, key := range keys {
		k, err := marshalKey(key)
		if err != nil {
			b.initResult(0, len(keys), err)
			return b
		}
		calls = append(calls, Delete(proto.Key(k)))
	}
	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
	return b
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// A new result will be appended to the batch which will contain 0 rows and
// Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) DelRange(s, e interface{}) *Batch {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	b.calls = append(b.calls, DeleteRange(proto.Key(begin), proto.Key(end)))
	b.initResult(1, 0, nil)
	return b
}

// adminMerge is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminMerge(key interface{}) *Batch {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	req := &proto.AdminMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key(k),
		},
	}
	resp := &proto.AdminMergeResponse{}
	b.calls = append(b.calls, Call{Args: req, Reply: resp})
	b.initResult(1, 0, nil)
	return b
}

// adminSplit is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminSplit(splitKey interface{}) *Batch {
	k, err := marshalKey(splitKey)
	if err != nil {
		b.initResult(0, 0, err)
		return b
	}
	req := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key(k),
		},
	}
	req.SplitKey = proto.Key(k)
	resp := &proto.AdminSplitResponse{}
	b.calls = append(b.calls, Call{Args: req, Reply: resp})
	b.initResult(1, 0, nil)
	return b
}

type batcher struct{}

func (b batcher) Get(key interface{}) *Batch {
	return (&Batch{}).Get(key)
}

func (b batcher) GetProto(key interface{}, msg gogoproto.Message) *Batch {
	return (&Batch{}).GetProto(key, msg)
}

func (b batcher) Put(key, value interface{}) *Batch {
	return (&Batch{}).Put(key, value)
}

func (b batcher) CPut(key, value, expValue interface{}) *Batch {
	return (&Batch{}).CPut(key, value, expValue)
}

func (b batcher) Inc(key interface{}, value int64) *Batch {
	return (&Batch{}).Inc(key, value)
}

func (b batcher) Scan(begin, end interface{}, maxRows int64) *Batch {
	return (&Batch{}).Scan(begin, end, maxRows)
}

func (b batcher) Del(keys ...interface{}) *Batch {
	return (&Batch{}).Del(keys...)
}

func (b batcher) DelRange(begin, end interface{}) *Batch {
	return (&Batch{}).DelRange(begin, end)
}

func marshalKey(k interface{}) ([]byte, error) {
	// Note that the ordering here is important. In particular, proto.Key is also
	// a fmt.Stringer.
	switch t := k.(type) {
	case string:
		return []byte(t), nil
	case proto.Key:
		return []byte(t), nil
	case []byte:
		return t, nil
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	case fmt.Stringer:
		return []byte(t.String()), nil
	}
	return nil, fmt.Errorf("unable to marshal key: %T", k)
}

func marshalValue(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(t), nil
	case []byte:
		return t, nil
	case gogoproto.Message:
		return gogoproto.Marshal(t)
	case encoding.BinaryMarshaler:
		return t.MarshalBinary()
	case fmt.Stringer:
		return []byte(t.String()), nil
	}
	return nil, fmt.Errorf("unable to marshal value: %T", v)
}

// Runner only exports the Run method on a batch of operations.
type Runner interface {
	Run(b *Batch) error
}

func runOneResult(r Runner, b *Batch) (Result, error) {
	if err := r.Run(b); err != nil {
		return Result{Err: err}, err
	}
	res := b.Results[0]
	return res, res.Err
}

func runOneRow(r Runner, b *Batch) (KeyValue, error) {
	if err := r.Run(b); err != nil {
		return KeyValue{}, err
	}
	res := b.Results[0]
	return res.Rows[0], res.Err
}
