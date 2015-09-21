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
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	roachencoding "github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
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

// ValueInt returns the value decoded as an int64. This method will panic if
// the value cannot be decoded as an int64.
func (kv *KeyValue) ValueInt() int64 {
	if kv.Value == nil {
		return 0
	}
	if i, ok := kv.Value.(*int64); ok {
		return *i
	}
	b := kv.ValueBytes()
	if len(b) == 0 {
		return 0
	}
	_, uint64val := roachencoding.DecodeUint64(b)
	return int64(uint64val)
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

// Result holds the result for a single DB or Txn operation (e.g. Get, Put,
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
	sender Sender

	// userPriority is the default user priority to set on API calls. If
	// userPriority is set non-zero in call arguments, this value is
	// ignored.
	userPriority    int32
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
		txnRetryOptions: DefaultTxnRetryOptions,
	}
}

// NewDBWithPriority returns a new DB.
func NewDBWithPriority(sender Sender, userPriority int32) *DB {
	db := NewDB(sender)
	db.userPriority = userPriority
	return db
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
// both.
//
// If not specified, the <user> field defaults to "root".
//
// The certs parameter can be used to override the default directory to use for
// client certificates. In tests, the directory "test_certs" uses the embedded
// test certificates.
//
// The priority parameter can be used to override the default priority for
// operations.
func Open(stopper *stop.Stopper, addr string) (*DB, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	ctx := &base.Context{}
	ctx.InitDefaults()
	if u.User != nil {
		ctx.User = u.User.Username()
	}

	q := u.Query()
	if dir := q["certs"]; len(dir) > 0 {
		ctx.Certs = dir[0]
	}

	retryOpts := defaultRetryOptions
	if failFast := q["failfast"]; len(failFast) > 0 {
		retryOpts.MaxRetries = 1
	}

	sender, err := newSender(u, ctx, retryOpts, stopper)
	if err != nil {
		return nil, err
	}
	if sender == nil {
		return nil, fmt.Errorf("\"%s\" no sender specified", addr)
	}

	db := &DB{
		sender:          sender,
		txnRetryOptions: DefaultTxnRetryOptions,
	}

	if priority := q["priority"]; len(priority) > 0 {
		p, err := strconv.Atoi(priority[0])
		if err != nil {
			return nil, err
		}
		db.userPriority = int32(p)
	}

	return db, nil
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
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Get(key interface{}) (KeyValue, error) {
	b := db.NewBatch()
	b.Get(key)
	return runOneRow(db, b)
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
	b := db.NewBatch()
	b.Put(key, value)
	_, err := runOneResult(db, b)
	return err
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (db *DB) CPut(key, value, expValue interface{}) error {
	b := db.NewBatch()
	b.CPut(key, value, expValue)
	_, err := runOneResult(db, b)
	return err
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Inc(key interface{}, value int64) (KeyValue, error) {
	b := db.NewBatch()
	b.Inc(key, value)
	return runOneRow(db, b)
}

func (db *DB) scan(begin, end interface{}, maxRows int64, isReverse bool) ([]KeyValue, error) {
	b := db.NewBatch()
	if !isReverse {
		b.Scan(begin, end, maxRows)
	} else {
		b.ReverseScan(begin, end, maxRows)
	}
	r, err := runOneResult(db, b)
	return r.Rows, err
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Scan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return db.scan(begin, end, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// The returned []KeyValue will contain up to maxRows elements.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) ReverseScan(begin, end interface{}, maxRows int64) ([]KeyValue, error) {
	return db.scan(begin, end, maxRows, true)
}

// Del deletes one or more keys.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) Del(keys ...interface{}) error {
	b := db.NewBatch()
	b.Del(keys...)
	_, err := runOneResult(db, b)
	return err
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// TODO(pmattis): Perhaps the result should return which rows were deleted.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) DelRange(begin, end interface{}) error {
	b := db.NewBatch()
	b.DelRange(begin, end)
	_, err := runOneResult(db, b)
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
	b := db.NewBatch()
	b.adminMerge(key)
	_, err := runOneResult(db, b)
	return err
}

// AdminSplit splits the range at splitkey.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (db *DB) AdminSplit(splitKey interface{}) error {
	b := db.NewBatch()
	b.adminSplit(splitKey)
	_, err := runOneResult(db, b)
	return err
}

// sendAndFill is a helper which sends the given batch and fills its results,
// returning the appropriate error which is either from the first failing call,
// or an "internal" error.
func sendAndFill(send func(...proto.Call) error, b *Batch) error {
	// Errors here will be attached to the results, so we will get them from
	// the call to fillResults in the regular case in which an individual call
	// fails. But send() also returns its own errors, so there's some dancing
	// here to do because we want to run fillResults() so that the individual
	// result gets initialized with an error from the corresponding call.
	err1 := send(b.calls...)
	err2 := b.fillResults()
	if err2 != nil {
		return err2
	}
	return err1
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
	return sendAndFill(db.send, b)
}

// Txn executes retryable in the context of a distributed transaction. The
// transaction is automatically aborted if retryable returns any error aside
// from recoverable internal errors, and is automatically committed
// otherwise. The retryable function should have no side effects which could
// cause problems in the event it must be run more than once.
//
// TODO(pmattis): Allow transaction options to be specified.
func (db *DB) Txn(retryable func(txn *Txn) error) error {
	txn := NewTxn(*db)
	txn.SetDebugName("", 1)
	return txn.exec(retryable)
}

// send runs the specified calls synchronously in a single batch and
// returns any errors.
func (db *DB) send(calls ...proto.Call) (err error) {
	if len(calls) == 0 {
		return nil
	}

	if len(calls) == 1 {
		c := calls[0]
		// We only send BatchRequest. Everything else needs to go into one.
		if _, ok := calls[0].Args.(*proto.BatchRequest); ok {
			if c.Args.Header().UserPriority == nil && db.userPriority != 0 {
				c.Args.Header().UserPriority = gogoproto.Int32(db.userPriority)
			}
			resetClientCmdID(c.Args)
			db.sender.Send(context.TODO(), c)
			err = c.Reply.Header().GoError()
			if err != nil {
				if log.V(1) {
					log.Infof("failed %s: %s", c.Method(), err)
				}
			} else if c.Post != nil {
				err = c.Post()
			}
			return
		}
	}

	ba, br := &proto.BatchRequest{}, &proto.BatchResponse{}
	for _, call := range calls {
		ba.Add(call.Args)
	}
	err = db.send(proto.Call{Args: ba, Reply: br})

	// Recover from protobuf merge panics.
	defer func() {
		if r := recover(); r != nil {
			// Take care to log merge error and to return it if no error has
			// already been set.
			mergeErr := util.Errorf("unable to merge response: %s", r)
			log.Error(mergeErr)
			if err == nil {
				err = mergeErr
			}
		}
	}()

	// Transfer individual responses from batch response to prepared replies.
	for i, reply := range br.Responses {
		c := calls[i]
		gogoproto.Merge(c.Reply, reply.GetInner())
		if c.Post != nil {
			if e := c.Post(); e != nil && err != nil {
				err = e
			}
		}
	}
	return
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

// resetClientCmdID sets the client command ID if the call is for a
// read-write method. The client command ID provides idempotency
// protection in conjunction with the server.
func resetClientCmdID(args proto.Request) {
	args.Header().CmdID = proto.ClientCmdID{
		WallTime: time.Now().UnixNano(),
		Random:   rand.Int63(),
	}
}
