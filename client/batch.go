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

	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

// Batch provides for the parallel execution of a number of database
// operations. Operations are added to the Batch and then the Batch is executed
// via either DB.Run, Txn.Run or Txn.Commit.
//
// TODO(pmattis): Allow a timestamp to be specified which is applied to all
// operations within the batch.
type Batch struct {
	// The DB the batch is associated with. This field may be nil if the batch
	// was not created via DB.NewBatch or Txn.NewBatch.
	DB *DB
	// Results contains an entry for each operation added to the batch. The order
	// of the results matches the order the operations were added to the
	// batch. For example:
	//
	//   b := db.NewBatch()
	//   b.Put("a", "1")
	//   b.Put("b", "2")
	//   _ = db.Run(b)
	//   // string(b.Results[0].Rows[0].Key) == "a"
	//   // string(b.Results[1].Rows[0].Key) == "b"
	Results    []Result
	calls      []proto.Call
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
					// TODO(pmattis): This is odd. KeyValue.Value is otherwise always a
					// []byte except for setting it to a *int64 here.
					row.Value = &t.NewValue
					row.setTimestamp(t.Timestamp)
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
func (b *Batch) InternalAddCall(call proto.Call) {
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
func (b *Batch) Get(key interface{}) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.calls = append(b.calls, proto.GetCall(proto.Key(k)))
	b.initResult(1, 1, nil)
}

// GetProto retrieves the value for a key and decodes the result as a proto
// message. A new result will be appended to the batch which will contain a
// single row. Note that the proto will not be decoded until after the batch is
// executed using DB.Run or Txn.Run.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) GetProto(key interface{}, msg gogoproto.Message) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.calls = append(b.calls, proto.GetProtoCall(proto.Key(k), msg))
	b.initResult(1, 1, nil)
}

// Put sets the value for a key.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler. value can be any key type or a proto.Message.
func (b *Batch) Put(key, value interface{}) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.calls = append(b.calls, proto.PutCall(proto.Key(k), v))
	b.initResult(1, 1, nil)
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
func (b *Batch) CPut(key, value, expValue interface{}) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	ev, err := marshalValue(expValue)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.calls = append(b.calls, proto.ConditionalPutCall(proto.Key(k), v, ev))
	b.initResult(1, 1, nil)
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
func (b *Batch) Inc(key interface{}, value int64) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.calls = append(b.calls, proto.IncrementCall(proto.Key(k), value))
	b.initResult(1, 1, nil)
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive).
//
// A new result will be appended to the batch which will contain up to maxRows
// rows and Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) Scan(s, e interface{}, maxRows int64) {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	b.calls = append(b.calls, proto.ScanCall(proto.Key(begin), proto.Key(end), maxRows))
	b.initResult(1, 0, nil)
}

// Del deletes one or more keys.
//
// A new result will be appended to the batch and each key will have a
// corresponding row in the returned Result.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) Del(keys ...interface{}) {
	var calls []proto.Call
	for _, key := range keys {
		k, err := marshalKey(key)
		if err != nil {
			b.initResult(0, len(keys), err)
			return
		}
		calls = append(calls, proto.DeleteCall(proto.Key(k)))
	}
	b.calls = append(b.calls, calls...)
	b.initResult(len(calls), len(calls), nil)
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// A new result will be appended to the batch which will contain 0 rows and
// Result.Err will indicate success or failure.
//
// key can be either a byte slice, a string, a fmt.Stringer or an
// encoding.BinaryMarshaler.
func (b *Batch) DelRange(s, e interface{}) {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	b.calls = append(b.calls, proto.DeleteRangeCall(proto.Key(begin), proto.Key(end)))
	b.initResult(1, 0, nil)
}

// adminMerge is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminMerge(key interface{}) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	req := &proto.AdminMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key(k),
		},
	}
	resp := &proto.AdminMergeResponse{}
	b.calls = append(b.calls, proto.Call{Args: req, Reply: resp})
	b.initResult(1, 0, nil)
}

// adminSplit is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminSplit(splitKey interface{}) {
	k, err := marshalKey(splitKey)
	if err != nil {
		b.initResult(0, 0, err)
		return
	}
	req := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key: proto.Key(k),
		},
	}
	req.SplitKey = proto.Key(k)
	resp := &proto.AdminSplitResponse{}
	b.calls = append(b.calls, proto.Call{Args: req, Reply: resp})
	b.initResult(1, 0, nil)
}
