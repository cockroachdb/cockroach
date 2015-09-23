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
	"github.com/cockroachdb/cockroach/util/encoding"
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
	reqs       []proto.Request
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
	// TODO(tschottdorf): assert that calls is 0 or 1?
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

func (b *Batch) fillResults(br *proto.BatchResponse, pErr *proto.Error) error {
	offset := 0
	for i := range b.Results {
		result := &b.Results[i]

		for k := 0; k < result.calls; k++ {
			args := b.reqs[offset+k]

			var reply proto.Response
			if result.Err == nil {
				// TODO(tschottdorf): always do this (no errors on individual requests).
				result.Err = pErr.GoError()
				if result.Err == nil {
					if offset+k < len(br.Responses) {
						reply = br.Responses[offset+k].GetValue().(proto.Response)
					} else if args.Method() != proto.EndTransaction {
						// TODO(tschottdorf): EndTransaction is excepted here
						// because it may be elided (r/o txns). Might prefer to
						// simulate an EndTransaction response instead; this
						// effectively just leaks here.
						panic("not enough responses for calls")
					}
				}
			}

			switch req := args.(type) {
			case *proto.GetRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = reply.(*proto.GetResponse).Value
				}
			case *proto.PutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
					row.setTimestamp(reply.(*proto.PutResponse).Timestamp)
				}
			case *proto.ConditionalPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
					row.setTimestamp(reply.(*proto.ConditionalPutResponse).Timestamp)
				}
			case *proto.IncrementRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					t := reply.(*proto.IncrementResponse)
					row.Value = &proto.Value{
						Bytes: encoding.EncodeUint64(nil, uint64(t.NewValue)),
						Tag:   proto.ValueType_INT,
					}
					row.setTimestamp(t.Timestamp)
				}
			case *proto.ScanRequest:
				if result.Err == nil {
					t := reply.(*proto.ScanResponse)
					result.Rows = make([]KeyValue, len(t.Rows))
					for j := range t.Rows {
						src := &t.Rows[j]
						dst := &result.Rows[j]
						dst.Key = src.Key
						dst.Value = &src.Value
					}
				}
			case *proto.ReverseScanRequest:
				if result.Err == nil {
					t := reply.(*proto.ReverseScanResponse)
					result.Rows = make([]KeyValue, len(t.Rows))
					for j := range t.Rows {
						src := &t.Rows[j]
						dst := &result.Rows[j]
						dst.Key = src.Key
						dst.Value = &src.Value
					}
				}
			case *proto.DeleteRequest:
				row := &result.Rows[k]
				row.Key = []byte(args.(*proto.DeleteRequest).Key)

			case *proto.DeleteRangeRequest:
			case *proto.EndTransactionRequest:
			case *proto.AdminMergeRequest:
			case *proto.AdminSplitRequest:
			case *proto.HeartbeatTxnRequest:
			case *proto.GCRequest:
			case *proto.PushTxnRequest:
			case *proto.RangeLookupRequest:
			case *proto.ResolveIntentRequest:
			case *proto.ResolveIntentRangeRequest:
			case *proto.MergeRequest:
			case *proto.TruncateLogRequest:
			case *proto.LeaderLeaseRequest:
			case *proto.BatchRequest:
				// Nothing to do for these methods as they do not generate any
				// rows.

			default:
				if result.Err == nil {
					result.Err = fmt.Errorf("unsupported reply: %T", reply)
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

// InternalAddRequest adds the specified call to the batch. It is intended for
// internal use only.
func (b *Batch) InternalAddRequest(args proto.Request) {
	numRows := 0
	switch args.(type) {
	case *proto.GetRequest,
		*proto.PutRequest,
		*proto.ConditionalPutRequest,
		*proto.IncrementRequest,
		*proto.DeleteRequest:
		numRows = 1
	}
	b.reqs = append(b.reqs, args)
	b.initResult(1 /* calls */, numRows, nil)
}

// Get retrieves the value for a key. A new result will be appended to the
// batch which will contain a single row.
//
//   r, err := db.Get("a")
//   // string(r.Rows[0].Key) == "a"
//
// key can be either a byte slice or a string.
func (b *Batch) Get(key interface{}) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.reqs = append(b.reqs, proto.NewGet(k))
	b.initResult(1, 1, nil)
}

// Put sets the value for a key.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
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
	b.reqs = append(b.reqs, proto.NewPut(k, v))
	b.initResult(1, 1, nil)
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
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
	b.reqs = append(b.reqs, proto.NewConditionalPut(k, v, ev))
	b.initResult(1, 1, nil)
}

// Inc increments the integer value at key. If the key does not exist it will
// be created with an initial value of 0 which will then be incremented. If the
// key exists but was set using Put or CPut an error will be returned.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) Inc(key interface{}, value int64) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, err)
		return
	}
	b.reqs = append(b.reqs, proto.NewIncrement(k, value))
	b.initResult(1, 1, nil)
}

func (b *Batch) scan(s, e interface{}, maxRows int64, isReverse bool) {
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
	if !isReverse {
		b.reqs = append(b.reqs, proto.NewScan(proto.Key(begin), proto.Key(end), maxRows))
	} else {
		b.reqs = append(b.reqs, proto.NewReverseScan(proto.Key(begin), proto.Key(end), maxRows))
	}
	b.initResult(1, 0, nil)
}

// Scan retrieves the rows between begin (inclusive) and end (exclusive) in
// ascending order.
//
// A new result will be appended to the batch which will contain up to maxRows
// rows and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) Scan(s, e interface{}, maxRows int64) {
	b.scan(s, e, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// A new result will be appended to the batch which will contain up to maxRows
// rows and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) ReverseScan(s, e interface{}, maxRows int64) {
	b.scan(s, e, maxRows, true)
}

// Del deletes one or more keys.
//
// A new result will be appended to the batch and each key will have a
// corresponding row in the returned Result.
//
// key can be either a byte slice or a string.
func (b *Batch) Del(keys ...interface{}) {
	var reqs []proto.Request
	for _, key := range keys {
		k, err := marshalKey(key)
		if err != nil {
			b.initResult(0, len(keys), err)
			return
		}
		reqs = append(reqs, proto.NewDelete(k))
	}
	b.reqs = append(b.reqs, reqs...)
	b.initResult(len(reqs), len(reqs), nil)
}

// DelRange deletes the rows between begin (inclusive) and end (exclusive).
//
// A new result will be appended to the batch which will contain 0 rows and
// Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
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
	b.reqs = append(b.reqs, proto.NewDeleteRange(proto.Key(begin), proto.Key(end)))
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
			Key: k,
		},
	}
	b.reqs = append(b.reqs, req)
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
			Key: k,
		},
	}
	req.SplitKey = k
	b.reqs = append(b.reqs, req)
	b.initResult(1, 0, nil)
}
