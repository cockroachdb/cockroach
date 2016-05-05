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

import "github.com/cockroachdb/cockroach/roachpb"

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
	// The Txn the batch is associated with. This field may be nil if the batch
	// was not created via Txn.NewBatch.
	txn *Txn
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
	Results []Result
	reqs    []roachpb.Request
	// If nonzero, limits the total amount of key/values returned by all Scan/ReverseScan operations
	// in the batch. This can only be used if all requests are of the same type, and that type is
	// Scan or ReverseScan.
	MaxScanResults int64
	// ReadConsistency specifies the consistency for read operations. The default
	// is CONSISTENT. This value is ignored for write operations.
	ReadConsistency roachpb.ReadConsistencyType

	// We use pre-allocated buffers to avoid dynamic allocations for small batches.
	resultsBuf    [8]Result
	rowsBuf       []KeyValue
	rowsStaticBuf [8]KeyValue
	rowsStaticIdx int
}

func (b *Batch) prepare() error {
	for _, r := range b.Results {
		if r.Err() != nil {
			return r.Err()
		}
	}
	return nil
}

func (b *Batch) initResult(calls, numRows int, err error) {
	// TODO(tschottdorf): assert that calls is 0 or 1?
	r := Result{calls: calls, PErr: roachpb.NewError(err)}
	if numRows > 0 {
		if b.rowsStaticIdx+numRows <= len(b.rowsStaticBuf) {
			r.Rows = b.rowsStaticBuf[b.rowsStaticIdx : b.rowsStaticIdx+numRows]
			b.rowsStaticIdx += numRows
		} else {
			// Most requests produce 0 (unknown) or 1 result rows, so optimize for
			// that case.
			switch numRows {
			case 1:
				// Use a buffer to batch allocate the result rows.
				if cap(b.rowsBuf)-len(b.rowsBuf) == 0 {
					const minSize = 16
					const maxSize = 128
					size := cap(b.rowsBuf) * 2
					if size < minSize {
						size = minSize
					} else if size > maxSize {
						size = maxSize
					}
					b.rowsBuf = make([]KeyValue, 0, size)
				}
				pos := len(b.rowsBuf)
				r.Rows = b.rowsBuf[pos : pos+1 : pos+1]
				b.rowsBuf = b.rowsBuf[:pos+1]
			default:
				r.Rows = make([]KeyValue, numRows)
			}
		}
	}
	if b.Results == nil {
		b.Results = b.resultsBuf[:0]
	}
	b.Results = append(b.Results, r)
}

// Returns the first error.
func (b *Batch) fillResults(br *roachpb.BatchResponse, pErr *roachpb.Error) error {
	offset := 0
	for i := range b.Results {
		result := &b.Results[i]

		for k := 0; k < result.calls; k++ {
			args := b.reqs[offset+k]

			var reply roachpb.Response
			if result.PErr == nil {
				result.PErr = pErr
				if result.PErr == nil {
					if br != nil && offset+k < len(br.Responses) {
						reply = br.Responses[offset+k].GetInner()
					} else if args.Method() != roachpb.EndTransaction {
						// TODO(tschottdorf): EndTransaction is special-cased
						// here because it may be elided (r/o txns). Might
						// prefer to simulate an EndTransaction response
						// instead; this effectively just leaks here.
						panic("not enough responses for calls")
					}
				}
			}

			switch req := args.(type) {
			case *roachpb.GetRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err() == nil {
					row.Value = reply.(*roachpb.GetResponse).Value
				}
			case *roachpb.PutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err() == nil {
					row.Value = &req.Value
				}
			case *roachpb.ConditionalPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err() == nil {
					row.Value = &req.Value
				}
			case *roachpb.InitPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err() == nil {
					row.Value = &req.Value
				}
			case *roachpb.IncrementRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err() == nil {
					t := reply.(*roachpb.IncrementResponse)
					row.Value = &roachpb.Value{}
					row.Value.SetInt(t.NewValue)
				}
			case *roachpb.ScanRequest:
				if result.Err() == nil {
					t := reply.(*roachpb.ScanResponse)
					result.Rows = make([]KeyValue, len(t.Rows))
					for j := range t.Rows {
						src := &t.Rows[j]
						dst := &result.Rows[j]
						dst.Key = src.Key
						dst.Value = &src.Value
					}
				}
			case *roachpb.ReverseScanRequest:
				if result.Err() == nil {
					t := reply.(*roachpb.ReverseScanResponse)
					result.Rows = make([]KeyValue, len(t.Rows))
					for j := range t.Rows {
						src := &t.Rows[j]
						dst := &result.Rows[j]
						dst.Key = src.Key
						dst.Value = &src.Value
					}
				}
			case *roachpb.DeleteRequest:
				row := &result.Rows[k]
				row.Key = []byte(args.(*roachpb.DeleteRequest).Key)

			case *roachpb.DeleteRangeRequest:
				if result.Err() == nil {
					result.Keys = reply.(*roachpb.DeleteRangeResponse).Keys
				}

			case *roachpb.ChangeFrozenRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.PErr == nil {
					t := reply.(*roachpb.ChangeFrozenResponse)
					row.Value = &roachpb.Value{}
					if err := row.Value.SetProto(t); err != nil {
						panic(err)
					}
				}

			default:
				if result.PErr == nil {
					result.PErr = roachpb.NewErrorf("unsupported reply: %T", reply)
				}

				// Nothing to do for all methods below as they do not generate
				// any rows.
			case *roachpb.BeginTransactionRequest:
			case *roachpb.EndTransactionRequest:
			case *roachpb.AdminMergeRequest:
			case *roachpb.AdminSplitRequest:
			case *roachpb.HeartbeatTxnRequest:
			case *roachpb.GCRequest:
			case *roachpb.PushTxnRequest:
			case *roachpb.RangeLookupRequest:
			case *roachpb.ResolveIntentRequest:
			case *roachpb.ResolveIntentRangeRequest:
			case *roachpb.MergeRequest:
			case *roachpb.TruncateLogRequest:
			case *roachpb.LeaderLeaseRequest:
			case *roachpb.CheckConsistencyRequest:
			}
		}
		offset += result.calls
	}

	for i := range b.Results {
		result := &b.Results[i]
		if result.PErr != nil {
			return result.Err()
		}
	}
	return nil
}

// InternalAddRequest adds the specified requests to the batch. It is intended
// for internal use only.
func (b *Batch) InternalAddRequest(reqs ...roachpb.Request) {
	for _, args := range reqs {
		numRows := 0
		switch args.(type) {
		case *roachpb.GetRequest,
			*roachpb.PutRequest,
			*roachpb.ConditionalPutRequest,
			*roachpb.IncrementRequest,
			*roachpb.DeleteRequest:
			numRows = 1
		}
		b.reqs = append(b.reqs, args)
		b.initResult(1 /* calls */, numRows, nil)
	}
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
	b.reqs = append(b.reqs, roachpb.NewGet(k))
	b.initResult(1, 1, nil)
}

func (b *Batch) put(key, value interface{}, inline bool) {
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
	if inline {
		b.reqs = append(b.reqs, roachpb.NewPutInline(k, v))
	} else {
		b.reqs = append(b.reqs, roachpb.NewPut(k, v))
	}
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
	b.put(key, value, false)
}

// PutInline sets the value for a key, but does not maintain
// multi-version values. The most recent value is always overwritten.
// Inline values cannot be mutated transactionally and should be used
// with caution.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (b *Batch) PutInline(key, value interface{}) {
	b.put(key, value, true)
}

// CPut conditionally sets the value for a key if the existing value is equal
// to expValue. To conditionally set a value only if there is no existing entry
// pass nil for expValue. Note that this must be an interface{}(nil), not a
// typed nil value (e.g. []byte(nil)).
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
	b.reqs = append(b.reqs, roachpb.NewConditionalPut(k, v, ev))
	b.initResult(1, 1, nil)
}

// InitPut sets the first value for a key to value. An error is reported if a
// value already exists for the key and it's not equal to the value passed in.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc). It is illegal to
// set value to nil.
func (b *Batch) InitPut(key, value interface{}) {
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
	b.reqs = append(b.reqs, roachpb.NewInitPut(k, v))
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
	b.reqs = append(b.reqs, roachpb.NewIncrement(k, value))
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
		b.reqs = append(b.reqs, roachpb.NewScan(roachpb.Key(begin), roachpb.Key(end), maxRows))
	} else {
		b.reqs = append(b.reqs, roachpb.NewReverseScan(roachpb.Key(begin), roachpb.Key(end), maxRows))
	}
	b.initResult(1, 0, nil)
}

// Scan retrieves the key/values between begin (inclusive) and end (exclusive) in
// ascending order.
//
// A new result will be appended to the batch which will contain up to maxRows
// "rows" (each row is a key/value pair) and Result.Err will indicate success or
// failure.
//
// key can be either a byte slice or a string.
func (b *Batch) Scan(s, e interface{}, maxRows int64) {
	b.scan(s, e, maxRows, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// A new result will be appended to the batch which will contain up to maxRows
// rows (each "row" is a key/value pair) and Result.Err will indicate success or
// failure.
//
// key can be either a byte slice or a string.
func (b *Batch) ReverseScan(s, e interface{}, maxRows int64) {
	b.scan(s, e, maxRows, true)
}

// CheckConsistency creates a batch request to check the consistency of the
// ranges holding the span of keys from s to e. It logs a diff of all the
// keys that are inconsistent when withDiff is set to true.
func (b *Batch) CheckConsistency(s, e interface{}, withDiff bool) {
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
	b.reqs = append(b.reqs, roachpb.NewCheckConsistency(roachpb.Key(begin), roachpb.Key(end), withDiff))
	b.initResult(1, 0, nil)
}

// ChangeFrozen attempts to freeze or unfreeze all Ranges with StartKey
// covered by the given key range.
func (b *Batch) ChangeFrozen(s, e interface{}, mustVersion string, frozen bool) {
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
	b.reqs = append(b.reqs,
		roachpb.NewChangeFrozen(roachpb.Key(begin), roachpb.Key(end), frozen, mustVersion))
	b.initResult(1, 1, nil)
}

// Del deletes one or more keys.
//
// A new result will be appended to the batch and each key will have a
// corresponding row in the returned Result.
//
// key can be either a byte slice or a string.
func (b *Batch) Del(keys ...interface{}) {
	var reqs []roachpb.Request
	for _, key := range keys {
		k, err := marshalKey(key)
		if err != nil {
			b.initResult(0, len(keys), err)
			return
		}
		reqs = append(reqs, roachpb.NewDelete(k))
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
func (b *Batch) DelRange(s, e interface{}, returnKeys bool) {
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
	b.reqs = append(b.reqs, roachpb.NewDeleteRange(roachpb.Key(begin), roachpb.Key(end), returnKeys))
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
	req := &roachpb.AdminMergeRequest{
		Span: roachpb.Span{
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
	req := &roachpb.AdminSplitRequest{
		Span: roachpb.Span{
			Key: k,
		},
	}
	req.SplitKey = k
	b.reqs = append(b.reqs, req)
	b.initResult(1, 0, nil)
}
