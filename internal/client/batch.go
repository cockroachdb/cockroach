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
	"github.com/pkg/errors"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	raw    = true
	notRaw = false
)

// Batch provides for the parallel execution of a number of database
// operations. Operations are added to the Batch and then the Batch is executed
// via either DB.Run, Txn.Run or Txn.Commit.
//
// TODO(pmattis): Allow a timestamp to be specified which is applied to all
// operations within the batch.
type Batch struct {
	// The Txn the batch is associated with. This field may be nil if the batch
	// was not created via Txn.NewBatch.
	txn *Txn
	ctx context.Context
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
	// The Header which will be used to send the resulting BatchRequest.
	// To be modified directly.
	Header roachpb.Header
	reqs   []roachpb.RequestUnion
	// Set when AddRawRequest is used, in which case using the "other"
	// operations renders the batch unusable.
	raw bool
	// Once received, the response from a successful batch.
	response *roachpb.BatchResponse
	// Once received, any error encountered sending the batch.
	pErr *roachpb.Error

	// We use pre-allocated buffers to avoid dynamic allocations for small batches.
	resultsBuf    [8]Result
	rowsBuf       []KeyValue
	rowsStaticBuf [8]KeyValue
	rowsStaticIdx int
}

// RawResponse returns the BatchResponse which was the result of a successful
// execution of the batch, and nil otherwise.
func (b *Batch) RawResponse() *roachpb.BatchResponse {
	return b.response
}

// MustPErr returns the structured error resulting from a failed execution of
// the batch, asserting that that error is non-nil.
func (b *Batch) MustPErr() *roachpb.Error {
	if b.pErr == nil {
		panic(errors.Errorf("expected non-nil pErr for batch %+v", b))
	}
	return b.pErr
}

func (b *Batch) prepare() error {
	for _, r := range b.Results {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

func (b *Batch) initResult(calls, numRows int, raw bool, err error) {
	if err == nil && b.raw && !raw {
		err = errors.Errorf("must not use non-raw operations on a raw batch")
	}
	// TODO(tschottdorf): assert that calls is 0 or 1?
	r := Result{calls: calls, Err: err}
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

// fillResults walks through the results and updates them either with the
// data or error which was the result of running the batch previously.
func (b *Batch) fillResults() error {
	offset := 0
	for i := range b.Results {
		result := &b.Results[i]

		for k := 0; k < result.calls; k++ {
			args := b.reqs[offset+k].GetInner()

			var reply roachpb.Response
			// It's possible that result.Err was populated early, for example
			// when PutProto is called and the proto marshaling errored out.
			// In that case, we don't want to mutate this result's error
			// further.
			if result.Err == nil {
				// The outcome of each result is that of the batch as a whole.
				result.Err = b.pErr.GoError()
				if result.Err == nil {
					// For a successful request, load the reply to populate in
					// this pass.
					if b.response != nil && offset+k < len(b.response.Responses) {
						reply = b.response.Responses[offset+k].GetInner()
					} else if args.Method() != roachpb.EndTransaction {
						// TODO(tschottdorf): EndTransaction is special-cased
						// here because it may be elided (r/o txns). Might
						// prefer to simulate an EndTransaction response
						// instead; this effectively just leaks here.
						// TODO(tschottdorf): returning an error here seems
						// to get swallowed.
						panic(errors.Errorf("not enough responses for calls: %+v, %+v",
							b.reqs, b.response))
					}
				}
			}

			switch req := args.(type) {
			case *roachpb.GetRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = reply.(*roachpb.GetResponse).Value
				}
			case *roachpb.PutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
				}
			case *roachpb.ConditionalPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
				}
			case *roachpb.InitPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
				}
			case *roachpb.IncrementRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					t := reply.(*roachpb.IncrementResponse)
					row.Value = &roachpb.Value{}
					row.Value.SetInt(t.NewValue)
				}
			case *roachpb.ScanRequest:
				if result.Err == nil {
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
				if result.Err == nil {
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
				if result.Err == nil {
					result.Keys = reply.(*roachpb.DeleteRangeResponse).Keys
				}

			default:
				if result.Err == nil {
					result.Err = errors.Errorf("unsupported reply: %T for %T",
						reply, args)
				}

				// Nothing to do for all methods below as they do not generate
				// any rows.
			case *roachpb.BeginTransactionRequest:
			case *roachpb.EndTransactionRequest:
			case *roachpb.AdminMergeRequest:
			case *roachpb.AdminSplitRequest:
			case *roachpb.AdminTransferLeaseRequest:
			case *roachpb.HeartbeatTxnRequest:
			case *roachpb.GCRequest:
			case *roachpb.PushTxnRequest:
			case *roachpb.RangeLookupRequest:
			case *roachpb.ResolveIntentRequest:
			case *roachpb.ResolveIntentRangeRequest:
			case *roachpb.MergeRequest:
			case *roachpb.TruncateLogRequest:
			case *roachpb.RequestLeaseRequest:
			case *roachpb.CheckConsistencyRequest:
			case *roachpb.ChangeFrozenRequest:
			}
			// Fill up the resume span.
			if result.Err == nil && reply != nil && reply.Header().ResumeSpan != nil {
				result.ResumeSpan = *reply.Header().ResumeSpan
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

func (b *Batch) growReqs(n int) {
	if len(b.reqs)+n > cap(b.reqs) {
		newSize := 2 * cap(b.reqs)
		if newSize == 0 {
			newSize = 8
		}
		for newSize < len(b.reqs)+n {
			newSize *= 2
		}
		newReqs := make([]roachpb.RequestUnion, len(b.reqs), newSize)
		copy(newReqs, b.reqs)
		b.reqs = newReqs
	}
	b.reqs = b.reqs[:len(b.reqs)+n]
}

func (b *Batch) logRequest(arg roachpb.Request, level int) {
	if !log.VDepth(2, level) {
		return
	}
	switch req := arg.(type) {
	case *roachpb.GetRequest:
		log.InfofDepth(b.ctx, level, "Get %s", req.Key)
	case *roachpb.PutRequest:
		log.InfofDepth(b.ctx, level, "Put %s -> %s", req.Key, req.Value.PrettyPrint())
	case *roachpb.ConditionalPutRequest:
		log.InfofDepth(b.ctx, level, "CPut %s -> %s", req.Key, req.Value.PrettyPrint())
	case *roachpb.InitPutRequest:
		log.InfofDepth(b.ctx, level, "InitPut %s -> %s", req.Key, req.Value.PrettyPrint())
	case *roachpb.IncrementRequest:
		log.InfofDepth(b.ctx, level, "Increment %s by %v", req.Key, req.Increment)
	case *roachpb.ScanRequest:
		log.InfofDepth(b.ctx, level, "Scan from %s to %s", req.Key, req.EndKey)
	case *roachpb.ReverseScanRequest:
		log.InfofDepth(b.ctx, level, "Reverse scan from %s to %s", req.Key, req.EndKey)
	case *roachpb.DeleteRequest:
		log.InfofDepth(b.ctx, level, "Del %s", req.Key)
	case *roachpb.DeleteRangeRequest:
		log.InfofDepth(b.ctx, level, "DelRange %s", req.Key)
	case *roachpb.BeginTransactionRequest:
		log.InfofDepth(b.ctx, level, "BeginTransaction %s", req.Key)
	case *roachpb.EndTransactionRequest:
		log.InfofDepth(b.ctx, level, "EndTransaction %s", req.Key)
	case *roachpb.AdminMergeRequest:
		log.InfofDepth(b.ctx, level, "AdminMerge from %s to %s", req.Key, req.EndKey)
	case *roachpb.AdminSplitRequest:
		log.InfofDepth(b.ctx, level, "AdminSplit %s at %s", req.Key, req.SplitKey)
	case *roachpb.AdminTransferLeaseRequest:
		log.InfofDepth(b.ctx, level, "AdminTransferLease %s to %s", req.Key, req.Target)
	case *roachpb.HeartbeatTxnRequest:
		log.InfofDepth(b.ctx, level, "HeartbeatTxn %s at %v", req.Key, req.Now)
	case *roachpb.GCRequest:
		log.InfofDepth(b.ctx, level, "GC from %s to %s", req.Key, req.EndKey)
	case *roachpb.PushTxnRequest:
		log.InfofDepth(b.ctx, level, "PushTxn %s from %s to %s", req.Key, req.PusherTxn, req.PusheeTxn)
	case *roachpb.RangeLookupRequest:
		log.InfofDepth(b.ctx, level, "RangeLook at %s", req.Key)
	case *roachpb.ResolveIntentRequest:
		log.InfofDepth(b.ctx, level, "ResolveIntent at %s for txn %s", req.Span, req.IntentTxn)
	case *roachpb.ResolveIntentRangeRequest:
		log.InfofDepth(b.ctx, level, "ResolveIntentRange at %s for txn %s", req.Span, req.IntentTxn)
	case *roachpb.MergeRequest:
		log.InfofDepth(b.ctx, level, "Merge at %s -> %s", req.Key, req.Value.PrettyPrint())
	case *roachpb.TruncateLogRequest:
		log.InfofDepth(b.ctx, level, "TruncateLog at %s for range %v", req.Index, req.RangeID)
	case *roachpb.RequestLeaseRequest:
		log.InfofDepth(b.ctx, level, "RequestLease at %s for lease %s", req.Key, req.Lease)
	case *roachpb.CheckConsistencyRequest:
		log.InfofDepth(b.ctx, level, "CheckConsistency from %s to %s", req.Key, req.EndKey)
	case *roachpb.ChangeFrozenRequest:
		log.InfofDepth(b.ctx, level, "ChangeFrozen from %s to %s", req.Key, req.EndKey)
	default:
		log.InfofDepth(b.ctx, level, "%T <unknown>", req)

	}
}

func (b *Batch) appendReqs(level int, args ...roachpb.Request) {
	n := len(b.reqs)
	b.growReqs(len(args))
	for i := range args {
		b.logRequest(args[i], level+1)
		b.reqs[n+i].MustSetInner(args[i])
	}
}

// AddRawRequest adds the specified requests to the batch. No responses will
// be allocated for them, and using any of the non-raw operations will result
// in an error when running the batch.
func (b *Batch) AddRawRequest(reqs ...roachpb.Request) {
	b.raw = true
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
		b.appendReqs(1, args)
		b.initResult(1 /* calls */, numRows, raw, nil)
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
		b.initResult(0, 1, notRaw, err)
		return
	}
	b.appendReqs(1, roachpb.NewGet(k))
	b.initResult(1, 1, notRaw, nil)
}

func (b *Batch) putDepth(key, value interface{}, inline bool, depth int) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	if inline {
		b.appendReqs(depth+1, roachpb.NewPutInline(k, v))
	} else {
		b.appendReqs(depth+1, roachpb.NewPut(k, v))
	}
	b.initResult(1, 1, notRaw, nil)
}

// Put sets the value for a key.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string. value can be any key type, a
// proto.Message or any Go primitive type (bool, int, etc).
func (b *Batch) Put(key, value interface{}) {
	b.PutDepth(key, value, 1)
}

// PutDepth sets the value for a key and spefify the caller's depth.
func (b *Batch) PutDepth(key, value interface{}, depth int) {
	b.putDepth(key, value, false, depth+1)
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
	b.putDepth(key, value, true, 1)
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
	b.CPutDepth(key, value, expValue, 1)
}

// CPutDepth conditionally sets the value for a key and specify the caller's depth.
func (b *Batch) CPutDepth(key, value, expValue interface{}, depth int) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	ev, err := marshalValue(expValue)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	b.appendReqs(depth+1, roachpb.NewConditionalPut(k, v, ev))
	b.initResult(1, 1, notRaw, nil)
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
		b.initResult(0, 1, notRaw, err)
		return
	}
	v, err := marshalValue(value)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	b.appendReqs(1, roachpb.NewInitPut(k, v))
	b.initResult(1, 1, notRaw, nil)
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
		b.initResult(0, 1, notRaw, err)
		return
	}
	b.appendReqs(1, roachpb.NewIncrement(k, value))
	b.initResult(1, 1, notRaw, nil)
}

func (b *Batch) scan(s, e interface{}, isReverse bool) {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	if !isReverse {
		b.appendReqs(1, roachpb.NewScan(begin, end))
	} else {
		b.appendReqs(1, roachpb.NewReverseScan(begin, end))
	}
	b.initResult(1, 0, notRaw, nil)
}

// Scan retrieves the key/values between begin (inclusive) and end (exclusive) in
// ascending order.
//
// A new result will be appended to the batch which will contain  "rows" (each
// row is a key/value pair) and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) Scan(s, e interface{}) {
	b.scan(s, e, false)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// A new result will be appended to the batch which will contain "rows" (each
// "row" is a key/value pair) and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) ReverseScan(s, e interface{}) {
	b.scan(s, e, true)
}

// CheckConsistency creates a batch request to check the consistency of the
// ranges holding the span of keys from s to e. It logs a diff of all the
// keys that are inconsistent when withDiff is set to true.
func (b *Batch) CheckConsistency(s, e interface{}, withDiff bool) {
	begin, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	b.appendReqs(1, roachpb.NewCheckConsistency(begin, end, withDiff))
	b.initResult(1, 0, notRaw, nil)
}

// Del deletes one or more keys.
//
// A new result will be appended to the batch and each key will have a
// corresponding row in the returned Result.
//
// key can be either a byte slice or a string.
func (b *Batch) Del(keys ...interface{}) {
	reqs := make([]roachpb.Request, 0, len(keys))
	for _, key := range keys {
		k, err := marshalKey(key)
		if err != nil {
			b.initResult(0, len(keys), notRaw, err)
			return
		}
		reqs = append(reqs, roachpb.NewDelete(k))
	}
	b.appendReqs(1, reqs...)
	b.initResult(len(reqs), len(reqs), notRaw, nil)
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
		b.initResult(0, 0, notRaw, err)
		return
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	b.appendReqs(1, roachpb.NewDeleteRange(begin, end, returnKeys))
	b.initResult(1, 0, notRaw, nil)
}

// adminMerge is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminMerge(key interface{}) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &roachpb.AdminMergeRequest{
		Span: roachpb.Span{
			Key: k,
		},
	}
	b.appendReqs(1, req)
	b.initResult(1, 0, notRaw, nil)
}

// adminSplit is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminSplit(splitKey interface{}) {
	k, err := marshalKey(splitKey)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &roachpb.AdminSplitRequest{
		Span: roachpb.Span{
			Key: k,
		},
	}
	req.SplitKey = k
	b.appendReqs(1, req)
	b.initResult(1, 0, notRaw, nil)
}

// adminTransferLease is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminTransferLease(key interface{}, target roachpb.StoreID) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &roachpb.AdminTransferLeaseRequest{
		Span: roachpb.Span{
			Key: k,
		},
		Target: target,
	}
	b.appendReqs(1, req)
	b.initResult(1, 0, notRaw, nil)
}
