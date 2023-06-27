// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
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
	Header kvpb.Header
	// The AdmissionHeader which will be used when sending the resulting
	// BatchRequest. To be modified directly.
	AdmissionHeader kvpb.AdmissionHeader
	reqs            []kvpb.RequestUnion

	// approxMutationReqBytes tracks the approximate size of keys and values in
	// mutations added to this batch via Put, CPut, InitPut, Del, etc.
	approxMutationReqBytes int
	// Set when AddRawRequest is used, in which case using the "other"
	// operations renders the batch unusable.
	raw bool
	// Once received, the response from a successful batch.
	response *kvpb.BatchResponse
	// Once received, any error encountered sending the batch.
	pErr *kvpb.Error

	// We use pre-allocated buffers to avoid dynamic allocations for small batches.
	resultsBuf    [8]Result
	rowsBuf       []KeyValue
	rowsStaticBuf [8]KeyValue
	rowsStaticIdx int
}

// GValue is a generic value for use in generic code.
type GValue interface {
	[]byte | roachpb.Value
}

// BulkSource is a generator interface for efficiently adding lots of requests.
type BulkSource[T GValue] interface {
	// Len will be called to batch allocate resources, the iterator should return
	// exactly Len KVs.
	Len() int
	// Iter will be called to retrieve KVs and add them to the Batch.
	Iter() BulkSourceIterator[T]
}

// BulkSourceIterator is the iterator interface for bulk put operations.
type BulkSourceIterator[T GValue] interface {
	// Next returns the next KV, calling this more than Len() times is undefined.
	Next() (roachpb.Key, T)
}

// ApproximateMutationBytes returns the approximate byte size of the mutations
// added to this batch via Put, CPut, InitPut, Del, etc methods. Mutations added
// via AddRawRequest are not tracked.
func (b *Batch) ApproximateMutationBytes() int {
	return b.approxMutationReqBytes
}

// Requests exposes the requests stashed in the batch thus far.
func (b *Batch) Requests() []kvpb.RequestUnion {
	return b.reqs
}

// RawResponse returns the BatchResponse which was the result of a successful
// execution of the batch, and nil otherwise.
func (b *Batch) RawResponse() *kvpb.BatchResponse {
	return b.response
}

// MustPErr returns the structured error resulting from a failed execution of
// the batch, asserting that that error is non-nil.
func (b *Batch) MustPErr() *kvpb.Error {
	if b.pErr == nil {
		panic(errors.Errorf("expected non-nil pErr for batch %+v", b))
	}
	return b.pErr
}

// validate that there were no errors while marshaling keys and values.
func (b *Batch) validate() error {
	err := b.resultErr()
	if err != nil {
		// Set pErr just as sendAndFill does, so that higher layers can find it
		// using MustPErr.
		b.pErr = kvpb.NewError(err)
	}
	return err
}

func (b *Batch) initResult(calls, numRows int, raw bool, err error) {
	if err == nil && b.raw && !raw {
		err = errors.Errorf("must not use non-raw operations on a raw batch")
	}
	// TODO(tschottdorf): assert that calls is 0 or 1?
	r := Result{calls: calls, Err: err}
	if numRows > 0 && !b.raw {
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
func (b *Batch) fillResults(ctx context.Context) {
	// No-op if Batch is raw.
	if b.raw {
		return
	}

	offset := 0
	for i := range b.Results {
		result := &b.Results[i]

		for k := 0; k < result.calls; k++ {
			args := b.reqs[offset+k].GetInner()

			var reply kvpb.Response
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
					} else if args.Method() != kvpb.EndTxn {
						// TODO(tschottdorf): EndTxn is special-cased here
						// because it may be elided (r/o txns). Might prefer to
						// simulate an EndTxn response instead; this effectively
						// just leaks here. TODO(tschottdorf): returning an
						// error here seems to get swallowed.
						panic(errors.Errorf("not enough responses for calls: (%T) %+v\nresponses: %+v",
							args, args, b.response))
					}
				}
			}

			switch req := args.(type) {
			case *kvpb.GetRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = reply.(*kvpb.GetResponse).Value
				}
			case *kvpb.PutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
				}
			case *kvpb.ConditionalPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
				}
			case *kvpb.InitPutRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					row.Value = &req.Value
				}
			case *kvpb.IncrementRequest:
				row := &result.Rows[k]
				row.Key = []byte(req.Key)
				if result.Err == nil {
					t := reply.(*kvpb.IncrementResponse)
					row.Value = &roachpb.Value{}
					row.Value.SetInt(t.NewValue)
				}
			case *kvpb.ScanRequest:
				if result.Err == nil {
					t := reply.(*kvpb.ScanResponse)
					result.Rows = make([]KeyValue, len(t.Rows))
					for j := range t.Rows {
						src := &t.Rows[j]
						dst := &result.Rows[j]
						dst.Key = src.Key
						dst.Value = &src.Value
					}
				}
			case *kvpb.ReverseScanRequest:
				if result.Err == nil {
					t := reply.(*kvpb.ReverseScanResponse)
					result.Rows = make([]KeyValue, len(t.Rows))
					for j := range t.Rows {
						src := &t.Rows[j]
						dst := &result.Rows[j]
						dst.Key = src.Key
						dst.Value = &src.Value
					}
				}
			case *kvpb.DeleteRequest:
				if result.Err == nil {
					resp := reply.(*kvpb.DeleteResponse)
					if resp.FoundKey {
						// Accumulate all keys that were deleted as part of a
						// single Del() operation.
						result.Keys = append(result.Keys, args.(*kvpb.DeleteRequest).Key)
					}
				}
			case *kvpb.DeleteRangeRequest:
				if result.Err == nil {
					result.Keys = reply.(*kvpb.DeleteRangeResponse).Keys
				}
			// Nothing to do for all methods below as they do not generate
			// any rows.
			case *kvpb.EndTxnRequest:
			case *kvpb.AdminMergeRequest:
			case *kvpb.AdminSplitRequest:
			case *kvpb.AdminUnsplitRequest:
			case *kvpb.AdminTransferLeaseRequest:
			case *kvpb.AdminChangeReplicasRequest:
			case *kvpb.AdminRelocateRangeRequest:
			case *kvpb.HeartbeatTxnRequest:
			case *kvpb.GCRequest:
			case *kvpb.LeaseInfoRequest:
			case *kvpb.PushTxnRequest:
			case *kvpb.QueryTxnRequest:
			case *kvpb.QueryIntentRequest:
			case *kvpb.ResolveIntentRequest:
			case *kvpb.ResolveIntentRangeRequest:
			case *kvpb.MergeRequest:
			case *kvpb.TruncateLogRequest:
			case *kvpb.RequestLeaseRequest:
			case *kvpb.CheckConsistencyRequest:
			case *kvpb.AdminScatterRequest:
			case *kvpb.AddSSTableRequest:
			case *kvpb.MigrateRequest:
			case *kvpb.QueryResolvedTimestampRequest:
			case *kvpb.BarrierRequest:
			default:
				if result.Err == nil {
					result.Err = errors.Errorf("unsupported reply: %T for %T",
						reply, args)
				}
			}
			// Fill up the resume span.
			if result.Err == nil && reply != nil {
				if h := reply.Header(); h.ResumeSpan != nil {
					result.ResumeSpan = h.ResumeSpan
					result.ResumeReason = h.ResumeReason
					result.ResumeNextBytes = h.ResumeNextBytes
				}
			}
		}
		offset += result.calls
	}
}

// resultErr walks through the result slice and returns the first error found,
// if one exists.
func (b *Batch) resultErr() error {
	for i := range b.Results {
		if err := b.Results[i].Err; err != nil {
			return err
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
		newReqs := make([]kvpb.RequestUnion, len(b.reqs), newSize)
		copy(newReqs, b.reqs)
		b.reqs = newReqs
	}
	b.reqs = b.reqs[:len(b.reqs)+n]
}

func (b *Batch) appendReqs(args ...kvpb.Request) {
	n := len(b.reqs)
	b.growReqs(len(args))
	for i := range args {
		b.reqs[n+i].MustSetInner(args[i])
	}
}

// AddRawRequest adds the specified requests to the batch. No responses will
// be allocated for them, and using any of the non-raw operations will result
// in an error when running the batch.
func (b *Batch) AddRawRequest(reqs ...kvpb.Request) {
	b.raw = true
	for _, args := range reqs {
		numRows := 0
		switch args.(type) {
		case *kvpb.GetRequest,
			*kvpb.PutRequest,
			*kvpb.ConditionalPutRequest,
			*kvpb.IncrementRequest,
			*kvpb.DeleteRequest:
			numRows = 1
		}
		b.appendReqs(args)
		b.initResult(1 /* calls */, numRows, raw, nil)
	}
}

func (b *Batch) get(key interface{}, forUpdate bool) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 1, notRaw, err)
		return
	}
	b.appendReqs(kvpb.NewGet(k, forUpdate))
	b.initResult(1, 1, notRaw, nil)
}

// Get retrieves the value for a key. A new result will be appended to the batch
// which will contain a single row.
//
//	r, err := db.Get("a")
//	// string(r.Rows[0].Key) == "a"
//
// key can be either a byte slice or a string.
func (b *Batch) Get(key interface{}) {
	b.get(key, false /* forUpdate */)
}

// GetForUpdate retrieves the value for a key. An unreplicated, exclusive lock
// is acquired on the key, if it exists. A new result will be appended to the
// batch which will contain a single row.
//
//	r, err := db.GetForUpdate("a")
//	// string(r.Rows[0].Key) == "a"
//
// key can be either a byte slice or a string.
func (b *Batch) GetForUpdate(key interface{}) {
	b.get(key, true /* forUpdate */)
}

func (b *Batch) put(key, value interface{}, inline bool) {
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
		b.appendReqs(kvpb.NewPutInline(k, v))
	} else {
		b.appendReqs(kvpb.NewPut(k, v))
	}
	b.approxMutationReqBytes += len(k) + len(v.RawBytes)
	b.initResult(1, 1, notRaw, nil)
}

// Put sets the value for a key.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
func (b *Batch) Put(key, value interface{}) {
	if value == nil {
		// Empty values are used as deletion tombstones, so one can't write an empty
		// value. If the intention was indeed to delete the key, use Del() instead.
		panic("can't Put an empty Value; did you mean to Del() instead?")
	}
	b.put(key, value, false)
}

// PutBytes allows an arbitrary number of PutRequests to be added to the batch.
func (b *Batch) PutBytes(bs BulkSource[[]byte]) {
	numKeys := bs.Len()
	reqs := make([]struct {
		req   kvpb.PutRequest
		union kvpb.RequestUnion_Put
	}, numKeys)
	i := 0
	bsi := bs.Iter()
	b.bulkRequest(numKeys, func() (kvpb.RequestUnion, int) {
		pr := &reqs[i].req
		union := &reqs[i].union
		union.Put = pr
		i++
		k, v := bsi.Next()
		pr.Key = k
		pr.Value.SetBytes(v)
		pr.Value.InitChecksum(k)
		return kvpb.RequestUnion{Value: union}, len(k) + len(pr.Value.RawBytes)
	})
}

// PutTuples allows multiple tuple value type puts to be added to the batch using
// BulkSource interface.
func (b *Batch) PutTuples(bs BulkSource[[]byte]) {
	numKeys := bs.Len()
	reqs := make([]struct {
		req   kvpb.PutRequest
		union kvpb.RequestUnion_Put
	}, numKeys)
	i := 0
	bsi := bs.Iter()
	b.bulkRequest(numKeys, func() (kvpb.RequestUnion, int) {
		pr := &reqs[i].req
		union := &reqs[i].union
		union.Put = pr
		i++
		k, v := bsi.Next()
		pr.Key = k
		pr.Value.SetTuple(v)
		pr.Value.InitChecksum(k)
		return kvpb.RequestUnion{Value: union}, len(k) + len(pr.Value.RawBytes)
	})
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
// protoutil.Message or any Go primitive type (bool, int, etc).
//
// A nil value can be used to delete the respective key, since there is no
// DelInline(). This is different from Put().
func (b *Batch) PutInline(key, value interface{}) {
	b.put(key, value, true)
}

// CPut conditionally sets the value for a key if the existing value is equal to
// expValue. To conditionally set a value only if the key doesn't currently
// exist, pass an empty expValue.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
//
// value can be any key type, a protoutil.Message or any Go primitive type
// (bool, int, etc). A nil value means delete the key.
//
// An empty expValue means that the key is expected to not exist. If not empty,
// expValue needs to correspond to a Value.TagAndDataBytes() - i.e. a key's
// value without the checksum (as the checksum includes the key too).
func (b *Batch) CPut(key, value interface{}, expValue []byte) {
	b.cputInternal(key, value, expValue, false, false)
}

// CPutAllowingIfNotExists is like CPut except it also allows the Put when the
// existing entry does not exist -- i.e. it succeeds if there is no existing
// entry or the existing entry has the expected value.
func (b *Batch) CPutAllowingIfNotExists(key, value interface{}, expValue []byte) {
	b.cputInternal(key, value, expValue, true, false)
}

// CPutInline conditionally sets the value for a key if the existing value is
// equal to expValue, but does not maintain multi-version values. To
// conditionally set a value only if the key doesn't currently exist, pass an
// empty expValue. The most recent value is always overwritten. Inline values
// cannot be mutated transactionally and should be used with caution.
//
// A new result will be appended to the batch which will contain a single row
// and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc).
//
// A nil value can be used to delete the respective key, since there is no
// DelInline(). This is different from CPut().
func (b *Batch) CPutInline(key, value interface{}, expValue []byte) {
	b.cputInternal(key, value, expValue, false, true)
}

func (b *Batch) cputInternal(
	key, value interface{}, expValue []byte, allowNotExist bool, inline bool,
) {
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
		b.appendReqs(kvpb.NewConditionalPutInline(k, v, expValue, allowNotExist))
	} else {
		b.appendReqs(kvpb.NewConditionalPut(k, v, expValue, allowNotExist))
	}
	b.approxMutationReqBytes += len(k) + len(v.RawBytes)
	b.initResult(1, 1, notRaw, nil)
}

// CPutTuplesEmpty allows multiple CPut tuple requests to be added to the batch
// as tuples using the BulkSource interface. The values for these keys are
// expected to be empty.
func (b *Batch) CPutTuplesEmpty(bs BulkSource[[]byte]) {
	numKeys := bs.Len()
	reqs := make([]struct {
		req   kvpb.ConditionalPutRequest
		union kvpb.RequestUnion_ConditionalPut
	}, numKeys)
	i := 0
	bsi := bs.Iter()
	b.bulkRequest(numKeys, func() (kvpb.RequestUnion, int) {
		pr := &reqs[i].req
		union := &reqs[i].union
		union.ConditionalPut = pr
		pr.AllowIfDoesNotExist = false
		pr.ExpBytes = nil
		i++
		k, v := bsi.Next()
		pr.Key = k
		pr.Value.SetTuple(v)
		pr.Value.InitChecksum(k)
		return kvpb.RequestUnion{Value: union}, len(k) + len(pr.Value.RawBytes)
	})
}

// CPutValuesEmpty allows multiple CPut tuple requests to be added to the batch
// as roachpb.Values using the BulkSource interface. The values for these keys
// are expected to be empty.
func (b *Batch) CPutValuesEmpty(bs BulkSource[roachpb.Value]) {
	numKeys := bs.Len()
	reqs := make([]struct {
		req   kvpb.ConditionalPutRequest
		union kvpb.RequestUnion_ConditionalPut
	}, numKeys)
	i := 0
	bsi := bs.Iter()
	b.bulkRequest(numKeys, func() (kvpb.RequestUnion, int) {
		pr := &reqs[i].req
		union := &reqs[i].union
		union.ConditionalPut = pr
		pr.AllowIfDoesNotExist = false
		pr.ExpBytes = nil
		i++
		k, v := bsi.Next()
		pr.Key = k
		pr.Value = v
		pr.Value.InitChecksum(k)
		return kvpb.RequestUnion{Value: union}, len(k) + len(pr.Value.RawBytes)
	})
}

// InitPut sets the first value for a key to value. An ConditionFailedError is
// reported if a value already exists for the key and it's not equal to the
// value passed in. If failOnTombstones is set to true, tombstones will return
// a ConditionFailedError just like a mismatched value.
//
// key can be either a byte slice or a string. value can be any key type, a
// protoutil.Message or any Go primitive type (bool, int, etc). It is illegal
// to set value to nil.
func (b *Batch) InitPut(key, value interface{}, failOnTombstones bool) {
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
	b.appendReqs(kvpb.NewInitPut(k, v, failOnTombstones))
	b.approxMutationReqBytes += len(k) + len(v.RawBytes)
	b.initResult(1, 1, notRaw, nil)
}

// InitPutBytes allows multiple []byte value type InitPut requests to be added to
// the batch using BulkSource interface.
func (b *Batch) InitPutBytes(bs BulkSource[[]byte]) {
	numKeys := bs.Len()
	reqs := make([]struct {
		req   kvpb.InitPutRequest
		union kvpb.RequestUnion_InitPut
	}, numKeys)
	i := 0
	bsi := bs.Iter()
	b.bulkRequest(numKeys, func() (kvpb.RequestUnion, int) {
		pr := &reqs[i].req
		union := &reqs[i].union
		union.InitPut = pr
		i++
		k, v := bsi.Next()
		pr.Key = k
		pr.Value.SetBytes(v)
		pr.Value.InitChecksum(k)
		return kvpb.RequestUnion{Value: union}, len(k) + len(pr.Value.RawBytes)
	})
}

// InitPutTuples allows multiple tuple value type InitPut to be added to the
// batch using BulkSource interface.
func (b *Batch) InitPutTuples(bs BulkSource[[]byte]) {
	numKeys := bs.Len()
	reqs := make([]struct {
		req   kvpb.InitPutRequest
		union kvpb.RequestUnion_InitPut
	}, numKeys)
	i := 0
	bsi := bs.Iter()
	b.bulkRequest(numKeys, func() (kvpb.RequestUnion, int) {
		pr := &reqs[i].req
		union := &reqs[i].union
		union.InitPut = pr
		i++
		k, v := bsi.Next()
		pr.Key = k
		pr.Value.SetTuple(v)
		pr.Value.InitChecksum(k)
		return kvpb.RequestUnion{Value: union}, len(k) + len(pr.Value.RawBytes)
	})
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
	b.appendReqs(kvpb.NewIncrement(k, value))
	b.initResult(1, 1, notRaw, nil)
}

func (b *Batch) scan(s, e interface{}, isReverse, forUpdate bool) {
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
		b.appendReqs(kvpb.NewScan(begin, end, forUpdate))
	} else {
		b.appendReqs(kvpb.NewReverseScan(begin, end, forUpdate))
	}
	b.initResult(1, 0, notRaw, nil)
}

// Scan retrieves the key/values between begin (inclusive) and end (exclusive) in
// ascending order.
//
// A new result will be appended to the batch which will contain "rows" (each
// row is a key/value pair) and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) Scan(s, e interface{}) {
	b.scan(s, e, false /* isReverse */, false /* forUpdate */)
}

// ScanForUpdate retrieves the key/values between begin (inclusive) and end
// (exclusive) in ascending order. Unreplicated, exclusive locks are acquired on
// each of the returned keys.
//
// A new result will be appended to the batch which will contain "rows" (each
// row is a key/value pair) and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) ScanForUpdate(s, e interface{}) {
	b.scan(s, e, false /* isReverse */, true /* forUpdate */)
}

// ReverseScan retrieves the rows between begin (inclusive) and end (exclusive)
// in descending order.
//
// A new result will be appended to the batch which will contain "rows" (each
// "row" is a key/value pair) and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) ReverseScan(s, e interface{}) {
	b.scan(s, e, true /* isReverse */, false /* forUpdate */)
}

// ReverseScanForUpdate retrieves the rows between begin (inclusive) and end
// (exclusive) in descending order. Unreplicated, exclusive locks are acquired
// on each of the returned keys.
//
// A new result will be appended to the batch which will contain "rows" (each
// "row" is a key/value pair) and Result.Err will indicate success or failure.
//
// key can be either a byte slice or a string.
func (b *Batch) ReverseScanForUpdate(s, e interface{}) {
	b.scan(s, e, true /* isReverse */, true /* forUpdate */)
}

// Del deletes one or more keys.
//
// A new result will be appended to the batch which will contain 0 rows and
// Result.Err will indicate success or failure. Each key will be included in
// Result.Keys if it was actually deleted.
//
// key can be either a byte slice or a string.
func (b *Batch) Del(keys ...interface{}) {
	reqs := make([]kvpb.Request, 0, len(keys))
	for _, key := range keys {
		k, err := marshalKey(key)
		if err != nil {
			b.initResult(len(keys), 0, notRaw, err)
			return
		}
		reqs = append(reqs, kvpb.NewDelete(k))
		b.approxMutationReqBytes += len(k)
	}
	b.appendReqs(reqs...)
	b.initResult(len(reqs), 0, notRaw, nil)
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
	b.appendReqs(kvpb.NewDeleteRange(begin, end, returnKeys))
	b.initResult(1, 0, notRaw, nil)
}

// DelRangeUsingTombstone deletes the rows between begin (inclusive) and end
// (exclusive) using an MVCC range tombstone. Callers must check
// storage.CanUseMVCCRangeTombstones before using this.
func (b *Batch) DelRangeUsingTombstone(s, e interface{}) {
	start, err := marshalKey(s)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	end, err := marshalKey(e)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	b.appendReqs(&kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
		UseRangeTombstone: true,
	})
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
	req := &kvpb.AdminMergeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: k,
		},
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// adminSplit is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminSplit(
	splitKeyIn interface{}, expirationTime hlc.Timestamp, predicateKeys []roachpb.Key,
) {
	splitKey, err := marshalKey(splitKeyIn)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &kvpb.AdminSplitRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: splitKey,
		},
		SplitKey:       splitKey,
		ExpirationTime: expirationTime,
		PredicateKeys:  predicateKeys,
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

func (b *Batch) adminUnsplit(splitKeyIn interface{}) {
	splitKey, err := marshalKey(splitKeyIn)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
	}
	req := &kvpb.AdminUnsplitRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: splitKey,
		},
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// adminTransferLease is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminTransferLease(
	key interface{}, target roachpb.StoreID, bypassSafetyChecks bool,
) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &kvpb.AdminTransferLeaseRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: k,
		},
		Target:             target,
		BypassSafetyChecks: bypassSafetyChecks,
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// adminChangeReplicas is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminChangeReplicas(
	key interface{}, expDesc roachpb.RangeDescriptor, chgs []kvpb.ReplicationChange,
) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &kvpb.AdminChangeReplicasRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: k,
		},
		ExpDesc: expDesc,
	}
	req.AddChanges(chgs...)

	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// adminRelocateRange is only exported on DB. It is here for symmetry with the
// other operations.
func (b *Batch) adminRelocateRange(
	key interface{},
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) {
	k, err := marshalKey(key)
	if err != nil {
		b.initResult(0, 0, notRaw, err)
		return
	}
	req := &kvpb.AdminRelocateRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: k,
		},
		VoterTargets:                      voterTargets,
		NonVoterTargets:                   nonVoterTargets,
		TransferLeaseToFirstVoter:         transferLeaseToFirstVoter,
		TransferLeaseToFirstVoterAccurate: true,
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// addSSTable is only exported on DB.
func (b *Batch) addSSTable(
	s, e interface{},
	data []byte,
	remoteFile kvpb.AddSSTableRequest_RemoteFile,
	disallowConflicts bool,
	disallowShadowing bool,
	disallowShadowingBelow hlc.Timestamp,
	stats *enginepb.MVCCStats,
	ingestAsWrites bool,
	sstTimestampToRequestTimestamp hlc.Timestamp,
) {
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
	req := &kvpb.AddSSTableRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    begin,
			EndKey: end,
		},
		Data:                           data,
		RemoteFile:                     remoteFile,
		DisallowConflicts:              disallowConflicts,
		DisallowShadowing:              disallowShadowing,
		DisallowShadowingBelow:         disallowShadowingBelow,
		MVCCStats:                      stats,
		IngestAsWrites:                 ingestAsWrites,
		SSTTimestampToRequestTimestamp: sstTimestampToRequestTimestamp,
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// migrate is only exported on DB.
func (b *Batch) migrate(s, e interface{}, version roachpb.Version) {
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
	req := &kvpb.MigrateRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    begin,
			EndKey: end,
		},
		Version: version,
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

// queryResolvedTimestamp is only exported on DB.
func (b *Batch) queryResolvedTimestamp(s, e interface{}) {
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
	req := &kvpb.QueryResolvedTimestampRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    begin,
			EndKey: end,
		},
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

func (b *Batch) barrier(s, e interface{}) {
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
	req := &kvpb.BarrierRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    begin,
			EndKey: end,
		},
	}
	b.appendReqs(req)
	b.initResult(1, 0, notRaw, nil)
}

func (b *Batch) bulkRequest(
	numKeys int, requestFactory func() (req kvpb.RequestUnion, kvSize int),
) {
	n := len(b.reqs)
	b.growReqs(numKeys)
	newReqs := b.reqs[n:]
	for i := 0; i < numKeys; i++ {
		req, numBytes := requestFactory()
		b.approxMutationReqBytes += numBytes
		newReqs[i] = req
	}
	b.initResult(numKeys, numKeys, notRaw, nil)
}

// GetResult retrieves the Result and Result row KeyValue for a particular index.
func (b *Batch) GetResult(idx int) (*Result, KeyValue, error) {
	origIdx := idx
	for i := range b.Results {
		r := &b.Results[i]
		if idx < r.calls {
			if idx < len(r.Rows) {
				return r, r.Rows[idx], nil
			} else {
				return r, KeyValue{}, nil
			}
		}
		idx -= r.calls
	}
	return nil, KeyValue{}, errors.AssertionFailedf("index %d outside of results: %+v", origIdx, b.Results)
}
