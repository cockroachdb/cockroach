// Copyright 2014 The Cockroach Authors.
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
// Author: Tobias Schottdorf

package roachpb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/util/hlc"
)

// SetActiveTimestamp sets the correct timestamp at which the request is to be
// carried out. For transactional requests, ba.Timestamp must be zero initially
// and it will be set to txn.OrigTimestamp. For non-transactional requests, if
// no timestamp is specified, nowFn is used to create and set one.
func (ba *BatchRequest) SetActiveTimestamp(nowFn func() hlc.Timestamp) error {
	if ba.Txn == nil {
		// When not transactional, allow empty timestamp and  use nowFn instead.
		if ba.Timestamp.Equal(hlc.ZeroTimestamp) {
			ba.Timestamp.Forward(nowFn())
		}
	} else if !ba.Timestamp.Equal(hlc.ZeroTimestamp) {
		return errors.New("transactional request must not set batch timestamp")
	} else {
		// Always use the original timestamp for reads and writes, even
		// though some intents may be written at higher timestamps in the
		// event of a WriteTooOldError.
		ba.Timestamp = ba.Txn.OrigTimestamp
	}
	return nil
}

// IsFreeze returns whether the batch consists of a single ChangeFrozen request.
func (ba *BatchRequest) IsFreeze() bool {
	if len(ba.Requests) != 1 {
		return false
	}
	_, ok := ba.GetArg(ChangeFrozen)
	return ok
}

// IsLease returns whether the batch consists of a single RequestLease request.
func (ba *BatchRequest) IsLease() bool {
	if len(ba.Requests) != 1 {
		return false
	}
	_, ok := ba.GetArg(RequestLease)
	return ok
}

// IsAdmin returns true iff the BatchRequest contains an admin request.
func (ba *BatchRequest) IsAdmin() bool {
	return ba.hasFlag(isAdmin)
}

// IsWrite returns true iff the BatchRequest contains a write.
func (ba *BatchRequest) IsWrite() bool {
	return ba.hasFlag(isWrite)
}

// IsReadOnly returns true if all requests within are read-only.
func (ba *BatchRequest) IsReadOnly() bool {
	return len(ba.Requests) > 0 && !ba.hasFlag(isWrite|isAdmin)
}

// IsReverse returns true iff the BatchRequest contains a reverse request.
func (ba *BatchRequest) IsReverse() bool {
	return ba.hasFlag(isReverse)
}

// IsPossibleTransaction returns true iff the BatchRequest contains
// requests that can be part of a transaction.
func (ba *BatchRequest) IsPossibleTransaction() bool {
	return ba.hasFlag(isTxn)
}

// IsTransactionWrite returns true iff the BatchRequest contains a txn write.
func (ba *BatchRequest) IsTransactionWrite() bool {
	return ba.hasFlag(isTxnWrite)
}

// hasFlag returns true iff one of the requests within the batch contains the
// specified flag.
func (ba *BatchRequest) hasFlag(flag int) bool {
	for _, union := range ba.Requests {
		if (union.GetInner().flags() & flag) != 0 {
			return true
		}
	}
	return false
}

// GetArg returns a request of the given type if one is contained in the
// Batch. The request returned is the first of its kind, with the exception
// of EndTransaction, where it examines the very last request only.
func (ba *BatchRequest) GetArg(method Method) (Request, bool) {
	// when looking for EndTransaction, just look at the last entry.
	if method == EndTransaction {
		if length := len(ba.Requests); length > 0 {
			if req := ba.Requests[length-1].GetInner(); req.Method() == EndTransaction {
				return req, true
			}
		}
		return nil, false
	}

	for _, arg := range ba.Requests {
		if req := arg.GetInner(); req.Method() == method {
			return req, true
		}
	}
	return nil, false
}

func (br *BatchResponse) String() string {
	var str []string
	for _, union := range br.Responses {
		str = append(str, fmt.Sprintf("%T", union.GetInner()))
	}
	return strings.Join(str, ", ")
}

// IntentSpanIterate calls the passed method with the key ranges of the
// transactional writes contained in the batch.
func (ba *BatchRequest) IntentSpanIterate(fn func(key, endKey Key)) {
	for _, arg := range ba.Requests {
		req := arg.GetInner()
		if !IsTransactionWrite(req) {
			continue
		}
		h := req.Header()
		fn(h.Key, h.EndKey)
	}
}

// Combine implements the Combinable interface. It combines each slot of the
// given request into the corresponding slot of the base response. The number
// of slots must be equal and the respective slots must be combinable.
// On error, the receiver BatchResponse is in an invalid state.
// TODO(tschottdorf): write tests.
func (br *BatchResponse) Combine(otherBatch *BatchResponse) error {
	if len(otherBatch.Responses) != len(br.Responses) {
		return errors.New("unable to combine batch responses of different length")
	}
	for i, l := 0, len(br.Responses); i < l; i++ {
		valLeft := br.Responses[i].GetInner()
		valRight := otherBatch.Responses[i].GetInner()
		cValLeft, lOK := valLeft.(combinable)
		cValRight, rOK := valRight.(combinable)
		if lOK && rOK {
			if err := cValLeft.combine(cValRight); err != nil {
				return err
			}
			continue
		}
		// If our slot is a NoopResponse, then whatever the other batch has is
		// the result. Note that the result can still be a NoopResponse, to be
		// filled in by a future Combine().
		if _, ok := valLeft.(*NoopResponse); ok {
			br.Responses[i] = otherBatch.Responses[i]
		}
	}
	br.Txn.Update(otherBatch.Txn)
	br.CollectedSpans = append(br.CollectedSpans, otherBatch.CollectedSpans...)
	return nil
}

// Add adds a request to the batch request. It's a convenience method;
// requests may also be added directly into the slice.
func (ba *BatchRequest) Add(requests ...Request) {
	for _, args := range requests {
		ba.Requests = append(ba.Requests, RequestUnion{})
		ba.Requests[len(ba.Requests)-1].MustSetInner(args)
	}
}

// Add adds a response to the batch response. It's a convenience method;
// responses may also be added directly.
func (br *BatchResponse) Add(reply Response) {
	br.Responses = append(br.Responses, ResponseUnion{})
	br.Responses[len(br.Responses)-1].MustSetInner(reply)
}

// Methods returns a slice of the contained methods.
func (ba *BatchRequest) Methods() []Method {
	var res []Method
	for _, arg := range ba.Requests {
		res = append(res, arg.GetInner().Method())
	}
	return res
}

// CreateReply creates replies for each of the contained requests, wrapped in a
// BatchResponse. The response objects are batch allocated to minimize
// allocation overhead which adds a bit of complexity to this function.
func (ba *BatchRequest) CreateReply() *BatchResponse {
	br := &BatchResponse{}
	br.Responses = make([]ResponseUnion, len(ba.Requests))

	var counts struct {
		get                int
		put                int
		conditionalPut     int
		initPut            int
		increment          int
		delete             int
		deleteRange        int
		scan               int
		beginTransaction   int
		endTransaction     int
		adminSplit         int
		adminMerge         int
		heartbeatTxn       int
		gc                 int
		pushTxn            int
		rangeLookup        int
		resolveIntent      int
		resolveIntentRange int
		merge              int
		truncateLog        int
		lease              int
		leaseTransfer      int
		reverseScan        int
		computeChecksum    int
		verifyChecksum     int
		checkConsistency   int
		noop               int
		changeFrozen       int
	}
	for _, union := range ba.Requests {
		switch union.GetInner().(type) {
		case *GetRequest:
			counts.get++
		case *PutRequest:
			counts.put++
		case *ConditionalPutRequest:
			counts.conditionalPut++
		case *InitPutRequest:
			counts.initPut++
		case *IncrementRequest:
			counts.increment++
		case *DeleteRequest:
			counts.delete++
		case *DeleteRangeRequest:
			counts.deleteRange++
		case *ScanRequest:
			counts.scan++
		case *BeginTransactionRequest:
			counts.beginTransaction++
		case *EndTransactionRequest:
			counts.endTransaction++
		case *AdminSplitRequest:
			counts.adminSplit++
		case *AdminMergeRequest:
			counts.adminMerge++
		case *HeartbeatTxnRequest:
			counts.heartbeatTxn++
		case *GCRequest:
			counts.gc++
		case *PushTxnRequest:
			counts.pushTxn++
		case *RangeLookupRequest:
			counts.rangeLookup++
		case *ResolveIntentRequest:
			counts.resolveIntent++
		case *ResolveIntentRangeRequest:
			counts.resolveIntentRange++
		case *MergeRequest:
			counts.merge++
		case *TruncateLogRequest:
			counts.truncateLog++
		case *RequestLeaseRequest:
			counts.lease++
		case *TransferLeaseRequest:
			counts.leaseTransfer++
		case *ReverseScanRequest:
			counts.reverseScan++
		case *ComputeChecksumRequest:
			counts.computeChecksum++
		case *VerifyChecksumRequest:
			counts.verifyChecksum++
		case *CheckConsistencyRequest:
			counts.checkConsistency++
		case *NoopRequest:
			counts.noop++
		case *ChangeFrozenRequest:
			counts.changeFrozen++
		default:
			panic(fmt.Sprintf("unsupported type %T", union.GetInner()))
		}
	}

	var bufs struct {
		get                []GetResponse
		put                []PutResponse
		conditionalPut     []ConditionalPutResponse
		initPut            []InitPutResponse
		increment          []IncrementResponse
		delete             []DeleteResponse
		deleteRange        []DeleteRangeResponse
		scan               []ScanResponse
		beginTransaction   []BeginTransactionResponse
		endTransaction     []EndTransactionResponse
		adminSplit         []AdminSplitResponse
		adminMerge         []AdminMergeResponse
		heartbeatTxn       []HeartbeatTxnResponse
		gc                 []GCResponse
		pushTxn            []PushTxnResponse
		rangeLookup        []RangeLookupResponse
		resolveIntent      []ResolveIntentResponse
		resolveIntentRange []ResolveIntentRangeResponse
		merge              []MergeResponse
		truncateLog        []TruncateLogResponse
		lease              []RequestLeaseResponse
		leaseTransfer      []RequestLeaseResponse
		reverseScan        []ReverseScanResponse
		computeChecksum    []ComputeChecksumResponse
		verifyChecksum     []VerifyChecksumResponse
		checkConsistency   []CheckConsistencyResponse
		noop               []NoopResponse
		changeFrozen       []ChangeFrozenResponse
	}
	for i, union := range ba.Requests {
		var reply Response
		switch union.GetInner().(type) {
		case *GetRequest:
			if bufs.get == nil {
				bufs.get = make([]GetResponse, counts.get)
			}
			reply, bufs.get = &bufs.get[0], bufs.get[1:]
		case *PutRequest:
			if bufs.put == nil {
				bufs.put = make([]PutResponse, counts.put)
			}
			reply, bufs.put = &bufs.put[0], bufs.put[1:]
		case *ConditionalPutRequest:
			if bufs.conditionalPut == nil {
				bufs.conditionalPut = make([]ConditionalPutResponse, counts.conditionalPut)
			}
			reply, bufs.conditionalPut = &bufs.conditionalPut[0], bufs.conditionalPut[1:]
		case *InitPutRequest:
			if bufs.initPut == nil {
				bufs.initPut = make([]InitPutResponse, counts.initPut)
			}
			reply, bufs.initPut = &bufs.initPut[0], bufs.initPut[1:]
		case *IncrementRequest:
			if bufs.increment == nil {
				bufs.increment = make([]IncrementResponse, counts.increment)
			}
			reply, bufs.increment = &bufs.increment[0], bufs.increment[1:]
		case *DeleteRequest:
			if bufs.delete == nil {
				bufs.delete = make([]DeleteResponse, counts.delete)
			}
			reply, bufs.delete = &bufs.delete[0], bufs.delete[1:]
		case *DeleteRangeRequest:
			if bufs.deleteRange == nil {
				bufs.deleteRange = make([]DeleteRangeResponse, counts.deleteRange)
			}
			reply, bufs.deleteRange = &bufs.deleteRange[0], bufs.deleteRange[1:]
		case *ScanRequest:
			if bufs.scan == nil {
				bufs.scan = make([]ScanResponse, counts.scan)
			}
			reply, bufs.scan = &bufs.scan[0], bufs.scan[1:]
		case *BeginTransactionRequest:
			if bufs.beginTransaction == nil {
				bufs.beginTransaction = make([]BeginTransactionResponse, counts.beginTransaction)
			}
			reply, bufs.beginTransaction = &bufs.beginTransaction[0], bufs.beginTransaction[1:]
		case *EndTransactionRequest:
			if bufs.endTransaction == nil {
				bufs.endTransaction = make([]EndTransactionResponse, counts.endTransaction)
			}
			reply, bufs.endTransaction = &bufs.endTransaction[0], bufs.endTransaction[1:]
		case *AdminSplitRequest:
			if bufs.adminSplit == nil {
				bufs.adminSplit = make([]AdminSplitResponse, counts.adminSplit)
			}
			reply, bufs.adminSplit = &bufs.adminSplit[0], bufs.adminSplit[1:]
		case *AdminMergeRequest:
			if bufs.adminMerge == nil {
				bufs.adminMerge = make([]AdminMergeResponse, counts.adminMerge)
			}
			reply, bufs.adminMerge = &bufs.adminMerge[0], bufs.adminMerge[1:]
		case *HeartbeatTxnRequest:
			if bufs.heartbeatTxn == nil {
				bufs.heartbeatTxn = make([]HeartbeatTxnResponse, counts.heartbeatTxn)
			}
			reply, bufs.heartbeatTxn = &bufs.heartbeatTxn[0], bufs.heartbeatTxn[1:]
		case *GCRequest:
			if bufs.gc == nil {
				bufs.gc = make([]GCResponse, counts.gc)
			}
			reply, bufs.gc = &bufs.gc[0], bufs.gc[1:]
		case *PushTxnRequest:
			if bufs.pushTxn == nil {
				bufs.pushTxn = make([]PushTxnResponse, counts.pushTxn)
			}
			reply, bufs.pushTxn = &bufs.pushTxn[0], bufs.pushTxn[1:]
		case *RangeLookupRequest:
			if bufs.rangeLookup == nil {
				bufs.rangeLookup = make([]RangeLookupResponse, counts.rangeLookup)
			}
			reply, bufs.rangeLookup = &bufs.rangeLookup[0], bufs.rangeLookup[1:]
		case *ResolveIntentRequest:
			if bufs.resolveIntent == nil {
				bufs.resolveIntent = make([]ResolveIntentResponse, counts.resolveIntent)
			}
			reply, bufs.resolveIntent = &bufs.resolveIntent[0], bufs.resolveIntent[1:]
		case *ResolveIntentRangeRequest:
			if bufs.resolveIntentRange == nil {
				bufs.resolveIntentRange = make([]ResolveIntentRangeResponse, counts.resolveIntentRange)
			}
			reply, bufs.resolveIntentRange = &bufs.resolveIntentRange[0], bufs.resolveIntentRange[1:]
		case *MergeRequest:
			if bufs.merge == nil {
				bufs.merge = make([]MergeResponse, counts.merge)
			}
			reply, bufs.merge = &bufs.merge[0], bufs.merge[1:]
		case *TruncateLogRequest:
			if bufs.truncateLog == nil {
				bufs.truncateLog = make([]TruncateLogResponse, counts.truncateLog)
			}
			reply, bufs.truncateLog = &bufs.truncateLog[0], bufs.truncateLog[1:]
		case *RequestLeaseRequest:
			if bufs.lease == nil {
				bufs.lease = make([]RequestLeaseResponse, counts.lease)
			}
			reply, bufs.lease = &bufs.lease[0], bufs.lease[1:]
		case *TransferLeaseRequest:
			if bufs.leaseTransfer == nil {
				bufs.leaseTransfer = make([]RequestLeaseResponse, counts.leaseTransfer)
			}
			reply, bufs.leaseTransfer = &bufs.leaseTransfer[0], bufs.leaseTransfer[1:]
		case *ReverseScanRequest:
			if bufs.reverseScan == nil {
				bufs.reverseScan = make([]ReverseScanResponse, counts.reverseScan)
			}
			reply, bufs.reverseScan = &bufs.reverseScan[0], bufs.reverseScan[1:]
		case *ComputeChecksumRequest:
			if bufs.computeChecksum == nil {
				bufs.computeChecksum = make([]ComputeChecksumResponse, counts.computeChecksum)
			}
			reply, bufs.computeChecksum = &bufs.computeChecksum[0], bufs.computeChecksum[1:]
		case *VerifyChecksumRequest:
			if bufs.verifyChecksum == nil {
				bufs.verifyChecksum = make([]VerifyChecksumResponse, counts.verifyChecksum)
			}
			reply, bufs.verifyChecksum = &bufs.verifyChecksum[0], bufs.verifyChecksum[1:]
		case *CheckConsistencyRequest:
			if bufs.checkConsistency == nil {
				bufs.checkConsistency = make([]CheckConsistencyResponse, counts.checkConsistency)
			}
			reply, bufs.checkConsistency = &bufs.checkConsistency[0], bufs.checkConsistency[1:]
		case *NoopRequest:
			if bufs.noop == nil {
				bufs.noop = make([]NoopResponse, counts.noop)
			}
			reply, bufs.noop = &bufs.noop[0], bufs.noop[1:]
		case *ChangeFrozenRequest:
			if bufs.changeFrozen == nil {
				bufs.changeFrozen = make([]ChangeFrozenResponse, counts.changeFrozen)
			}
			reply, bufs.changeFrozen = &bufs.changeFrozen[0], bufs.changeFrozen[1:]
		default:
			panic(fmt.Sprintf("unsupported type %T", union.GetInner()))
		}
		br.Responses[i].MustSetInner(reply)
	}
	return br
}

// Split separates the requests contained in a batch so that each subset of
// requests can be executed by a Store (without changing order). In particular,
// Admin requests are always singled out and mutating requests separated from
// reads. The boolean parameter indicates whether EndTransaction should be
// special-cased: If false, an EndTransaction request will never be split into
// a new chunk (otherwise, it is treated according to its flags). This allows
// sending a whole transaction in a single Batch when addressing a single
// range.
func (ba BatchRequest) Split(canSplitET bool) [][]RequestUnion {
	compatible := func(method Method, exFlags, newFlags int) bool {
		// If no flags are set so far, everything goes.
		if exFlags == 0 || (!canSplitET && method == EndTransaction) {
			return true
		}
		if (newFlags & isAlone) != 0 {
			return false
		}
		// Otherwise, the flags below must remain the same with the new
		// request added.
		//
		// Note that we're not checking isRead: The invariants we're
		// enforcing are that a batch can't mix non-writes with writes.
		// Checking isRead would cause ConditionalPut and Put to conflict,
		// which is not what we want.
		const mask = isWrite | isAdmin | isReverse
		return (mask & exFlags) == (mask & newFlags)
	}
	var parts [][]RequestUnion
	for len(ba.Requests) > 0 {
		part := ba.Requests
		var gFlags int
		for i, union := range ba.Requests {
			args := union.GetInner()
			flags := args.flags()
			method := args.Method()
			// Regardless of flags, a NoopRequest is always compatible.
			if method == Noop {
				continue
			}
			if !compatible(method, gFlags, flags) {
				part = ba.Requests[:i]
				break
			}
			gFlags |= flags
		}
		parts = append(parts, part)
		ba.Requests = ba.Requests[len(part):]
	}
	return parts
}

// String gives a brief summary of the contained requests and keys in the batch.
// TODO(tschottdorf): the key range is useful information, but requires `keys`.
// See #2198.
func (ba BatchRequest) String() string {
	var str []string
	for count, arg := range ba.Requests {
		// Limit the strings to provide just a summary. Without this limit
		// a log message with a BatchRequest can be very long.
		if count >= 20 && count < len(ba.Requests)-5 {
			if count == 20 {
				str = append(str, fmt.Sprintf("... %d skipped ...", len(ba.Requests)-25))
			}
			continue
		}
		req := arg.GetInner()
		h := req.Header()
		str = append(str, fmt.Sprintf("%s [%s,%s)", req.Method(), h.Key, h.EndKey))
	}
	return strings.Join(str, ", ")
}

// TODO(marc): we should assert
// var _ security.RequestWithUser = &BatchRequest{}
// here, but we need to break cycles first.

// GetUser implements security.RequestWithUser.
// KV messages are always sent by the node user.
func (*BatchRequest) GetUser() string {
	// TODO(marc): we should use security.NodeUser here, but we need to break cycles first.
	return "node"
}

// SetNewRequest increases the internal sequence counter of this batch request.
// The sequence counter is used for replay and reordering protection. At the
// Store, a sequence counter less than or equal to the last observed one incurs
// a transaction restart (if the request is transactional).
func (ba *BatchRequest) SetNewRequest() {
	if ba.Txn == nil {
		return
	}
	ba.Txn.Sequence++
}
