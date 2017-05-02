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
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

//go:generate go run -tags gen-batch gen_batch.go

// SetActiveTimestamp sets the correct timestamp at which the request is to be
// carried out. For transactional requests, ba.Timestamp must be zero initially
// and it will be set to txn.OrigTimestamp. For non-transactional requests, if
// no timestamp is specified, nowFn is used to create and set one.
func (ba *BatchRequest) SetActiveTimestamp(nowFn func() hlc.Timestamp) error {
	if txn := ba.Txn; txn != nil {
		if ba.Timestamp != (hlc.Timestamp{}) {
			return errors.New("transactional request must not set batch timestamp")
		}

		// Always use the original timestamp for reads and writes, even
		// though some intents may be written at higher timestamps in the
		// event of a WriteTooOldError.
		ba.Timestamp = txn.OrigTimestamp
	} else {
		// When not transactional, allow empty timestamp and use nowFn instead
		if ba.Timestamp == (hlc.Timestamp{}) {
			ba.Timestamp = nowFn()
		}
	}
	return nil
}

// UpdateTxn updates the batch transaction from the supplied one in
// a copy-on-write fashion, i.e. without mutating an existing
// Transaction struct.
func (ba *BatchRequest) UpdateTxn(otherTxn *Transaction) {
	if otherTxn == nil {
		return
	}
	if ba.Txn == nil {
		ba.Txn = otherTxn
		return
	}
	clonedTxn := ba.Txn.Clone()
	clonedTxn.Update(otherTxn)
	ba.Txn = &clonedTxn
}

// IsLeaseRequest returns whether the batch consists of a single RequestLease
// request. Note that TransferLease requests return false.
// RequestLease requests are special because they're the only type of requests a
// non-lease-holder can propose.
func (ba *BatchRequest) IsLeaseRequest() bool {
	if !ba.IsSingleRequest() {
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

// IsSingleRequest returns true iff the BatchRequest contains a single request.
func (ba *BatchRequest) IsSingleRequest() bool {
	return len(ba.Requests) == 1
}

// IsSingleSkipLeaseCheckRequest returns true iff the batch contains a single
// request, and that request has the skipLeaseCheck flag set.
func (ba *BatchRequest) IsSingleSkipLeaseCheckRequest() bool {
	return ba.IsSingleRequest() && ba.hasFlag(skipLeaseCheck)
}

// IsSinglePushTxnRequest returns true iff the batch contains a single
// request, and that request is for a PushTxn.
func (ba *BatchRequest) IsSinglePushTxnRequest() bool {
	if ba.IsSingleRequest() {
		_, ok := ba.Requests[0].GetInner().(*PushTxnRequest)
		return ok
	}
	return false
}

// GetPrevLeaseForLeaseRequest returns the previous lease, at the time
// of proposal, for a request lease or transfer lease request. If the
// batch does not contain a single lease request, this method will panic.
func (ba *BatchRequest) GetPrevLeaseForLeaseRequest() *Lease {
	return ba.Requests[0].GetInner().(leaseRequestor).prevLease()
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
	str = append(str, fmt.Sprintf("(err: %v)", br.Error))
	for _, union := range br.Responses {
		str = append(str, fmt.Sprintf("%T", union.GetInner()))
	}
	return strings.Join(str, ", ")
}

// IntentSpanIterate calls the passed method with the key ranges of the
// transactional writes contained in the batch. Usually the key spans
// contained in the requests are used, but when a response contains a
// ResumeSpan the ResumeSpan is subtracted from the request span to provide a
// more minimal span of keys affected by the request.
func (ba *BatchRequest) IntentSpanIterate(br *BatchResponse, fn func(key, endKey Key)) {
	for i, arg := range ba.Requests {
		req := arg.GetInner()
		if !IsTransactionWrite(req) {
			continue
		}
		h := req.Header()
		if br != nil {
			resumeSpan := br.Responses[i].GetInner().Header().ResumeSpan
			// If a resume span exists we need to cull the span.
			if resumeSpan != nil {
				if bytes.Equal(resumeSpan.Key, h.Key) {
					if bytes.Equal(resumeSpan.EndKey, h.EndKey) {
						// Nothing was written.
						continue
					}
					fn(resumeSpan.EndKey, h.EndKey)
				} else {
					fn(h.Key, resumeSpan.Key)
				}
				continue
			}
		}
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
	if ba.Txn != nil {
		str = append(str, fmt.Sprintf("[txn: %s]", ba.Txn.Short()))
	}
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
		if _, ok := req.(*NoopRequest); ok {
			str = append(str, req.Method().String())
		} else {
			h := req.Header()
			str = append(str, fmt.Sprintf("%s [%s,%s)", req.Method(), h.Key, h.EndKey))
		}
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
	if ba.Txn != nil {
		txn := *ba.Txn
		txn.Sequence++
		ba.Txn = &txn
	}
}
