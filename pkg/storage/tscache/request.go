// Copyright 2017 The Cockroach Authors.
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

package tscache

import (
	"sync"

	"github.com/google/btree"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Request holds the timestamp cache data from a single batch request. The
// requests are stored in a btree keyed by the timestamp and are "expanded" to
// populate the read/write interval caches if a potential conflict is detected
// due to an earlier request (based on timestamp) arriving.
type Request struct {
	Span      roachpb.RSpan
	Reads     []roachpb.Span
	Writes    []roachpb.Span
	Txn       roachpb.Span
	TxnID     uuid.UUID
	Timestamp hlc.Timestamp
	// Used to distinguish requests with identical timestamps. For actual
	// requests, the uniqueID value is >0. When probing the btree for requests
	// later than a particular timestamp a value of 0 is used.
	uniqueID int64
}

// Less implements the btree.Item interface.
func (cr *Request) Less(other btree.Item) bool {
	otherReq := other.(*Request)
	if cr.Timestamp.Less(otherReq.Timestamp) {
		return true
	}
	if otherReq.Timestamp.Less(cr.Timestamp) {
		return false
	}
	// Fallback to comparison of the uniqueID as a tie-breaker. This allows
	// multiple requests with the same timestamp to exist in the requests btree.
	return cr.uniqueID < otherReq.uniqueID
}

// numSpans returns the number of spans the request will expand into.
func (cr *Request) numSpans() int {
	n := len(cr.Reads) + len(cr.Writes)
	if cr.Txn.Key != nil {
		n++
	}
	return n
}

func (cr *Request) size() uint64 {
	var n uint64
	for i := range cr.Reads {
		s := &cr.Reads[i]
		n += cacheEntrySize(interval.Comparable(s.Key), interval.Comparable(s.EndKey))
	}
	for i := range cr.Writes {
		s := &cr.Writes[i]
		n += cacheEntrySize(interval.Comparable(s.Key), interval.Comparable(s.EndKey))
	}
	if cr.Txn.Key != nil {
		n += cacheEntrySize(interval.Comparable(cr.Txn.Key), nil)
	}
	return n
}

func (cr *Request) release() {
	// We want to keep the Request.Reads backing array co-located with the
	// Request (see requestAlloc). If they have split because cr needed more
	// than requestAllocSpans read spans, throw out the entire Request instead
	// of putting it back in the pool. Before doing so though, make sure we
	// didn't assign the backing array to the Writes slice.
	if cap(cr.Reads) > requestAllocSpans {
		if cap(cr.Writes) > requestAllocSpans {
			// Throw away.
			return
		}
		cr.Reads = cr.Writes
	}
	for i := range cr.Reads {
		cr.Reads[i] = roachpb.Span{}
	}
	*cr = Request{
		Reads: cr.Reads[:0],
	}
	requestPool.Put(cr)
}

const requestAllocSpans = 2

type requestAlloc struct {
	cr    Request
	spans [requestAllocSpans]roachpb.Span
}

var requestPool = sync.Pool{
	New: func() interface{} {
		crAlloc := new(requestAlloc)
		cr := &crAlloc.cr
		cr.Reads = crAlloc.spans[:0]
		return cr
	},
}

// NewRequest returns a new Request object.
func NewRequest() *Request {
	return requestPool.Get().(*Request)
}
