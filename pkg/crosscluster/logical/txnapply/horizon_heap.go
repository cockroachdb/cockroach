// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// horizonWaiter pairs a TxnID with a horizon timestamp for use in the
// horizonHeap min-heap. The txnID field carries context that varies by
// caller:
//   - In the Applier, it is the full TxnID of the waiting transaction.
//   - In the dependency resolver, it is a synthesized
//     TxnID{ApplierID: waitingID} so the caller can identify which
//     applier to notify.
type horizonWaiter struct {
	txnID   ldrdecoder.TxnID
	horizon hlc.Timestamp
}

// horizonHeap is a min-heap of horizonWaiters ordered by horizon
// timestamp. It implements the generic heap.Interface[horizonWaiter]
// from pkg/util/container/heap.
type horizonHeap []horizonWaiter

func (h horizonHeap) Len() int              { return len(h) }
func (h horizonHeap) Less(i, j int) bool    { return h[i].horizon.Less(h[j].horizon) }
func (h horizonHeap) Swap(i, j int)         { h[i], h[j] = h[j], h[i] }
func (h *horizonHeap) Push(x horizonWaiter) { *h = append(*h, x) }

func (h *horizonHeap) Pop() horizonWaiter {
	n := len(*h)
	item := (*h)[n-1]
	(*h)[n-1] = horizonWaiter{} // avoid retaining references in the underlying array
	*h = (*h)[:n-1]
	return item
}

// peek returns the minimum element without removing it from the heap.
// Returns a zero-valued horizonWaiter if the heap is empty.
func (h *horizonHeap) peek() horizonWaiter {
	if len(*h) == 0 {
		return horizonWaiter{}
	}
	return (*h)[0]
}
