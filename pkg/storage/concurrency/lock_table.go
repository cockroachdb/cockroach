// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"container/list"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/google/btree"
)

// lockTableImpl implements the lockTable interface.
//
// WIP: this is not a real implementation. It's just to get things working.
// Sumeer is working on a better version that can actually detect deadlocks.
type lockTableImpl struct {
	mu         syncutil.Mutex
	qs         *btree.BTree
	tmp1, tmp2 perKeyWaitQueue
}

type perKeyWaitQueue struct {
	in   roachpb.Intent
	held bool
	ll   list.List // List<*perKeyWaitQueueElem>
}

type perKeyWaitQueueElem struct {
	wq     *perKeyWaitQueue
	req    Request
	ref    uint16
	done   chan struct{}
	closed bool
	waited bool

	// WIP: remove the need for this
	elem *list.Element
}

// Less implements the btree.Item interface.
func (a *perKeyWaitQueue) Less(b btree.Item) bool {
	return a.in.Key.Compare(b.(*perKeyWaitQueue).in.Key) < 0
}

func (lt *lockTableImpl) AcquireLock(in roachpb.Intent) {
	if in.EndKey != nil {
		panic("must acquire point locks")
	}

	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.tmp1.in = in
	wqI := lt.qs.Get(&lt.tmp1)
	var wq *perKeyWaitQueue
	if wqI == nil {
		// TODO(nvanbenschoten): memory recycling.
		wq = new(perKeyWaitQueue)
		wq.in = in
		wq.held = true
		wq.ll.Init()
		lt.qs.ReplaceOrInsert(wq)
	} else {
		wq = wqI.(*perKeyWaitQueue)
		wq.in.Txn = in.Txn
		wq.held = true
	}
}

func (lt *lockTableImpl) ReleaseLock(in roachpb.Intent) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.tmp1.in = in
	wqI := lt.qs.Get(&lt.tmp1)
	if wqI == nil {
		return
	}
	wq := wqI.(*perKeyWaitQueue)
	if !wq.held || wq.in.Txn.ID != in.Txn.ID {
		return
	}
	if wq.ll.Len() == 0 {
		lt.qs.Delete(wq)
		return
	}
	wq.in.Txn = enginepb.TxnMeta{}
	wq.held = false
	front := wq.ll.Front().Value.(*perKeyWaitQueueElem)
	if !front.closed {
		close(front.done)
		front.closed = true
	}
}

func (lt *lockTableImpl) AddDiscoveredLock(req Request, in roachpb.Intent) lockWaitQueueGuard {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.tmp1.in = in
	wqI := lt.qs.Get(&lt.tmp1)
	var wq *perKeyWaitQueue
	if wqI == nil {
		// TODO(nvanbenschoten): memory recycling.
		wq = new(perKeyWaitQueue)
		wq.in = in
		wq.held = true
		wq.ll.Init()
		lt.qs.ReplaceOrInsert(wq)
	} else {
		wq = wqI.(*perKeyWaitQueue)
		wq.in.Txn = in.Txn
		wq.held = true
	}

	if req.isReadOnly() {
		for e := wq.ll.Front(); e != nil; e = e.Next() {
			elem := e.Value.(*perKeyWaitQueueElem)
			if elem.req.isReadOnly() {
				elem.ref++
				return elem
			}
		}
	}

	// TODO(nvanbenschoten): memory recycling.
	elem := &perKeyWaitQueueElem{
		wq:   wq,
		req:  req,
		ref:  1,
		done: make(chan struct{}),
	}
	elem.elem = wq.ll.PushBack(elem)
	return elem
}

func (lt *lockTableImpl) ScanAndEnqueue(req Request, g *Guard) []lockWaitQueueGuard {
	var elems []lockWaitQueueGuard
	readOnly := req.isReadOnly()

	lt.mu.Lock()
	defer lt.mu.Unlock()
	for s := spanset.SpanScope(0); s < spanset.NumSpanScope; s++ {
		for a := spanset.SpanAccess(0); a < spanset.NumSpanAccess; a++ {
			ss := req.Spans.GetSpans(a, s)
			for _, span := range ss {
				shouldEnqueue := func(lock *enginepb.TxnMeta, held bool) bool {
					if req.Txn != nil && req.Txn.ID == lock.ID {
						return false
					}
					switch a {
					case spanset.SpanReadOnly:
						return held && !span.Timestamp.Less(lock.WriteTimestamp)
					case spanset.SpanReadWrite:
						return true
					default:
						panic("unknown access")
					}
				}
				maybeEnqueue := func(i btree.Item) bool {
					wq := i.(*perKeyWaitQueue)
					if !shouldEnqueue(&wq.in.Txn, wq.held) {
						return true
					}
					// WIP: this is awful.
					for _, ex := range g.wqgs {
						if ex.(*perKeyWaitQueueElem).wq == wq {
							return true
						}
					}
					// WIP: this is more awful.
					if readOnly {
						for e := wq.ll.Front(); e != nil; e = e.Next() {
							elem := e.Value.(*perKeyWaitQueueElem)
							if elem.req.isReadOnly() {
								elem.ref++
								elems = append(elems, elem)
								return true
							}
						}
					}
					elem := &perKeyWaitQueueElem{
						wq:   wq,
						req:  req,
						ref:  1,
						done: make(chan struct{}),
					}
					elem.elem = wq.ll.PushBack(elem)
					elems = append(elems, elem)
					return true
				}

				lt.tmp1.in.Key = span.Key
				if span.EndKey == nil {
					if i := lt.qs.Get(&lt.tmp1); i != nil {
						maybeEnqueue(i)
					}
				} else {
					lt.tmp2.in.Key = span.EndKey
					lt.qs.AscendRange(&lt.tmp1, &lt.tmp2, maybeEnqueue)
				}
			}
		}
	}
	return elems
}

func (lt *lockTableImpl) Dequeue(wqg lockWaitQueueGuard) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	elem := wqg.(*perKeyWaitQueueElem)
	elem.ref--
	if elem.ref > 0 {
		return
	}
	wq := elem.wq
	front := wq.ll.Front() == elem.elem
	wq.ll.Remove(elem.elem)
	if !wq.held {
		if wq.ll.Len() == 0 {
			lt.qs.Delete(wq)
		} else if front {
			newFront := wq.ll.Front().Value.(*perKeyWaitQueueElem)
			if !newFront.closed {
				close(newFront.done)
				newFront.closed = true
			}
		}
	}
}

func (lt *lockTableImpl) SetBounds(start, end roachpb.RKey) {
	// No-op, for now.
}

func (lt *lockTableImpl) Clear() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.qs.Ascend(func(i btree.Item) bool {
		wq := i.(*perKeyWaitQueue)
		for e := wq.ll.Front(); e != nil; e = e.Next() {
			elem := e.Value.(*perKeyWaitQueueElem)
			if !elem.closed {
				close(elem.done)
				elem.closed = true
			}
		}
		return true
	})
	lt.qs.Clear(false)
}

// lockWaitQueueWaiterImpl implements the lockWaitQueueWaiter interface.
type lockWaitQueueWaiterImpl struct{}

func (wq *lockWaitQueueWaiterImpl) MustWaitOnAny(wqgs []lockWaitQueueGuard) bool {
	// WIP: iterate in reverse.
	for _, wqg := range wqgs {
		if !wqg.(*perKeyWaitQueueElem).waited {
			return true
		}
	}
	return false
}

func (wq *lockWaitQueueWaiterImpl) WaitOn(
	ctx context.Context, req Request, wqg lockWaitQueueGuard,
) *Error {
	elem := wqg.(*perKeyWaitQueueElem)
	if elem.waited {
		return nil
	}
	select {
	case <-elem.done:
		elem.waited = true
		return nil
	case <-ctx.Done():
		return roachpb.NewError(ctx.Err())
	}
}

func (wq *lockWaitQueueWaiterImpl) SetBounds(start, end roachpb.RKey) {
	// No-op, for now.
}

// // contentionQueuelockWaitQueueWaiterImpl implements the lockWaitQueueWaiter interface.
// type contentionQueuelockWaitQueueWaiterImpl struct {
// 	c  *hlc.Clock
// 	ir *intentresolver.IntentResolver
// }

// func (wq *contentionQueuelockWaitQueueWaiterImpl) waitOn(
// 	ctx context.Context, req Request, wqg lockWaitQueueGuard,
// ) *Error {
// 	pkwq := wqg.(perKeyWaitQueue)

// 	// WIP: Just to get it working.
// 	wiErr := roachpb.NewError(&roachpb.WriteIntentError{Intents: []roachpb.Intent{pkwq.in}})

// 	h := roachpb.Header{
// 		Timestamp:    req.Timestamp,
// 		UserPriority: req.Priority,
// 	}
// 	if req.Txn != nil {
// 		// We must push at least to req.Timestamp, but in fact we want to
// 		// go all the way up to a timestamp which was taken off the HLC
// 		// after our operation started. This allows us to not have to
// 		// restart for uncertainty as we come back and read.
// 		h.Timestamp = wq.c.Now()
// 		// We are going to hand the header (and thus the transaction proto)
// 		// to the RPC framework, after which it must not be changed (since
// 		// that could race). Since the subsequent execution of the original
// 		// request might mutate the transaction, make a copy here.
// 		//
// 		// See #9130.
// 		h.Txn = req.Txn.Clone()
// 	}

// 	var pushType roachpb.PushTxnType
// 	if req.Spans.Contains(spanset.SpanReadWrite) {
// 		pushType = roachpb.PUSH_ABORT
// 	} else {
// 		pushType = roachpb.PUSH_TIMESTAMP
// 	}

// 	var err *Error
// 	if _, err = wq.ir.ProcessWriteIntentError(ctx, wiErr, h, pushType); err != nil {
// 		// Do not propagate ambiguous results; assume success and retry original op.
// 		if _, ok := err.GetDetail().(*roachpb.AmbiguousResultError); !ok {
// 			return err
// 		}
// 	}
// 	return nil
// }
