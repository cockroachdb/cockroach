// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool

import (
	"context"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

// IntPool manages allocating integer units of quota to clients.
// Clients may acquire quota in two ways, using Acquire which requires the
// client to specify the quantity of quota at call time and AcquireFunc which
// allows the client to provide a function which will be used to determine
// whether a quantity of quota is sufficient when it becomes available.
type IntPool struct {
	qp  *QuotaPool
	max int64

	intAllocSyncPool       sync.Pool
	intRequestSyncPool     sync.Pool
	intFuncRequestSyncPool sync.Pool
}

// IntAlloc is an allocated quantity which should be released.
type IntAlloc struct {
	alloc int64
	p     *IntPool
}

// Release releases an IntAlloc back into the IntPool.
func (ia *IntAlloc) Release() {
	ia.p.qp.Add((*intAlloc)(ia))
}

// Acquired returns the quantity that this alloc has acquired.
func (ia *IntAlloc) Acquired() int64 {
	return ia.alloc
}

// Merge adds the acquired resources in other to ia. Other may not be used after
// it has been merged. It is illegal to merge allocs from different pools and
// doing so will result in a panic.
func (ia *IntAlloc) Merge(other *IntAlloc) {
	if ia.p != other.p {
		panic("cannot merge IntAllocs from two different pools")
	}
	ia.alloc = min(ia.p.max, ia.alloc+other.alloc)
	ia.p.putIntAlloc(other)
}

// String formats an IntAlloc as a string.
func (ia *IntAlloc) String() string {
	if ia == nil {
		return strconv.Itoa(0)
	}
	return strconv.FormatInt(ia.alloc, 10)
}

// intAlloc is used to make IntAlloc implement Resource without muddling its
// exported interface.
type intAlloc IntAlloc

// Merge makes intAlloc a Resource.
func (ia *intAlloc) Merge(other Resource) {
	(*IntAlloc)(ia).Merge((*IntAlloc)(other.(*intAlloc)))
}

// NewIntPool creates a new named IntPool with a maximum quota value.
func NewIntPool(name string, max int64, options ...Option) *IntPool {
	p := IntPool{
		max: max,
		intAllocSyncPool: sync.Pool{
			New: func() interface{} { return new(IntAlloc) },
		},
		intRequestSyncPool: sync.Pool{
			New: func() interface{} { return new(intRequest) },
		},
		intFuncRequestSyncPool: sync.Pool{
			New: func() interface{} { return new(intFuncRequest) },
		},
	}
	p.qp = New(name, (*intAlloc)(p.newIntAlloc(max)), options...)
	return &p
}

// Acquire acquires the specified amount of quota from the pool. On success,
// nil is returned and the caller must call add(v) or otherwise arrange for the
// quota to be returned to the pool. If 'v' is greater than the total capacity
// of the pool, we instead try to acquire quota equal to the maximum capacity.
//
// Acquisitions of 0 return immediately with no error, even if the IntPool is
// closed.
//
// Safe for concurrent use.
func (p *IntPool) Acquire(ctx context.Context, v int64) (*IntAlloc, error) {
	// Special case acquisitions of size 0.
	if v < 0 {
		panic("cannot acquire negative quota")
	} else if v == 0 {
		return p.newIntAlloc(v), nil
	}
	r := p.newIntRequest(v)
	defer p.putIntRequest(r)
	if err := p.qp.Acquire(ctx, r); err != nil {
		return nil, err
	}
	return p.newIntAlloc(r.want), nil
}

// IntRequestFunc is used to request a quantity of quota determined when quota is
// available rather than before requesting.
type IntRequestFunc func(ctx context.Context, v int64) (fulfilled bool, took int64)

// AcquireFunc acquires a quantity of quota determined by a function which is
// called with a quantity of available quota.
func (p *IntPool) AcquireFunc(ctx context.Context, f IntRequestFunc) (*IntAlloc, error) {
	r := p.newIntFuncRequest(f)
	defer p.putIntFuncRequest(r)
	err := p.qp.Acquire(ctx, r)
	if err != nil {
		return nil, err
	}
	return p.newIntAlloc(r.took), nil
}

// ApproximateQuota will report approximately the amount of quota available in
// the pool. It is precise if there are no ongoing acquisitions. If there are,
// the return value can be up to 'v' less than actual available quota where 'v'
// is the value the acquisition goroutine first in line is attempting to
// acquire.
func (p *IntPool) ApproximateQuota() (q int64) {
	p.qp.ApproximateQuota(func(r Resource) {
		if ia, ok := r.(*intAlloc); ok {
			q = ia.alloc
		}
	})
	return q
}

// Close signals to all ongoing and subsequent acquisitions that the pool is
// closed and that an error should be returned.
//
// Safe for concurrent use.
func (p *IntPool) Close(reason string) {
	p.qp.Close(reason)
}

func (p *IntPool) newIntAlloc(v int64) *IntAlloc {
	ia := p.intAllocSyncPool.Get().(*IntAlloc)
	*ia = IntAlloc{p: p, alloc: v}
	return ia
}

func (p *IntPool) putIntAlloc(ia *IntAlloc) {
	p.intAllocSyncPool.Put(ia)
}

// newIntRequest allocates an intRequest from the sync.Pool.
// It should be returned with putIntRequest.
func (p *IntPool) newIntRequest(v int64) *intRequest {
	r := p.intRequestSyncPool.Get().(*intRequest)
	r.want = min(v, p.max)
	return r
}

func (p *IntPool) putIntRequest(r *intRequest) {
	p.intRequestSyncPool.Put(r)
}

// newIntRequest allocates an intFuncRequest from the sync.Pool.
// It should be returned with putIntFuncRequest.
func (p *IntPool) newIntFuncRequest(f IntRequestFunc) *intFuncRequest {
	r := p.intFuncRequestSyncPool.Get().(*intFuncRequest)
	r.f = f
	return r
}

func (p *IntPool) putIntFuncRequest(r *intFuncRequest) {
	r.f = nil
	p.intFuncRequestSyncPool.Put(r)
}

// intRequest is used to acquire a quantity from the quota known ahead of time.
type intRequest struct {
	want int64
}

func (r *intRequest) Acquire(ctx context.Context, v Resource) (fulfilled bool, extra Resource) {
	ia := v.(*intAlloc)
	if ia.alloc < r.want {
		return false, nil
	}
	ia.alloc -= r.want
	return true, ia
}

// intFuncRequest is used to acquire a quantity from the pool which is not
// known ahead of time.
type intFuncRequest struct {
	f    IntRequestFunc
	took int64
}

func (r *intFuncRequest) Acquire(ctx context.Context, v Resource) (fulfilled bool, extra Resource) {
	ia := v.(*intAlloc)
	ok, took := r.f(ctx, ia.alloc)
	if !ok {
		return false, nil
	}
	if took > ia.alloc {
		panic(errors.Errorf("took %d quota > %d allocated", took, ia.alloc))
	}
	r.took = took
	ia.alloc -= took
	return true, ia
}

func min(a, b int64) (v int64) {
	if a < b {
		return a
	}
	return b
}
