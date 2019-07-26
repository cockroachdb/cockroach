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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

// IntPool manages allocating integer units of quota to clients.
// Clients may acquire quota in two ways, using Acquire which requires the
// client to specify the quantity of quota at call time and AcquireFunc which
// allows the client to provide a function which will be used to determine
// whether a quantity of quota is sufficient when it becomes available.
type IntPool struct {
	qp *QuotaPool

	// capacity maintains how much total quota there is (not necessarily available).
	// Accessed atomically!
	// The capacity is originally set when the IntPool is constructed, and then it
	// can be decreased by IntAlloc.Freeze().
	capacity uint64
}

// IntAlloc is an allocated quantity which should be released.
type IntAlloc struct {
	alloc uint64
	p     *IntPool
}

// Release releases an IntAlloc back into the IntPool.
func (ia *IntAlloc) Release() {
	ia.p.qp.Add((*intAlloc)(ia))
}

// Acquired returns the quantity that this alloc has acquired.
func (ia *IntAlloc) Acquired() uint64 {
	return ia.alloc
}

// Merge adds the acquired resources in other to ia. Other may not be used after
// it has been merged. It is illegal to merge allocs from different pools and
// doing so will result in a panic.
func (ia *IntAlloc) Merge(other *IntAlloc) {
	if ia.p != other.p {
		panic("cannot merge IntAllocs from two different pools")
	}
	ia.alloc = min(ia.p.Capacity(), ia.alloc+other.alloc)
	ia.p.putIntAlloc(other)
}

// Freeze informs the quota pool that this allocation will never be Release()ed.
// Releasing it later is illegal.
//
// AcquireFunc() requests will be woken up with an updated Capacity, and Alloc()
// requests will be trimmed accordingly.
func (ia *IntAlloc) Freeze() {
	ia.p.decCapacity(ia.alloc)
}

// String formats an IntAlloc as a string.
func (ia *IntAlloc) String() string {
	if ia == nil {
		return strconv.Itoa(0)
	}
	return strconv.FormatUint(ia.alloc, 10)
}

// intAlloc is used to make IntAlloc implement Resource without muddling its
// exported interface.
type intAlloc IntAlloc

// Merge makes intAlloc a Resource.
func (ia *intAlloc) Merge(other Resource) {
	(*IntAlloc)(ia).Merge((*IntAlloc)(other.(*intAlloc)))
}

// NewIntPool creates a new named IntPool.
//
// capacity is the amount of quota initially available.
func NewIntPool(name string, capacity uint64, options ...Option) *IntPool {
	p := IntPool{
		capacity: capacity,
	}
	p.qp = New(name, (*intAlloc)(p.newIntAlloc(capacity)), options...)
	return &p
}

// Acquire acquires the specified amount of quota from the pool. On success, a
// non-nil alloc is returned and Release() must be called on it to return the
// quota to the pool.
//
// If 'v' is greater than the total capacity of the pool, we instead try to
// acquire quota equal to the maximum capacity. If the maximum capacity is
// decreased while this request is ongoing, the request is again truncated to
// the maximum capacity.
//
// Acquisitions of 0 return immediately with no error, even if the IntPool is
// closed.
//
// Safe for concurrent use.
func (p *IntPool) Acquire(ctx context.Context, v uint64) (*IntAlloc, error) {
	// Special case acquisitions of size 0.
	if v == 0 {
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
//
// If the request is satisfied, the function returns the amount of quota
// consumed and no error. If the request is not satisfied because there's no
// enough quota currently available, ErrNotEnoughQuota is returned to cause the
// function to be called again where more quota becomes available. took has to
// be 0 (i.e. it is not allowed for the request to save some quota for later
// use). If any other error is returned, took again has to be 0. The function
// will not be called any more and the error will be returned from
// IntPool.AcquireFunc().
type IntRequestFunc func(ctx context.Context, p PoolInfo) (took uint64, err error)

// ErrNotEnoughQuota is returned by IntRequestFuncs when they want to be called
// again once there's new resources.
var ErrNotEnoughQuota = fmt.Errorf("not enough quota available")

// PoolInfo represents the information that the IntRequestFunc gets about the current quota pool conditions.
type PoolInfo struct {
	// Available is the amount of quota available to be consumed. This is the
	// maximum value that the `took` return value from IntRequestFunc can be set
	// to.
	// Note that Available() can be 0. This happens when the IntRequestFunc() is
	// called as a result of the pool's capacity decreasing.
	Available uint64

	// Capacity returns the maximum capacity available in the pool. This can
	// decrease over time. It can be used to determine that the resources required
	// by a request will never be available.
	Capacity uint64
}

// AcquireFunc acquires a quantity of quota determined by a function which is
// called with a quantity of available quota.
func (p *IntPool) AcquireFunc(ctx context.Context, f IntRequestFunc) (*IntAlloc, error) {
	r := p.newIntFuncRequest(f)
	defer p.putIntFuncRequest(r)
	err := p.qp.Acquire(ctx, r)
	if err != nil {
		return nil, err
	}
	if r.err != nil {
		if r.took != 0 {
			panic(fmt.Sprintf("both took set (%d) and err (%s)", r.took, r.err))
		}
		return nil, r.err
	}
	return p.newIntAlloc(r.took), nil
}

// Len returns the current length of the queue for this IntPool.
func (p *IntPool) Len() int {
	return p.qp.Len()
}

// ApproximateQuota will report approximately the amount of quota available in
// the pool.
func (p *IntPool) ApproximateQuota() (q uint64) {
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

var intAllocSyncPool = sync.Pool{
	New: func() interface{} { return new(IntAlloc) },
}

func (p *IntPool) newIntAlloc(v uint64) *IntAlloc {
	ia := intAllocSyncPool.Get().(*IntAlloc)
	*ia = IntAlloc{p: p, alloc: v}
	return ia
}

func (p *IntPool) putIntAlloc(ia *IntAlloc) {
	*ia = IntAlloc{}
	intAllocSyncPool.Put(ia)
}

var intRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(intRequest) },
}

// newIntRequest allocates an intRequest from the sync.Pool.
// It should be returned with putIntRequest.
func (p *IntPool) newIntRequest(v uint64) *intRequest {
	r := intRequestSyncPool.Get().(*intRequest)
	*r = intRequest{want: v}
	return r
}

func (p *IntPool) putIntRequest(r *intRequest) {
	*r = intRequest{}
	intRequestSyncPool.Put(r)
}

var intFuncRequestSyncPool = sync.Pool{
	New: func() interface{} { return new(intFuncRequest) },
}

// newIntRequest allocates an intFuncRequest from the sync.Pool.
// It should be returned with putIntFuncRequest.
func (p *IntPool) newIntFuncRequest(f IntRequestFunc) *intFuncRequest {
	r := intFuncRequestSyncPool.Get().(*intFuncRequest)
	*r = intFuncRequest{f: f, p: p}
	return r
}

func (p *IntPool) putIntFuncRequest(r *intFuncRequest) {
	*r = intFuncRequest{}
	intFuncRequestSyncPool.Put(r)
}

// Capacity returns the amount of quota managed by this pool.
func (p *IntPool) Capacity() uint64 {
	return atomic.LoadUint64(&p.capacity)
}

// decCapacity decrements the capacity by c.
func (p *IntPool) decCapacity(c uint64) {
	// This is how you decrement from a uint64.
	atomic.AddUint64(&p.capacity, ^uint64(c-1))
	// Wake up the request at the front of the queue. The decrement above may race
	// with an ongoing request (which is why it's an atomic access), but in any
	// case that request is evaluated again.
	p.qp.Add(&intAlloc{alloc: 0, p: p})
}

// intRequest is used to acquire a quantity from the quota known ahead of time.
type intRequest struct {
	want uint64
}

func (r *intRequest) Acquire(ctx context.Context, v Resource) (fulfilled bool, extra Resource) {
	ia := v.(*intAlloc)
	r.want = min(r.want, ia.p.Capacity())
	if ia.alloc < r.want {
		return false, nil
	}
	ia.alloc -= r.want
	return true, ia
}

// intFuncRequest is used to acquire a quantity from the pool which is not
// known ahead of time.
type intFuncRequest struct {
	p    *IntPool
	f    IntRequestFunc
	took uint64
	// err saves the error returned by r.f, if other than ErrNotEnoughQuota.
	err error
}

func (r *intFuncRequest) Acquire(ctx context.Context, v Resource) (fulfilled bool, extra Resource) {
	ia := v.(*intAlloc)
	pi := PoolInfo{
		Available: ia.alloc,
		Capacity:  ia.p.Capacity(),
	}
	took, err := r.f(ctx, pi)
	if err != nil {
		if took != 0 {
			panic(fmt.Sprintf("IntRequestFunc returned both took: %d and err: %s", took, err))
		}
		if err == ErrNotEnoughQuota {
			return false, nil
		}
		r.err = err
		// Take the request out of the queue and put all the quota back.
		return true, ia
	}
	if took > ia.alloc {
		panic(errors.Errorf("took %d quota > %d allocated", took, ia.alloc))
	}
	r.took = took
	ia.alloc -= took
	return true, ia
}

func min(a, b uint64) (v uint64) {
	if a < b {
		return a
	}
	return b
}
