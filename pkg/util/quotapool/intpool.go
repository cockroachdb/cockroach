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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// IntPool manages allocating integer units of quota to clients.
// Clients may acquire quota in two ways, using Acquire which requires the
// client to specify the quantity of quota at call time and AcquireFunc which
// allows the client to provide a function which will be used to determine
// whether a quantity of quota is sufficient when it becomes available.
type IntPool struct {
	qp *AbstractPool

	// capacity maintains how much total quota there is (not necessarily available).
	// Accessed atomically!
	// The capacity is originally set when the IntPool is constructed, and then it
	// can be decreased by IntAlloc.Freeze().
	capacity uint64

	// updateCapacityMu synchronizes accesses to the capacity.
	updateCapacityMu syncutil.Mutex
}

// IntAlloc is an allocated quantity which should be released.
type IntAlloc struct {
	// alloc may be negative when used as the current quota for an IntPool or when
	// lowering the capacity (see UpdateCapacity). Allocs acquired by clients of
	// this package will never be negative.
	alloc int64
	p     *IntPool
}

// Release releases an IntAlloc back into the IntPool.
// It is safe to release into a closed pool.
func (ia *IntAlloc) Release() {
	ia.p.qp.Update(func(res Resource) (shouldNotify bool) {
		r := res.(*intAlloc)
		(*IntAlloc)(r).Merge(ia)
		return true
	})
}

// Acquired returns the quantity that this alloc has acquired.
func (ia *IntAlloc) Acquired() uint64 {
	return uint64(ia.alloc)
}

// Merge adds the acquired resources in other to ia. Other may not be used after
// it has been merged. It is illegal to merge allocs from different pools and
// doing so will result in a panic.
func (ia *IntAlloc) Merge(other *IntAlloc) {
	if ia.p != other.p {
		panic("cannot merge IntAllocs from two different pools")
	}
	ia.alloc = min(int64(ia.p.Capacity()), ia.alloc+other.alloc)
	ia.p.putIntAlloc(other)
}

// Freeze informs the quota pool that this allocation will never be Release()ed.
// Releasing it later is illegal and will lead to panics.
//
// Using Freeze and UpdateCapacity on the same pool may require explicit
// coordination. It is illegal to freeze allocated capacity which is no longer
// available - specifically it is illegal to make the capacity of an IntPool
// negative. Imagine the case where the capacity of an IntPool is initially 10.
// An allocation of 10 is acquired. Then, while it is held, the pool's capacity
// is updated to be 9. Then the outstanding allocation is frozen. This would
// make the total capacity of the IntPool negative which is not allowed and will
// lead to a panic. In general it's a bad idea to freeze allocated quota from a
// pool which will ever have its capacity decreased.
//
// AcquireFunc() requests will be woken up with an updated Capacity, and Alloc()
// requests will be trimmed accordingly.
func (ia *IntAlloc) Freeze() {
	ia.p.decCapacity(uint64(ia.alloc))
	ia.p = nil // ensure that future uses of this alloc will panic
}

// String formats an IntAlloc as a string.
func (ia *IntAlloc) String() string {
	if ia == nil {
		return strconv.Itoa(0)
	}
	return strconv.FormatInt(ia.alloc, 10)
}

// from returns true if this IntAlloc is from p.
func (ia *IntAlloc) from(p *IntPool) bool {
	return ia.p == p
}

// intAlloc is used to make IntAlloc implement Resource without muddling its
// exported interface.
type intAlloc IntAlloc

// NewIntPool creates a new named IntPool.
//
// capacity is the amount of quota initially available. The maximum capacity
// is math.MaxInt64. If the capacity argument exceeds that value, this function
// will panic.
func NewIntPool(name string, capacity uint64, options ...Option) *IntPool {
	assertCapacityIsValid(capacity)
	p := IntPool{
		capacity: capacity,
	}
	p.qp = New(name, (*intAlloc)(p.newIntAlloc(int64(capacity))), options...)
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
// Acquisitions of more than 0 from a pool with 0 capacity always returns an
// ErrNotEnoughQuota.
//
// Safe for concurrent use.
func (p *IntPool) Acquire(ctx context.Context, v uint64) (*IntAlloc, error) {
	return p.acquireMaybeWait(ctx, v, true /* wait */)
}

// TryAcquire is like Acquire but if there is insufficient quota to acquire
// immediately the method will return ErrNotEnoughQuota.
func (p *IntPool) TryAcquire(ctx context.Context, v uint64) (*IntAlloc, error) {
	return p.acquireMaybeWait(ctx, v, false /* wait */)
}

func (p *IntPool) acquireMaybeWait(ctx context.Context, v uint64, wait bool) (*IntAlloc, error) {
	// Special case acquisitions of size 0.
	if v == 0 {
		return p.newIntAlloc(0), nil
	}
	// Special case capacity of 0.
	if p.Capacity() == 0 {
		return nil, ErrNotEnoughQuota
	}
	// The maximum capacity is math.MaxInt64 so we can always truncate requests
	// to that value.
	if v > math.MaxInt64 {
		v = math.MaxInt64
	}
	r := p.newIntRequest(v)
	defer p.putIntRequest(r)
	var req Request
	if wait {
		req = r
	} else {
		req = (*intRequestNoWait)(r)
	}
	if err := p.qp.Acquire(ctx, req); err != nil {
		return nil, err
	}
	return p.newIntAlloc(int64(r.want)), nil
}

// Release will release allocs back to their pool. Allocs which are from p are
// merged into a single alloc before being added to avoid synchronizing
// on o multiple times. Allocs which did not come from p are released
// one at a time. It is legal to pass nil values in allocs.
func (p *IntPool) Release(allocs ...*IntAlloc) {
	var toRelease *IntAlloc
	for _, alloc := range allocs {
		switch {
		case alloc == nil:
			continue
		case !alloc.from(p):
			// If alloc is not from p, call Release() on it directly.
			alloc.Release()
			continue
		case toRelease == nil:
			toRelease = alloc
		default:
			toRelease.Merge(alloc)
		}
	}
	if toRelease != nil {
		toRelease.Release()
	}
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

// HasErrClosed returns true if this error is or contains an ErrClosed error.
func HasErrClosed(err error) bool {
	return errors.HasType(err, (*ErrClosed)(nil))
}

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
	return p.acquireFuncMaybeWait(ctx, f, true /* wait */)
}

// TryAcquireFunc is like AcquireFunc but if insufficient quota exists the
// method will return ErrNotEnoughQuota rather than waiting for quota to become
// available.
func (p *IntPool) TryAcquireFunc(ctx context.Context, f IntRequestFunc) (*IntAlloc, error) {
	return p.acquireFuncMaybeWait(ctx, f, false /* wait */)
}

func (p *IntPool) acquireFuncMaybeWait(
	ctx context.Context, f IntRequestFunc, wait bool,
) (*IntAlloc, error) {
	r := p.newIntFuncRequest(f)
	defer p.putIntFuncRequest(r)
	var req Request
	if wait {
		req = r
	} else {
		req = (*intFuncRequestNoWait)(r)
	}
	err := p.qp.Acquire(ctx, req)
	if err != nil {
		return nil, err
	}
	if r.err != nil {
		if r.took != 0 {
			panic(fmt.Sprintf("both took set (%d) and err (%s)", r.took, r.err))
		}
		return nil, r.err
	}
	// NB: We know that r.took must be less than math.MaxInt64 because capacity
	// cannot exceed that value and took cannot exceed capacity.
	return p.newIntAlloc(int64(r.took)), nil
}

// Len returns the current length of the queue for this IntPool.
func (p *IntPool) Len() int {
	return p.qp.Len()
}

// ApproximateQuota will report approximately the amount of quota available in
// the pool. It's "approximate" because, if there's an acquisition in progress,
// this might return an "intermediate" value - one that does not fully reflect
// the capacity either before that acquisitions started or after it will have
// finished.
func (p *IntPool) ApproximateQuota() (q uint64) {
	p.qp.Update(func(r Resource) (shouldNotify bool) {
		if ia, ok := r.(*intAlloc); ok {
			q = uint64(max(0, ia.alloc))
		}
		return false
	})
	return q
}

// Full returns true if no quota is outstanding.
func (p *IntPool) Full() bool {
	return p.ApproximateQuota() == p.Capacity()
}

// Close signals to all ongoing and subsequent acquisitions that the pool is
// closed and that an error should be returned.
//
// Safe for concurrent use.
func (p *IntPool) Close(reason string) {
	p.qp.Close(reason)
}

// IntPoolCloser implements stop.Closer.
type IntPoolCloser struct {
	reason string
	p      *IntPool
}

// Close makes the IntPoolCloser a stop.Closer.
func (ipc IntPoolCloser) Close() {
	ipc.p.Close(ipc.reason)
}

// Closer returns a struct which implements stop.Closer.
func (p *IntPool) Closer(reason string) IntPoolCloser {
	return IntPoolCloser{p: p, reason: reason}
}

var intAllocSyncPool = sync.Pool{
	New: func() interface{} { return new(IntAlloc) },
}

func (p *IntPool) newIntAlloc(v int64) *IntAlloc {
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

// UpdateCapacity sets the capacity to newCapacity. If the current capacity
// is higher than the new capacity, currently running requests will not be
// affected. When the capacity is increased, new quota will be added. The total
// quantity of outstanding quota will never exceed the maximum value of the
// capacity which existed when any outstanding quota was acquired.
func (p *IntPool) UpdateCapacity(newCapacity uint64) {
	assertCapacityIsValid(newCapacity)

	// Synchronize updates so that we never lose any quota. If we did not
	// synchronize then a rapid succession of increases and decreases could have
	// their associated updates to the current quota re-ordered.
	//
	// Imagine the capacity moves through:
	//   100, 1, 100, 1, 100
	// It would lead to the following intAllocs being released:
	//   -99, 99, -99, 99
	// If was reordered to:
	//   99, 99, -99, -99
	// Then we'd effectively ignore the additions at the beginning because they
	// would push the alloc above the capacity and then we'd make the
	// corresponding subtractions, leading to a final state of -98 even though
	// we'd like it to be 1.
	p.updateCapacityMu.Lock()
	defer p.updateCapacityMu.Unlock()

	// NB: if we're going to be lowering the capacity, we need to remove the
	// quota from the pool before we update the capacity value. Imagine the case
	// where there's a concurrent release of quota back into the pool. If it sees
	// the newer capacity but the old quota value, it would push the value above
	// the new capacity and thus get truncated. This is a bummer. We prevent this
	// by subtracting from the quota pool (perhaps pushing it into negative
	// territory), before we change the capacity.
	oldCapacity := atomic.LoadUint64(&p.capacity)
	delta := int64(newCapacity) - int64(oldCapacity)
	if delta < 0 {
		p.newIntAlloc(delta).Release()
		delta = 0 // we still want to release after the update to wake goroutines
	}
	atomic.SwapUint64(&p.capacity, newCapacity)
	p.newIntAlloc(delta).Release()
}

// decCapacity decrements the capacity by c.
func (p *IntPool) decCapacity(c uint64) {
	p.updateCapacityMu.Lock()
	defer p.updateCapacityMu.Unlock()

	oldCapacity := p.Capacity()
	if int64(oldCapacity)-int64(c) < 0 {
		panic("cannot freeze quota which is no longer part of the pool")
	}
	newCapacity := oldCapacity - c
	atomic.SwapUint64(&p.capacity, newCapacity)

	// Wake up the request at the front of the queue. The decrement above may race
	// with an ongoing request (which is why it's an atomic access), but in any
	// case that request is evaluated again.
	p.newIntAlloc(0).Release()
}

// intRequest is used to acquire a quantity from the quota known ahead of time.
type intRequest struct {
	want uint64
}

func (r *intRequest) Acquire(
	ctx context.Context, v Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	ia := v.(*intAlloc)
	want := min(int64(r.want), int64(ia.p.Capacity()))
	if ia.alloc < want {
		return false, 0
	}
	r.want = uint64(want)
	ia.alloc -= want
	return true, 0
}

func (r *intRequest) ShouldWait() bool { return true }

type intRequestNoWait intRequest

func (r *intRequestNoWait) Acquire(
	ctx context.Context, v Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	return (*intRequest)(r).Acquire(ctx, v)
}

func (r *intRequestNoWait) ShouldWait() bool { return false }

// intFuncRequest is used to acquire a quantity from the pool which is not
// known ahead of time.
type intFuncRequest struct {
	p    *IntPool
	f    IntRequestFunc
	took uint64
	// err saves the error returned by r.f, if other than ErrNotEnoughQuota.
	err error
}

func (r *intFuncRequest) Acquire(
	ctx context.Context, v Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	ia := v.(*intAlloc)
	pi := PoolInfo{
		Available: uint64(max(0, ia.alloc)),
		Capacity:  ia.p.Capacity(),
	}
	took, err := r.f(ctx, pi)
	if err != nil {
		if took != 0 {
			panic(fmt.Sprintf("IntRequestFunc returned both took: %d and err: %s", took, err))
		}
		if errors.Is(err, ErrNotEnoughQuota) {
			return false, 0
		}
		r.err = err
		// Take the request out of the queue and put all the quota back.
		return true, 0
	}
	if took > math.MaxInt64 || int64(took) > ia.alloc {
		panic(errors.Errorf("took %d quota > %d allocated", took, ia.alloc))
	}
	r.took = took
	ia.alloc -= int64(took)
	return true, 0
}

func min(a, b int64) (v int64) {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) (v int64) {
	if a > b {
		return a
	}
	return b
}

func (r *intFuncRequest) ShouldWait() bool { return true }

type intFuncRequestNoWait intFuncRequest

func (r *intFuncRequestNoWait) Acquire(
	ctx context.Context, v Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	return (*intFuncRequest)(r).Acquire(ctx, v)
}

func (r *intFuncRequestNoWait) ShouldWait() bool { return false }

// assertCapacityIsValid panics if capacity exceeds math.MaxInt64.
func assertCapacityIsValid(capacity uint64) {
	if capacity > math.MaxInt64 {
		panic(errors.Errorf("capacity %d exceeds max capacity %d", capacity, math.MaxInt64))
	}
}
