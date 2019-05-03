// Copyright 2019 The Cockroach Authors.
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

package quotapool

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
)

// IntPool manages allocating integer units of quota to clients.
// Clients may acquire quota in two ways, using Acquire which requires the
// client to specify the quantity of quota at call time and AcquireFunc which
// allows the client to provide a function which will be used to determine
// whether a quantity of quota is sufficient when it becomes available.
type IntPool struct {
	qp  *QuotaPool
	max intAlloc

	intAllocSyncPool       sync.Pool
	intRequestSyncPool     sync.Pool
	intFuncRequestSyncPool sync.Pool
}

// NewIntPool creates a new IntPool with a maximum quota value.
func NewIntPool(name string, max int64) *IntPool {
	p := IntPool{
		max: intAlloc(max),
		intAllocSyncPool: sync.Pool{
			New: func() interface{} { return new(intAlloc) },
		},
		intRequestSyncPool: sync.Pool{
			New: func() interface{} { return new(intRequest) },
		},
		intFuncRequestSyncPool: sync.Pool{
			New: func() interface{} { return new(intFuncRequest) },
		},
	}
	p.qp = New(name, (*intPool)(&p))
	return &p
}

// Acquire acquires the desired quantity of quota.
func (p *IntPool) Acquire(ctx context.Context, v int64) error {
	r := p.newIntRequest(v)
	defer p.putIntRequest(r)
	return p.qp.Acquire(ctx, r)
}

// IntRequestFunc is used to request a quantity of quota determined when quota is
// available rather than before requesting.
type IntRequestFunc func(ctx context.Context, v int64) (fulfilled bool, took int64)

// AcquireFunc acquires a quantity of quota determined by a function which is
// called with a quantity of available quota.
func (p *IntPool) AcquireFunc(ctx context.Context, f IntRequestFunc) (took int64, _ error) {
	r := p.newIntFuncRequest(f)
	defer p.putIntFuncRequest(r)
	err := p.qp.Acquire(ctx, r)
	if err != nil {
		return 0, err
	}
	return r.took, nil
}

// Add returns quota to the pool.
func (p *IntPool) Add(v int64) {
	vv := p.intAllocSyncPool.Get().(*intAlloc)
	*vv = intAlloc(v)
	p.qp.Add(vv)
}

// ApproximateQuota will correctly report approximately the amount of quota
// available in the pool. It is accurate only if there are no ongoing
// acquisition goroutines. If there are, the return value can be up to 'v' less
// than actual available quota where 'v' is the value the acquisition goroutine
// first in line is attempting to acquire.
func (p *IntPool) ApproximateQuota() int64 {
	var ret int64
	p.qp.ApproximateQuota(func(q Alloc) {
		if ia, ok := q.(*intAlloc); ok {
			ret = int64(*ia)
		}
	})
	return ret
}

// Close signals to all ongoing and subsequent acquisitions that the pool is
// closed and that an error should be returned.
//
// Safe for concurrent use.
func (p *IntPool) Close(reason string) {
	p.qp.Close(reason)
}

// newIntRequest allocates an intRequest from the sync.Pool.
// It should be returned with putIntRequest.
func (p *IntPool) newIntRequest(v int64) *intRequest {
	r := p.intRequestSyncPool.Get().(*intRequest)
	r.want = intAlloc(v)
	if r.want > p.max {
		r.want = p.max
	}
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

// intPool implements Pool.
type intPool IntPool

var _ Pool = (*intPool)(nil)

// InitialAlloc initializes the quotapool with a quantity of Quota.
func (p *intPool) InitialAlloc() Alloc {
	q := p.intAllocSyncPool.Get().(*intAlloc)
	*q = p.max
	return q
}

// Merge combines two Quota values into one.
func (p *intPool) Merge(a, b Alloc) Alloc {
	aa, bb := a.(*intAlloc), b.(*intAlloc)
	*aa += *bb
	*bb = 0
	if *aa > p.max {
		*aa = p.max
	}
	p.intAllocSyncPool.Put(b)
	return a
}

// intAlloc is intended to be used as an Alloc.
type intAlloc int64

func (q *intAlloc) String() string {
	if q == nil {
		return humanizeutil.IBytes(0)
	}
	return humanizeutil.IBytes(int64(*q))
}

// intRequest is used to acquire a quantity from the quota known ahead of time.
type intRequest struct {
	want intAlloc
}

func (r *intRequest) Acquire(ctx context.Context, p Pool, v Alloc) (extra Alloc, fulfilled bool) {
	ia := v.(*intAlloc)
	if *ia < r.want {
		return nil, false
	}
	*ia -= r.want
	return ia, true
}

// intFuncRequest is used to acquire a quantity from the pool which is not
// known ahead of time.
type intFuncRequest struct {
	took int64
	f    IntRequestFunc
}

func (r *intFuncRequest) Acquire(
	ctx context.Context, p Pool, v Alloc,
) (extra Alloc, fulfilled bool) {
	ia := v.(*intAlloc)
	ok, took := r.f(ctx, int64(*ia))
	if ok {
		r.took = took
		*ia -= intAlloc(took)
		return ia, true
	}
	return nil, false
}
