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

// IntPool manages dispensing integer quota to clients which know the size of
// an acquisition in integer units when calling Acquire.
type IntPool struct {
	qp  *QuotaPool
	max intAlloc

	allocSyncPool   sync.Pool
	requestSyncPool sync.Pool
}

// NewIntPool creates a new ProposalQuota with a maximum number of bytes.
func NewIntPool(name string, max int64) *IntPool {
	p := IntPool{
		max: intAlloc(max),
		allocSyncPool: sync.Pool{
			New: func() interface{} { return new(intAlloc) },
		},
		requestSyncPool: sync.Pool{
			New: func() interface{} { return new(intRequest) },
		},
	}
	p.qp = New(name, (*intPool)(&p))
	return &p
}

// Acquire acquires the desired quantity of quota.
func (p *IntPool) Acquire(ctx context.Context, v int64) error {
	r := p.newRequest(v)
	defer p.putRequest(r)
	return p.qp.Acquire(ctx, r)
}

func (p *IntPool) AcquireCb(
	ctx context.Context, cb func(context.Context, int64) (bool, int64),
) (int64, error) {
	r := &cbRequest{cb: cb}
	if err := p.qp.Acquire(ctx, r); err != nil {
		return 0, err
	}
	return r.alloc, nil
}

// Add returns quota to the pool.
func (p *IntPool) Add(v int64) {
	vv := p.allocSyncPool.Get().(*intAlloc)
	*vv = intAlloc(v)
	p.qp.Add(vv)
}

// ApproximateQuota will correctly report approximately the amount of quota
// available in the pool. It is accurate only if there are no ongoing
// acquisition goroutines. If there are, the return value can be up to 'v' less
// than actual available quota where 'v' is the value the acquisition goroutine
// first in line is attempting to acquire.
func (p *IntPool) ApproximateQuota() int64 {
	q := p.qp.ApproximateQuota()
	if q == nil {
		return 0
	}
	return int64(*q.(*intAlloc))
}

// Close signals to all ongoing and subsequent acquisitions that they are
// free to return to their callers without error.
//
// Safe for concurrent use.
func (p *IntPool) Close() {
	p.qp.Close()
}

func (p *IntPool) newRequest(v int64) *intRequest {
	r := p.requestSyncPool.Get().(*intRequest)
	r.want = intAlloc(v)
	if r.want > p.max {
		r.want = p.max
	}
	return r
}

func (p *IntPool) putRequest(r *intRequest) {
	p.requestSyncPool.Put(r)
}

// pool implements Pool.
var _ Pool = (*intPool)(nil)

type intPool IntPool

// InitialAlloc initializes the quotapool with a quantity of Quota.
func (p *intPool) InitialAlloc() Alloc {
	q := p.allocSyncPool.Get().(*intAlloc)
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
	p.allocSyncPool.Put(b)
	return a
}

type intAlloc int64

type intRequest struct {
	want intAlloc
}

func (q *intAlloc) String() string {
	if q == nil {
		return humanizeutil.IBytes(0)
	}
	return humanizeutil.IBytes(int64(*q))
}

func (r *intRequest) Acquire(_ context.Context, _ Pool, v Alloc) (extra Alloc, fulfilled bool) {
	ia := v.(*intAlloc)
	if *ia < r.want {
		return nil, false
	}
	*ia -= r.want
	return ia, true
}

type cbRequest struct {
	alloc int64
	cb    func(context.Context, int64) (fulfilled bool, took int64)
}

func (r *cbRequest) Acquire(ctx context.Context, _ Pool, v Alloc) (extra Alloc, fulfilled bool) {
	ia := v.(*intAlloc)
	ok, took := r.cb(ctx, int64(*ia))
	if !ok {
		return nil, false
	}
	r.alloc = took
	*ia -= intAlloc(took)
	return ia, true
}
