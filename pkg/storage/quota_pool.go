package storage

import (
	"math"
	"sync"

	"golang.org/x/net/context"
)

const (
	leaderProposalQuota   = 1000
	followerProposalQuota = math.MaxInt64 / 2
)

type quotaPool struct {
	c chan int64

	mu    sync.Mutex
	quota int64
}

func newQuotaPool(q int64) *quotaPool {
	qb := &quotaPool{
		c: make(chan int64, 1),
	}
	if q > 0 {
		qb.c <- q
	} else {
		qb.quota = q
	}
	return qb
}

func (qb *quotaPool) add(v int64) {
	qb.mu.Lock()
	defer qb.mu.Unlock()
	select {
	case n := <-qb.c:
		qb.quota += n
	default:
	}
	qb.quota += v
	if qb.quota <= 0 {
		return
	}
	select {
	case qb.c <- qb.quota:
		qb.quota = 0
	default:
	}
}

func (qb *quotaPool) reset(v int64) {
	qb.mu.Lock()
	defer qb.mu.Unlock()
	select {
	case _ = <-qb.c:
	default:
	}
	qb.quota = v
	if qb.quota <= 0 {
		return
	}
	select {
	case qb.c <- qb.quota:
		qb.quota = 0
	default:
	}
}

func (qb *quotaPool) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case n := <-qb.c:
		if n > 1 {
			qb.add(n - 1)
		}
		return nil
	}
}
