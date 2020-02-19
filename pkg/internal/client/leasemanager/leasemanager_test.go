package leasemanager_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// What do we want, we want to ensure that all of the histories are non-overlapping
type lockSession struct {
	id         int64
	exclusive  bool
	lock       int
	start, end hlc.Timestamp
	err        error

	r tracing.Recording
}

func TestLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	tc.Server(0).DB()
	const P, perP, locks, wait, delay, exFrac = 100, 50, 2, 100 * time.Millisecond, true, .01
	lm := leasemanager.New(keys.MakeTablePrefix(42), tc.Server(0).DB())
	var lockKeys []roachpb.Key
	for i := 0; i < locks; i++ {
		lockKeys = append(lockKeys, roachpb.Key(fmt.Sprint(i)))
	}
	sessions := make(chan lockSession, P*perP)
	var idAlloc int64
	c := tc.Server(0).Clock()

	runOne := func(ctx context.Context) (err error) {
		s := lockSession{
			id:        atomic.AddInt64(&idAlloc, 1),
			exclusive: rand.Float64() < exFrac,
			lock:      rand.Intn(locks),
		}
		ctx, get, cancel := tracing.ContextWithRecordingSpan(ctx, fmt.Sprint(s.id, s.exclusive, s.lock))
		defer cancel()
		defer func() {
			s.err = err
			s.r = get()
			select {
			case sessions <- s:
			case <-ctx.Done():
			}
		}()
		acquire := lm.AcquireShared
		if s.exclusive {
			acquire = lm.AcquireExclusive
		}
		txn := tc.Server(0).DB().NewTxn(ctx, "")

		if _, err := acquire(ctx, txn, lockKeys[s.lock]); err != nil {
			return err
		}

		log.VEventf(ctx, 2, "a %v %v %v", s.lock, s.exclusive, s.id)
		s.start = c.Now()
		if delay {
			select {
			case <-time.After(time.Duration(rand.Int63n(wait.Nanoseconds()))):
			case <-ctx.Done():
				return nil
			}
		}
		s.end = c.Now()
		log.VEventf(ctx, 2, "d %v %v %v", s.lock, s.exclusive, s.id)
		txn.ProvisionalCommitTimestamp()
		txn.PushTo(s.end)
		if err := txn.Commit(ctx); err != nil {
			//log.Errorf(ctx, "error on commit: %v %v %v", s, err, get())
			return err
		}
		return nil
	}
	run := func(ctx context.Context) error {
		for i := 0; i < perP; i++ {
			_ = runOne(ctx)
		}
		return nil
	}
	g := ctxgroup.WithContext(ctx)
	for i := 0; i < P; i++ {
		g.GoCtx(run)
	}
	go func() { _ = g.Wait(); close(sessions) }()
	var ls, failed []lockSession
	for s := range sessions {
		if s.err != nil {
			failed = append(failed, s)
		} else {
			ls = append(ls, s)
		}
	}
	os := mergeOverlapping(t, ls)
	for i := 1; i < len(os); i++ {
		cur, prev := os[i], os[i-1]
		if cur.ls[0].lock != prev.ls[0].lock {
			continue
		}
		if log.V(2) {
			log.Infof(ctx, "%d %6v %v-%v %v", cur.ls[0].lock, cur.ls[0].exclusive, cur.start, cur.end, len(cur.ls))
		}
		if cur.start.Less(prev.end) {
			t.Fatal(os)
		}
	}
}

type overlappingSessions struct {
	start, end hlc.Timestamp
	ls         []lockSession
}

func mergeOverlapping(t *testing.T, ls []lockSession) []*overlappingSessions {
	sort.Slice(ls, func(i, j int) bool {
		if ls[i].lock != ls[j].lock {
			return ls[i].lock < ls[j].lock
		}
		return ls[i].start.Less(ls[j].start)
	})
	var os []*overlappingSessions
	var cur *overlappingSessions
	mkCur := func(s *lockSession) {
		cur = &overlappingSessions{
			start: s.start,
			end:   s.end,
			ls:    []lockSession{*s},
		}
		os = append(os, cur)
	}
	for i := range ls {
		s := &ls[i]
		if cur == nil || s.lock != cur.ls[0].lock || s.exclusive != cur.ls[0].exclusive || cur.end.Less(s.start) {
			mkCur(s)
			continue
		}
		if cur.end.Less(s.end) {
			cur.end = s.end
		}
		cur.ls = append(cur.ls, *s)
	}
	return os
}
