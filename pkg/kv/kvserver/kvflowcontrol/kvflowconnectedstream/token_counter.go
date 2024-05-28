// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// tokenCounterPerWorkClass is a helper struct for implementing tokenCounter.
// tokens and stats are protected by the mutex in tokenCounter. Operations on
// the signalCh may not be protected by that mutex -- see the comment below.
type tokenCounterPerWorkClass struct {
	wc     admissionpb.WorkClass
	tokens kvflowcontrol.Tokens
	// Waiting requests do so by waiting on signalCh without holding a mutex.
	//
	// Requests first check for available tokens (by acquiring and releasing the
	// mutex), and then wait if tokens for their work class are unavailable. The
	// risk in such waiting after releasing the mutex is the following race:
	// tokens become available after the waiter releases the mutex and before it
	// starts waiting. We handle this race by ensuring that signalCh always has
	// an entry if tokens are available:
	//
	// - Whenever tokens are returned, signalCh is signaled, waking up a single
	//   waiting request. If the request finds no available tokens, it starts
	//   waiting again.
	// - Whenever a request gets admitted, it signals the next waiter if any.
	//
	// So at least one request that observed unavailable tokens will get
	// unblocked, which will in turn unblock others. This turn by turn admission
	// provides some throttling to over-admission since the goroutine scheduler
	// needs to schedule the goroutine that got the entry for it to unblock
	// another. Hopefully, before we over-admit much, some of the scheduled
	// goroutines will complete proposal evaluation and deduct the tokens they
	// need.
	//
	// TODO(irfansharif): Right now we continue forwarding admission grants to
	// request while the available tokens > 0, which can lead to over-admission
	// since deduction is deferred (see testdata/simulation/over_admission).
	//
	// A. One mitigation could be terminating grant forwarding if the
	//    'tentatively deducted tokens' exceeds some amount (say, 16
	//    MiB). When tokens are actually deducted, we'll reduce from
	//    this 'tentatively deducted' count. We can re-signal() on every
	//    actual token deduction where available tokens is still > 0.
	// B. The pathological case with A is when all these tentatively
	//    deducted tokens are due to requests that are waiting on
	//    latches and locks. And the next request that wants to be
	//    admitted is not contending with those latches/locks but gets
	//    stuck behind it anyway. We could instead count the # of
	//    requests that go through this tokenCounter and are also past the
	//    latch/lock acquisition but not yet evaluated, and block if
	//    that count is greater than some small multiple of GOMAXPROCS.

	signalCh chan struct{}
	stats    struct {
		deltaStats
		noTokenStartTime time.Time
	}
}

type deltaStats struct {
	noTokenDuration time.Duration
	tokensDeducted  kvflowcontrol.Tokens
}

func makeTokenCounterPerWorkClass(
	wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, now time.Time,
) tokenCounterPerWorkClass {
	bwc := tokenCounterPerWorkClass{
		wc:       wc,
		tokens:   tokens,
		signalCh: make(chan struct{}, 1),
	}
	bwc.stats.noTokenStartTime = now
	return bwc
}

func (bwc *tokenCounterPerWorkClass) adjustTokensLocked(
	ctx context.Context,
	delta kvflowcontrol.Tokens,
	limit kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted kvflowcontrol.Tokens) {
	before := bwc.tokens
	bwc.tokens += delta
	if delta > 0 {
		if bwc.tokens > limit {
			unaccounted = bwc.tokens - limit
			bwc.tokens = limit
		}
		if before <= 0 && bwc.tokens > 0 {
			bwc.signal()
			bwc.stats.noTokenDuration += now.Sub(bwc.stats.noTokenStartTime)
		}
	} else {
		bwc.stats.deltaStats.tokensDeducted -= delta
		if before > 0 && bwc.tokens <= 0 {
			bwc.stats.noTokenStartTime = now
		}
	}
	if buildutil.CrdbTestBuild && !admin && unaccounted != 0 {
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d", bwc.wc, unaccounted, delta, limit)
	}
	adjustment = bwc.tokens - before
	return adjustment, unaccounted
}

func (bwc *tokenCounterPerWorkClass) signal() {
	select {
	case bwc.signalCh <- struct{}{}: // non-blocking channel write that ensures it's topped up to 1 entry
	default:
	}
}

type WaitEndState int32

const (
	WaitSuccess WaitEndState = iota
	ContextCanceled
	RefreshWaitSignaled
)

func (bwc *tokenCounterPerWorkClass) getAndResetStats(now time.Time) deltaStats {
	stats := bwc.stats.deltaStats
	if bwc.tokens <= 0 {
		stats.noTokenDuration += now.Sub(bwc.stats.noTokenStartTime)
	}
	bwc.stats.deltaStats = deltaStats{}
	// Doesn't matter if bwc.tokens is actually > 0 since in that case we won't
	// use this value.
	bwc.stats.noTokenStartTime = now
	return stats
}

// tokenCounter holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type tokenCounter struct {
	clock *hlc.Clock

	mu struct {
		syncutil.RWMutex

		limit    tokensPerWorkClass
		counters [admissionpb.NumWorkClasses]tokenCounterPerWorkClass
	}
}

var _ TokenCounter = &tokenCounter{}

func newTokenCounter(tokensPerWorkClass tokensPerWorkClass, clock *hlc.Clock) *tokenCounter {
	b := tokenCounter{clock: clock}
	now := clock.PhysicalTime()
	b.mu.counters[admissionpb.RegularWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.RegularWorkClass, tokensPerWorkClass.regular, now)
	b.mu.counters[admissionpb.ElasticWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.ElasticWorkClass, tokensPerWorkClass.elastic, now)
	b.mu.limit = tokensPerWorkClass
	return &b
}

func (b *tokenCounter) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tokensLocked(wc)
}

func (b *tokenCounter) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	return b.mu.counters[wc].tokens
}

// TokensAvailable returns true if tokens are available. If false, it returns a
// handle to use for waiting using kvflowcontroller.WaitForHandlesAndChannels.
// This is for waiting pre-evaluation.
func (b *tokenCounter) TokensAvailable(
	wc admissionpb.WorkClass,
) (available bool, handle TokenWaitingHandle) {
	if b.tokens(wc) > 0 {
		return true, nil
	}
	return false, waitHandle{wc: wc, b: b}
}

func (b *tokenCounter) TryDeduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) kvflowcontrol.Tokens {
	tokensAvailable := b.tokens(wc)

	if tokensAvailable <= 0 {
		return 0
	}

	// TODO(kvoli): Calculating the number of tokens to deduct and actually
	// deducting them is not atomic here.
	adjust := -min(tokensAvailable, tokens)
	b.adjust(ctx, wc, adjust, false /* admin */, b.clock.PhysicalTime())
	// TODO: Should we instead be using the adjusted return value here? It is
	// split across both elastic and regular classes, so perhaps its the minimum
	// of the two for regular and otherwise the elastic value
	return -adjust
}

// Deduct deducts (without blocking) flow tokens for the given priority.
func (b *tokenCounter) Deduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, wc, -tokens, false /* admin */, b.clock.PhysicalTime())
}

// Return returns flow tokens for the given priority.
func (b *tokenCounter) Return(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, wc, tokens, false /* admin */, b.clock.PhysicalTime())
}

type waitHandle struct {
	wc admissionpb.WorkClass
	b  *tokenCounter
}

// TODO(kvoli): implement TokenWaitingHandle methods.
var _ TokenWaitingHandle = waitHandle{}

// WaitChannel is the channel that will be signaled if tokens are possibly
// available. If signaled, the caller must call TryDeductAndUnblockNextWaiter.
func (wh waitHandle) WaitChannel() <-chan struct{} {
	return wh.b.mu.counters[wh.wc].signalCh
}

// TryDeductAndUnblockNextWaiter is called to deduct some tokens. The tokens
// parameter can be zero, when the waiter is only waiting for positive tokens
// (such as when waiting before eval). granted <= tokens and the tokens that
// have been deducted. haveTokens is true iff there are tokens available after
// this grant. When the tokens parameter is zero, granted will be zero, and
// haveTokens represents whether there were positive tokens. If the caller is
// unsatisfied with the return values, it can resume waiting using WaitChannel.
func (wh waitHandle) TryDeductAndUnblockNextWaiter(
	tokens kvflowcontrol.Tokens,
) (granted kvflowcontrol.Tokens, haveTokens bool) {
	defer func() {
		// Signal the next waiter if we have tokens available upon returning.
		if haveTokens {
			wh.b.mu.counters[wh.wc].signal()
		}
	}()

	if tokens > 0 {
		granted = wh.b.TryDeduct(context.Background(), wh.wc, tokens)
	}

	return granted, wh.b.tokens(wh.wc) > 0
}

// WaitForEval ...
// We always need minNumHandlesToWaitFor, even if the stopWaitCh channels have
// been closed/signaled. If the replica stream is no longer connected, and
// then later reconnects, the stopWaitCh will change, but that is ok, since we
// are simply waiting on the handle.
//
// TODO: add identity of leader. We also want a special stopWaitCh across all
// that is closed if the RangeController is closed, or the leaseholder
// changes. Also add the identity of the leader or leaseholder since
// specifically waiting for those eval tokens to be > 0. We will let in a
// burst if the leaseholder changes, while the local replica is still the
// leader -- so be it.
//
// NO! The above is all wrong. We want to internalize this implementation in
// RangeController. RangeController will get a list of all the voters, and
// which ones are required (leader, leaseholder, and for elastic traffic, all
// the ones in StateReplicate). There will also be a refresh channel for the waiter
// which it will notify if it needs to refresh. Then the waiter will wait.
// If leaseholder, leader change, or for elastic the ones in StateReplica are
// no longer in StateReplicate, or the sets of voter change, refresh will be
// signalled. All this state will either need to be copy-on-write or read locked
// since there are many waiting requests for eval.
//
// TODO: revise the above comment and implement, and then delete
// WaitForHandlesAndChannelsOld.
//
// Will wait for a quorum at least, which must include the requiredWait handles.
func WaitForEval(
	ctx context.Context,
	refreshWaitCh <-chan struct{},
	handles []tokenWaitingHandleInfo,
	scratch []reflect.SelectCase,
) (state WaitEndState, scratch2 []reflect.SelectCase) {
	// TODO: implement.
	return 0, nil
}

type tokenWaitingHandleInfo struct {
	handle TokenWaitingHandle
	// For regular work, this will be set for the leaseholder and leader. For
	// elastic work this will be set for the aforementioned, and all replicas
	// which are in StateReplicate.
	requiredWait bool
}

func WaitForHandlesAndChannelsOld(
	ctx context.Context,
	stopWaitCh <-chan struct{},
	numHandlesToWaitFor int,
	handles []interface{},
	scratch []reflect.SelectCase,
) (state WaitEndState, scratch2 []reflect.SelectCase) {
	scratch = scratch[:0]
	if len(handles) < numHandlesToWaitFor || numHandlesToWaitFor == 0 {
		panic("")
	}
	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopWaitCh)})
	for _, h := range handles {
		handle := h.(waitHandle)
		scratch = append(scratch,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(handle.b.mu.counters[handle.wc].signalCh)})
	}
	m := len(scratch)
	for numHandlesToWaitFor > 0 {
		chosen, _, _ := reflect.Select(scratch)
		switch chosen {
		case 0:
			return ContextCanceled, scratch
		case 1:
			return RefreshWaitSignaled, scratch
		default:
			h := handles[chosen-2].(waitHandle)
			h.b.mu.RLock()
			tokens := h.b.tokensLocked(h.wc)
			h.b.mu.RUnlock()
			if tokens > 0 {
				// Consumed the entry in the channel. Unblock another waiter.
				h.b.mu.counters[h.wc].signal()
				numHandlesToWaitFor--
				if numHandlesToWaitFor == 0 {
					return WaitSuccess, scratch
				}
				m--
				scratch[chosen], scratch[m] = scratch[m], scratch[chosen]
				scratch = scratch[:m]
			}
			// Else tokens < 0, so keep this SelectCase.
		}
	}
	panic("unreachable")
}

// admin is set to true when this method is called because of a settings
// change. In that case the class is interpreted narrowly as only updating the
// tokens for that class.
func (b *tokenCounter) adjust(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted tokensPerWorkClass) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// TODO(irfansharif,aaditya): On kv0/enc=false/nodes=3/cpu=96 this mutex is
	// responsible for ~1.8% of the mutex contention. Maybe address it as part
	// of #104154. We want to effectively increment two values but cap each at
	// some limit, and when incrementing, figure out what the adjustment was.
	// What if reads always capped it at the limit? And when incrementing
	// atomically by +delta, if we're over the limit, since we tried to increase
	// the value by +delta, at most we need to adjust back down by -delta.
	// Something like it.
	//
	//	var c int64 = 0
	//	var limit int64 = rand.Int63n(10000000000)
	//	for i := 0; i < 50; i++ {
	//		go func() {
	//			for j := 0; j < 2000; j++ {
	//				delta := rand.Int63()
	//				v := atomic.AddInt64(&c, delta)
	//				if v > limit {
	//					overlimit := v - limit
	//					var adjustment int64 = overlimit
	//					if delta < overlimit {
	//						adjustment = delta
	//					}
	//					n := atomic.AddInt64(&c, -adjustment)
	//					fmt.Printf("%d > %d by %d, adjusted by %d to %d)\n",
	//						v, limit, v-limit, -adjustment, n)
	//				}
	//			}
	//		}()
	//	}

	switch class {
	case admissionpb.RegularWorkClass:
		adjustment.regular, unaccounted.regular =
			b.mu.counters[admissionpb.RegularWorkClass].adjustTokensLocked(
				ctx, delta, b.mu.limit.regular, admin, now)
		if !admin {
			// Regular {deductions,returns} also affect elastic flow tokens.
			adjustment.elastic, unaccounted.elastic =
				b.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
					ctx, delta, b.mu.limit.elastic, admin, now)
		}
	case admissionpb.ElasticWorkClass:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		adjustment.elastic, unaccounted.elastic =
			b.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
				ctx, delta, b.mu.limit.elastic, admin, now)
	}
	return adjustment, unaccounted
}

func (b *tokenCounter) getAndResetStats(now time.Time) (regularStats, elasticStats deltaStats) {
	b.mu.Lock()
	defer b.mu.Unlock()
	regularStats = b.mu.counters[admissionpb.RegularWorkClass].getAndResetStats(now)
	elasticStats = b.mu.counters[admissionpb.ElasticWorkClass].getAndResetStats(now)
	return regularStats, elasticStats
}

func (b *tokenCounter) testingGetChannel(wc admissionpb.WorkClass) <-chan struct{} {
	return b.mu.counters[wc].signalCh
}

func (b *tokenCounter) testingSignalChannel(wc admissionpb.WorkClass) {
	b.mu.counters[wc].signal()
}

type tokensPerWorkClass struct {
	regular, elastic kvflowcontrol.Tokens
}

const (
	minTokensPerStream kvflowcontrol.Tokens = 1 << 20  // 1 MiB
	maxTokensPerStream kvflowcontrol.Tokens = 64 << 20 // 64 MiB
)

var validateTokenRange = settings.WithValidateInt(func(b int64) error {
	t := kvflowcontrol.Tokens(b)
	if t < minTokensPerStream {
		return fmt.Errorf("minimum flowed tokens allowed is %s, got %s", minTokensPerStream, t)
	}
	if t > maxTokensPerStream {
		return fmt.Errorf("maximum flow tokens allowed is %s, got %s", maxTokensPerStream, t)
	}
	return nil
})
