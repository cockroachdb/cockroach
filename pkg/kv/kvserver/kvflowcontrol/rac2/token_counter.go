// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TokenCounter is the interface for a token counter that can be used to deduct
// and return flow control tokens. Additionally, it can be used to wait for
// tokens to become available, and to check if tokens are available without
// blocking.
//
// TODO(kvoli): Consider de-interfacing if not necessary for testing.
type TokenCounter interface {
	// TokensAvailable returns true if tokens are available. If false, it returns
	// a handle that may be used for waiting for tokens to become available.
	TokensAvailable(admissionpb.WorkClass) (available bool, tokenWaitingHandle TokenWaitingHandle)
	// TryDeduct attempts to deduct flow tokens for the given work class. If
	// there are no tokens available, 0 tokens are returned. When less than the
	// requested token count is available, partial tokens are returned
	// corresponding to this partial amount.
	TryDeduct(
		context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens) kvflowcontrol.Tokens
	// Deduct deducts (without blocking) flow tokens for the given work class. If
	// there are not enough available tokens, the token counter will go into debt
	// (negative available count) and still issue the requested number of tokens.
	Deduct(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	// Return returns flow tokens for the given work class.
	Return(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
}

// TokenWaitingHandle is the interface for waiting for positive tokens from a
// token counter.
type TokenWaitingHandle interface {
	// WaitChannel is the channel that will be signaled if tokens are possibly
	// available. If signaled, the caller must call
	// ConfirmHaveTokensAndUnblockNextWaiter. There is no guarantee of tokens
	// being available after this channel is signaled, just that tokens were
	// available recently. A typical usage pattern is:
	//
	//   for {
	//     select {
	//     case <-handle.WaitChannel():
	//       if handle.ConfirmHaveTokensAndUnblockNextWaiter() {
	//         break
	//       }
	//     }
	//   }
	//   tokenCounter.Deduct(...)
	//
	// There is a possibility for races, where multiple goroutines may be
	// signaled and deduct tokens, sending the counter into debt. These cases are
	// acceptable, as in aggregate the counter provides pacing over time.
	WaitChannel() <-chan struct{}
	// ConfirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
	// available. True is returned if tokens are available, false otherwise. If
	// no tokens are available, the caller can resume waiting using WaitChannel.
	ConfirmHaveTokensAndUnblockNextWaiter() bool
}

// tokenCounterPerWorkClass is a helper struct for implementing tokenCounter.
// tokens are protected by the mutex in tokenCounter. Operations on the
// signalCh may not be protected by that mutex -- see the comment below.
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
	// another.
	signalCh chan struct{}
}

func makeTokenCounterPerWorkClass(
	wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, now time.Time,
) tokenCounterPerWorkClass {
	bwc := tokenCounterPerWorkClass{
		wc:       wc,
		tokens:   tokens,
		signalCh: make(chan struct{}, 1),
	}
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
		}
	}
	if buildutil.CrdbTestBuild && !admin && unaccounted != 0 {
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d",
			bwc.wc, unaccounted, delta, limit)
	}
	adjustment = bwc.tokens - before
	return adjustment, unaccounted
}

func (bwc *tokenCounterPerWorkClass) signal() {
	select {
	// Non-blocking channel write that ensures it's topped up to 1 entry.
	case bwc.signalCh <- struct{}{}:
	default:
	}
}

type tokensPerWorkClass struct {
	regular, elastic kvflowcontrol.Tokens
}

// tokenCounter holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type tokenCounter struct {
	clock    *hlc.Clock
	settings *cluster.Settings

	mu struct {
		syncutil.RWMutex

		// Token limit per work class, tracking
		// kvadmission.flow_controller.{regular,elastic}_tokens_per_stream.
		limit    tokensPerWorkClass
		counters [admissionpb.NumWorkClasses]tokenCounterPerWorkClass
	}
}

var _ TokenCounter = &tokenCounter{}

func newTokenCounter(settings *cluster.Settings, clock *hlc.Clock) *tokenCounter {
	b := &tokenCounter{
		clock:    clock,
		settings: settings,
	}

	regularTokens := kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV))
	elasticTokens := kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV))
	b.mu.limit = tokensPerWorkClass{
		regular: regularTokens,
		elastic: elasticTokens,
	}
	b.mu.counters[admissionpb.RegularWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.RegularWorkClass, b.mu.limit.regular, b.clock.PhysicalTime())
	b.mu.counters[admissionpb.ElasticWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.ElasticWorkClass, b.mu.limit.elastic, b.clock.PhysicalTime())

	onChangeFunc := func(ctx context.Context) {
		b.mu.Lock()
		defer b.mu.Unlock()

		before := b.mu.limit
		now := tokensPerWorkClass{
			regular: kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)),
			elastic: kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)),
		}
		adjustment := tokensPerWorkClass{
			regular: now.regular - before.regular,
			elastic: now.elastic - before.elastic,
		}
		b.mu.limit = now

		b.mu.counters[admissionpb.RegularWorkClass].adjustTokensLocked(
			ctx, adjustment.regular, now.regular, true /* admin */, b.clock.PhysicalTime())
		b.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
			ctx, adjustment.elastic, now.elastic, true /* admin */, b.clock.PhysicalTime())
	}

	kvflowcontrol.RegularTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	kvflowcontrol.ElasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	return b
}

func (b *tokenCounter) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.tokensLocked(wc)
}

func (b *tokenCounter) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	return b.mu.counters[wc].tokens
}

// TokensAvailable returns true if tokens are available. If false, it returns
// a handle that may be used for waiting for tokens to become available.
func (b *tokenCounter) TokensAvailable(
	wc admissionpb.WorkClass,
) (available bool, handle TokenWaitingHandle) {
	if b.tokens(wc) > 0 {
		return true, nil
	}
	return false, waitHandle{wc: wc, b: b}
}

// TryDeduct attempts to deduct flow tokens for the given work class. If there
// are no tokens available, 0 tokens are returned. When less than the requested
// token count is available, partial tokens are returned corresponding to this
// partial amount.
func (b *tokenCounter) TryDeduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) kvflowcontrol.Tokens {
	b.mu.Lock()
	defer b.mu.Unlock()

	tokensAvailable := b.tokensLocked(wc)
	if tokensAvailable <= 0 {
		return 0
	}

	adjust := -min(tokensAvailable, tokens)
	b.adjustLocked(ctx, wc, adjust, false /* admin */, b.clock.PhysicalTime())
	return -adjust
}

// Deduct deducts (without blocking) flow tokens for the given work class. If
// there are not enough available tokens, the token counter will go into debt
// (negative available count) and still issue the requested number of tokens.
func (b *tokenCounter) Deduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, wc, -tokens, false /* admin */, b.clock.PhysicalTime())
}

// Return returns flow tokens for the given work class.
func (b *tokenCounter) Return(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	b.adjust(ctx, wc, tokens, false /* admin */, b.clock.PhysicalTime())
}

// waitHandle is a handle for waiting for tokens to become available from a
// token counter.
type waitHandle struct {
	wc admissionpb.WorkClass
	b  *tokenCounter
}

var _ TokenWaitingHandle = waitHandle{}

// WaitChannel is the channel that will be signaled if tokens are possibly
// available. If signaled, the caller must call TryDeductAndUnblockNextWaiter.
func (wh waitHandle) WaitChannel() <-chan struct{} {
	return wh.b.mu.counters[wh.wc].signalCh
}

// ConfirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
// available. True is returned if tokens are available, false otherwise. If no
// tokens are available, the caller can resume waiting using WaitChannel.
func (wh waitHandle) ConfirmHaveTokensAndUnblockNextWaiter() (haveTokens bool) {
	defer func() {
		// Signal the next waiter if we have tokens available upon returning.
		if haveTokens {
			wh.b.mu.counters[wh.wc].signal()
		}
	}()

	return wh.b.tokens(wh.wc) > 0
}

// adjust the tokens for the given work class by delta. The adjustment is
// performed atomically. The adjustment made and any unaccounted for tokens are
// returned. When admin is set to true when this method is called because of a
// settings change. In that case the class is interpreted narrowly as only
// updating the tokens for that class.
func (b *tokenCounter) adjust(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted tokensPerWorkClass) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.adjustLocked(ctx, class, delta, admin, now)
}

func (b *tokenCounter) adjustLocked(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	admin bool,
	now time.Time,
) (adjustment, unaccounted tokensPerWorkClass) {
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

func (b *tokenCounter) testingGetLimit() tokensPerWorkClass {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.limit
}
