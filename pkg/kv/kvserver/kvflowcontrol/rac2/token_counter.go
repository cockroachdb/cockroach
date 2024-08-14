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
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	wc            admissionpb.WorkClass
	tokens, limit kvflowcontrol.Tokens
	// signalCh is used to wait on available tokens without holding a mutex.
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
	wc admissionpb.WorkClass, limit kvflowcontrol.Tokens,
) tokenCounterPerWorkClass {
	return tokenCounterPerWorkClass{
		wc:       wc,
		tokens:   limit,
		limit:    limit,
		signalCh: make(chan struct{}, 1),
	}
}

// adjustTokensLocked adjusts the tokens for the given work class by delta.
func (twc *tokenCounterPerWorkClass) adjustTokensLocked(
	ctx context.Context, delta kvflowcontrol.Tokens,
) {
	var unaccounted kvflowcontrol.Tokens
	before := twc.tokens
	twc.tokens += delta

	if delta <= 0 {
		// Nothing left to do, since we know tokens didn't increase.
		return
	}
	if twc.tokens > twc.limit {
		unaccounted = twc.tokens - twc.limit
		twc.tokens = twc.limit
	}
	if before <= 0 && twc.tokens > 0 {
		twc.signal()
	}
	if buildutil.CrdbTestBuild && unaccounted != 0 {
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d",
			twc.wc, unaccounted, delta, twc.limit)
	}
}

func (twc *tokenCounterPerWorkClass) setLimitLocked(
	ctx context.Context, limit kvflowcontrol.Tokens,
) {
	before := twc.limit
	twc.limit = limit
	twc.adjustTokensLocked(ctx, twc.limit-before)
}

func (twc *tokenCounterPerWorkClass) signal() {
	select {
	// Non-blocking channel write that ensures it's topped up to 1 entry.
	case twc.signalCh <- struct{}{}:
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
	settings *cluster.Settings

	mu struct {
		syncutil.RWMutex

		counters [admissionpb.NumWorkClasses]tokenCounterPerWorkClass
	}
}

var _ TokenCounter = &tokenCounter{}

func newTokenCounter(settings *cluster.Settings) *tokenCounter {
	t := &tokenCounter{
		settings: settings,
	}
	limit := tokensPerWorkClass{
		regular: kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)),
		elastic: kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)),
	}
	t.mu.counters[admissionpb.RegularWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.RegularWorkClass, limit.regular)
	t.mu.counters[admissionpb.ElasticWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.ElasticWorkClass, limit.elastic)

	onChangeFunc := func(ctx context.Context) {
		t.mu.Lock()
		defer t.mu.Unlock()

		t.mu.counters[admissionpb.RegularWorkClass].setLimitLocked(
			ctx, kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)))
		t.mu.counters[admissionpb.ElasticWorkClass].setLimitLocked(
			ctx, kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)))
	}

	kvflowcontrol.RegularTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	kvflowcontrol.ElasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	return t
}

func (t *tokenCounter) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tokensLocked(wc)
}

func (b *tokenCounter) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	return b.mu.counters[wc].tokens
}

// TokensAvailable returns true if tokens are available. If false, it returns
// a handle that may be used for waiting for tokens to become available.
func (t *tokenCounter) TokensAvailable(
	wc admissionpb.WorkClass,
) (available bool, handle TokenWaitingHandle) {
	if t.tokens(wc) > 0 {
		return true, nil
	}
	return false, waitHandle{wc: wc, b: t}
}

// TryDeduct attempts to deduct flow tokens for the given work class. If there
// are no tokens available, 0 tokens are returned. When less than the requested
// token count is available, partial tokens are returned corresponding to this
// partial amount.
func (t *tokenCounter) TryDeduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) kvflowcontrol.Tokens {
	t.mu.Lock()
	defer t.mu.Unlock()

	tokensAvailable := t.tokensLocked(wc)
	if tokensAvailable <= 0 {
		return 0
	}

	adjust := min(tokensAvailable, tokens)
	t.adjustLocked(ctx, wc, -adjust)
	return adjust
}

// Deduct deducts (without blocking) flow tokens for the given work class. If
// there are not enough available tokens, the token counter will go into debt
// (negative available count) and still issue the requested number of tokens.
func (t *tokenCounter) Deduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	t.adjust(ctx, wc, -tokens)
}

// Return returns flow tokens for the given work class.
func (t *tokenCounter) Return(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	t.adjust(ctx, wc, tokens)
}

// waitHandle is a handle for waiting for tokens to become available from a
// token counter.
type waitHandle struct {
	wc admissionpb.WorkClass
	b  *tokenCounter
}

var _ TokenWaitingHandle = waitHandle{}

// WaitChannel is the channel that will be signaled if tokens are possibly
// available. If signaled, the caller must call
// ConfirmHaveTokensAndUnblockNextWaiter. There is no guarantee of tokens being
// available after this channel is signaled, just that tokens were available
// recently. A typical usage pattern is:
//
//	for {
//	  select {
//	  case <-handle.WaitChannel():
//	    if handle.ConfirmHaveTokensAndUnblockNextWaiter() {
//	      break
//	    }
//	  }
//	}
//	tokenCounter.Deduct(...)
//
// There is a possibility for races, where multiple goroutines may be signaled
// and deduct tokens, sending the counter into debt. These cases are
// acceptable, as in aggregate the counter provides pacing over time.
func (wh waitHandle) WaitChannel() <-chan struct{} {
	return wh.b.mu.counters[wh.wc].signalCh
}

// ConfirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
// available. True is returned if tokens are available, false otherwise. If no
// tokens are available, the caller can resume waiting using WaitChannel.
func (wh waitHandle) ConfirmHaveTokensAndUnblockNextWaiter() (haveTokens bool) {
	haveTokens = wh.b.tokens(wh.wc) > 0
	if haveTokens {
		// Signal the next waiter if we have tokens available before returning.
		wh.b.mu.counters[wh.wc].signal()
	}
	return haveTokens
}

type tokenWaitingHandleInfo struct {
	handle TokenWaitingHandle
	// For regular work, this will be set for the leaseholder and leader. For
	// elastic work this will be set for the aforementioned, and all replicas
	// which are in StateReplicate.
	requiredWait bool
}

// WaitEndState is the state returned by WaitForEval and indicates the result
// of waiting.
type WaitEndState int32

const (
	// WaitSuccess indicates that the required quorum and required wait handles
	// were signaled and had tokens available.
	WaitSuccess WaitEndState = iota
	// ContextCanceled indicates that the context was canceled.
	ContextCanceled
	// RefreshWaitSignaled indicates that the refresh channel was signaled.
	RefreshWaitSignaled
)

func (s WaitEndState) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s WaitEndState) SafeFormat(w redact.SafePrinter, _ rune) {
	switch s {
	case WaitSuccess:
		w.Print("wait_success")
	case ContextCanceled:
		w.Print("context_cancelled")
	case RefreshWaitSignaled:
		w.Print("refresh_wait_signaled")
	default:
		panic(fmt.Sprintf("unknown wait_end_state(%d)", int(s)))
	}
}

// WaitForEval waits for a quorum of handles to be signaled and have tokens
// available, including all the required wait handles. The caller can provide a
// refresh channel, which when signaled will cause the function to return
// RefreshWaitSignaled, allowing the caller to retry waiting with updated
// handles.
//
// If the required quorum and required wait handles are signaled and have
// tokens available, the function checks the availability of tokens for all
// handles.
func WaitForEval(
	ctx context.Context,
	refreshWaitCh <-chan struct{},
	handles []tokenWaitingHandleInfo,
	requiredQuorum int,
	scratch []reflect.SelectCase,
) (state WaitEndState, scratch2 []reflect.SelectCase) {
	scratch = scratch[:0]
	if len(handles) < requiredQuorum {
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"invalid arguments to WaitForEval: len(handles)=%d < required_quorum=%d",
			len(handles), requiredQuorum))
	}

	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(refreshWaitCh)})

	requiredWaitCount := 0
	for _, h := range handles {
		if h.requiredWait {
			requiredWaitCount++
		}
		scratch = append(scratch,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(h.handle.WaitChannel())})
	}
	if requiredQuorum == 0 && requiredWaitCount == 0 {
		log.Fatalf(ctx, "both requiredQuorum and requiredWaitCount are zero")
	}
	// We track the original length of the scratch slice and also the current
	// length, where after each successful iteration we decrement the current
	// length.
	m := len(scratch)
	signaledCount := 0

	// Wait for (1) at least a quorumCount of handles to be signaled and have
	// available tokens; as well as (2) all of the required wait handles to be
	// signaled and have tokens available.
	for signaledCount < requiredQuorum || requiredWaitCount > 0 {
		chosen, _, _ := reflect.Select(scratch)
		switch chosen {
		case 0:
			return ContextCanceled, scratch
		case 1:
			return RefreshWaitSignaled, scratch
		default:
			handleInfo := handles[chosen-2]
			if available := handleInfo.handle.ConfirmHaveTokensAndUnblockNextWaiter(); !available {
				// The handle was signaled but does not currently have tokens
				// available. Continue waiting on this handle.
				continue
			}

			signaledCount++
			if handleInfo.requiredWait {
				requiredWaitCount--
			}
			m--
			scratch[chosen], scratch[m] = scratch[m], scratch[chosen]
			scratch = scratch[:m]
			handles[chosen-2], handles[m-2] = handles[m-2], handles[chosen-2]
		}
	}

	return WaitSuccess, scratch
}

// adjust the tokens for the given work class by delta. The adjustment is
// performed atomically.
func (t *tokenCounter) adjust(
	ctx context.Context, class admissionpb.WorkClass, delta kvflowcontrol.Tokens,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.adjustLocked(ctx, class, delta)
}

func (t *tokenCounter) adjustLocked(
	ctx context.Context, class admissionpb.WorkClass, delta kvflowcontrol.Tokens,
) {
	switch class {
	case admissionpb.RegularWorkClass:
		t.mu.counters[admissionpb.RegularWorkClass].adjustTokensLocked(ctx, delta)
		// Regular {deductions,returns} also affect elastic flow tokens.
		t.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(ctx, delta)
	case admissionpb.ElasticWorkClass:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		t.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(ctx, delta)
	}
}
