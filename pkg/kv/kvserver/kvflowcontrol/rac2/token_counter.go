// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// tokenCounterPerWorkClassOrInflight is a helper struct for implementing
// tokenCounter. tokens are protected by the mutex in tokenCounter. Operations
// on the signalCh may not be protected by that mutex -- see the comment
// below.
type tokenCounterPerWorkClassOrInflight struct {
	// wc is only uses for logging.
	wc            WorkClassOrInflight
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
	stats    struct {
		deltaStats
		noTokenStartTime time.Time
	}
}

type deltaStats struct {
	noTokenDuration                time.Duration
	tokensDeducted, tokensReturned kvflowcontrol.Tokens
	// Can only be non-zero for send tokens and {RegularWC, ElasticWC}.
	// tokensDeductedForceFlush is further restricted to ElasticWC.
	//
	// NB: we could also count these for Inflight, but the main case that
	// inflight tokens are trying to protect for is during a burst in
	// force-flushing, and we do wait for inflight tokens in that case (for
	// which the above stats are sufficient). So we don't bother with additional
	// metrics that we are unlikely to care about.
	tokensDeductedForceFlush, tokensDeductedPreventSendQueue kvflowcontrol.Tokens
}

func makeTokenCounterPerWorkClass(
	wc WorkClassOrInflight, limit kvflowcontrol.Tokens, now time.Time,
) tokenCounterPerWorkClassOrInflight {
	twc := tokenCounterPerWorkClassOrInflight{
		wc:       wc,
		tokens:   limit,
		limit:    limit,
		signalCh: make(chan struct{}, 1),
	}
	twc.stats.noTokenStartTime = now
	return twc
}

// adjustTokensLocked adjusts the tokens for the given work class by delta.
func (twc *tokenCounterPerWorkClassOrInflight) adjustTokensLocked(
	ctx context.Context,
	delta kvflowcontrol.Tokens,
	now time.Time,
	isReset bool,
	flag TokenAdjustFlag,
) (adjustment, unaccounted kvflowcontrol.Tokens) {
	before := twc.tokens
	twc.tokens += delta
	if delta > 0 {
		twc.stats.tokensReturned += delta
		if twc.tokens > twc.limit {
			unaccounted = twc.tokens - twc.limit
			twc.tokens = twc.limit
		}
		if before <= 0 && twc.tokens > 0 {
			twc.signal()
			twc.stats.noTokenDuration += now.Sub(twc.stats.noTokenStartTime)
		}
	} else {
		twc.stats.tokensDeducted -= delta
		switch flag {
		case AdjForceFlush:
			twc.stats.tokensDeductedForceFlush -= delta
		case AdjPreventSendQueue:
			twc.stats.tokensDeductedPreventSendQueue -= delta
		}
		if before > 0 && twc.tokens <= 0 {
			twc.stats.noTokenStartTime = now
		}
	}
	if buildutil.CrdbTestBuild && !isReset && unaccounted != 0 {
		log.KvDistribution.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d",
			twc.wc, unaccounted, delta, twc.limit)
	}

	adjustment = twc.tokens - before
	return adjustment, unaccounted
}

func (twc *tokenCounterPerWorkClassOrInflight) setLimitLocked(
	ctx context.Context, limit kvflowcontrol.Tokens, now time.Time,
) {
	before := twc.limit
	twc.limit = limit
	twc.adjustTokensLocked(ctx, twc.limit-before, now, false /* isReset */, AdjNormal)
}

func (twc *tokenCounterPerWorkClassOrInflight) resetLocked(ctx context.Context, now time.Time) {
	if twc.limit <= twc.tokens {
		return
	}
	twc.adjustTokensLocked(ctx, twc.limit-twc.tokens, now, true /* isReset */, AdjNormal)
}

func (twc *tokenCounterPerWorkClassOrInflight) signal() {
	select {
	// Non-blocking channel write that ensures it's topped up to 1 entry.
	case twc.signalCh <- struct{}{}:
	default:
	}
}

func (twc *tokenCounterPerWorkClassOrInflight) getAndResetStats(now time.Time) deltaStats {
	stats := twc.stats.deltaStats
	if twc.tokens <= 0 {
		stats.noTokenDuration += now.Sub(twc.stats.noTokenStartTime)
	}
	twc.stats.deltaStats = deltaStats{}
	// Doesn't matter if bwc.tokens is actually > 0 since in that case we won't
	// use this value.
	twc.stats.noTokenStartTime = now
	return stats
}

type tokensPerWorkClass struct {
	regular, elastic kvflowcontrol.Tokens
}

// TokenType represents the type of token being adjusted, distinct from the
// class of token (elastic or regular). A TokenCounter will have a TokenType
// for which it adjusts tokens.
type TokenType int

const (
	EvalToken TokenType = iota
	SendToken
	NumTokenTypes
)

func (f TokenType) String() string {
	return redact.StringWithoutMarkers(f)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (f TokenType) SafeFormat(p redact.SafePrinter, _ rune) {
	switch f {
	case EvalToken:
		p.SafeString("eval")
	case SendToken:
		p.SafeString("send")
	default:
		panic("unknown flowControlMetricType")
	}
}

// Wrapper type to give mutex contention events in mutex profiles a leaf frame
// that references tokenCounterMu. This makes it easier to look at contention
// on this mutex specifically.
type tokenCounterMu syncutil.RWMutex

func (mu *tokenCounterMu) Lock() {
	(*syncutil.RWMutex)(mu).Lock()
}

func (mu *tokenCounterMu) TryLock() {
	(*syncutil.RWMutex)(mu).TryLock()
}

func (mu *tokenCounterMu) Unlock() {
	(*syncutil.RWMutex)(mu).Unlock()
}

func (mu *tokenCounterMu) RLock() {
	(*syncutil.RWMutex)(mu).RLock()
}

func (mu *tokenCounterMu) TryRLock() {
	(*syncutil.RWMutex)(mu).TryRLock()
}

func (mu *tokenCounterMu) RUnlock() {
	(*syncutil.RWMutex)(mu).RUnlock()
}

func (mu *tokenCounterMu) AssertHeld() {
	(*syncutil.RWMutex)(mu).AssertHeld()
}

func (mu *tokenCounterMu) AssertRHeld() {
	(*syncutil.RWMutex)(mu).AssertRHeld()
}

// tokenCounter holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. When the TokenType is send tokens, it also holds flow
// tokens for inflight traffic. It's used to synchronize handoff between
// threads returning and waiting for flow tokens.
type tokenCounter struct {
	settings *cluster.Settings
	clock    *hlc.Clock
	metrics  *TokenCounterMetrics
	// stream is the stream for which tokens are being adjusted, it is only used
	// in logging.
	stream    kvflowcontrol.Stream
	tokenType TokenType

	mu struct {
		tokenCounterMu

		// Inflight index is only initialized when TokenType represents a send
		// tokenCounter.
		counters [NumWorkClassOrInflight]tokenCounterPerWorkClassOrInflight
	}
}

// newTokenCounter creates a new TokenCounter.
func newTokenCounter(
	settings *cluster.Settings,
	clock *hlc.Clock,
	metrics *TokenCounterMetrics,
	stream kvflowcontrol.Stream,
	tokenType TokenType,
) *tokenCounter {
	t := &tokenCounter{
		settings:  settings,
		clock:     clock,
		metrics:   metrics,
		stream:    stream,
		tokenType: tokenType,
	}
	now := clock.PhysicalTime()

	t.mu.counters[RegularWC] = makeTokenCounterPerWorkClass(
		RegularWC, kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)), now)
	t.mu.counters[ElasticWC] = makeTokenCounterPerWorkClass(
		ElasticWC, kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)), now)
	if tokenType == SendToken {
		t.mu.counters[InFlight] = makeTokenCounterPerWorkClass(
			InFlight,
			kvflowcontrol.Tokens(kvflowcontrol.InFlightTokensPerStream.Get(&settings.SV)), now)
	}

	onChangeFunc := func(ctx context.Context) {
		now := t.clock.PhysicalTime()
		t.mu.Lock()
		defer t.mu.Unlock()

		t.mu.counters[RegularWC].setLimitLocked(
			ctx, kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)), now)
		t.mu.counters[ElasticWC].setLimitLocked(
			ctx, kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)), now)
		if tokenType == SendToken {
			t.mu.counters[InFlight].setLimitLocked(ctx,
				kvflowcontrol.Tokens(kvflowcontrol.InFlightTokensPerStream.Get(&settings.SV)), now)
		}
	}

	kvflowcontrol.RegularTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	kvflowcontrol.ElasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	if tokenType == SendToken {
		kvflowcontrol.InFlightTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	}
	kvflowcontrol.TokenCounterResetEpoch.SetOnChange(&settings.SV, func(ctx context.Context) {
		now := t.clock.PhysicalTime()
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.counters[RegularWC].resetLocked(ctx, now)
		t.mu.counters[ElasticWC].resetLocked(ctx, now)
		if tokenType == SendToken {
			t.mu.counters[InFlight].resetLocked(ctx, now)
		}
	})
	return t
}

// String returns a string representation of the token counter.
func (t *tokenCounter) String() string {
	return redact.StringWithoutMarkers(t)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (t *tokenCounter) SafeFormat(w redact.SafePrinter, _ rune) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	w.Printf("reg=%v/%v ela=%v/%v",
		t.mu.counters[RegularWC].tokens,
		t.mu.counters[RegularWC].limit,
		t.mu.counters[ElasticWC].tokens,
		t.mu.counters[ElasticWC].limit)
	if t.tokenType == SendToken {
		w.Printf(" inflight=%v/%v",
			t.mu.counters[InFlight].tokens, t.mu.counters[InFlight].limit)
	}
}

// Stream returns the flow control stream for which tokens are being adjusted.
func (t *tokenCounter) Stream() kvflowcontrol.Stream {
	return t.stream
}

func (t *tokenCounter) tokensPerWorkClass() tokensPerWorkClass {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return tokensPerWorkClass{
		regular: t.tokensLocked(RegularWC),
		elastic: t.tokensLocked(ElasticWC),
	}
}

func (t *tokenCounter) tokens(wc WorkClassOrInflight) kvflowcontrol.Tokens {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tokensLocked(wc)
}

func (t *tokenCounter) tokensLocked(wc WorkClassOrInflight) kvflowcontrol.Tokens {
	return t.mu.counters[wc].tokens
}

func (t *tokenCounter) limit(wc WorkClassOrInflight) kvflowcontrol.Tokens {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.counters[wc].limit
}

// TokensAvailable returns true if tokens are available, in which case handle
// is empty and should be ignored. If false, it returns a handle that may be
// used for waiting for tokens to become available.
func (t *tokenCounter) TokensAvailable(
	wc WorkClassOrInflight,
) (available bool, handle tokenWaitHandle) {
	if t.tokens(wc) > 0 {
		return true, tokenWaitHandle{}
	}
	return false, tokenWaitHandle{wc: wc, b: t}
}

// TryDeduct attempts to deduct flow tokens for the given work class. If there
// are no tokens available, 0 tokens are returned. When less than the requested
// token count is available, partial tokens are returned corresponding to this
// partial amount.
func (t *tokenCounter) TryDeduct(
	ctx context.Context, wc WorkClassOrInflight, tokens kvflowcontrol.Tokens, flag TokenAdjustFlag,
) kvflowcontrol.Tokens {
	now := t.clock.PhysicalTime()
	var expensiveLog bool
	if log.V(2) {
		expensiveLog = true
	}
	t.mu.Lock()

	tokensAvailable := t.tokensLocked(wc) // nolint:deferunlockcheck
	if tokensAvailable <= 0 {
		t.mu.Unlock() // nolint:deferunlockcheck
		return 0
	}

	adjust := min(tokensAvailable, tokens)
	t.adjustLockedAndUnlock(ctx, wc, -adjust, now, flag, expensiveLog)
	return adjust
}

// Deduct deducts (without blocking) flow tokens for the given work class. If
// there are not enough available tokens, the token counter will go into debt
// (negative available count) and still issue the requested number of tokens.
func (t *tokenCounter) Deduct(
	ctx context.Context, wc WorkClassOrInflight, tokens kvflowcontrol.Tokens, flag TokenAdjustFlag,
) {
	t.adjust(ctx, wc, -tokens, flag)
}

// Return returns flow tokens for the given work class. When disconnect is
// true, the tokens being returned are not associated with admission of any
// specific request, rather the leader returning tracked tokens for a replica
// it doesn't expect to hear from again.
func (t *tokenCounter) Return(
	ctx context.Context, wc WorkClassOrInflight, tokens kvflowcontrol.Tokens, flag TokenAdjustFlag,
) {
	t.adjust(ctx, wc, tokens, flag)
}

// tokenWaitHandle is a handle for waiting for tokens to become available from
// a token counter.
type tokenWaitHandle struct {
	wc WorkClassOrInflight
	b  *tokenCounter
}

// waitChannel is the channel that will be signaled if tokens are possibly
// available. If signaled, the caller must call
// confirmHaveTokensAndUnblockNextWaiter. There is no guarantee of tokens being
// available after this channel is signaled, just that tokens were available
// recently. A typical usage pattern is:
//
//	for {
//	  select {
//	  case <-handle.waitChannel():
//	    if handle.confirmHaveTokensAndUnblockNextWaiter() {
//	      break
//	    }
//	  }
//	}
//	tokenCounter.Deduct(...)
//
// There is a possibility for races, where multiple goroutines may be signaled
// and deduct tokens, sending the counter into debt. These cases are
// acceptable, as in aggregate the counter provides pacing over time.
func (wh tokenWaitHandle) waitChannel() <-chan struct{} {
	return wh.b.mu.counters[wh.wc].signalCh
}

// confirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
// available. True is returned if tokens are available, false otherwise. If no
// tokens are available, the caller can resume waiting using waitChannel.
func (wh tokenWaitHandle) confirmHaveTokensAndUnblockNextWaiter() (haveTokens bool) {
	haveTokens = wh.b.tokens(wh.wc) > 0
	if haveTokens {
		// Signal the next waiter if we have tokens available before returning.
		wh.b.mu.counters[wh.wc].signal()
	}
	return haveTokens
}

// streamString returns a string representation of the stream. Used for
// tracing.
func (wh tokenWaitHandle) streamString() string {
	return wh.b.stream.String()
}

type tokenWaitingHandleInfo struct {
	// Can be empty, in which case no methods should be called on it.
	handle tokenWaitHandle
	// requiredWait will be set for the leaseholder and leader for regular work.
	// For elastic work this will be set for the aforementioned, and all replicas
	// which are in StateReplicate.
	requiredWait bool
	// partOfQuorum will be set for all voting replicas which can contribute to
	// quorum.
	partOfQuorum bool
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
	// ConfigRefreshWaitSignaled indicates that the config refresh channel was
	// signaled.
	ConfigRefreshWaitSignaled
	// ReplicaRefreshWaitSignaled indicates that the replica refresh channel was
	// signaled.
	ReplicaRefreshWaitSignaled
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
	case ConfigRefreshWaitSignaled:
		w.Print("config_refresh_wait_signaled")
	case ReplicaRefreshWaitSignaled:
		w.Print("replica_refresh_wait_signaled")
	default:
		panic(fmt.Sprintf("unknown wait_end_state(%d)", int(s)))
	}
}

// WaitForEval waits for a quorum of handles to be signaled and have tokens
// available, including all the required wait handles. The caller provides two
// refresh channels, which when signaled will cause the function to return
// {Config,Replica}RefreshWaitSignaled, allowing the caller to retry waiting
// with updated handles, or abandon waiting.
func WaitForEval(
	ctx context.Context,
	configRefreshWaitCh <-chan struct{},
	replicaRefreshWaitCh <-chan struct{},
	handles []tokenWaitingHandleInfo,
	requiredQuorum int,
	traceIndividualWaits bool,
	scratch []reflect.SelectCase,
) (state WaitEndState, scratch2 []reflect.SelectCase) {
	scratch = scratch[:0]
	if len(handles) < requiredQuorum {
		log.KvDistribution.Fatalf(ctx, "%v", errors.AssertionFailedf(
			"invalid arguments to WaitForEval: len(handles)=%d < required_quorum=%d",
			len(handles), requiredQuorum))
	}

	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(configRefreshWaitCh)})
	scratch = append(scratch,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(replicaRefreshWaitCh)})

	requiredWaitCount := 0
	for _, h := range handles {
		if h.requiredWait {
			requiredWaitCount++
		}
		var chanValue reflect.Value
		if h.handle != (tokenWaitHandle{}) {
			chanValue = reflect.ValueOf(h.handle.waitChannel())
		}
		// Else, zero Value, so will never be selected.
		scratch = append(scratch,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: chanValue})
	}
	if requiredQuorum == 0 && requiredWaitCount == 0 {
		log.KvDistribution.Fatalf(ctx, "both requiredQuorum and requiredWaitCount are zero")
	}

	// m is the current length of the scratch slice.
	m := len(scratch)
	signaledQuorumCount := 0

	// Wait for (1) at least a quorumCount of partOfQuorum handles to be signaled
	// and have available tokens; as well as (2) all of the required wait handles
	// to be signaled and have tokens available.
	for signaledQuorumCount < requiredQuorum || requiredWaitCount > 0 {
		chosen, _, _ := reflect.Select(scratch)
		switch chosen {
		case 0:
			if traceIndividualWaits {
				log.Eventf(ctx, "wait-for-eval: waited until context cancellation")
			}
			return ContextCanceled, scratch
		case 1:
			if traceIndividualWaits {
				log.Eventf(ctx, "wait-for-eval: waited until channel1 refreshed")
			}
			return ConfigRefreshWaitSignaled, scratch
		case 2:
			if traceIndividualWaits {
				log.Eventf(ctx, "wait-for-eval: waited until channel2 refreshed")
			}
			return ReplicaRefreshWaitSignaled, scratch
		default:
			handleInfo := handles[chosen-3]
			if available := handleInfo.handle.confirmHaveTokensAndUnblockNextWaiter(); !available {
				// The handle was signaled but does not currently have tokens
				// available. Continue waiting on this handle.
				continue
			}

			if traceIndividualWaits {
				log.Eventf(ctx, "wait-for-eval: waited until %s tokens available",
					handleInfo.handle.streamString())
			}
			if handleInfo.partOfQuorum {
				signaledQuorumCount++
			}
			if handleInfo.requiredWait {
				requiredWaitCount--
			}
			m--
			scratch[chosen], scratch[m] = scratch[m], scratch[chosen]
			scratch = scratch[:m]
			handles[chosen-3], handles[m-3] = handles[m-3], handles[chosen-3]
		}
	}

	return WaitSuccess, scratch
}

// TokenAdjustFlag are used to inform token adjustments about the context in
// which they are being made. Currently used for observability.
type TokenAdjustFlag uint8

const (
	AdjNormal TokenAdjustFlag = iota
	// AdjDisconnect is set when (regular|elastic) tokens are being returned without
	// corresponding admission. This is used to track tokens that are being
	// returned for a replica that is not expected to reply anymore.
	AdjDisconnect
	// AdjForceFlush is set when elastic send tokens are being deducted without
	// waiting for them to be available, due to force flush.
	AdjForceFlush
	// AdjPreventSendQueue is set when not enough (regular|elastic) send tokens
	// are available but being deducted anyway (negative balance) to prevent a
	// delay in quorum by forming a send queue.
	AdjPreventSendQueue
)

func (a TokenAdjustFlag) String() string {
	return redact.StringWithoutMarkers(a)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (a TokenAdjustFlag) SafeFormat(w redact.SafePrinter, _ rune) {
	switch a {
	case AdjNormal:
		w.Print("normal")
	case AdjDisconnect:
		w.Print("disconnect")
	case AdjForceFlush:
		w.Print("force_flush")
	case AdjPreventSendQueue:
		w.Print("prevent_send_queue")
	default:
		panic("unknown token_adjust_flag")
	}
}

// adjust the tokens for the given work class by delta. The adjustment is
// performed atomically.
func (t *tokenCounter) adjust(
	ctx context.Context, class WorkClassOrInflight, delta kvflowcontrol.Tokens, flag TokenAdjustFlag,
) {
	now := t.clock.PhysicalTime()
	var expensiveLog bool
	if log.V(2) {
		expensiveLog = true
	}
	t.mu.Lock()
	t.adjustLockedAndUnlock(ctx, class, delta, now, flag, expensiveLog) // nolint:deferunlockcheck
}

func (t *tokenCounter) adjustLockedAndUnlock(
	ctx context.Context,
	class WorkClassOrInflight,
	delta kvflowcontrol.Tokens,
	now time.Time,
	flag TokenAdjustFlag,
	expensiveLog bool,
) {
	t.mu.AssertHeld()
	var adjustmentRegular, unaccountedRegular kvflowcontrol.Tokens
	var adjustmentElastic, unaccountedElastic kvflowcontrol.Tokens
	var adjustmentInflight, unaccountedInflight kvflowcontrol.Tokens
	// Only populated when expensiveLog is true.
	var regularTokens, elasticTokens, inflightTokens kvflowcontrol.Tokens
	func() {
		defer t.mu.Unlock()
		switch class {
		case RegularWC:
			adjustmentRegular, unaccountedRegular =
				t.mu.counters[RegularWC].adjustTokensLocked(
					ctx, delta, now, false /* isReset */, flag)
			// Regular {deductions,returns} also affect elastic flow tokens.
			adjustmentElastic, unaccountedElastic =
				t.mu.counters[ElasticWC].adjustTokensLocked(
					ctx, delta, now, false /* isReset */, flag)
		case ElasticWC:
			// Elastic {deductions,returns} only affect elastic flow tokens.
			adjustmentElastic, unaccountedElastic =
				t.mu.counters[ElasticWC].adjustTokensLocked(
					ctx, delta, now, false /* isReset */, flag)
		case InFlight:
			// Force flush {deductions,returns} only affect force flush tokens.
			adjustmentInflight, unaccountedInflight = t.mu.counters[InFlight].adjustTokensLocked(
				ctx, delta, now, false /* isReset */, flag)
		}
		if expensiveLog {
			regularTokens = t.tokensLocked(RegularWC)
			elasticTokens = t.tokensLocked(ElasticWC)
			inflightTokens = t.tokensLocked(InFlight)
		}
	}()

	// Adjust metrics if any tokens were actually adjusted or unaccounted for
	// tokens were detected.
	if adjustmentRegular != 0 {
		t.metrics.onTokenAdjustmentRegular(adjustmentRegular, flag)
	}
	if adjustmentElastic != 0 {
		t.metrics.onTokenAdjustmentElastic(adjustmentElastic, flag)
	}
	if adjustmentInflight != 0 {
		t.metrics.onTokenAdjustmentInFlight(adjustmentInflight, flag)
	}
	if unaccountedRegular != 0 {
		t.metrics.onUnaccountedRegular(unaccountedRegular)
	}
	if unaccountedElastic != 0 {
		t.metrics.onUnaccountedElastic(unaccountedElastic)
	}
	if unaccountedInflight != 0 {
		t.metrics.onUnaccountedInFlight(unaccountedInflight)
	}

	if expensiveLog {
		var inflightStr string
		if t.tokenType == SendToken {
			inflightStr = fmt.Sprintf(" inflight=%v", inflightTokens)
		}
		log.KvDistribution.Infof(ctx, "adjusted %v flow tokens (wc=%v stream=%v delta=%v flag=%v): regular=%v elastic=%v%s",
			t.tokenType, class, t.stream, delta, flag, regularTokens, elasticTokens, redact.SafeString(inflightStr))
	}
}

// testingSetTokens is used in tests to set the tokens for a given work class,
// ignoring any adjustments.
func (t *tokenCounter) testingSetTokens(
	ctx context.Context, wc WorkClassOrInflight, tokens kvflowcontrol.Tokens,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mu.counters[wc].adjustTokensLocked(ctx,
		tokens-t.mu.counters[wc].tokens, t.clock.PhysicalTime(), false /* isReset */, AdjNormal)
}

func (t *tokenCounter) GetAndResetStats(
	now time.Time,
) (regularStats, elasticStats, inflightStats deltaStats) {
	t.mu.Lock()
	defer t.mu.Unlock()

	regularStats = t.mu.counters[RegularWC].getAndResetStats(now)
	elasticStats = t.mu.counters[ElasticWC].getAndResetStats(now)
	if t.tokenType == SendToken {
		inflightStats = t.mu.counters[InFlight].getAndResetStats(now)
	}
	return regularStats, elasticStats, inflightStats
}
