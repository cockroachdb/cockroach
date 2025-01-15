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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

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
	stats    struct {
		deltaStats
		noTokenStartTime time.Time
	}
}

type deltaStats struct {
	noTokenDuration                time.Duration
	tokensDeducted, tokensReturned kvflowcontrol.Tokens
	// Can only be non-zero for send tokens.
	tokensDeductedForceFlush, tokensDeductedPreventSendQueue kvflowcontrol.Tokens
}

func makeTokenCounterPerWorkClass(
	wc admissionpb.WorkClass, limit kvflowcontrol.Tokens, now time.Time,
) tokenCounterPerWorkClass {
	twc := tokenCounterPerWorkClass{
		wc:       wc,
		tokens:   limit,
		limit:    limit,
		signalCh: make(chan struct{}, 1),
	}
	twc.stats.noTokenStartTime = now
	return twc
}

// adjustTokensLocked adjusts the tokens for the given work class by delta.
func (twc *tokenCounterPerWorkClass) adjustTokensLocked(
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
		log.Fatalf(ctx, "unaccounted[%s]=%d delta=%d limit=%d",
			twc.wc, unaccounted, delta, twc.limit)
	}

	adjustment = twc.tokens - before
	return adjustment, unaccounted
}

func (twc *tokenCounterPerWorkClass) setLimitLocked(
	ctx context.Context, limit kvflowcontrol.Tokens, now time.Time,
) {
	before := twc.limit
	twc.limit = limit
	twc.adjustTokensLocked(ctx, twc.limit-before, now, false /* isReset */, AdjNormal)
}

func (twc *tokenCounterPerWorkClass) resetLocked(ctx context.Context, now time.Time) {
	if twc.limit <= twc.tokens {
		return
	}
	twc.adjustTokensLocked(ctx, twc.limit-twc.tokens, now, true /* isReset */, AdjNormal)
}

func (twc *tokenCounterPerWorkClass) signal() {
	select {
	// Non-blocking channel write that ensures it's topped up to 1 entry.
	case twc.signalCh <- struct{}{}:
	default:
	}
}

func (twc *tokenCounterPerWorkClass) getAndResetStats(now time.Time) deltaStats {
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

// tokenCounter holds flow tokens for {regular,elastic} traffic over a
// kvflowcontrol.Stream. It's used to synchronize handoff between threads
// returning and waiting for flow tokens.
type tokenCounter struct {
	settings *cluster.Settings
	clock    *hlc.Clock
	metrics  *TokenCounterMetrics
	// stream is the stream for which tokens are being adjusted, it is only used
	// in logging.
	stream    kvflowcontrol.Stream
	tokenType TokenType

	mu struct {
		syncutil.RWMutex

		counters [admissionpb.NumWorkClasses]tokenCounterPerWorkClass
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
	limit := tokensPerWorkClass{
		regular: kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)),
		elastic: kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)),
	}
	now := clock.PhysicalTime()

	t.mu.counters[admissionpb.RegularWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.RegularWorkClass, limit.regular, now)
	t.mu.counters[admissionpb.ElasticWorkClass] = makeTokenCounterPerWorkClass(
		admissionpb.ElasticWorkClass, limit.elastic, now)

	onChangeFunc := func(ctx context.Context) {
		now := t.clock.PhysicalTime()
		t.mu.Lock()
		defer t.mu.Unlock()

		t.mu.counters[admissionpb.RegularWorkClass].setLimitLocked(
			ctx, kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&settings.SV)), now)
		t.mu.counters[admissionpb.ElasticWorkClass].setLimitLocked(
			ctx, kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&settings.SV)), now)
	}

	kvflowcontrol.RegularTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	kvflowcontrol.ElasticTokensPerStream.SetOnChange(&settings.SV, onChangeFunc)
	kvflowcontrol.TokenCounterResetEpoch.SetOnChange(&settings.SV, func(ctx context.Context) {
		now := t.clock.PhysicalTime()
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.counters[admissionpb.RegularWorkClass].resetLocked(ctx, now)
		t.mu.counters[admissionpb.ElasticWorkClass].resetLocked(ctx, now)
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
		t.mu.counters[admissionpb.RegularWorkClass].tokens,
		t.mu.counters[admissionpb.RegularWorkClass].limit,
		t.mu.counters[admissionpb.ElasticWorkClass].tokens,
		t.mu.counters[admissionpb.ElasticWorkClass].limit)
}

// Stream returns the flow control stream for which tokens are being adjusted.
func (t *tokenCounter) Stream() kvflowcontrol.Stream {
	return t.stream
}

func (t *tokenCounter) tokensPerWorkClass() tokensPerWorkClass {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return tokensPerWorkClass{
		regular: t.tokensLocked(admissionpb.RegularWorkClass),
		elastic: t.tokensLocked(admissionpb.ElasticWorkClass),
	}
}

func (t *tokenCounter) tokens(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tokensLocked(wc)
}

func (t *tokenCounter) tokensLocked(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	return t.mu.counters[wc].tokens
}

func (t *tokenCounter) limit(wc admissionpb.WorkClass) kvflowcontrol.Tokens {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.counters[wc].limit
}

// TokensAvailable returns true if tokens are available, in which case handle
// is empty and should be ignored. If false, it returns a handle that may be
// used for waiting for tokens to become available.
func (t *tokenCounter) TokensAvailable(
	wc admissionpb.WorkClass,
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
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, flag TokenAdjustFlag,
) kvflowcontrol.Tokens {
	now := t.clock.PhysicalTime()
	var expensiveLog bool
	if log.V(2) {
		expensiveLog = true
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	tokensAvailable := t.tokensLocked(wc)
	if tokensAvailable <= 0 {
		return 0
	}

	adjust := min(tokensAvailable, tokens)
	t.adjustLocked(ctx, wc, -adjust, now, flag, expensiveLog)
	return adjust
}

// Deduct deducts (without blocking) flow tokens for the given work class. If
// there are not enough available tokens, the token counter will go into debt
// (negative available count) and still issue the requested number of tokens.
func (t *tokenCounter) Deduct(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, flag TokenAdjustFlag,
) {
	t.adjust(ctx, wc, -tokens, flag)
}

// Return returns flow tokens for the given work class. When disconnect is
// true, the tokens being returned are not associated with admission of any
// specific request, rather the leader returning tracked tokens for a replica
// it doesn't expect to hear from again.
func (t *tokenCounter) Return(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens, flag TokenAdjustFlag,
) {
	t.adjust(ctx, wc, tokens, flag)
}

// tokenWaitHandle is a handle for waiting for tokens to become available from
// a token counter.
type tokenWaitHandle struct {
	wc admissionpb.WorkClass
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
		log.Fatalf(ctx, "%v", errors.AssertionFailedf(
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
		log.Fatalf(ctx, "both requiredQuorum and requiredWaitCount are zero")
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
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	flag TokenAdjustFlag,
) {
	now := t.clock.PhysicalTime()
	var expensiveLog bool
	if log.V(2) {
		expensiveLog = true
	}
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.adjustLocked(ctx, class, delta, now, flag, expensiveLog)
	}()

}

func (t *tokenCounter) adjustLocked(
	ctx context.Context,
	class admissionpb.WorkClass,
	delta kvflowcontrol.Tokens,
	now time.Time,
	flag TokenAdjustFlag,
	expensiveLog bool,
) {
	var adjustment, unaccounted tokensPerWorkClass
	switch class {
	case admissionpb.RegularWorkClass:
		adjustment.regular, unaccounted.regular =
			t.mu.counters[admissionpb.RegularWorkClass].adjustTokensLocked(
				ctx, delta, now, false /* isReset */, flag)
			// Regular {deductions,returns} also affect elastic flow tokens.
		adjustment.elastic, unaccounted.elastic =
			t.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
				ctx, delta, now, false /* isReset */, flag)

	case admissionpb.ElasticWorkClass:
		// Elastic {deductions,returns} only affect elastic flow tokens.
		adjustment.elastic, unaccounted.elastic =
			t.mu.counters[admissionpb.ElasticWorkClass].adjustTokensLocked(
				ctx, delta, now, false /* isReset */, flag)
	}

	// Adjust metrics if any tokens were actually adjusted or unaccounted for
	// tokens were detected.
	if adjustment.regular != 0 || adjustment.elastic != 0 {
		t.metrics.onTokenAdjustment(adjustment, flag)
	}
	if unaccounted.regular != 0 || unaccounted.elastic != 0 {
		t.metrics.onUnaccounted(unaccounted)
	}
	if expensiveLog {
		log.Infof(ctx, "adjusted %v flow tokens (wc=%v stream=%v delta=%v flag=%v): regular=%v elastic=%v",
			t.tokenType, class, t.stream, delta, flag, t.tokensLocked(regular), t.tokensLocked(elastic))
	}
}

// testingSetTokens is used in tests to set the tokens for a given work class,
// ignoring any adjustments.
func (t *tokenCounter) testingSetTokens(
	ctx context.Context, wc admissionpb.WorkClass, tokens kvflowcontrol.Tokens,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mu.counters[wc].adjustTokensLocked(ctx,
		tokens-t.mu.counters[wc].tokens, t.clock.PhysicalTime(), false /* isReset */, AdjNormal)
}

func (t *tokenCounter) GetAndResetStats(now time.Time) (regularStats, elasticStats deltaStats) {
	t.mu.Lock()
	defer t.mu.Unlock()

	regularStats = t.mu.counters[admissionpb.RegularWorkClass].getAndResetStats(now)
	elasticStats = t.mu.counters[admissionpb.ElasticWorkClass].getAndResetStats(now)
	return regularStats, elasticStats
}
