// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/petermattis/goid"
)

// SQLCPUProvider is used to get a SQLCPUHandle that is used for CPU
// accounting and admission.
type SQLCPUProvider interface {
	// GetHandle returns a SQLCPUHandle for the supplied work info.
	// atGateway indicates whether the work is being executed at the
	// gateway node, used for CPU accounting to distinguish gateway
	// vs DistSQL CPU usage.
	GetHandle(work WorkInfo, atGateway bool) *SQLCPUHandle
	GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64)
}

var sqlCPUAdmissionHandleContextKey = ctxutil.RegisterFastValueKey()

// ContextWithSQLCPUHandle returns a Context wrapping the supplied handle, if
// any.
func ContextWithSQLCPUHandle(ctx context.Context, h *SQLCPUHandle) context.Context {
	if h == nil {
		return ctx
	}
	return ctxutil.WithFastValue(ctx, sqlCPUAdmissionHandleContextKey, h)
}

// SQLCPUHandleFromContext returns the handle contained in the Context, if
// any.
func SQLCPUHandleFromContext(ctx context.Context) *SQLCPUHandle {
	val := ctxutil.FastValue(ctx, sqlCPUAdmissionHandleContextKey)
	h, ok := val.(*SQLCPUHandle)
	if !ok {
		return nil
	}
	return h
}

var goroutineCPUHandlePool = sync.Pool{
	New: func() interface{} {
		return &GoroutineCPUHandle{}
	},
}

// SQLCPUHandle manages CPU accounting and admission for SQL work across
// multiple goroutines.
//
// SQL CPU admission differs from KV admission in several important ways:
//
//   - Admission model: KV uses an estimate-then-correct model. WorkQueue.Admit
//     is called before execution with an estimated RequestedCount (via
//     cpuTimeTokenEstimator), and AdmittedWorkDone is called after execution to
//     correct the estimate using the actual CPU time from grunning. SQL CPU
//     admission uses a measure-then-admit model: measureAndAdmit is called
//     periodically during execution (every ~1024 rows or every vectorized batch
//     via the CancelChecker), and the exact CPU consumed since the last call is
//     known from grunning. There is no estimation and no correction step.
//
//   - Lifetime: A KV request typically consumes microseconds to low
//     milliseconds of on-CPU time (as measured by grunning), with a
//     single Admit/AdmittedWorkDone pair. A SQLCPUHandle is created
//     per-statement (in MakeCPUHandle) and lives until the statement
//     completes. It spans the entire operator tree: each goroutine
//     in the DistSQL flow registers via RegisterGoroutine and gets a
//     GoroutineCPUHandle. For a simple query this may be a single goroutine
//     lasting milliseconds; for a long-running OLAP query, IMPORT, or BACKUP,
//     the handle can live for minutes or hours across many goroutines, with
//     measureAndAdmit called thousands or millions of times.
//
//   - Why admit-after-consume is acceptable: The goroutine runs freely between
//     measureAndAdmit calls — CPU is consumed without permission and then
//     retroactively deducted from the shared token bucket. If the bucket is
//     depleted, the goroutine blocks, preventing it from consuming more CPU.
//     Each uncontrolled burst is bounded by the work between two CancelChecker
//     calls (~1024 rows), so the amount of unpermitted CPU per check is limited.
//     The throttling does not prevent past usage but gates future usage.
type SQLCPUHandle struct {
	workInfo  WorkInfo
	atGateway bool
	p         *sqlCPUProviderImpl
	wq        *WorkQueue

	// admitTurn gates the blocking WorkQueue.Admit path for this handle. A
	// statement can run many DistSQL goroutines; when the token reservation is
	// short, each of them could otherwise enter Admit at once. Serializing that
	// path keeps at most one blocking Admit per handle at a time.
	//
	// Any goroutine that needs that path must take the turn first: buffer size
	// 1, send to acquire and deferred receive to release. The send blocks while
	// another goroutine holds the turn (so only one runs the serialized section
	// at a time). A channel is used so that wait can select on <-ctx.Done();
	// Mutex.Lock cannot.
	//
	// Holding the turn is a precondition for blocking Admit, not a
	// commitment to call it. After the turn is acquired, two conditions
	// can prevent blocking Admit from running:
	//  1. The handle was closed while waiting for the turn. The
	//     remaining deficit is accounted via BypassAdmission.
	//  2. The previous turn-holder refilled reservation while this
	//     goroutine waited, and the second deductFromReservation
	//     covers the shortfall entirely. No Admit call is needed.
	admitTurn chan struct{}

	// nowFn returns the current monotonic time; overridable for tests.
	nowFn func() crtime.Mono

	// lastAdmitBuffer and lastAdmitMono are state for refillHeuristic,
	// read/written only inside the admitTurn-serialized slow-path section.
	// lastAdmitMono == 0 marks "no Admit yet" and is checked explicitly
	// (see refillHeuristic).
	lastAdmitBuffer int64
	lastAdmitMono   crtime.Mono

	mu struct {
		syncutil.Mutex

		// Once true, cannot be set to false.
		closed bool

		// reservation holds tokens obtained from WorkQueue.Admit in
		// excess of the current checkpoint's deficit. These surplus
		// tokens cover future checkpoints without re-acquiring
		// WorkQueue.mu. Without the reservation, every measureAndAdmit
		// call would call WorkQueue.Admit directly, acquiring
		// WorkQueue.mu on every checkpoint. The reservation allows
		// lock-free CAS deductions that skip the Admit call entirely.
		//
		// reservation is modified in three places:
		//
		//  - CAS decrement (tryDeductReservation): does not need
		//    mu. Goroutines race only with each other and with
		//    Close's Swap(0); a failed CAS retries and eventually
		//    sees 0. No tokens are lost.
		//
		//  - Swap(0) drain (Close): does not need mu. Close sets
		//    closed=true under mu first, which prevents any future
		//    Add. After that, Swap(0) races only with CAS
		//    decrements, which is safe (see above).
		//
		//  - Add increment (after Admit): needs mu. The caller
		//    must check closed before adding — if closed, tokens
		//    go to AdmittedSQLWorkDone instead. Without mu, Close
		//    could set closed=true and Swap(0) between the check
		//    and the Add, leaking tokens.
		//
		// INVARIANT: reservation >= 0.
		// INVARIANT: closed == true => reservation == 0.
		reservation atomic.Int64

		// reservationSourceGroup is the groupKey of the container the
		// most recent successful Admit credited. Close uses it to
		// return the drained reservation to the same container,
		// without re-deriving from the WorkQueue's current
		// useResourceGroup state — which may have flipped between
		// Admit and Close. In RM mode the container is rg-keyed; the
		// prior tenantGroupKey-based lookup missed and silently leaked
		// the drained reservation.
		//
		// Invariant: at any moment, every token in reservation came
		// from the most recent successful Admit. Each Admit only fires
		// when tryDeductReservation found reservation < needed (i.e.,
		// drained reservation to 0); Admit then adds its buffer to a
		// reservation that is 0. By the time the next Admit fires, that
		// buffer has been drained back to 0 (which is what triggered
		// the next Admit). The slow path is serialized via admitTurn,
		// so no two Admits' buffers ever coexist in reservation.
		// Therefore reservationSourceGroup is always the exact source
		// of any tokens Close drains.
		reservationSourceGroup groupKey

		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle
	}
}

func newSQLCPUAdmissionHandle(
	workInfo WorkInfo, atGateway bool, p *sqlCPUProviderImpl, wq *WorkQueue,
) *SQLCPUHandle {
	h := &SQLCPUHandle{
		workInfo:  workInfo,
		atGateway: atGateway,
		p:         p,
		wq:        wq,
		admitTurn: make(chan struct{}, 1),
		nowFn:     crtime.NowMono,
	}
	h.mu.gHandles = h.mu.handlesBacking[:0]
	return h
}

// reportCPU atomically adds the CPU time difference to the appropriate
// cumulative counter.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	if h.atGateway {
		h.p.cumulativeGatewayCPUNanos.Add(diff.Nanoseconds())
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(diff.Nanoseconds())
	}
}

// tryDeductReservation deducts up to diffNanos from reservation via
// CAS. Returns the amount grabbed (may be less than diffNanos).
func (h *SQLCPUHandle) tryDeductReservation(diffNanos int64) int64 {
	for {
		current := h.mu.reservation.Load()
		if current <= 0 {
			return 0
		}
		grab := min(current, diffNanos)
		if h.mu.reservation.CompareAndSwap(current, current-grab) {
			return grab
		}
	}
}

// maxRefillBuffer caps the reservation buffer per Admit call to
// prevent large checkpoints from holding excessive tokens idle.
const maxRefillBuffer = int64(1 * time.Millisecond)

// bufferSeed is the smallest non-zero buffer the refill heuristic ever
// requests; it kicks off the doubling ramp.
const bufferSeed = int64(10 * time.Microsecond)

// idleResetThreshold is the gap between successive slow-path Admits above
// which the buffer resets to zero — if the previous buffer wasn't drained
// within this window, the handle isn't hot enough to warrant one.
const idleResetThreshold = 100 * time.Millisecond

// refillHeuristic returns deficit + buffer to request from
// WorkQueue.Admit when the local reservation runs out. The buffer
// reflects slow-path call frequency, not deficit magnitude:
//
//   - Fresh handle: buffer = 0. Many SQLCPUHandles are short-lived; a
//     buffer for them is wasted since it's just returned via
//     AdmittedSQLWorkDone at Close.
//   - Subsequent close-together Admits: buffer ramps from bufferSeed,
//     doubling each call, capped at maxRefillBuffer.
//   - After an idle gap >= idleResetThreshold: buffer resets to 0.
//
// REQUIRES: caller holds admitTurn (state is mutated unsynchronized).
func (h *SQLCPUHandle) refillHeuristic(deficit int64) int64 {
	now := h.nowFn()
	switch {
	case h.lastAdmitMono == 0 || now.Sub(h.lastAdmitMono) >= idleResetThreshold:
		// First Admit (or first admit after reset).
		h.lastAdmitBuffer = 0
	case h.lastAdmitBuffer == 0:
		// Second Admit, set to seed value.
		h.lastAdmitBuffer = bufferSeed
	default:
		h.lastAdmitBuffer = min(2*h.lastAdmitBuffer, maxRefillBuffer)
	}
	h.lastAdmitMono = now
	return deficit + h.lastAdmitBuffer
}

// constructWorkInfo returns a WorkInfo copy with the given
// RequestedCount and BypassAdmission.
//
// NB: setting RequestedCount > 0 causes WorkQueue.Admit to skip the
// cpuTimeTokenEstimator (see callerSetRequestedCount in Admit). This is
// required for SQL CPU admission, which already knows the exact CPU consumed
// from grunning and does not need/want estimation.
//
// REQUIRES: reqCount > 0
func (h *SQLCPUHandle) constructWorkInfo(reqCount int64, noWait bool) WorkInfo {
	workInfo := h.workInfo
	workInfo.RequestedCount = reqCount
	workInfo.BypassAdmission = noWait
	return workInfo
}

func (h *SQLCPUHandle) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.closed
}

// deductFromReservation deducts what it can from reservation via CAS.
// Returns the shortfall still needed after the deduction.
func (h *SQLCPUHandle) deductFromReservation(needed int64) (shortfall int64) {
	grabbed := h.tryDeductReservation(needed)
	return needed - grabbed
}

// reportAndAcquireConsumedCPU acquires tokens for consumed CPU.
//
//  1. Fast path: reservation covers the deficit via CAS. No Admit.
//  2. noWait path: deduct what is available, account the rest via
//     BypassAdmission.
//  3. Slow path: take a turn via admitTurn, call Admit for the
//     deficit plus a buffer, store the buffer in reservation.
//
// In winding-down cases (noWait, context cancellation, Close having
// run), the goroutine deducts what it can and accounts the rest via
// BypassAdmission. It never blocks and never refills the reservation.
//
// INVARIANT: Every token obtained from WorkQueue.Admit must be accounted for:
// tokens are either
// (a) consumed to pay for measured CPU usage,
// (b) held in reservation for future usage, or
// (c) returned via AdmittedSQLWorkDone when the handle is closed.
//
// When SQLCPUHandle is closed, any remaining reservation is returned
// via AdmittedSQLWorkDone, so this code does not leak tokens.
func (h *SQLCPUHandle) reportAndAcquireConsumedCPU(
	ctx context.Context, diff time.Duration, noWait bool,
) error {
	h.reportCPU(diff)

	if h.wq == nil {
		return nil
	}

	diffNanos := diff.Nanoseconds()

	// Deduct from reservation (lock-free CAS).
	remaining := h.deductFromReservation(diffNanos)

	if noWait {
		// Winding down: account the deficit without blocking.
		if remaining > 0 {
			_, _ = h.wq.Admit(ctx, h.constructWorkInfo(remaining, true /*noWait*/))
		}
		return nil
	}

	// Fast path: reservation covered the deficit.
	if remaining == 0 {
		return nil
	}

	// Slow path: serialize blocking Admit calls via admitTurn.
	select {
	case h.admitTurn <- struct{}{}:
		defer func() { <-h.admitTurn }()
	case <-ctx.Done():
		// Winding down: account the deficit without blocking.
		_, _ = h.wq.Admit(ctx, h.constructWorkInfo(remaining, true /*noWait*/))
		return ctx.Err()
	}

	// Close may have run while waiting for the turn.
	if h.isClosed() {
		// Winding down: account the deficit without blocking.
		_, _ = h.wq.Admit(ctx, h.constructWorkInfo(remaining, true /*noWait*/))
		return nil
	}

	// The previous turn-holder may have refilled the reservation.
	remaining = h.deductFromReservation(remaining)
	if remaining == 0 {
		return nil
	}

	// Request the deficit plus a buffer (see refillHeuristic).
	resp, err := h.wq.Admit(ctx, h.constructWorkInfo(h.refillHeuristic(remaining), false /*noWait*/))
	if err != nil {
		// Error is only due to context cancellation. Account the deficit
		// without blocking.
		_, _ = h.wq.Admit(ctx, h.constructWorkInfo(remaining, true /*noWait*/))
		return err
	}
	if resp.Enabled {
		buffer := resp.requestedCount - remaining
		if buffer > 0 {
			closed := func() bool {
				h.mu.Lock()
				defer h.mu.Unlock()
				if h.mu.closed {
					return true
				}
				// NB: closed check and reservation.Add must be atomic under mu.
				// Without the lock, this goroutine could read closed=false, then
				// Close sets closed=true and Swap(0) drains reservation, then this
				// goroutine does Add(buffer), leaking tokens.
				h.mu.reservation.Add(buffer)
				h.mu.reservationSourceGroup = resp.groupKey
				return false
			}()
			// Close already ran and drained reservation. Return the buffer
			// directly using this admit's resp.groupKey —
			// reservationSourceGroup wasn't stored (we're in the closed
			// branch above), so we use the key from the just-completed
			// Admit.
			if closed {
				h.wq.AdmittedSQLWorkDone(resp.groupKey, buffer)
			}
		}
	}
	return nil
}

// TODO(sumeer): see the comment
// https://github.com/cockroachdb/cockroach/pull/161952#pullrequestreview-3741525716
// on additional integrations that may need to call RegisterGoroutine.

// AtGateway returns true if this handle is for work executing at the gateway
// node, as opposed to DistSQL work on a remote node.
func (h *SQLCPUHandle) AtGateway() bool {
	return h.atGateway
}

// IsGoroutineRegistered returns true if the calling goroutine already has a
// registered handle. Unlike RegisterGoroutine, this does not create a new
// handle as a side effect.
func (h *SQLCPUHandle) IsGoroutineRegistered() bool {
	gid := goid.Get()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, gh := range h.mu.gHandles {
		if gh.gid == gid {
			return true
		}
	}
	return false
}

// RegisterGoroutine returns a GoroutineCPUHandle to use for reporting and
// admission. If the goroutine was already registered, the existing handle
// will be returned. CPU time is accounted for at this goroutine from the
// instant the handle was first created for this goroutine. The cpu accounting
// will end for this goroutine when it is closed by calling
// GoroutineCPUHandle.Close, or if never closed, until the last call to
// GoroutineCPUHandle.MeasureAndAdmit.
func (h *SQLCPUHandle) RegisterGoroutine() *GoroutineCPUHandle {
	gid := goid.Get()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, gh := range h.mu.gHandles {
		if gh.gid == gid {
			// Already registered.
			return gh
		}
	}
	// Not registered, create a new handle.
	gh := newGoroutineCPUHandle(gid, h)
	h.mu.gHandles = append(h.mu.gHandles, gh)

	return gh
}

// Close sets closed=true under mu, drains reservation via Swap(0),
// and returns any remaining tokens via AdmittedSQLWorkDone.
//
// Close returns even if there exist goroutines in reportAndAcquireConsumedCPU
// blocked on Admit. reportAndAcquireConsumedCPU that raced with Close are
// handled in two ways:
//   - Before taking the turn: they see closed=true after acquiring
//     the turn and fall back to BypassAdmission.
//   - After Admit returns: they see closed=true under mu and return
//     the surplus buffer via AdmittedSQLWorkDone instead of adding
//     it to reservation.
//
// Closed GoroutineCPUHandles are pooled; unclosed ones are left for GC.
func (h *SQLCPUHandle) Close() {
	// After this, goroutines in reportAndAcquireConsumedCPU observe
	// closed=true when they acquire mu.
	func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.mu.closed = true
		for i, gh := range h.mu.gHandles {
			if gh.closed.Load() {
				gh.reset()
				goroutineCPUHandlePool.Put(gh)
			}
			h.mu.gHandles[i] = nil
		}
		h.mu.gHandles = nil
	}()
	if h.wq == nil {
		return
	}
	// Drain reservation outside the lock. Swap(0) races safely with CAS
	// deductions (CAS retries on conflict and finds 0). NB: No new tokens should
	// be added to mu.reservation after this.
	remaining := h.mu.reservation.Swap(0)
	if remaining > 0 {
		// closed=true was set under mu before this Swap, so no further
		// Admit can update reservationSourceGroup; reading it here
		// without mu is safe.
		h.wq.AdmittedSQLWorkDone(h.mu.reservationSourceGroup, remaining)
	}
}

// GoroutineCPUHandle is used for CPU accounting on a single goroutine. It
// should be closed by calling Close when the goroutine's flow-related work is
// done. The Close call must be at the goroutine boundary, to structurally
// guarantee that MeasureAndAdmit is never called after Close. This structural
// guarantee is essential for safe pooling - the handle may be reused for a
// different goroutine after being pooled. It is safe to never Close a
// GoroutineCPUHandle -- it will be garbage collected.
type GoroutineCPUHandle struct {
	gid int64
	h   *SQLCPUHandle

	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// cpuAccounted is the total CPU time accounted for on this goroutine.
	// Monotonically increasing.
	cpuAccounted time.Duration

	pauseDur   time.Duration
	paused     int
	pauseStart time.Duration

	// closed is set to true when Close() is called. This is primarily for
	// debugging - the structural guarantee (Close at goroutine boundary)
	// is the primary safety mechanism, not this field.
	closed atomic.Bool
}

func newGoroutineCPUHandle(gid int64, h *SQLCPUHandle) *GoroutineCPUHandle {
	gh := goroutineCPUHandlePool.Get().(*GoroutineCPUHandle)
	*gh = GoroutineCPUHandle{
		gid:      gid,
		h:        h,
		cpuStart: grunning.Time(),
	}
	return gh
}

// reset clears all fields in preparation for returning to the pool.
func (h *GoroutineCPUHandle) reset() {
	*h = GoroutineCPUHandle{}
}

// Close marks this handle as closed. It must be called at the goroutine
// boundary when the goroutine's SQL work is done. After Close, the handle
// will be pooled when SQLCPUHandle.Close is called, so MeasureAndAdmit must
// never be called after Close.
func (h *GoroutineCPUHandle) Close(ctx context.Context) {
	_ = h.measureAndAdmit(ctx, true /* noWait */)
	h.closed.Store(true)
}

// MeasureAndAdmit should be called frequently. The callee will measure the
// CPU time spent in the goroutine and decide whether more CPU needs to be
// allocated. If more CPU is needed, it can block in acquiring CPU tokens.
// Returns a non-nil error iff the context is canceled while waiting.
//
// TODO(sumeer): implement the measurement and admission logic.
func (h *GoroutineCPUHandle) MeasureAndAdmit(ctx context.Context) error {
	return h.measureAndAdmit(ctx, false /* noWait */)
}

// measureAndAdmit is the internal implementation of MeasureAndAdmit. The
// noWait parameter should only be set to true when the work is finished, so
// only measurement is desired (blocking is no longer productive). When noWait
// is true, this function never returns an error.
//
// See SQLCPUHandle for how SQL CPU admission differs from KV admission.
func (h *GoroutineCPUHandle) measureAndAdmit(ctx context.Context, noWait bool) error {
	if h.paused > 0 {
		return nil
	}
	cpuUsed := grunning.Elapsed(h.cpuStart, grunning.Time()) - h.pauseDur
	diff := cpuUsed - h.cpuAccounted
	if diff <= 0 {
		return nil
	}
	// TODO(sumeer): adding this diff to an atomic in SQLCPUHandle may be too
	// much overhead. An alternative would be implement an atomic here, and
	// only update the SQLCPUHandle when enough has accumulated. The reason
	// we would need an atomic here is that when SQLCPUHandle is closed, it
	// needs to reach in and grab whatever CPU has not yet been reported.
	h.cpuAccounted += diff
	return h.h.reportAndAcquireConsumedCPU(ctx, diff, noWait)
}

// PauseMeasuring is used to pause the CPU accounting for this goroutine. It
// must be paired with UnpauseMeasuring. Used when the goroutine is being used
// for KV work. If PauseMeasuring is called multiple times, an equal number of
// UnpauseMeasuring calls are needed to resume measuring.
func (h *GoroutineCPUHandle) PauseMeasuring() {
	h.paused++
	if h.paused == 1 {
		h.pauseStart = grunning.Time()
	}
}

// UnpauseMeasuring resumes CPU accounting for this goroutine.
func (h *GoroutineCPUHandle) UnpauseMeasuring() {
	h.paused--
	if h.paused == 0 {
		h.pauseDur += grunning.Elapsed(h.pauseStart, grunning.Time())
	}
}

type sqlCPUProviderImpl struct {
	// cumulativeGatewayCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for SQL work executed at gateway nodes. This value is
	// monotonically increasing and is updated atomically as CPU time is
	// reported via SQLCPUHandle.reportAndAcquireConsumedCPU.
	cumulativeGatewayCPUNanos atomic.Int64
	// cumulativeDistSQLCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for distributed SQL work. This value is monotonically
	// increasing and is updated atomically as CPU time is reported via
	// SQLCPUHandle.reportAndAcquireConsumedCPU.
	cumulativeDistSQLCPUNanos atomic.Int64
	// sv is the settings values used to check if CTT AC is enabled.
	sv *settings.Values
	// getWorkQueue returns the CTT WorkQueue for the given tenant. This
	// allows SQL to share the KV CTT WorkQueue, using the appropriate
	// tier (system vs app) based on tenant ID. Can be nil in testing.
	getWorkQueue func(roachpb.TenantID) *WorkQueue
}

func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.cumulativeGatewayCPUNanos.Load(), p.cumulativeDistSQLCPUNanos.Load()
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo WorkInfo, atGateway bool) *SQLCPUHandle {
	var wq *WorkQueue
	if p.getWorkQueue != nil && sqlCPUTimeTokenACIsEnabled(p.sv) {
		wq = p.getWorkQueue(workInfo.TenantID)
	}
	return newSQLCPUAdmissionHandle(workInfo, atGateway, p, wq)
}

// NewSQLCPUProvider creates a new SQLCPUProvider. The sv parameter is required
// and provides access to cluster settings for checking if SQL CPU time token
// AC is enabled. The getWorkQueue function returns the CTT WorkQueue for a
// given tenant, allowing SQL to share the KV CTT WorkQueue; it may be nil when
// CTT AC is not available (e.g. separate-process tenants).
func NewSQLCPUProvider(
	sv *settings.Values, getWorkQueue func(roachpb.TenantID) *WorkQueue,
) SQLCPUProvider {
	return &sqlCPUProviderImpl{
		sv:           sv,
		getWorkQueue: getWorkQueue,
	}
}
