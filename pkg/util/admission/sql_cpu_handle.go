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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
)

// SQLWorkInfo captures identifying information about SQL work for CPU
// accounting and admission.
type SQLWorkInfo struct {
	// AtGateway is true if the work is being executed at a gateway node.
	AtGateway bool
	// TenantID is the id of the tenant. For single-tenant clusters, this will
	// always be the SystemTenantID.
	TenantID roachpb.TenantID
	// Priority is utilized within a tenant.
	Priority admissionpb.WorkPriority
	// CreateTime is equivalent to Time.UnixNano() at the creation time of this
	// work or a parent work (e.g. could be the start time of the transaction,
	// if this work was created as part of a transaction). It is used to order
	// work within a (WorkloadID, Priority) pair -- earlier CreateTime is given
	// preference.
	CreateTime int64
}

// SQLCPUProvider is used to get a SQLCPUHandle that is used for CPU
// accounting and admission.
type SQLCPUProvider interface {
	// GetHandle returns a SQLCPUHandle for the supplied work info.
	GetHandle(work SQLWorkInfo) *SQLCPUHandle
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
// TODO(sumeer): fill in more details.
type SQLCPUHandle struct {
	workInfo SQLWorkInfo
	p        *sqlCPUProviderImpl

	// workQueue is the CTT WorkQueue used to acquire CPU tokens. Nil when
	// CTT admission is disabled.
	workQueue *WorkQueue

	// reservation holds the remaining CPU time tokens (nanoseconds).
	// Accessed atomically via CAS on the fast path: goroutines deduct
	// only when the reservation has sufficient tokens, ensuring it never
	// goes negative. This allows goroutines to consume tokens without
	// blocking on a concurrent refill.
	reservation atomic.Int64

	// refillCh serializes refill attempts (WorkQueue.Admit calls).
	// Buffered channel of capacity 1: sending acquires the turn to call
	// Admit, receiving releases it. Using a channel (rather than a mutex)
	// allows goroutines to select on ctx.Done() while waiting for the
	// turn, enabling prompt cancellation when the context is cancelled.
	refillCh chan struct{}

	// lastRefillTime and lastHeuristic are the adaptive heuristic state.
	// Protected by the refillCh turn — only the goroutine that has sent
	// to refillCh may access these fields.
	lastRefillTime time.Time
	lastHeuristic  int64

	// tearingDown is set at the start of SQLCPUHandle.Close(). Used
	// only as a test-build assertion to catch lifecycle violations
	// where consumed() is called after Close().
	tearingDown atomic.Bool

	mu struct {
		syncutil.Mutex
		closed   bool
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle
	}
}

func newSQLCPUAdmissionHandle(
	workInfo SQLWorkInfo, p *sqlCPUProviderImpl, workQueue *WorkQueue,
) *SQLCPUHandle {
	h := &SQLCPUHandle{
		workInfo:  workInfo,
		p:         p,
		workQueue: workQueue,
		refillCh:  make(chan struct{}, 1),
	}
	h.mu.gHandles = h.mu.handlesBacking[:0]
	return h
}

// reportCPU atomically adds the CPU time difference to the appropriate
// cumulative counter.
func (h *SQLCPUHandle) reportCPU(diff time.Duration) {
	if h.workInfo.AtGateway {
		h.p.cumulativeGatewayCPUNanos.Add(diff.Nanoseconds())
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(diff.Nanoseconds())
	}
}

const (
	// refillGrowThreshold is the wall-clock duration below which we consider
	// refills too frequent and double the heuristic. Below this threshold,
	// contention on the WorkQueue mutex is the primary concern.
	refillGrowThreshold = time.Millisecond
	// refillDecayThreshold is the wall-clock duration above which we
	// consider the heuristic too large and halve it. Above this threshold,
	// overcounting in tenant.used is the primary concern. Between
	// refillGrowThreshold and refillDecayThreshold is the acceptable
	// deadband where the heuristic is stable.
	refillDecayThreshold = 5 * time.Millisecond
	// maxRefillHeuristic caps the heuristic to bound overcounting.
	// 10ms of CPU tokens per handle.
	maxRefillHeuristic = int64(10 * time.Millisecond)
)

// tryDeductReservation attempts to deduct diffNanos from the reservation
// via CAS. Returns true if successful (reservation had enough tokens).
// Never drives the reservation negative.
func (h *SQLCPUHandle) tryDeductReservation(diffNanos int64) bool {
	for {
		current := h.reservation.Load()
		if current < diffNanos {
			return false
		}
		if h.reservation.CompareAndSwap(current, current-diffNanos) {
			return true
		}
	}
}

// refillHeuristic returns the number of extra tokens to request beyond
// covering the current checkpoint's consumption. It uses exponential
// backoff based on wall time between refills:
//
//   - elapsed < 1ms: heuristic doubles (refills too frequent, reduce
//     WorkQueue contention)
//   - 1ms <= elapsed <= 5ms: heuristic unchanged (acceptable range)
//   - elapsed > 5ms: heuristic halves (over-requested, reduce
//     overcounting in tenant.used)
//
// This deadband eliminates steady-state oscillation: the heuristic
// grows until it reaches the acceptable range, then stabilizes. It
// only decays when the workload genuinely becomes lighter.
//
// Must be called while holding the refillCh turn.
func (h *SQLCPUHandle) refillHeuristic(consumed int64) int64 {
	now := timeutil.Now()
	if h.lastRefillTime.IsZero() {
		// Bootstrap: start with consumed (same as current 2x behavior).
		h.lastHeuristic = consumed
	} else {
		elapsed := now.Sub(h.lastRefillTime)
		if elapsed < refillGrowThreshold {
			// Came back too soon — double to reduce call frequency.
			h.lastHeuristic = min(
				h.lastHeuristic*2, maxRefillHeuristic,
			)
		} else if elapsed > refillDecayThreshold {
			// Buffer lasted too long — halve to reduce overcounting.
			h.lastHeuristic = max(consumed, h.lastHeuristic/2)
		}
		// else: in [1ms, 5ms] deadband — no change.
	}
	h.lastRefillTime = now
	return h.lastHeuristic
}

// consumed deducts measured CPU time from the local reservation. The fast
// path uses CAS to deduct atomically without any locking, so goroutines
// with sufficient reservation are never blocked by a concurrent refill.
//
// When the reservation is insufficient, the slow path acquires a turn
// via refillCh and calls WorkQueue.Admit to refill. Goroutines waiting
// for the turn can bail out on ctx.Done().
//
// When noWait is true (called from GoroutineCPUHandle.Close), Admit is
// called with BypassAdmission so the consumption is properly accounted
// in tenant.used and the granter without blocking.
func (h *SQLCPUHandle) consumed(ctx context.Context, diff time.Duration, noWait bool) error {
	if h.workQueue == nil {
		return nil
	}
	diffNanos := diff.Nanoseconds()

	if buildutil.CrdbTestBuild && h.tearingDown.Load() {
		panic(errors.AssertionFailedf(
			"consumed() called after SQLCPUHandle.Close()",
		))
	}

	// Fast path: CAS deducts only if the reservation has enough
	// tokens. Never drives the reservation negative. No lock or
	// channel interaction needed.
	if h.tryDeductReservation(diffNanos) {
		return nil
	}

	if noWait {
		// Closing: account the CPU via BypassAdmission (non-blocking).
		// Do NOT deduct from reservation — driving it negative would
		// poison CAS for other goroutines. No turn needed since
		// BypassAdmission just updates accounting without waiting.
		_ = h.workQueue.Admit(ctx, WorkInfo{
			TenantID:        h.workInfo.TenantID,
			Priority:        h.workInfo.Priority,
			CreateTime:      h.workInfo.CreateTime,
			RequestedCount:  diffNanos,
			BypassAdmission: true,
			IsSQLCPU:        true,
		})
		return nil
	}

	// Slow path: acquire the turn to call Admit, or bail if ctx is
	// cancelled.
	select {
	case h.refillCh <- struct{}{}:
		// Got the turn.
	case <-ctx.Done():
		return ctx.Err()
	}

	// Re-check: another goroutine may have refilled while we waited.
	if h.tryDeductReservation(diffNanos) {
		<-h.refillCh
		return nil
	}

	toRequest := diffNanos + h.refillHeuristic(diffNanos)
	resp, err := h.workQueue.Admit(ctx, WorkInfo{
		TenantID:       h.workInfo.TenantID,
		Priority:       h.workInfo.Priority,
		CreateTime:     h.workInfo.CreateTime,
		RequestedCount: toRequest,
		IsSQLCPU:       true,
	})
	if err != nil {
		<-h.refillCh
		return err
	}
	if resp.Enabled {
		// Add the heuristic portion to reservation. We consume diffNanos
		// ourselves, so only the extra (toRequest - diffNanos) becomes
		// buffer for other goroutines.
		h.reservation.Add(toRequest - diffNanos)
	}
	// If !resp.Enabled, AC is disabled — Admit took no tokens from
	// the granter. We must NOT add to reservation (would create phantom
	// tokens that corrupt the granter when returned at Close via
	// returnGrant). The goroutine proceeds without being tracked. Next
	// checkpoint will try Admit again; if AC stays disabled, Admit
	// returns instantly each time.
	<-h.refillCh
	return nil
}

// TODO(sumeer): see the comment
// https://github.com/cockroachdb/cockroach/pull/161952#pullrequestreview-3741525716
// on additional integrations that may need to call RegisterGoroutine.

// WorkInfo returns the SQLWorkInfo that was used to create this handle.
func (h *SQLCPUHandle) WorkInfo() SQLWorkInfo {
	return h.workInfo
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

func (h *SQLCPUHandle) returnUnusedReservation() {
	if h.workQueue == nil {
		return
	}
	h.refillCh <- struct{}{} // acquire turn
	remaining := h.reservation.Swap(0)
	if remaining != 0 {
		h.workQueue.AdmittedSQLWorkDone(h.workInfo.TenantID, remaining)
	}
	<-h.refillCh
}

// Close is called when no more reporting is needed. It pools
// GoroutineCPUHandles that have been closed. GoroutineCPUHandles that are not
// yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	h.tearingDown.Store(true)
	h.returnUnusedReservation()
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
	h.h.reportCPU(diff)
	return h.h.consumed(ctx, diff, noWait)
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
	// reported via SQLCPUHandle.reportCPU.
	cumulativeGatewayCPUNanos atomic.Int64
	// cumulativeDistSQLCPUNanos tracks the cumulative CPU time in nanoseconds
	// accounted for distributed SQL work. This value is monotonically
	// increasing and is updated atomically as CPU time is reported via
	// SQLCPUHandle.reportCPU.
	cumulativeDistSQLCPUNanos atomic.Int64
	// sv is the settings values used to check if CTT AC is enabled.
	sv *settings.Values
	// getWorkQueue returns the CTT WorkQueue for the given tenant. This
	// allows SQL to share the KV CTT WorkQueue, using the appropriate
	// tier (system vs app) based on tenant ID. Nil when CTT is not
	// available (e.g. in test configurations).
	getWorkQueue func(roachpb.TenantID) *WorkQueue
}

func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.cumulativeGatewayCPUNanos.Load(), p.cumulativeDistSQLCPUNanos.Load()
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo SQLWorkInfo) *SQLCPUHandle {
	var wq *WorkQueue
	if sqlCPUTimeTokenACIsEnabled(p.sv) {
		wq = p.getWorkQueue(workInfo.TenantID)
	}
	return newSQLCPUAdmissionHandle(workInfo, p, wq)
}

// NewSQLCPUProvider creates a new SQLCPUProvider. The sv parameter provides
// access to cluster settings for checking if CTT AC is enabled. The
// getWorkQueue function returns the CTT WorkQueue for a given tenant,
// allowing SQL to share the KV CTT WorkQueue. Both may be nil in test
// configurations where CTT is not available.
func NewSQLCPUProvider(
	sv *settings.Values, getWorkQueue func(roachpb.TenantID) *WorkQueue,
) SQLCPUProvider {
	return &sqlCPUProviderImpl{
		sv:           sv,
		getWorkQueue: getWorkQueue,
	}
}
