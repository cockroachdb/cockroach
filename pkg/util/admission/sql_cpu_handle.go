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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

	refillMu struct {
		syncutil.Mutex
		// reservation is accessed atomically without the lock on the
		// fast path (CAS deduction) and the noWait path. This is safe
		// because CAS is self-protecting (never drives negative).
		// closed is read under refillMu in the slow path; the noWait
		// path skips the closed check since BypassAdmission is safe
		// after Close. The lock serializes the slow path (Admit + Add)
		// with Close (set closed + Swap reservation).
		reservation atomic.Int64
		closed      atomic.Bool
		// lastRefillTime is the wall-clock time of the last Admit call,
		// used to compute the interval for adaptive sizing.
		lastRefillTime time.Time
		// lastHeuristic is the adaptive buffer size (in nanoseconds)
		// to request beyond the consumed CPU on the next Admit call.
		// The total RequestedCount is diffNanos + lastHeuristic.
		lastHeuristic int64
	}

	mu struct {
		syncutil.Mutex
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

// tryDeductReservation attempts to deduct diffNanos from the reservation
// via CAS. Returns true if successful (reservation had enough tokens).
// Never drives the reservation negative.
func (h *SQLCPUHandle) tryDeductReservation(diffNanos int64) bool {
	for {
		current := h.refillMu.reservation.Load()
		if current < diffNanos {
			return false
		}
		if h.refillMu.reservation.CompareAndSwap(current, current-diffNanos) {
			return true
		}
	}
}

const (
	// refillGrowThreshold is the wall-clock duration below which we
	// consider refills too frequent and double the heuristic. Below this
	// threshold, contention on the WorkQueue mutex is the primary concern.
	refillGrowThreshold = time.Millisecond
	// refillDecayThreshold is the wall-clock duration above which we
	// consider the heuristic too large and halve it. Above this threshold,
	// overcounting in tenant.used is the primary concern. Between
	// refillGrowThreshold and refillDecayThreshold is the acceptable
	// deadband where the heuristic is stable.
	refillDecayThreshold = 5 * time.Millisecond
	// maxRefillHeuristic caps the buffer portion of the heuristic to
	// bound overcounting. 10ms of CPU buffer per handle.
	maxRefillHeuristic = int64(10 * time.Millisecond)
)

// refillHeuristicLocked returns the total RequestedCount to pass to Admit:
// the consumed CPU (diffNanos) plus an adaptive buffer for future fast-path
// CAS deductions. The buffer (lastHeuristic) is adjusted based on the
// interval between Admit calls: doubled when calls are too frequent
// (< 1ms), halved when too infrequent (> 5ms), and stable otherwise.
// The buffer is capped at maxRefillHeuristic. The return value is always
// >= diffNanos, so (resp.requestedCount - diffNanos) is non-negative.
func (h *SQLCPUHandle) refillHeuristicLocked(diffNanos int64) int64 {
	h.refillMu.AssertHeld()
	now := timeutil.Now()
	if h.refillMu.lastHeuristic == 0 {
		// Bootstrap: seed buffer equal to consumed.
		h.refillMu.lastHeuristic = diffNanos
	} else {
		elapsed := now.Sub(h.refillMu.lastRefillTime)
		if elapsed < refillGrowThreshold {
			// Came back too soon — double to reduce call frequency.
			h.refillMu.lastHeuristic *= 2
		} else if elapsed > refillDecayThreshold {
			// Buffer lasted too long — halve to reduce overcounting.
			h.refillMu.lastHeuristic /= 2
		}
		// else: in [1ms, 5ms] deadband — no change.
	}
	// Cap buffer at maxRefillHeuristic.
	h.refillMu.lastHeuristic = min(h.refillMu.lastHeuristic, maxRefillHeuristic)
	h.refillMu.lastRefillTime = now
	return diffNanos + h.refillMu.lastHeuristic
}

// constructWorkInfo returns a copy of the handle's WorkInfo with the given
// RequestedCount and BypassAdmission values set.
func (h *SQLCPUHandle) constructWorkInfo(reqCount int64, noWait bool) WorkInfo {
	workInfo := h.workInfo
	workInfo.RequestedCount = reqCount
	workInfo.BypassAdmission = noWait
	return workInfo
}

// reportAndAcquireConsumedCPU updates cumulative CPU counters and, if a CTT
// WorkQueue is attached, deducts the consumed CPU from the token bucket. Three
// paths are tried in order:
//
//  1. Fast path — CAS deduction from the local reservation (no lock, no Admit).
//  2. noWait path — BypassAdmission Admit (non-blocking accounting only, used
//     by GoroutineCPUHandle.Close).
//  3. Slow path — acquire refillMu, call Admit to replenish the reservation.
//     May block until tokens are available.
func (h *SQLCPUHandle) reportAndAcquireConsumedCPU(
	ctx context.Context, diff time.Duration, noWait bool,
) error {
	h.reportCPU(diff)

	if h.wq == nil {
		return nil
	}

	diffNanos := diff.Nanoseconds()

	// Fast path: deduct from reservation via CAS. No lock needed.
	// After Close, reservation is 0 (Swap'd), so CAS fails immediately.
	if h.tryDeductReservation(diffNanos) {
		return nil
	}

	if noWait {
		// Account the CPU via BypassAdmission (non-blocking). This
		// updates tenant.used and tells the granter tokens were taken,
		// but never blocks. We do not deduct from reservation here
		// because driving it negative would break the CAS invariant
		// for other goroutines. This path is safe after Close because
		// BypassAdmission never blocks and keeps tenant.used accurate
		// for CPU consumed by goroutines that haven't closed yet.
		_, _ = h.wq.Admit(ctx, h.constructWorkInfo(diffNanos, true))
		return nil
	}

	// Slow path: serialize refills under refillMu so only one goroutine
	// calls Admit at a time.
	h.refillMu.Lock()
	defer h.refillMu.Unlock()

	// Re-check after acquiring the lock: Close may have run, or another
	// goroutine's refill may have replenished the reservation.
	if h.refillMu.closed.Load() {
		return nil
	}
	if h.tryDeductReservation(diffNanos) {
		return nil
	}

	// Request the consumed CPU plus a buffer (see refillHeuristic). Setting
	// RequestedCount > 0 skips the WorkQueue's CPU time token estimator
	// (see callerSetRequestedCount in Admit), which is designed for KV
	// requests and should not be trained with SQL CPU data. The buffer
	// portion goes into reservation for future fast-path CAS deductions.
	// Because the exact amount is deducted at Admit time, there is no
	// estimate to correct, so AdmittedWorkDone is not called.
	resp, err := h.wq.Admit(ctx, h.constructWorkInfo(h.refillHeuristicLocked(diffNanos), false))
	if err != nil {
		return err
	}

	if resp.Enabled {
		// Add the buffer portion (requestedCount - diffNanos) to the
		// reservation. Close cannot race here because we hold refillMu;
		// it will return these tokens when it acquires the lock.
		h.refillMu.reservation.Add(resp.requestedCount - diffNanos)
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

// setClosed marks the handle as closed and returns any remaining reservation
// tokens to the granter. Holding refillMu ensures no concurrent slow-path
// refill is in progress — any in-flight Admit must complete and release the
// lock before we proceed, so Swap(0) captures all outstanding tokens.
func (h *SQLCPUHandle) setClosed() {
	h.refillMu.Lock()
	defer h.refillMu.Unlock()
	h.refillMu.closed.Store(true)
	if h.wq != nil {
		remaining := h.refillMu.reservation.Swap(0)
		h.wq.AdmittedSQLWorkDone(h.workInfo.TenantID, remaining)
	}
}

// Close is called when no more reporting is needed. It pools
// GoroutineCPUHandles that have been closed. GoroutineCPUHandles that are not
// yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
	h.setClosed()
	h.mu.Lock()
	defer h.mu.Unlock()
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
