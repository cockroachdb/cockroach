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

// reportAndAcquireConsumedCPU updates cumulative CPU counters and, if a CTT
// WorkQueue is attached, calls Admit to deduct the consumed CPU from the token
// bucket. This may block until tokens are available unless noWait is true.
func (h *SQLCPUHandle) reportAndAcquireConsumedCPU(
	ctx context.Context, diff time.Duration, noWait bool,
) error {
	if h.atGateway {
		h.p.cumulativeGatewayCPUNanos.Add(diff.Nanoseconds())
	} else {
		h.p.cumulativeDistSQLCPUNanos.Add(diff.Nanoseconds())
	}

	if h.wq == nil {
		return nil
	}

	// RequestedCount is set to the exact CPU consumed (from grunning), so the
	// WorkQueue's CPU time token estimator is skipped (see Admit). Because the
	// exact amount is deducted at Admit time, there is no estimate to correct,
	// so AdmittedWorkDone is not called. This also avoids training the KV
	// estimator with SQL CPU data, which would corrupt its estimates.
	//
	// TODO(wenyi): Currently we call Admit on every measureAndAdmit invocation,
	// which happens every ~1024 rows. This means each SQL goroutine takes the
	// WorkQueue mutex on every check. Consider reserving more tokens than the
	// exact amount consumed (e.g., 2x the last diff, or a smoothed estimate of
	// upcoming usage) and tracking remaining reservation locally. This would
	// allow subsequent measureAndAdmit calls to deduct from the local
	// reservation without calling Admit, reducing contention on the WorkQueue.
	workInfo := h.workInfo
	workInfo.RequestedCount = diff.Nanoseconds()
	workInfo.BypassAdmission = noWait
	_, err := h.wq.Admit(ctx, workInfo)
	return err
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

// Close is called when no more reporting is needed. It pools
// GoroutineCPUHandles that have been closed. GoroutineCPUHandles that are not
// yet closed are left for GC.
func (h *SQLCPUHandle) Close() {
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
