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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// ContextWithoutSQLCPUHandle shadows any SQLCPUHandle in ctx, so that
// SQLCPUHandleFromContext returns nil.
func ContextWithoutSQLCPUHandle(ctx context.Context) context.Context {
	if SQLCPUHandleFromContext(ctx) == nil {
		return ctx
	}
	return ctxutil.WithFastValue(ctx, sqlCPUAdmissionHandleContextKey, nil)
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

	mu struct {
		syncutil.Mutex
		closed bool

		// gHandles holds the currently registered goroutines. Entries are
		// added by RegisterGoroutine and removed by GoroutineCPUHandle.Close.
		gHandles []*GoroutineCPUHandle
		// Backing for up to 2 goroutine handles, to avoid allocations in
		// gHandles when there are 2 or fewer goroutines.
		handlesBacking [2]*GoroutineCPUHandle
	}
}

func newSQLCPUAdmissionHandle(workInfo SQLWorkInfo, p *sqlCPUProviderImpl) *SQLCPUHandle {
	h := &SQLCPUHandle{
		workInfo: workInfo,
		p:        p,
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
	return h.LookupGoroutine() != nil
}

// LookupGoroutine returns the calling goroutine's registered handle, or nil.
// Unlike RegisterGoroutine, it never creates one.
func (h *SQLCPUHandle) LookupGoroutine() *GoroutineCPUHandle {
	gid := goid.Get()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, gh := range h.mu.gHandles {
		if gh.gid == gid {
			return gh
		}
	}
	return nil
}

// TestingNumRegisteredGoroutines returns the number of registered goroutines.
func (h *SQLCPUHandle) TestingNumRegisteredGoroutines() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.mu.gHandles)
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

// unregisterGoroutine removes gh from gHandles and pools it. If
// SQLCPUHandle.Close already ran, gh is left for GC so that only one path
// pools it.
func (h *SQLCPUHandle) unregisterGoroutine(gh *GoroutineCPUHandle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, cur := range h.mu.gHandles {
		if cur != gh {
			continue
		}
		last := len(h.mu.gHandles) - 1
		h.mu.gHandles[i] = h.mu.gHandles[last]
		h.mu.gHandles[last] = nil
		h.mu.gHandles = h.mu.gHandles[:last]
		gh.reset()
		goroutineCPUHandlePool.Put(gh)
		return
	}
}

// Close is called when no more reporting is needed. Handles still registered
// here were never closed by their goroutines; they are left for GC.
func (h *SQLCPUHandle) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.closed = true
	clear(h.mu.gHandles)
	h.mu.gHandles = nil
}

// GoroutineCPUHandle is used for CPU accounting on a single goroutine. It
// should be closed by calling Close when the goroutine's flow-related work is
// done. The Close call must be at the goroutine boundary, to structurally
// guarantee that MeasureAndAdmit is never called after Close. This structural
// guarantee is essential for safe pooling - the handle may be reused for a
// different goroutine after being pooled. A handle that is never closed stays
// registered until SQLCPUHandle.Close.
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

	// closed is set by Close. It is a debugging aid; the structural guarantee
	// above is the safety mechanism.
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

// Close accounts the goroutine's remaining CPU, unregisters the handle and
// pools it. It must be called at the goroutine boundary, and the handle must
// not be used afterwards.
func (h *GoroutineCPUHandle) Close(ctx context.Context) {
	_ = h.measureAndAdmit(ctx, true /* noWait */)
	h.closed.Store(true)
	h.h.unregisterGoroutine(h)
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
	return nil
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
}

func (p *sqlCPUProviderImpl) GetCumulativeSQLCPUNanos() (gatewayCPUNanos, distCPUNanos int64) {
	return p.cumulativeGatewayCPUNanos.Load(), p.cumulativeDistSQLCPUNanos.Load()
}

func (p *sqlCPUProviderImpl) GetHandle(workInfo SQLWorkInfo) *SQLCPUHandle {
	// TODO(sumeer): implement.
	return newSQLCPUAdmissionHandle(workInfo, p)
}

// NewSQLCPUProvider creates a new SQLCPUProvider.
//
// TODO(sumeer): real implementation.
func NewSQLCPUProvider() SQLCPUProvider {
	return &sqlCPUProviderImpl{}
}
