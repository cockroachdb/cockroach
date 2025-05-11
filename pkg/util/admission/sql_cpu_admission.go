// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/petermattis/goid"
)

type SQLWorkInfo struct {
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

// SQLCPUAdmissionQueue is used to admit SQL work that consumes CPU.
type SQLCPUAdmissionQueue interface {
	GetHandle(work WorkInfo) *SQLCPUAdmissionHandle
	admitCPU(ctx context.Context, work SQLWorkInfo, cpuTime time.Duration) error
}

type sqlCPUAdmissionHandleKey struct{}

// ContextWithSQLCPUAdmissionHandle returns a Context wrapping the supplied
// handle, if any.
func ContextWithSQLCPUAdmissionHandle(
	ctx context.Context, h *SQLCPUAdmissionHandle,
) context.Context {
	if h == nil {
		return ctx
	}
	return context.WithValue(ctx, sqlCPUAdmissionHandleKey{}, h)
}

// SQLCPUAdmissionHandleFromContext returns the handle contained in the
// Context, if any.
func SQLCPUAdmissionHandleFromContext(ctx context.Context) *SQLCPUAdmissionHandle {
	val := ctx.Value(sqlCPUAdmissionHandleKey{})
	h, ok := val.(*SQLCPUAdmissionHandle)
	if !ok {
		return nil
	}
	return h
}

// SQLCPUAdmissionHandle manages CPU admission for SQL work across multiple
// goroutines.
type SQLCPUAdmissionHandle struct {
	workInfo              SQLWorkInfo
	allocationGranularity time.Duration
	q                     SQLCPUAdmissionQueue

	mu struct {
		syncutil.Mutex
		closed    bool
		remaining time.Duration
		gHandles  []*GoroutineCPUHandle
	}
	// acquiringCh is a buffered channel with a capacity of 1.
	acquiringCh chan struct{}
}

func newSQLCPUAdmissionHandle(
	workInfo SQLWorkInfo, allocationGranularity time.Duration, q SQLCPUAdmissionQueue,
) *SQLCPUAdmissionHandle {
	h := &SQLCPUAdmissionHandle{
		workInfo:              workInfo,
		allocationGranularity: allocationGranularity,
		q:                     q,
		acquiringCh:           make(chan struct{}, 1),
	}
	return h
}

// TryRegisterGoroutine returns a GoroutineCPUHandle to use for reporting and
// admission.
func (h *SQLCPUAdmissionHandle) TryRegisterGoroutine() *GoroutineCPUHandle {
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

// Close is called when no more reporting is needed. It is ok for some stray
// reporting to be attempted after Close, but it will be ignored.
func (h *SQLCPUAdmissionHandle) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.closed = true
}

func (h *SQLCPUAdmissionHandle) getCPU(ctx context.Context, d time.Duration) error {
	haveTurn := false
	for {
		done := func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.mu.closed || h.mu.remaining >= d {
				h.mu.remaining -= d
				if haveTurn {
					<-h.acquiringCh // Release the acquiring channel.
				}
				return true
			}
			if !haveTurn {
				// Optimistic acquire while holding the mutex.
				select {
				case h.acquiringCh <- struct{}{}:
					haveTurn = true
				default:
				}
			}
			return false
		}()
		if done {
			return nil
		}
		// If haveTurn, know that remaining is insufficient and not closed, since
		// haveTurn was true before we release the mutex.
		if haveTurn {
			// Try to acquire CPU.
			toAcquire := max(d, h.allocationGranularity)
			err := h.q.admitCPU(ctx, h.workInfo, toAcquire)
			if err != nil {
				<-h.acquiringCh // Release the acquiring channel.
				return err
			}
			func() {
				h.mu.Lock()
				defer h.mu.Unlock()
				h.mu.remaining += toAcquire - d
				if h.mu.remaining < 0 {
					panic("todo")
				}
			}()
			<-h.acquiringCh // Release the acquiring channel.
			return nil
		}
		// INVARIANT: !haveTurn.
		select {
		case <-ctx.Done():
			return ctx.Err() // Context canceled while waiting for CPU.
		case h.acquiringCh <- struct{}{}:
			// Got turn to wait for CPU. Will loop back around to see if closed or
			// remaining is already sufficient.
			haveTurn = true
		}
	}
}

type GoroutineCPUHandle struct {
	gid int64
	h   *SQLCPUAdmissionHandle

	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// cpuAllocated is the total CPU time allocated to this goroutine.
	cpuAllocated time.Duration

	pauseDur   time.Duration
	paused     int
	pauseStart time.Duration
}

func newGoroutineCPUHandle(gid int64, h *SQLCPUAdmissionHandle) *GoroutineCPUHandle {
	gh := &GoroutineCPUHandle{
		gid:      gid,
		h:        h,
		cpuStart: grunning.Time(),
	}
	return gh
}

// TryAdmit must be called frequently. The callee will measure the CPU time
// spent in the goroutine and decide whether more CPU needs to be allocated.
// If more CPU is needed, it can block in acquiring CPU tokens. Returns a
// non-nil error iff the context is canceled while waiting.
func (h *GoroutineCPUHandle) TryAdmit(ctx context.Context) error {
	if h.paused > 0 {
		return nil
	}
	cpuNeeded := grunning.Elapsed(grunning.Time(), h.cpuStart) - h.pauseDur
	diff := cpuNeeded - h.cpuAllocated
	if diff <= 0 {
		return nil
	}
	err := h.h.getCPU(ctx, diff)
	if err != nil {
		return err
	}
	h.cpuAllocated += diff
	return nil
}

// PauseMeasuring is used to pause the CPU accounting for this goroutine. It
// must be paired with UnpauseMeasuring. Used when the goroutine is being used
// for KV work. Note that the KV work will also see admission using the same
// SQLWorkInfo, so an alternative would be to just continue with reporting
// inside the KV layer, and skip doing separate admission. But that requires
// two separate paths in KV, for when it is being called locally by SQL (where
// there is a SQLCPUAdmissionHandle), and when it is being called remotely.
// Also, for observability, we will want to have some accounting of how much
// CPU was accounted for in SQL vs KV work, so we choose to pause and unpause.
func (h *GoroutineCPUHandle) PauseMeasuring() {
	h.paused++
	if h.paused == 1 {
		h.pauseStart = grunning.Time()
	}
}

func (h *GoroutineCPUHandle) UnpauseMeasuring() {
	h.paused--
	if h.paused == 0 {
		h.pauseDur += grunning.Elapsed(grunning.Time(), h.pauseStart)
	}
}

// Avoid unused lint errors.

// === Ignore old attempt below ====

// We list 3 possible interfaces for CPUAdmissionHandle below.
//
// The SQL CPU accounting must exclude time spent in KV. It is beneficial,
// though not required, if time spent in the RPC layer is also included.
//
// There are various ad-hoc integrations with the elastic CPU limiter, using
// admission.Pacer. We will need to examine these to see if they encompass
// code that would also get accounted for using the CPUAdmissionHandle, and
// make some changes in those integrations. For example,
// FileSSTSink.copyPointKeys which is eventually called by
// backupDataProcessor.Start; various code in rangefeed, changefeedccl
// packages; txnKVFetcher.maybeAdmitBatchResponse, when the local KV work is
// using elastic CPU (for TTL scans).

// Handle1 is the integration interface if we have a goroutine dedicated to
// SQL processing (i.e., excludes KV processing). MeasureAndAdmit should be
// called frequently, which will do accounting and potentially block asking
// for more CPU tokens for this work. Done shoould be called some time after
// all the work is done.
// type Handle1 interface {
// 	MeasureAndAdmit(ctx context.Context) error
// 	Done()
// }

// Handle2 is the integration interface is similar to Handle1 but allows for
// the same goroutine to also do KV work. {Pause,Unpause}Measuring should be
// used to bracket the KV work.
// type Handle2 interface {
//	Handle1
//	PauseMeasuring()
//	UnpauseMeasuring()
// }

// Handle3 is the integration interface that hides the least from the caller,
// when the caller has many different operators (all on the same goroutine,
// since Handle3 is not thread-safe) reporting CPU usage to the same handle.
// The caller must do their own measurement of CPU time and report. Done must
// be called once.
// type Handle3 interface {
// 	ReportUsageAndAdmit(ctx context.Context, cpuTime time.Duration) error
// 	Done()
// }
