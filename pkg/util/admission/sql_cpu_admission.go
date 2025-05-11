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

// SQLCPUAdmission is used to admit SQL work that consumes CPU.
type SQLCPUAdmission interface {
	GetHandle(work WorkInfo) SQLCPUAdmissionHandle
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
	// TODO: maintain a shared pool of CPU already acquired that each goroutine
	// can subtract from. And when that runs out, block in acquiring more.
	gHandles []*GoroutineCPUHandle
}

// TryRegisterGoroutine returns a GoroutineCPUHandle to use for reporting and
// admission.
func (h *SQLCPUAdmissionHandle) TryRegisterGoroutine() *GoroutineCPUHandle {
	// TODO: ignore if closed.
	_ = goid.Get()
	// TODO: find existing gHandle for gid or create new one.
	return nil
}

// Close is called when no more reporting is needed. It is ok for some stray
// reporting to be attempted after Close, but it will be ignored.
func (h *SQLCPUAdmissionHandle) Close() {
	// TODO
}

type GoroutineCPUHandle struct {
	gid int64
	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// cpuAllocated is the total CPU time allocated to this goroutine.
	cpuAllocated time.Duration
	// TODO: implement
}

// TryAdmit must be called frequently. The callee will measure the CPU time
// spent in the goroutine and decide whether more CPU needs to be allocated.
// If more CPU is needed, it can block in acquiring CPU tokens. Returns a
// non-nil error iff the context is canceled while waiting.
func (h *GoroutineCPUHandle) TryAdmit(ctx context.Context) error {
	// TODO:
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
	// TODO:
}
func (h *GoroutineCPUHandle) UnpauseMeasuring() {
	// TODO:
}

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
