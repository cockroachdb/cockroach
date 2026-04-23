// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

type WorkInfo struct {
	// WorkloadID is the id of the workload, where a workloads get a fair share
	// of the underlying resource (CPU). This could be the TenantID, or some
	// other finer-grained way of separating workloads. The admission control
	// sub-system is equipped to do weighted fair-sharing, and if needed we can
	// expose a way to set the weights.
	//
	// Don't use 0, since it is a reserved, invalid id.
	WorkloadID uint64
	// Priority is utilized within a workload. Lower priority work can be
	// starved by higher priority work.
	Priority admissionpb.WorkPriority
	// CreateTime is equivalent to Time.UnixNano() at the creation time of this
	// work or a parent work (e.g. could be the start time of the transaction,
	// if this work was created as part of a transaction). It is used to order
	// work within a (WorkloadID, Priority) pair -- earlier CreateTime is given
	// preference.
	CreateTime int64
}

type Handle struct {
	workInfo             WorkInfo
	cpuTime              time.Duration
	firstAdmitCalled     bool
	callAdmittedWorkDone bool
}

// Controller implements an admission control interface for SQL code.
// SQL code is assumed to be primarily CPU bound.
//
// This is a strawman based on the discussion in
// https://github.com/cockroachdb/cockroach/issues/85471. It is geared towards
// doing a blocking admit at a coarse granularity, while providing more
// fine-grained CPU consumption information (for fairness and accounting
// purposes). It assumes that the instrumentation points for these two are
// same, so it hides the details on when to do a blocking admit. See comment
// in
// https://github.com/cockroachdb/cockroach/issues/85471#issuecomment-1440771823.
type Controller interface {
	InitHandle(WorkInfo, *Handle)
	// TryAdmit and AdmittedWorkDone must be paired to encompass the CPU done within SQL.
	// - The work done within the (TryAdmit, AdmittedWorkDone) must not recursively call another
	//   TryAdmit. This can be accomplished by the buffering scheme described in
	//   https://github.com/cockroachdb/cockroach/issues/85471#issuecomment-1440771823 which does
	//   this bottom up in the operator tree.
	// - The work done within the (TryAdmit, AdmittedWorkDone) must not call
	//   into KV. This means the aforementioned bottom up scheme needs to be
	//   tweaked such that the leaf operator that is calling into KV should not
	//   do (TryAdmit, AdmittedWorkDone) around it's Next call. This means we
	//   need to exclude such operators from the generic admissionOp wrapper --
	//   hopefully there are only a few of these so not hard to make that
	//   exception.
	TryAdmit(context.Context, *Handle) error
	AdmittedWorkDone(h *Handle, cpuTime time.Duration)
}

type noopControllerImpl struct{}

var _ Controller = &noopControllerImpl{}

func (c *noopControllerImpl) InitHandle(WorkInfo, *Handle) {}

func (c *noopControllerImpl) TryAdmit(context.Context, *Handle) error {
	return nil
}

func (c *noopControllerImpl) AdmittedWorkDone(h *Handle, cpuTime time.Duration) {}

type controllerImpl struct {
	// Same as the current KV admission queue, since used for both KV and SQL.
	cpuAdmissionQ *admission.WorkQueue
}

func (c *controllerImpl) InitHandle(w WorkInfo, h *Handle) {
	*h = Handle{
		workInfo: w,
	}
}

func (c *controllerImpl) TryAdmit(ctx context.Context, h *Handle) error {
	if h.firstAdmitCalled && h.cpuTime < 5*time.Millisecond {
		return nil
	}
	enabled, err := c.cpuAdmissionQ.Admit(ctx, admission.WorkInfo{
		TenantID:   roachpb.TenantID{InternalValue: h.workInfo.WorkloadID},
		Priority:   h.workInfo.Priority,
		CreateTime: h.workInfo.CreateTime,
	})
	if err != nil {
		return err
	}
	if enabled {
		h.callAdmittedWorkDone = true
	}
	h.cpuTime = 0
	if !h.firstAdmitCalled {
		h.firstAdmitCalled = true
	}
	return nil
}

func (c *controllerImpl) AdmittedWorkDone(h *Handle, cpuTime time.Duration) {
	if h.callAdmittedWorkDone {
		c.cpuAdmissionQ.AdmittedWorkDone(roachpb.TenantID{InternalValue: h.workInfo.WorkloadID})
		h.callAdmittedWorkDone = false
	}
	// TODO(sumeer): also plumb the cpuTime to the WorkQueue when we do
	// https://github.com/cockroachdb/cockroach/issues/91533.
	h.cpuTime += cpuTime
}

// Discussion of issues etc.:
// - After the fact accounting is good: it helps with fairness in future
//   allocations and understanding which workloads are consuming CPU.
// - (TryAdmit, AdmittedWorkDone) pair should include the actual work. This is
//   because slots control instantaneous concurrency of work, and not shaping
//   to a rate.
//   - If they don't include the actual work the instantaneous concurrency
//     number is nonsensical. So probably what I said on
//     https://github.com/cockroachdb/cockroach/issues/85471#issuecomment-1440687926
//     about calling it "before sending a new ProducerMessage" does not make
//     sense.
//   - We are sampling in the (TryAdmit, AdmittedWorkDone) to reduce the
//     overhead of interacting with AC. Let's calls the sampled ones (Admit,
//     AdmittedWorkDone). How effective is AC with this sampling? Say we
//     sampled after 10ms of accumulated cpu time, and each TryAdmit did 1ms
//     of cpu time. So we are sampling 1 in 10 calls. Then the actual
//     concurrency will be 10x of what is being observed by AC.
// - Deadlock when consuming same concurrency slot as kv (different from the
//   deadlock discussed in
//   https://github.com/cockroachdb/cockroach/issues/85471 until Feb 22). If
//   some of the work in the (Admit, AdmittedWorkDone) interval involves
//   calling into KV, we can deadlock if that KV work blocks for a concurrency
//   slot. This is why I had originally been trying to do the Admit call on
//   the response path (and forgot about that in the recent discussions).
//
// Possible solution:
// - What if we shape the rate of cpu consumption of SQL?
//   - Harder to make it work conserving as discussed in
//     https://docs.google.com/document/d/16RISoB3zOORf24k0fQPDJ4tTCisBVh-PkCZEit4iwV8/edit#heading=h.amw25se2ob5m
//     and
//     https://docs.google.com/document/d/16RISoB3zOORf24k0fQPDJ4tTCisBVh-PkCZEit4iwV8/edit#bookmark=id.479u56nbiz7
//   - Harder to make it work along with KV slots. Would then want to convert
//     KV slots to also use cpu tokens.
//
// - Only solve the deadlock issue identified above.
//   - Ignore the 10x actual concurrency (in the example) due to sampling when
//     calling (Admit, AdmittedWorkDone). We are setting
//     admission.kv_slot_adjuster.overload_threshold=32 so we are already
//     allowing a significant backlog in the goroutine scheduler. So we retain
//     enough control via AC -- the kvSlotAdjuster will have see this
//     burstiness and decrease the slots.
//   - Solve the deadlock problem by not calling (Admit, AdmittedWorkDone) in
//     the operator that will call into KV for the batch.

// Next steps (in sequence):
// - TODO(irfansharif): look at discussion on https://github.com/cockroachdb/cockroach/issues/85471
//   and this code and check for sanity.
// - TODO(drewk): attempt to instrument SQL with this interface.
// - TODO(sumeer,irfansharif,drewk):
//   - after the instrumentation do the plumbing to create the real
//     controllerImpl and place it in a place that SQL can use.
//   - run experiments with the new scheme.
