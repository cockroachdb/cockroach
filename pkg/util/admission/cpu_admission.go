// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"context"
	"time"
)

// CPUAdmission is an alternative interface for (non-elastic) CPU admission,
// that discards the slot based mechanism, in favor of tokens and only
// after-the-consumption accounting and blocking. Allows for KV, SQL-KV,
// SQL-SQL to be unified.
//
// Why?
//
//   - Priority inversion discussed in
//     https://github.com/cockroachdb/cockroach/issues/85471. Solutions for SQL
//     that require actual work to be sandwiched between TryAdmit and
//     AdmittedWorkDone are unlikely to be viable.
//
//   - admission.kv_slot_adjuster.overload_threshold defaults to 32 and causes
//     excessive goroutine scheduling latency. We have discussed in
//     https://docs.google.com/document/d/16RISoB3zOORf24k0fQPDJ4tTCisBVh-PkCZEit4iwV8/edit#heading=h.amw25se2ob5m
//     and
//     https://docs.google.com/document/d/16RISoB3zOORf24k0fQPDJ4tTCisBVh-PkCZEit4iwV8/edit#bookmark=id.479u56nbiz7
//     that a token based scheme is hard to make work-conserving, but those
//     arguments were based on before-the-consumption blocking deduction (like
//     we do for elastic work, where work conserving is not necessary) and
//     after-the-consumption non-blocking correction. By switching to
//     after-the-consumption blocking deduction, we can decide on a fixed burst
//     budget as a function of the token generation rate and be ok. For example,
//     consider the simple case of 1ms of cpu tokens per ms and a burst budget
//     of 20ms. And say only 50% of the actual cpu work is instrumented to use
//     AC. If the non-instrumented work consumes 100% of the cpu (because of
//     excessive concurrency), the instrumented work over a 20ms interval can at
//     most admit 20ms+20ms = 40ms so another 200%, for a total work trying to
//     use CPU of 100+200=300%. This is still better than the 3200+100=3300%
//     that the current scheme allows.
//
// NB: this will subsume the need for
// https://github.com/cockroachdb/cockroach/issues/91536. However, we can
// still suffer from under-admittance if there is a burst of work finishing,
// that blocked after consuming most of its cpu and then finishes after
// consuming a tiny amount of additional cpu -- we will count all the token
// deduction on completion even though most of the consumption happened a
// while ago. We could rectify this making Admit block if tokens <= 0 AND
// there were idle P's in the last sample of P state (sampled at 1ms
// intervals).
//
// Admit consumes only 1ns of tokens. Since 1ns is tiny, when available tokens
// become positive, we could admit an arbitrary sized burst. We already have a
// solution for this: grant-chaining, which will now be used for for KV work
// too.
type CPUAdmission interface {
	Admit(ctx context.Context, work WorkInfo) (CPUAdmissionHandle, error)
}

type CPUAdmissionHandle interface {
	// Used can block. Do not use inside KV layer, which may be holding latches.
	// For KV wrap in KVWorkBoundingHandle.
	Used(ctx context.Context, cpuTime time.Duration) error
}

type KVWorkBoundingHandle struct {
	ah CPUAdmissionHandle
	// TODO: similar to ElasticCPUWorkHandle in tracking
}

func NewKVWorkBoundingHandle(ah CPUAdmissionHandle) *KVWorkBoundingHandle {
	// TODO:
}

func (h *KVWorkBoundingHandle) ShouldReturn() bool {
	// TODO: returns true when consumed more than N ms of cpu time on this goroutine.
}

func (h *KVWorkBoundingHandle) Done() bool {
	// TODO: calls ah.Used with the cpu consumed.
}
