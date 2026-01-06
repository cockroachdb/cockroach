// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestCPUTimeTokenEstimation(t *testing.T) {
	// WorkQueueWithCPUTimeTokenEstimators makes estimates of the number of
	// CPU time tokens used by some work that calls Admit before it executes.
	// This is a test of the estimation logic only. Therefore, we use a test
	// work queue, instead of WorkQueue. The test work queue enables making
	// assertions regarding the estimated CPU time tokens associated with
	// some call to Admit.
	twq := &testWorkQueue{t: t}
	q := initWorkQueueWithCPUTimeTokenEstimators(twq, nil)

	// At init time, the estimators haven't seen any requests yet. They are
	// hard-coded to return a single nanosecond token as an estimate in this
	// case.
	ctx := context.Background()
	info1 := WorkInfo{
		TenantID: roachpb.MustMakeTenantID(1),
	}
	twq.expectedRequestedCount = (1 * time.Nanosecond).Nanoseconds()
	resp1 := q.Admit(ctx, info1)

	// A bunch of work finishes. 3*100=300 pieces of work have
	// finished in total. Every 3*10=30 pieces of work that finish,
	// a call to update happens, and the exponentially-smoothed estimates
	// are updated. This simulates 30 pieces of work finishing every 1s,
	// for 10s total (see the ticker in start). The rest of the cases
	// are structured in a similar fashion, and so we will not repeat
	// this explanation.
	//
	// In this case, all work is for tenant 1. The mean is 100ms. So,
	// after a number of calls to update updating the exponentially-
	// smoothed estimates, we expect 100ms for future estimates (see
	// next call to Admit down below).
	for i := 1; i <= 100; i++ {
		q.AdmittedWorkDone(resp1, 100*time.Millisecond)
		q.AdmittedWorkDone(resp1, 50*time.Millisecond)
		q.AdmittedWorkDone(resp1, 150*time.Millisecond)

		if i%10 == 0 {
			q.updateEstimators()
		}
	}

	twq.expectedRequestedCount = (100 * time.Millisecond).Nanoseconds()
	resp1 = q.Admit(ctx, info1)
	info2 := WorkInfo{
		TenantID: roachpb.MustMakeTenantID(2),
	}
	resp2 := q.Admit(ctx, info2)

	// A bunch of work finishes for tenant 1 and tenant 2. The mean for
	// the tenant 1 work is 200ms. The mean for the tenant 2 work is
	// 20ms.
	for i := 1; i <= 100; i++ {
		q.AdmittedWorkDone(resp1, 200*time.Millisecond)
		q.AdmittedWorkDone(resp1, 150*time.Millisecond)
		q.AdmittedWorkDone(resp1, 250*time.Millisecond)

		q.AdmittedWorkDone(resp2, 20*time.Millisecond)
		q.AdmittedWorkDone(resp2, 30*time.Millisecond)
		q.AdmittedWorkDone(resp2, 10*time.Millisecond)

		if i%10 == 0 {
			q.updateEstimators()
		}
	}

	twq.expectedRequestedCount = (200 * time.Millisecond).Nanoseconds()
	resp1 = q.Admit(ctx, info1)

	twq.expectedRequestedCount = (20 * time.Millisecond).Nanoseconds()
	resp2 = q.Admit(ctx, info2)

	// Tenant 3 is a new tenant. So the global estimator should be
	// used. Lately, the estimator has seen equal number of 200ms mean
	// requests and 20ms mean requests. (200 + 20) / 2 = 220 / 2 =
	// 110. So 110ms is the expected estimate for tenant 3 work.
	info3 := WorkInfo{
		TenantID: roachpb.MustMakeTenantID(3),
	}
	twq.expectedRequestedCount = (110 * time.Millisecond).Nanoseconds()
	_ = q.Admit(ctx, info3)

	// This is a test of GC. If a call to update happens without any
	// work happening during that interval, the tenant's estimator should be
	// GCed. In this case, no tenant 1 or tenant 2 work have happened in the
	// last interval. So tenant 1 & tenant 2 should use the global estimator,
	// just like tenant 3.
	q.updateEstimators()
	twq.expectedRequestedCount = (110 * time.Millisecond).Nanoseconds()
	_ = q.Admit(ctx, info1)
	_ = q.Admit(ctx, info2)
	_ = q.Admit(ctx, info3)
}

type testWorkQueue struct {
	t                      *testing.T
	expectedRequestedCount int64
}

func (q *testWorkQueue) Admit(ctx context.Context, info WorkInfo) AdmitResponse {
	require.InDelta(
		q.t, q.expectedRequestedCount, info.RequestedCount, float64(time.Millisecond.Nanoseconds()),
		"expected %v, got %v (tolerance 1ms)", time.Duration(q.expectedRequestedCount), time.Duration(info.RequestedCount))
	return AdmitResponse{
		tenantID:       info.TenantID,
		requestedCount: info.RequestedCount,
	}
}

func (twq *testWorkQueue) AdmittedWorkDone(AdmitResponse, time.Duration) {
}
