// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenantcpu

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CPUUsageHelper is used to estimate the RUs consumed by a query due to CPU
// usage. It works by using a moving average for the process CPU usage to
// project what the usage *would* be if the query hadn't run, then subtracts
// that from the actual measured CPU usage.
//
// TODO(drewk): this isn't accurate when there are many concurrent queries.
// We should use the grunning library for a more accurate measurement.
type CPUUsageHelper struct {
	startCPU        float64
	avgCPUPerSecond float64
	startTime       time.Time
	costController  multitenant.TenantSideCostController
}

// StartCollection should be called at the beginning of execution for a flow.
// It is a no-op for non-tenants (e.g. when costController is nil).
func (h *CPUUsageHelper) StartCollection(
	ctx context.Context, costController multitenant.TenantSideCostController,
) {
	if costController == nil {
		return
	}
	h.costController = costController
	h.startTime = timeutil.Now()
	h.startCPU = GetCPUSeconds(ctx)
	h.avgCPUPerSecond = h.costController.GetCPUMovingAvg()
}

// EndCollection should be called at the end of execution for a flow in order to
// get the estimated number of RUs consumed due to CPU usage. It returns zero
// for non-tenants. It is a no-op if StartCollection was never called.
func (h *CPUUsageHelper) EndCollection(ctx context.Context) (ruFomCPU float64) {
	if h.costController == nil || h.costController.GetRequestUnitModel() == nil {
		return 0
	}
	cpuDelta := GetCPUSeconds(ctx) - h.startCPU
	timeElapsed := timeutil.Since(h.startTime).Seconds()
	expectedCPUDelta := timeElapsed * h.avgCPUPerSecond
	cpuUsageSeconds := cpuDelta - expectedCPUDelta
	if cpuUsageSeconds < 0 {
		cpuUsageSeconds = 0
	}
	if cpuUsageSeconds > timeElapsed {
		// It's unlikely that any single query will have such high CPU usage, so
		// bound the estimate above to reduce the possibility of gross error.
		cpuUsageSeconds = timeElapsed
	}
	return float64(h.costController.GetRequestUnitModel().PodCPUCost(cpuUsageSeconds))
}

// GetCPUSeconds returns the total CPU usage of the current process in seconds.
// It is used for measuring tenant RU consumption.
func GetCPUSeconds(ctx context.Context) (cpuSecs float64) {
	userTimeMillis, sysTimeMillis, err := status.GetProcCPUTime(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
		return 0
	}
	return float64(userTimeMillis+sysTimeMillis) * 1e-3
}
