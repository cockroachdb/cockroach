// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
type CPUUsageHelper struct {
	startCPU        float64
	avgCPUPerSecond float64
	startTime       time.Time
	CostController  multitenant.TenantSideCostController
}

// StartCollection should be called at the beginning of execution for a flow.
// It is a no-op for non-tenants.
func (h *CPUUsageHelper) StartCollection(ctx context.Context) {
	if h.CostController == nil {
		return
	}
	h.startTime = timeutil.Now()
	h.startCPU = GetCPUSeconds(ctx)
	h.avgCPUPerSecond = h.CostController.GetCPUMovingAvg()
}

// EndCollection should be called at the end of execution for a flow in order to
// get the estimated number of RUs consumed due to CPU usage. It returns zero
// for non-tenants.
func (h *CPUUsageHelper) EndCollection(ctx context.Context) (ruFomCPU float64) {
	if h.CostController == nil || h.CostController.GetCostConfig() == nil {
		return 0
	}
	cpuDelta := GetCPUSeconds(ctx) - h.startCPU
	timeElapsed := timeutil.Since(h.startTime)
	expectedCPUDelta := timeElapsed.Seconds() * h.avgCPUPerSecond
	cpuUsageSeconds := cpuDelta - expectedCPUDelta
	if cpuUsageSeconds < 0 {
		cpuUsageSeconds = 0
	}
	return float64(h.CostController.GetCostConfig().PodCPUCost(cpuUsageSeconds))
}

// GetCPUSeconds returns the total CPU usage of the current process in seconds.
// It is used for measuring tenant RU consumption.
func GetCPUSeconds(ctx context.Context) (cpuSecs float64) {
	userTimeMillis, sysTimeMillis, err := status.GetCPUTime(ctx)
	if err != nil {
		log.Ops.Errorf(ctx, "unable to get cpu usage: %v", err)
		return 0
	}
	return float64(userTimeMillis+sysTimeMillis) * 1e-3
}
