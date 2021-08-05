// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TargetPeriodSetting is exported for testing purposes.
var TargetPeriodSetting = settings.RegisterDurationSetting(
	"tenant_cost_control_period",
	"target duration between token bucket requests from tenants (requires restart)",
	10*time.Second,
	settings.PositiveDuration,
)

// CPUUsageAllowance is exported for testing purposes.
var CPUUsageAllowance = settings.RegisterDurationSetting(
	"tenant_cpu_usage_allowance",
	"this much CPU usage per second is considered background usage and "+
		"doesn't contribute to consumption; for example, if it is set to 10ms, "+
		"that corresponds to 1% of a CPU",
	10*time.Millisecond,
	settings.NonNegativeDuration,
)

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	st *cluster.Settings, tenantID roachpb.TenantID, provider kvtenant.TokenBucketProvider,
) (multitenant.TenantSideCostController, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.AssertionFailedf("cost controller can't be used for system tenant")
	}
	tc := &tenantSideCostController{
		settings: st,
		tenantID: tenantID,
		provider: provider,
	}
	tc.mu.costCfg = tenantcostmodel.ConfigFromSettings(&st.SV)
	sv := &st.SV
	tenantcostmodel.SetOnChange(sv, func(context.Context) {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		tc.mu.costCfg = tenantcostmodel.ConfigFromSettings(sv)
	})
	return tc, nil
}

func init() {
	server.NewTenantSideCostController = NewTenantSideCostController
}

type tenantSideCostController struct {
	settings *cluster.Settings
	tenantID roachpb.TenantID
	provider kvtenant.TokenBucketProvider

	mu struct {
		syncutil.Mutex

		costCfg     tenantcostmodel.Config
		consumption roachpb.TenantConsumption
	}
}

var _ multitenant.TenantSideCostController = (*tenantSideCostController)(nil)

// Start is part of multitenant.TenantSideCostController.
func (c *tenantSideCostController) Start(
	ctx context.Context, stopper *stop.Stopper, cpuSecsFn multitenant.CPUSecsFn,
) error {
	return stopper.RunAsyncTask(ctx, "cost-controller", func(ctx context.Context) {
		c.mainLoop(ctx, stopper, cpuSecsFn)
	})
}

func (c *tenantSideCostController) mainLoop(
	ctx context.Context, stopper *stop.Stopper, cpuSecsFn multitenant.CPUSecsFn,
) {
	ticker := time.NewTicker(TargetPeriodSetting.Get(&c.settings.SV))
	defer ticker.Stop()

	lastTime, lastCPUSecs := timeutil.Now(), cpuSecsFn(ctx)
	var lastConsumption roachpb.TenantConsumption

	for {
		select {
		case <-ticker.C:
			newTime, newCPUSecs := timeutil.Now(), cpuSecsFn(ctx)

			c.mu.Lock()
			newConsumption := c.mu.consumption
			configPodCPUSecond := c.mu.costCfg.PodCPUSecond
			c.mu.Unlock()

			deltaConsumption := newConsumption
			deltaConsumption.Sub(&lastConsumption)

			deltaCPU := newCPUSecs - lastCPUSecs

			// Subtract any allowance that we consider free background usage.
			if deltaTime := newTime.Sub(lastTime); deltaTime > 0 {
				deltaCPU -= CPUUsageAllowance.Get(&c.settings.SV).Seconds() * deltaTime.Seconds()
			}

			deltaConsumption.SQLPodsCPUSeconds = 0
			if deltaCPU > 0 {
				deltaConsumption.SQLPodsCPUSeconds = deltaCPU
				deltaConsumption.RU += deltaCPU * float64(configPodCPUSecond)
			}

			lastTime, lastCPUSecs, lastConsumption = newTime, newCPUSecs, newConsumption

			req := roachpb.TokenBucketRequest{
				TenantID: c.tenantID.ToUint64(),
				// TODO(radu): populate instance ID.
				InstanceID:                  1,
				ConsumptionSinceLastRequest: deltaConsumption,
			}
			_, err := c.provider.TokenBucket(ctx, &req)
			if err != nil {
				log.Warningf(ctx, "TokenBucket error: %v", err)
			}

		case <-stopper.ShouldQuiesce():
			// TODO(radu): send one last request to update consumption.
			return
		}
	}
}

// OnRequestWait is part of the multitenant.TenantSideKVInterceptor
// interface.
func (c *tenantSideCostController) OnRequestWait(
	ctx context.Context, info tenantcostmodel.RequestInfo,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if isWrite, writeBytes := info.IsWrite(); isWrite {
		c.mu.consumption.WriteRequests++
		c.mu.consumption.WriteBytes += uint64(writeBytes)
	} else {
		c.mu.consumption.ReadRequests++
	}
	c.mu.consumption.RU += float64(c.mu.costCfg.RequestCost(info))

	return nil
}

// OnResponse is part of the multitenant.TenantSideBatchInterceptor interface.
func (c *tenantSideCostController) OnResponse(
	ctx context.Context, info tenantcostmodel.ResponseInfo,
) {
	readBytes := info.ReadBytes()
	if readBytes == 0 {
		return
	}
	c.mu.Lock()
	c.mu.consumption.ReadBytes += uint64(readBytes)
	c.mu.consumption.RU += float64(c.mu.costCfg.ResponseCost(info))
	c.mu.Unlock()
}
