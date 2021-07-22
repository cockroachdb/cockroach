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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	ctx context.Context, tenantID roachpb.TenantID, provider kvtenant.TokenBucketProvider,
) (multitenant.TenantSideCostController, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.AssertionFailedf("cost controller can't be used for system tenant")
	}
	return &tenantSideCostController{
		tenantID: tenantID,
		provider: provider,
	}, nil
}

func init() {
	server.NewTenantSideCostController = NewTenantSideCostController
}

type tenantSideCostController struct {
	tenantID roachpb.TenantID
	provider kvtenant.TokenBucketProvider
}

var _ multitenant.TenantSideCostController = (*tenantSideCostController)(nil)

// Start is part of multitenant.TenantSideCostController.
func (c *tenantSideCostController) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "cost-controller", func(ctx context.Context) {
		c.mainLoop(ctx, stopper)
	})
}

func (c *tenantSideCostController) mainLoop(ctx context.Context, stopper *stop.Stopper) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			req := roachpb.TokenBucketRequest{
				ConsumptionSinceLastRequest: roachpb.TokenBucketRequest_Consumption{
					// Report a dummy 1 RU consumption each time.
					RU:               1,
					SQLPodCPUSeconds: 1,
				},
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
