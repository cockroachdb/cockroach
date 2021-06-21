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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// NewTenantSideCostController creates an object which implements the
// server.TenantSideCostController interface.
func NewTenantSideCostController(
	ctx context.Context,
	tenantID roachpb.TenantID,
	stopper *stop.Stopper,
	provider kvtenant.TokenBucketProvider,
) (server.TenantSideCostController, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.AssertionFailedf("cost controller can't be used for system tenant")
	}
	c := &tenantSideCostController{
		tenantID: tenantID,
		stopper:  stopper,
		provider: provider,
	}
	if err := stopper.RunAsyncTask(context.Background(), "refresher", func(ctx context.Context) {
		c.mainLoop(ctx)
	}); err != nil {
		return nil, err
	}
	return c, nil
}

func init() {
	server.NewTenantSideCostController = NewTenantSideCostController
}

type tenantSideCostController struct {
	tenantID roachpb.TenantID
	stopper  *stop.Stopper
	provider kvtenant.TokenBucketProvider
}

var _ server.TenantSideCostController = (*tenantSideCostController)(nil)

func (c *tenantSideCostController) mainLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			req := roachpb.TokenBucketRequest{
				OperationID: uuid.MakeV4(),
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

		case <-c.stopper.ShouldQuiesce():
			// TODO(radu): send one more request to update consumption.
			return
		}
	}
}
