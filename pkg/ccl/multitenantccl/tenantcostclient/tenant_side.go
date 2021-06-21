// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// NewTenantSIdeCostController creates an object which implements the
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
