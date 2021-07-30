// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TokenBucketRequest is part of the multitenant.TenantUsageServer and
// implements the TokenBucket API of the roachpb.Internal service. This code
// runs on the host cluster to service requests coming from tenants (through the
// kvtenant.Connector).
func (s *instance) TokenBucketRequest(
	ctx context.Context, tenantID roachpb.TenantID, in *roachpb.TokenBucketRequest,
) *roachpb.TokenBucketResponse {
	if tenantID == roachpb.SystemTenantID {
		return &roachpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.New("token bucket request for system tenant")),
		}
	}
	instanceID := base.SQLInstanceID(in.InstanceID)
	if instanceID < 1 {
		return &roachpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.Errorf("invalid instance ID %d", instanceID)),
		}
	}

	result := &roachpb.TokenBucketResponse{}
	var consumption roachpb.TokenBucketRequest_Consumption
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		*result = roachpb.TokenBucketResponse{}

		h := makeSysTableHelper(ctx, s.executor, txn, tenantID)
		tenant, instance, err := h.readTenantAndInstanceState(instanceID)
		if err != nil {
			return err
		}

		if !tenant.Present {
			// TODO(radu): initialize tenant state.
			tenant.FirstInstance = 0
		}

		if !instance.Present {
			if err := h.accomodateNewInstance(&tenant, &instance); err != nil {
				return err
			}
		}

		tenant.addConsumption(in.ConsumptionSinceLastRequest)

		// TODO(radu): update shares.
		*result = tenant.Bucket.Request(in, timeutil.Now())

		if err := h.updateTenantAndInstanceState(tenant, instance); err != nil {
			return err
		}

		if err := h.maybeCheckInvariants(); err != nil {
			panic(err)
		}
		consumption = tenant.Consumption
		return nil
	}); err != nil {
		return &roachpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, err),
		}
	}

	// Report current consumption.
	// TODO(radu): there is a possible race here, where two different requests
	// update the metrics in opposite order.
	m := s.metrics.getTenantMetrics(tenantID)
	m.totalRU.Update(consumption.RU)
	m.totalReadRequests.Update(int64(consumption.ReadRequests))
	m.totalReadBytes.Update(int64(consumption.ReadBytes))
	m.totalWriteRequests.Update(int64(consumption.WriteRequests))
	m.totalWriteBytes.Update(int64(consumption.WriteBytes))
	m.totalSQLPodsCPUSeconds.Update(consumption.SQLPodCPUSeconds)
	return result
}
