// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// TokenBucketRequest is part of the multitenant.TenantUsageServer and
// implements the TokenBucket API of the roachpb.Internal service. This code
// runs on the host cluster to service requests coming from tenants (through the
// kvtenant.Connector).
func (s *instance) TokenBucketRequest(
	ctx context.Context, tenantID roachpb.TenantID, in *kvpb.TokenBucketRequest,
) *kvpb.TokenBucketResponse {
	if tenantID == roachpb.SystemTenantID {
		return &kvpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.New("token bucket request for system tenant")),
		}
	}
	instanceID := base.SQLInstanceID(in.InstanceID)
	if instanceID < 1 {
		return &kvpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.Errorf("invalid instance ID %d", instanceID)),
		}
	}
	if in.RequestedTokens < 0 {
		return &kvpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.Errorf("negative requested tokens")),
		}
	}

	metrics := s.metrics.getTenantMetrics(tenantID)
	// Use a per-tenant mutex to serialize operations to the bucket. The
	// transactions will need to be serialized anyway, so this avoids more
	// expensive restarts. It also guarantees that the metric updates happen in
	// the same order with the system table changes.
	metrics.mutex.Lock()
	defer metrics.mutex.Unlock()

	result := &kvpb.TokenBucketResponse{}
	var consumption kvpb.TenantConsumption
	if err := s.ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		*result = kvpb.TokenBucketResponse{}

		h := makeSysTableHelper(ctx, tenantID)
		tenant, instance, err := h.readTenantAndInstanceState(txn, instanceID)
		if err != nil {
			return err
		}

		if !tenant.Present {
			// If there is no state, we will initialize it. But check that the tenant
			// is valid and active. It is possible that the tenant was deleted and an
			// existing tenant process is still sending requests.
			if err := s.checkTenantID(ctx, txn, tenantID); err != nil {
				return err
			}
		}

		now := s.timeSource.Now()
		last := tenant.LastUpdate.Time
		tenant.update(now)

		if !instance.Present {
			if err := h.accommodateNewInstance(txn, &tenant, &instance); err != nil {
				return err
			}
		}
		if string(instance.Lease) != string(in.InstanceLease) {
			// This must be a different incarnation of the same ID. Clear the sequence
			// number (the client starts with sequence number 1).
			instance.Seq = 0
			instance.Lease = tree.DBytes(in.InstanceLease)
		}

		if in.NextLiveInstanceID != 0 {
			if err := s.handleNextLiveInstanceID(
				&h, txn, &tenant, &instance, base.SQLInstanceID(in.NextLiveInstanceID),
			); err != nil {
				return err
			}
		}

		// Only update consumption if we are sure this is not a duplicate request
		// that we already counted. Note that if this is a duplicate request, it
		// will still use tokens from the bucket (TokenCurrent); we rely on a higher
		// level control loop that periodically reconfigures the token bucket to
		// correct such errors.
		if instance.Seq == 0 || instance.Seq < in.SeqNum {
			instance.Seq = in.SeqNum
			tenant.Consumption.Add(&in.ConsumptionSinceLastRequest)

			// Update consumption rates.
			tenant.Rates.Update(now, last, &in.ConsumptionSinceLastRequest, in.ConsumptionPeriod)
		}

		*result = tenant.Bucket.Request(ctx, in)
		result.ConsumptionRates = *tenant.Rates.Current()

		instance.LastUpdate.Time = now
		if err := h.updateTenantAndInstanceState(txn, &tenant, &instance); err != nil {
			return err
		}

		if err := h.maybeCheckInvariants(txn); err != nil {
			panic(err)
		}
		consumption = tenant.Consumption
		return nil
	}); err != nil {
		return &kvpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, err),
		}
	}

	// Report current consumption.
	metrics.totalRU.UpdateIfHigher(consumption.RU)
	metrics.totalKVRU.UpdateIfHigher(consumption.KVRU)
	metrics.totalReadBatches.Update(int64(consumption.ReadBatches))
	metrics.totalReadRequests.Update(int64(consumption.ReadRequests))
	metrics.totalReadBytes.Update(int64(consumption.ReadBytes))
	metrics.totalWriteBatches.Update(int64(consumption.WriteBatches))
	metrics.totalWriteRequests.Update(int64(consumption.WriteRequests))
	metrics.totalWriteBytes.Update(int64(consumption.WriteBytes))
	metrics.totalSQLPodsCPUSeconds.Update(consumption.SQLPodsCPUSeconds)
	metrics.totalPGWireEgressBytes.Update(int64(consumption.PGWireEgressBytes))
	metrics.totalExternalIOEgressBytes.Update(int64(consumption.ExternalIOEgressBytes))
	metrics.totalExternalIOIngressBytes.Update(int64(consumption.ExternalIOIngressBytes))
	metrics.totalCrossRegionNetworkRU.UpdateIfHigher(consumption.CrossRegionNetworkRU)
	return result
}

// handleNextLiveInstanceID checks the next live instance ID according to the
// tenant and potentially cleans up stale instances that follow this instance
// (in the circular order).
func (s *instance) handleNextLiveInstanceID(
	h *sysTableHelper,
	txn isql.Txn,
	tenant *tenantState,
	instance *instanceState,
	nextLiveInstanceID base.SQLInstanceID,
) error {
	// We use NextLiveInstanceID to figure out if there is a potential dead
	// instance after this instance.
	//
	// In the fast path, the server and the tenant will agree on the live set
	// and the values will match. If the values don't match, there are two cases:
	//
	//  1. There is an instance which the tenant is aware of and the server is
	//     not. This can either be an instance that is starting up, or an
	//     instance that was cleaned up and the tenant information is not up
	//     to date. In either case no cleanup needs to be triggered.
	//
	//  2. There is a range of instance IDs that contains no live IDs according
	//     to the tenant but contains at least one live ID according to the
	//     server. We trigger a cleanup check (which makes the final decision
	//     based on the last update timestamp).
	expected := instance.NextInstance
	if expected == 0 {
		expected = tenant.FirstInstance
	}
	if nextLiveInstanceID == expected {
		// Fast path: the values match.
		return nil
	}
	var err error
	cutoff := s.timeSource.Now().Add(-instanceInactivity.Get(&s.settings.SV))
	if nextLiveInstanceID > instance.ID {
		// According to the tenant, this is not the largest instance ID.
		if instance.NextInstance != 0 && instance.NextInstance < nextLiveInstanceID {
			// Case 2: range [instance.NextInstance, nextLiveInstanceID) potentially
			// needs cleanup.
			instance.NextInstance, err = h.maybeCleanupStaleInstances(
				txn, cutoff, instance.NextInstance, nextLiveInstanceID,
			)
			if err != nil {
				return err
			}
		}
	} else {
		// According to the tenant, this is the largest instance ID and
		// nextLiveInstanceID is the ID of the first live instance. There are
		// two potential ranges for cleanup, one around the smallest IDs and
		// one around the largest IDs.
		if tenant.FirstInstance < nextLiveInstanceID {
			// Case 2: range [tenant.FirstInstance, nextLiveInstanceID)
			// potentially needs cleanup.
			tenant.FirstInstance, err = h.maybeCleanupStaleInstances(
				txn, cutoff, tenant.FirstInstance, nextLiveInstanceID,
			)
			if err != nil {
				return err
			}
		}
		if instance.NextInstance != 0 {
			// Case 2: in our table, this is not the largest ID. The range
			// [instance.NextInstance, ∞) potentially needs cleanup.
			instance.NextInstance, err = h.maybeCleanupStaleInstances(
				txn, cutoff, instance.NextInstance, -1,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
