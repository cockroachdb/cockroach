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

	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver/tenanttokenbucket"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TokenBucketRequest is part of the multitenant.TenantUsageServer and
// implements the TokenBucket API of the roachpb.Internal service. This code
// runs on the host cluster to service requests coming from tenants (through the
// kvtenant.Connector).
func (s *instance) TokenBucketRequest(
	ctx context.Context, tenantID roachpb.TenantID, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.New("token bucket request for system tenant")
	}

	result := &roachpb.TokenBucketResponse{}
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		state, err := readTenantUsageState(ctx, s.executor, txn, tenantID)
		if err != nil {
			return err
		}
		state.Seq++

		// Update consumption.
		state.Consumption.RU += in.ConsumptionSinceLastRequest.RU
		state.Consumption.ReadRequests += in.ConsumptionSinceLastRequest.ReadRequests
		state.Consumption.ReadBytes += in.ConsumptionSinceLastRequest.ReadBytes
		state.Consumption.WriteRequests += in.ConsumptionSinceLastRequest.WriteRequests
		state.Consumption.WriteBytes += in.ConsumptionSinceLastRequest.WriteBytes
		state.Consumption.SQLPodCPUSeconds += in.ConsumptionSinceLastRequest.SQLPodCPUSeconds

		*result = state.Bucket.Request(in, timeutil.Now())

		return updateTenantUsageState(ctx, s.executor, txn, tenantID, state)
	}); err != nil {
		return nil, err
	}
	return result, nil
}

type tenantUsageState struct {
	// Seq is a sequence number identifying this state. All state changes are
	// strictly sequenced, with consecutive sequence numbers.
	Seq int64

	Bucket tenanttokenbucket.State

	// Current consumption information.
	Consumption roachpb.TokenBucketRequest_Consumption
}

// readCurrentBucketState reads the current (last) bucket state. The zero struct
// is returned if the state is not yet initialized.
func readTenantUsageState(
	ctx context.Context, ex *sql.InternalExecutor, txn *kv.Txn, tenantID roachpb.TenantID,
) (tenantUsageState, error) {
	// TODO(radu): interact with the system table.
	return tenantUsageState{}, nil
}

func updateTenantUsageState(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *kv.Txn,
	tenantID roachpb.TenantID,
	newState tenantUsageState,
) error {
	// TODO(radu): interact with the system table.
	return nil
}
