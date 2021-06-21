// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver/tenanttokenbucket"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func init() {
	server.TokenBucketRequestImpl = TokenBucketRequest
}

// TokenBucketRequest implements the TokenBucket API of the roachpb.Internal
// service. This code runs on the host cluster to service requests coming from
// tenants (through the kvtenant.Connector)
func TokenBucketRequest(
	ctx context.Context, db *kv.DB, ex *sql.InternalExecutor, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	tenantID, ok := roachpb.TenantFromContext(ctx)
	if !ok {
		return nil, errors.New("token bucket request with no tenant")
	}
	if tenantID == roachpb.SystemTenantID {
		return nil, errors.New("token bucket request for system tenant")
	}
	if in.OperationID == uuid.Nil {
		return nil, errors.New("token bucket request with unset OperationID")
	}

	result := &roachpb.TokenBucketResponse{}
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		state, err := readTenantUsageState(ctx, ex, txn, tenantID.ToUint64())
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

		if err := updateTenantUsageState(ctx, ex, txn, tenantID.ToUint64(), in.OperationID, state); err != nil {
			return err
		}

		return nil
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
	ctx context.Context, ex *sql.InternalExecutor, txn *kv.Txn, tenantID uint64,
) (tenantUsageState, error) {
	datums, err := ex.QueryRowEx(
		ctx, "tenant-usage-select", txn,
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
			seq,
			total_ru_usage,
			total_read_requests,
			total_read_bytes,
			total_write_requests,
			total_write_bytes,
			total_sql_pod_cpu_seconds
		 FROM system.tenant_usage WHERE tenant_id = $1 ORDER BY seq DESC LIMIT 1`,
		tenantID,
	)
	if err != nil {
		return tenantUsageState{}, err
	}

	if datums == nil {
		// No rows yet.
		return tenantUsageState{}, nil
	}
	return tenantUsageState{
		Seq: int64(*datums[0].(*tree.DInt)),
		Consumption: roachpb.TokenBucketRequest_Consumption{
			RU:               float64(tree.MustBeDFloat(datums[1])),
			ReadRequests:     uint64(tree.MustBeDInt(datums[2])),
			ReadBytes:        uint64(tree.MustBeDInt(datums[3])),
			WriteRequests:    uint64(tree.MustBeDInt(datums[4])),
			WriteBytes:       uint64(tree.MustBeDInt(datums[5])),
			SQLPodCPUSeconds: float64(tree.MustBeDFloat(datums[6])),
		},
	}, nil
}

func updateTenantUsageState(
	ctx context.Context,
	ex *sql.InternalExecutor,
	txn *kv.Txn,
	tenantID uint64,
	operationID uuid.UUID,
	newState tenantUsageState,
) error {
	if _, err := ex.ExecEx(
		ctx, "tenant-usage-insert", txn,
		sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.tenant_usage(
			  tenant_id,
				seq,
				op_id,
				ru_burst_limit,
				ru_refill_rate,
				ru_current,
				current_share_sum,
				total_ru_usage,
				total_read_requests,
				total_read_bytes,
				total_write_requests,
				total_write_bytes,
				total_sql_pod_cpu_seconds
			 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		tenantID,
		newState.Seq,
		operationID.String(),
		newState.Bucket.RUBurstLimit,
		newState.Bucket.RURefillRate,
		newState.Bucket.RUCurrent,
		newState.Bucket.CurrentShareSum,
		newState.Consumption.RU,
		newState.Consumption.ReadRequests,
		newState.Consumption.ReadBytes,
		newState.Consumption.WriteRequests,
		newState.Consumption.WriteBytes,
		newState.Consumption.SQLPodCPUSeconds,
	); err != nil {
		// TODO(radu): handle duplicate request causing UUID collision.
		return err
	}
	return nil
}
