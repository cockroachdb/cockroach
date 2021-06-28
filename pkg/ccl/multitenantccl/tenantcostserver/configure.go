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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func init() {
	sql.UpdateTenantResourceLimitsImpl = ReconfigureTokenBucket
}

// ReconfigureTokenBucket updates a tenant's token bucket settings.
//
// Arguments:
//
//  - operationUUID identifies the reconfiguration operation and allows
//    providing idempotency: if the function is called twice with the same
//    operationUUID, the second call will silently succeed.
//
//  - availableRU is the amount of Request Units that the tenant can consume at
//    will. Also known as "burst RUs".
//
//  - refillRate is the amount of Request Units per second that the tenant
//    receives.
//
//  - maxBurstRU is the maximum amount of Request Units that can be accumulated
//    from the refill rate, or 0 if there is no limit.
//
//  - asOf is a timestamp; the reconfiguration request is assumed to be based on
//    the consumption at that time. This timestamp is used to compensate for any
//    refill that would have happened in the meantime.
//
//  - asOfConsumedRequestUnits is the total number of consumed RUs based on
//    which the reconfiguration values were calculated (i.e. at the asOf time).
//    It is used to adjust availableRU with the consumption that happened in the
//    meantime.
//
func ReconfigureTokenBucket(
	ctx context.Context,
	txn *kv.Txn,
	ex *sql.InternalExecutor,
	tenantID uint64,
	operationUUID uuid.UUID,
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
) error {
	row, err := ex.QueryRowEx(
		ctx, "check-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`SELECT active FROM system.tenants WHERE id = $1`, tenantID,
	)
	if err != nil {
		return err
	}
	if row == nil {
		return pgerror.Newf(pgcode.UndefinedObject, "tenant \"%d\" does not exist", tenantID)
	}
	if active := *row[0].(*tree.DBool); !active {
		return errors.Errorf("tenant \"%d\" is not active", tenantID)
	}
	state, err := readTenantUsageState(ctx, ex, txn, tenantID)
	if err != nil {
		return err
	}
	state.Seq++
	state.Bucket.Reconfigure(
		availableRU, refillRate, maxBurstRU, asOf, asOfConsumedRequestUnits,
		timeutil.Now(), state.Consumption.RU,
	)
	if err := updateTenantUsageState(ctx, ex, txn, tenantID, operationUUID, state); err != nil {
		return err
	}
	return nil
}
