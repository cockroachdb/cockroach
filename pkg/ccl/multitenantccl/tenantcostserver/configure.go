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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// ReconfigureTokenBucket updates a tenant's token bucket settings. It is part
// of the TenantUsageServer interface; see that for more details.
func (s *instance) ReconfigureTokenBucket(
	ctx context.Context,
	txn isql.Txn,
	tenantID roachpb.TenantID,
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
) error {
	if err := s.checkTenantID(ctx, txn, tenantID); err != nil {
		return err
	}
	h := makeSysTableHelper(ctx, tenantID)
	state, err := h.readTenantState(txn)
	if err != nil {
		return err
	}
	now := s.timeSource.Now()
	state.update(now)
	state.Bucket.Reconfigure(
		ctx, tenantID, availableRU, refillRate, maxBurstRU, asOf, asOfConsumedRequestUnits,
		now, state.Consumption.RU,
	)
	if err := h.updateTenantState(txn, state); err != nil {
		return err
	}
	return nil
}

// checkTenantID verifies that the tenant exists and is active.
func (s *instance) checkTenantID(
	ctx context.Context, txn isql.Txn, tenantID roachpb.TenantID,
) error {
	row, err := txn.QueryRowEx(
		ctx, "check-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT active FROM system.tenants WHERE id = $1`, tenantID.ToUint64(),
	)
	if err != nil {
		return err
	}
	if row == nil {
		return pgerror.Newf(pgcode.UndefinedObject, "tenant %q does not exist", tenantID)
	}
	if active := *row[0].(*tree.DBool); !active {
		return errors.Errorf("tenant %q is not active", tenantID)
	}
	return nil
}
