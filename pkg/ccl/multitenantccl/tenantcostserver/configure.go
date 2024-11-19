// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostserver

import (
	"context"

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
	availableTokens float64,
	refillRate float64,
	maxBurstTokens float64,
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
	state.Bucket.Reconfigure(ctx, tenantID, availableTokens, refillRate, maxBurstTokens)
	if err := h.updateTenantAndInstanceState(txn, &state, nil); err != nil {
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
