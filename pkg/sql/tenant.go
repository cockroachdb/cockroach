// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// canCoordinateMultiTenancy returns whether the current tenant can coordinate
// tenant management operations on behalf of a multi-tenant cluster. Only the
// system tenant currently has permissions to do so.
func (p *planner) canCoordinateMultiTenancy() bool {
	return p.ExecCfg().Codec.ForSystemTenant()
}

// CreateTenant implements the tree.TenantOperator interface.
func (p *planner) CreateTenant(ctx context.Context, tenID uint64, tenInfo []byte) error {
	if !p.canCoordinateMultiTenancy() {
		// NOTE: even if we got this wrong, the rest of the function would fail
		// for a non-system tenant because they would be missing a system.tenant
		// table.
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can create other tenants")
	}

	// NB: interface{}([]byte(nil)) != interface{}(nil).
	var tenInfoArg interface{}
	if tenInfo != nil {
		tenInfoArg = tenInfo
	}

	// Insert into the tenant table and detect collisions.
	if num, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx, "create-tenant", p.Txn(), sqlbase.NodeUserSessionDataOverride,
		`INSERT INTO system.tenants (id, info) VALUES ($1, $2)`, tenID, tenInfoArg,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "tenant \"%d\" already exists", tenID)
		}
		return errors.Wrap(err, "inserting new tenant")
	} else if num != 1 {
		log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
	}

	// Initialize the tenant's keyspace.
	schema := sqlbase.MakeMetadataSchema(
		keys.MakeSQLCodec(roachpb.MakeTenantID(tenID)),
		zonepb.DefaultZoneConfigRef(),
		zonepb.DefaultSystemZoneConfigRef(),
	)
	kvs, splits := schema.GetInitialValues()
	b := p.Txn().NewBatch()
	for _, kv := range kvs {
		b.CPut(kv.Key, &kv.Value, nil)
	}
	if err := p.Txn().Run(ctx, b); err != nil {
		if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
			return errors.Wrap(err, "programming error: "+
				"tenant already exists but was not in system.tenants table")
		}
		return err
	}

	// TODO(nvanbenschoten): we currently neither split ranges between a single
	// tenant's tables nor split ranges between different tenant keyspaces. We
	// should do both. Performing the splits here won't have the desired effect
	// until we also teach SystemConfig.ComputeSplitKey about tenant tables and
	// tenant boundaries. Tracked in #48774.
	_ = splits

	// Tenant creation complete! Note that sqlmigrations have not been run yet.
	// They will be run when a sqlServer bound to this tenant is first launched.

	return nil
}

// DestroyTenant implements the tree.TenantOperator interface.
func (p *planner) DestroyTenant(ctx context.Context, tenID uint64) error {
	if !p.canCoordinateMultiTenancy() {
		// NOTE: even if we got this wrong, the rest of the function would fail
		// for a non-system tenant because they would be missing a system.tenant
		// table.
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can destroy other tenants")
	}

	// Query the tenant's active status. If it is marked as inactive, it is
	// already destroyed.
	if row, err := p.ExecCfg().InternalExecutor.QueryRowEx(
		ctx, "destroy-tenant", p.Txn(), sqlbase.NodeUserSessionDataOverride,
		`SELECT active FROM system.tenants WHERE id = $1`, tenID,
	); err != nil {
		return errors.Wrap(err, "deleting tenant")
	} else if row == nil {
		return pgerror.Newf(pgcode.UndefinedObject, "tenant \"%d\" does not exist", tenID)
	} else if !bool(tree.MustBeDBool(row[0])) {
		return nil // tenant already destroyed
	}

	// Mark the tenant as inactive.
	if num, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx, "destroy-tenant", p.Txn(), sqlbase.NodeUserSessionDataOverride,
		`UPDATE system.tenants SET active = false WHERE id = $1`, tenID,
	); err != nil {
		return errors.Wrap(err, "deleting tenant")
	} else if num != 1 {
		log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
	}

	// TODO(nvanbenschoten): actually clear tenant keyspace. Do we want to do
	// this synchronously or schedule it as a long-running job. Probably the
	// latter, because we could be deleteing a very large amount of data.
	// Tracked in #48775.

	return nil
}
