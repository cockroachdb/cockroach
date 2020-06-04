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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// rejectIfCantCoordinateMultiTenancy returns an error if the current tenant is
// disallowed from coordinating tenant management operations on behalf of a
// multi-tenant cluster. Only the system tenant has permissions to do so.
func rejectIfCantCoordinateMultiTenancy(codec keys.SQLCodec, op string) error {
	// NOTE: even if we got this wrong, the rest of the function would fail for
	// a non-system tenant because they would be missing a system.tenant table.
	if !codec.ForSystemTenant() {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only the system tenant can %s other tenants", op)
	}
	return nil
}

// rejectIfSystemTenant returns an error if the provided tenant ID is the system
// tenant's ID.
func rejectIfSystemTenant(tenID uint64, op string) error {
	if roachpb.IsSystemTenantID(tenID) {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"cannot %s tenant \"%d\", ID assigned to system tenant", op, tenID)
	}
	return nil
}

// CreateTenant implements the tree.TenantOperator interface.
func (p *planner) CreateTenant(ctx context.Context, tenID uint64, tenInfo []byte) error {
	const op = "create"
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
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
		nil, /* defaultZoneConfig */
		nil, /* defaultZoneConfig */
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

	// Create initial splits for the new tenant. This is performed
	// non-transactionally, so the range splits will remain even if the
	// statement's transaction is rolled back. In this case, the manual splits
	// can and will be merged away after its 1h expiration elapses.
	//
	// If the statement's transaction commits and updates the system.tenants
	// table, the manual splits' expirations will no longer be necessary to
	// prevent the split points from being merged away. Likewise, if the
	// transaction did happen to take long enough that the manual splits'
	// expirations did elapse and the splits were merged away, they would
	// quickly (but asynchronously) be recreated once the KV layer notices the
	// updated system.tenants table in the gossipped SystemConfig.
	expTime := p.ExecCfg().Clock.Now().Add(time.Hour.Nanoseconds(), 0)
	for _, key := range splits {
		if err := p.ExecCfg().DB.AdminSplit(ctx, key, expTime); err != nil {
			return err
		}
	}

	// Tenant creation complete! Note that sqlmigrations have not been run yet.
	// They will be run when a sqlServer bound to this tenant is first launched.
	return nil
}

// DestroyTenant implements the tree.TenantOperator interface.
func (p *planner) DestroyTenant(ctx context.Context, tenID uint64) error {
	const op = "destroy"
	if err := rejectIfCantCoordinateMultiTenancy(p.ExecCfg().Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
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

	// TODO(nvanbenschoten): actually clear tenant keyspace. We don't want to do
	// this synchronously in the same transaction, because we could be deleting
	// a very large amount of data. Tracked in #48775.

	return nil
}
