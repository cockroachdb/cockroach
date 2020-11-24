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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

// CreateTenantRecord creates a tenant in system.tenants.
func CreateTenantRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, info *descpb.TenantInfo,
) error {
	const op = "create"
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(info.ID, op); err != nil {
		return err
	}

	tenID := info.ID
	active := info.State == descpb.TenantInfo_ACTIVE
	infoBytes, err := protoutil.Marshal(info)
	if err != nil {
		return err
	}

	// Insert into the tenant table and detect collisions.
	if num, err := execCfg.InternalExecutor.ExecEx(
		ctx, "create-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.tenants (id, active, info) VALUES ($1, $2, $3)`,
		tenID, active, infoBytes,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "tenant \"%d\" already exists", tenID)
		}
		return errors.Wrap(err, "inserting new tenant")
	} else if num != 1 {
		log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
	}
	return nil
}

// getTenantRecord retrieves a tenant in system.tenants.
func getTenantRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, tenID uint64,
) (*descpb.TenantInfo, error) {
	row, err := execCfg.InternalExecutor.QueryRowEx(
		ctx, "activate-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`SELECT info FROM system.tenants WHERE id = $1`, tenID,
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant \"%d\" does not exist", tenID)
	}

	info := &descpb.TenantInfo{}
	infoBytes := []byte(tree.MustBeDBytes(row[0]))
	if err := protoutil.Unmarshal(infoBytes, info); err != nil {
		return nil, err
	}
	return info, nil
}

// updateTenantRecord updates a tenant in system.tenants.
func updateTenantRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, info *descpb.TenantInfo,
) error {
	tenID := info.ID
	active := info.State == descpb.TenantInfo_ACTIVE
	infoBytes, err := protoutil.Marshal(info)
	if err != nil {
		return err
	}

	if num, err := execCfg.InternalExecutor.ExecEx(
		ctx, "activate-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.tenants SET active = $2, info = $3 WHERE id = $1`,
		tenID, active, infoBytes,
	); err != nil {
		return errors.Wrap(err, "activating tenant")
	} else if num != 1 {
		log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
	}
	return nil
}

// CreateTenant implements the tree.TenantOperator interface.
func (p *planner) CreateTenant(ctx context.Context, tenID uint64) error {
	info := &descpb.TenantInfo{
		ID: tenID,
		// We synchronously initialize the tenant's keyspace below, so
		// we can skip the ADD state and go straight to an ACTIVE state.
		State: descpb.TenantInfo_ACTIVE,
	}
	if err := CreateTenantRecord(ctx, p.ExecCfg(), p.Txn(), info); err != nil {
		return err
	}

	// Initialize the tenant's keyspace.
	schema := bootstrap.MakeMetadataSchema(
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

// ActivateTenant marks a tenant active.
func ActivateTenant(ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, tenID uint64) error {
	const op = "activate"
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	// Retrieve the tenant's info.
	info, err := getTenantRecord(ctx, execCfg, txn, tenID)
	if err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	// Mark the tenant as active.
	info.State = descpb.TenantInfo_ACTIVE
	if err := updateTenantRecord(ctx, execCfg, txn, info); err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	return nil
}

// clearTenant deletes the tenant's data.
func clearTenant(ctx context.Context, execCfg *ExecutorConfig, info *descpb.TenantInfo) error {
	// Confirm tenant is ready to be cleared.
	if info.State != descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is not in state DROP", info.ID)
	}

	log.Infof(ctx, "clearing data for tenant %d", info.ID)

	prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(info.ID))
	prefixEnd := prefix.PrefixEnd()

	log.VEventf(ctx, 2, "ClearRange %s - %s", prefix, prefixEnd)
	// ClearRange cannot be run in a transaction, so create a non-transactional
	// batch to send the request.
	b := &kv.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{Key: prefix, EndKey: prefixEnd},
	})

	return errors.Wrapf(execCfg.DB.Run(ctx, b), "clearing tenant %d data", info.ID)
}

// DestroyTenant implements the tree.TenantOperator interface.
func (p *planner) DestroyTenant(ctx context.Context, tenID uint64) error {
	const op = "destroy"
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	// Retrieve the tenant's info.
	info, err := getTenantRecord(ctx, p.execCfg, p.txn, tenID)
	if err != nil {
		return errors.Wrap(err, "destroying tenant")
	}

	if info.State == descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is already in state DROP", tenID)
	}

	// Mark the tenant as dropping.
	info.State = descpb.TenantInfo_DROP
	return errors.Wrap(updateTenantRecord(ctx, p.execCfg, p.txn, info), "destroying tenant")
}

// GCTenant clears the tenant's data and removes its record.
func GCTenant(ctx context.Context, execCfg *ExecutorConfig, info *descpb.TenantInfo) error {
	const op = "gc"
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(info.ID, op); err != nil {
		return err
	}

	if err := clearTenant(ctx, execCfg, info); err != nil {
		return errors.Wrap(err, "clear tenant")
	}

	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if num, err := execCfg.InternalExecutor.ExecEx(
			ctx, "delete-tenant", txn, sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.tenants WHERE id = $1`, info.ID,
		); err != nil {
			return errors.Wrapf(err, "deleting tenant %d", info.ID)
		} else if num != 1 {
			log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
		}
		return nil
	})
	return errors.Wrapf(err, "deleting tenant %d record", info.ID)
}

// GCTenant implements the tree.TenantOperator interface.
func (p *planner) GCTenant(ctx context.Context, tenID uint64) error {
	if !p.ExtendedEvalContext().TxnImplicit {
		return errors.Errorf("gc_tenant cannot be used inside a transaction")
	}

	var info *descpb.TenantInfo
	var err error
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		info, err = getTenantRecord(ctx, p.execCfg, p.txn, tenID)
		if err != nil {
			return errors.Wrapf(err, "retrieving tenant %d", tenID)
		}
		return nil
	}); err != nil {
		return err
	}

	return GCTenant(ctx, p.ExecCfg(), info)
}
