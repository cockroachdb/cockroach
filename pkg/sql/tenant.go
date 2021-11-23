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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// CreateTenantRecord creates a tenant in system.tenants, and optionally
// initializes the usage data in system.tenant_usage (if info.Usage is set).
func CreateTenantRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, info *descpb.TenantInfoWithUsage,
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
	infoBytes, err := protoutil.Marshal(&info.TenantInfo)
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

	if u := info.Usage; u != nil {
		consumption, err := protoutil.Marshal(&u.Consumption)
		if err != nil {
			return errors.Wrap(err, "marshaling tenant usage data")
		}
		if num, err := execCfg.InternalExecutor.ExecEx(
			ctx, "create-tenant-usage", txn, sessiondata.NodeUserSessionDataOverride,
			`INSERT INTO system.tenant_usage (
			  tenant_id, instance_id, next_instance_id, last_update,
			  ru_burst_limit, ru_refill_rate, ru_current, current_share_sum,
			  total_consumption)
			VALUES (
				$1, 0, 0, now(),
				$2, $3, $4, 0, 
				$5)`,
			tenID,
			u.RUBurstLimit, u.RURefillRate, u.RUCurrent,
			tree.NewDBytes(tree.DBytes(consumption)),
		); err != nil {
			if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
				return pgerror.Newf(pgcode.DuplicateObject, "tenant \"%d\" already has usage data", tenID)
			}
			return errors.Wrap(err, "inserting tenant usage data")
		} else if num != 1 {
			log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
		}
	}
	return nil
}

// GetTenantRecord retrieves a tenant in system.tenants.
func GetTenantRecord(
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
	info := &descpb.TenantInfoWithUsage{
		TenantInfo: descpb.TenantInfo{
			ID: tenID,
			// We synchronously initialize the tenant's keyspace below, so
			// we can skip the ADD state and go straight to an ACTIVE state.
			State: descpb.TenantInfo_ACTIVE,
		},
	}
	if err := CreateTenantRecord(ctx, p.ExecCfg(), p.Txn(), info); err != nil {
		return err
	}

	codec := keys.MakeSQLCodec(roachpb.MakeTenantID(tenID))
	// Initialize the tenant's keyspace.
	schema := bootstrap.MakeMetadataSchema(
		codec,
		p.ExtendedEvalContext().ExecCfg.DefaultZoneConfig, /* defaultZoneConfig */
		nil, /* defaultSystemZoneConfig */
	)
	kvs, splits := schema.GetInitialValues()

	{
		// Populate the version setting for the tenant. This will allow the tenant
		// to know what migrations need to be run in the future. The choice to use
		// the active cluster version here is intentional; it allows tenants
		// created during the mixed-version state in the host cluster to avoid
		// using code which may be too new. The expectation is that the tenant
		// clusters will be updated to a version only after the system tenant has
		// been upgraded.
		v := p.EvalContext().Settings.Version.ActiveVersion(ctx)
		tenantSettingKV, err := generateTenantClusterSettingKV(codec, v)
		if err != nil {
			return err
		}
		kvs = append(kvs, tenantSettingKV)
	}

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

	return nil
}

// generateTenantClusterSettingKV generates the kv to be written to the store
// to populate the system.settings table of the tenant implied by codec. This
// bootstraps the cluster version for the new tenant.
func generateTenantClusterSettingKV(
	codec keys.SQLCodec, v clusterversion.ClusterVersion,
) (roachpb.KeyValue, error) {
	encoded, err := protoutil.Marshal(&v)
	if err != nil {
		return roachpb.KeyValue{}, errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to encode current cluster version %v", &v)
	}
	kvs, err := rowenc.EncodePrimaryIndex(
		codec,
		systemschema.SettingsTable,
		systemschema.SettingsTable.GetPrimaryIndex(),
		catalog.ColumnIDToOrdinalMap(systemschema.SettingsTable.PublicColumns()),
		[]tree.Datum{
			tree.NewDString(clusterversion.KeyVersionSetting),      // name
			tree.NewDString(string(encoded)),                       // value
			tree.NewDTimeTZFromTime(timeutil.Now()),                // lastUpdated
			tree.NewDString((*settings.VersionSetting)(nil).Typ()), // type
		},
		false, /* includeEmpty */
	)
	if err != nil {
		return roachpb.KeyValue{}, errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to encode cluster setting")
	}
	if len(kvs) != 1 {
		return roachpb.KeyValue{}, errors.AssertionFailedf(
			"failed to encode cluster setting: expected 1 key-value, got %d", len(kvs))
	}
	return roachpb.KeyValue{
		Key:   kvs[0].Key,
		Value: kvs[0].Value,
	}, nil
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
	info, err := GetTenantRecord(ctx, execCfg, txn, tenID)
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
	info, err := GetTenantRecord(ctx, p.execCfg, p.txn, tenID)
	if err != nil {
		return errors.Wrap(err, "destroying tenant")
	}

	if info.State == descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is already in state DROP", tenID)
	}

	// Mark the tenant as dropping.
	info.State = descpb.TenantInfo_DROP
	if err := updateTenantRecord(ctx, p.execCfg, p.txn, info); err != nil {
		return errors.Wrap(err, "destroying tenant")
	}

	return errors.Wrap(gcTenantJob(ctx, p.execCfg, p.txn, p.User(), tenID), "scheduling gc job")
}

// GCTenantSync clears the tenant's data and removes its record.
func GCTenantSync(ctx context.Context, execCfg *ExecutorConfig, info *descpb.TenantInfo) error {
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

		if _, err := execCfg.InternalExecutor.ExecEx(
			ctx, "delete-tenant-usage", txn, sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.tenant_usage WHERE tenant_id = $1`, info.ID,
		); err != nil {
			return errors.Wrapf(err, "deleting tenant %d usage", info.ID)
		}
		return nil
	})
	return errors.Wrapf(err, "deleting tenant %d record", info.ID)
}

// gcTenantJob clears the tenant's data and removes its record using a GC job.
func gcTenantJob(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	user security.SQLUsername,
	tenID uint64,
) error {
	// Queue a GC job that will delete the tenant data and finally remove the
	// row from `system.tenants`.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	gcDetails.Tenant = &jobspb.SchemaChangeGCDetails_DroppedTenant{
		ID:       tenID,
		DropTime: timeutil.Now().UnixNano(),
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for tenant %d", tenID),
		Username:      user,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := execCfg.JobRegistry.CreateJobWithTxn(
		ctx, gcJobRecord, execCfg.JobRegistry.MakeJobID(), txn); err != nil {
		return err
	}
	return nil
}

// GCTenant implements the tree.TenantOperator interface.
func (p *planner) GCTenant(ctx context.Context, tenID uint64) error {
	// TODO(jeffswenson): Delete internal_crdb.gc_tenant after the DestroyTenant
	// changes are deployed to all Cockroach Cloud serverless hosts.
	if !p.ExtendedEvalContext().TxnImplicit {
		return errors.Errorf("gc_tenant cannot be used inside a transaction")
	}
	var info *descpb.TenantInfo
	if txnErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		info, err = GetTenantRecord(ctx, p.execCfg, p.txn, tenID)
		return err
	}); txnErr != nil {
		return errors.Wrapf(txnErr, "retrieving tenant %d", tenID)
	}

	// Confirm tenant is ready to be cleared.
	if info.State != descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is not in state DROP", info.ID)
	}

	return gcTenantJob(ctx, p.ExecCfg(), p.Txn(), p.User(), tenID)
}

// UpdateTenantResourceLimits implements the tree.TenantOperator interface.
func (p *planner) UpdateTenantResourceLimits(
	ctx context.Context,
	tenantID uint64,
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
) error {
	const op = "update-resource-limits"
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, op); err != nil {
		return err
	}
	return p.ExecCfg().TenantUsageServer.ReconfigureTokenBucket(
		ctx, p.Txn(), roachpb.MakeTenantID(tenantID),
		availableRU, refillRate, maxBurstRU, asOf, asOfConsumedRequestUnits,
	)
}
