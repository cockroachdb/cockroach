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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// rejectIfCantCoordinateMultiTenancy returns an error if the current tenant is
// disallowed from coordinating tenant management operations on behalf of a
// multi-tenant cluster. Only the system tenant has permissions to do so.
func rejectIfCantCoordinateMultiTenancy(codec keys.SQLCodec, op string) error {
	// NOTE: even if we got this wrong, the rest of the function would fail for
	// a non-system tenant because they would be missing a system.tenants table.
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

// CreateTenantRecord creates a tenant in system.tenants and installs an initial
// span config (in system.span_configurations) for it. It also initializes the
// usage data in system.tenant_usage if info.Usage is set.
//
// If the passed in `info` has the `TenantID` field unset (= 0),
// CreateTenantRecord will assign the tenant the next available ID after
// consulting the system.tenants table.
func CreateTenantRecord(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	info *descpb.TenantInfoWithUsage,
	initialTenantZoneConfig *zonepb.ZoneConfig,
) (roachpb.TenantID, error) {
	const op = "create"
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return roachpb.TenantID{}, err
	}
	if err := rejectIfSystemTenant(info.ID, op); err != nil {
		return roachpb.TenantID{}, err
	}
	if info.Name != "" {
		if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNames) {
			return roachpb.TenantID{}, pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
		if err := info.Name.IsValid(); err != nil {
			return roachpb.TenantID{}, pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
	}

	tenID := info.ID
	if tenID == 0 {
		tenantID, err := getAvailableTenantID(ctx, info.Name, execCfg, txn)
		if err != nil {
			return roachpb.TenantID{}, err
		}
		tenID = tenantID.ToUint64()
		info.ID = tenID
	}

	if info.Name == "" {
		// No name: generate one if we are at the appropriate version.
		if execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNames) {
			info.Name = roachpb.TenantName(fmt.Sprintf("anonymous-tenant-%d", info.ID))
		}
	}

	active := info.State == descpb.TenantInfo_ACTIVE
	infoBytes, err := protoutil.Marshal(&info.TenantInfo)
	if err != nil {
		return roachpb.TenantID{}, err
	}

	// Insert into the tenant table and detect collisions.
	if info.Name != "" {
		if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNames) {
			return roachpb.TenantID{}, pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
	}
	if num, err := execCfg.InternalExecutor.ExecEx(
		ctx, "create-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.tenants (id, active, info) VALUES ($1, $2, $3)`,
		tenID, active, infoBytes,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			extra := redact.RedactableString("")
			if info.Name != "" {
				extra = redact.Sprintf(" with name %q", info.Name)
			}
			return roachpb.TenantID{}, pgerror.Newf(pgcode.DuplicateObject, "tenant \"%d\"%s already exists", tenID, extra)
		}
		return roachpb.TenantID{}, errors.Wrap(err, "inserting new tenant")
	} else if num != 1 {
		log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
	}

	if u := info.Usage; u != nil {
		consumption, err := protoutil.Marshal(&u.Consumption)
		if err != nil {
			return roachpb.TenantID{}, errors.Wrap(err, "marshaling tenant usage data")
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
				return roachpb.TenantID{}, pgerror.Newf(pgcode.DuplicateObject, "tenant \"%d\" already has usage data", tenID)
			}
			return roachpb.TenantID{}, errors.Wrap(err, "inserting tenant usage data")
		} else if num != 1 {
			log.Fatalf(ctx, "unexpected number of rows affected: %d", num)
		}
	}

	// Install a single key[1] span config at the start of tenant's keyspace;
	// elsewhere this ensures that we split on the tenant boundary. The subset
	// of entries with spans in the tenant keyspace are, henceforth, governed
	// by the tenant's SQL pods. This entry may be replaced with others when the
	// SQL pods reconcile their zone configs for the first time. When destroying
	// the tenant for good, we'll clear out any left over entries as part of the
	// GC-ing the tenant's record.
	//
	// [1]: It doesn't actually matter what span is inserted here as long as it
	//      starts at the tenant prefix and is fully contained within the tenant
	//      keyspace. The span does not need to extend all the way to the
	//      tenant's prefix end because we only look at start keys for split
	//      boundaries. Whatever is inserted will get cleared out by the
	//      tenant's reconciliation process.

	tenantSpanConfig := initialTenantZoneConfig.AsSpanConfig()
	// Make sure to enable rangefeeds; the tenant will need them on its system
	// tables as soon as it starts up. It's not unsafe/buggy if we didn't do this,
	// -- the tenant's span config reconciliation process would eventually install
	// appropriate (rangefeed.enabled = true) configs for its system tables, at
	// which point subsystems that rely on rangefeeds are able to proceed. All of
	// this can noticeably slow down pod startup, so we just enable things to
	// start with.
	tenantSpanConfig.RangefeedEnabled = true
	// Make it behave like usual system database ranges, for good measure.
	tenantSpanConfig.GCPolicy.IgnoreStrictEnforcement = true

	tenantPrefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenID))
	record, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSpan(roachpb.Span{
		Key:    tenantPrefix,
		EndKey: tenantPrefix.Next(),
	}), tenantSpanConfig)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	toUpsert := []spanconfig.Record{record}
	scKVAccessor := execCfg.SpanConfigKVAccessor.WithTxn(ctx, txn)
	return roachpb.MustMakeTenantID(tenID), scKVAccessor.UpdateSpanConfigRecords(
		ctx, nil, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
	)
}

// GetAllNonDropTenantIDs returns all tenants in the system table, excluding
// those in the DROP state.
func GetAllNonDropTenantIDs(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn,
) ([]roachpb.TenantID, error) {
	rows, err := execCfg.InternalExecutor.QueryBuffered(
		ctx, "get-tenant-ids", txn, `
		 SELECT id
		 FROM system.tenants
		 WHERE crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info, true)->>'state' != 'DROP'
		 ORDER BY id
		 `)
	if err != nil {
		return nil, err
	}

	tenants := make([]roachpb.TenantID, 0, len(rows))
	for _, tenant := range rows {
		iTenantId := uint64(tree.MustBeDInt(tenant[0]))
		tenantId, err := roachpb.MakeTenantID(iTenantId)
		if err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(
				err, "stored tenant ID %d does not convert to TenantID", iTenantId)
		}
		tenants = append(tenants, tenantId)
	}

	return tenants, nil
}

// GetTenantRecordByName retrieves a tenant with the provided name from
// system.tenants.
func GetTenantRecordByName(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, tenantName roachpb.TenantName,
) (*descpb.TenantInfo, error) {
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNames) {
		return nil, errors.Newf("tenant names not supported until upgrade to %s or higher is completed",
			clusterversion.V23_1TenantNames.String())
	}
	row, err := execCfg.InternalExecutor.QueryRowEx(
		ctx, "get-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`SELECT info FROM system.tenants WHERE name = $1`, tenantName,
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant %q does not exist", tenantName)
	}

	info := &descpb.TenantInfo{}
	infoBytes := []byte(tree.MustBeDBytes(row[0]))
	if err := protoutil.Unmarshal(infoBytes, info); err != nil {
		return nil, err
	}
	return info, nil
}

// GetTenantRecordByID retrieves a tenant in system.tenants.
func GetTenantRecordByID(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, tenID roachpb.TenantID,
) (*descpb.TenantInfo, error) {
	row, err := execCfg.InternalExecutor.QueryRowEx(
		ctx, "get-tenant", txn, sessiondata.NodeUserSessionDataOverride,
		`SELECT info FROM system.tenants WHERE id = $1`, tenID.ToUint64(),
	)
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "tenant \"%d\" does not exist", tenID.ToUint64())
	}

	info := &descpb.TenantInfo{}
	infoBytes := []byte(tree.MustBeDBytes(row[0]))
	if err := protoutil.Unmarshal(infoBytes, info); err != nil {
		return nil, err
	}
	return info, nil
}

// UpdateTenantRecord updates a tenant in system.tenants.
//
// Caller is expected to check the user's permission.
func UpdateTenantRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, info *descpb.TenantInfo,
) error {
	if err := validateTenantInfo(info); err != nil {
		return err
	}

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

func validateTenantInfo(info *descpb.TenantInfo) error {
	if info.TenantReplicationJobID != 0 && info.State == descpb.TenantInfo_ACTIVE {
		return errors.Newf("tenant in state %v with replication job ID %d", info.State, info.TenantReplicationJobID)
	}
	if info.DroppedName != "" && info.State != descpb.TenantInfo_DROP {
		return errors.Newf("tenant in state %v with dropped name %q", info.State, info.DroppedName)
	}
	return nil
}

// getAvailableTenantID returns the first available ID that can be assigned to
// the created tenant. Note, this ID could have previously belonged to another
// tenant that has since been dropped and gc'ed.
func getAvailableTenantID(
	ctx context.Context, tenantName roachpb.TenantName, execCfg *ExecutorConfig, txn *kv.Txn,
) (roachpb.TenantID, error) {
	// Find the first available ID that can be assigned to the created tenant.
	// Note, this ID could have previously belonged to another tenant that has
	// since been dropped and gc'ed.
	row, err := execCfg.InternalExecutor.QueryRowEx(ctx, "next-tenant-id", txn,
		sessiondata.NodeUserSessionDataOverride, `
   SELECT id+1 AS newid
    FROM (VALUES (1) UNION ALL SELECT id FROM system.tenants) AS u(id)
   WHERE NOT EXISTS (SELECT 1 FROM system.tenants t WHERE t.id=u.id+1)
     AND ($1 = '' OR NOT EXISTS (SELECT 1 FROM system.tenants t WHERE t.name=$1))
   ORDER BY id LIMIT 1
`, tenantName)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	if row == nil {
		return roachpb.TenantID{}, errors.Newf("tenant with name %q already exists", tenantName)
	}
	nextID := *row[0].(*tree.DInt)
	return roachpb.MustMakeTenantID(uint64(nextID)), nil
}

// CreateTenant implements the tree.TenantOperator interface.
func (p *planner) CreateTenant(
	ctx context.Context, tenantID uint64, name roachpb.TenantName,
) (tid roachpb.TenantID, err error) {
	if p.EvalContext().TxnReadOnly {
		return tid, readOnlyError("create_tenant()")
	}
	const op = "create tenant"
	if err := p.RequireAdminRole(ctx, op); err != nil {
		return tid, err
	}
	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "create"); err != nil {
		return tid, err
	}

	info := &descpb.TenantInfoWithUsage{
		TenantInfo: descpb.TenantInfo{
			ID: tenantID,
			// We synchronously initialize the tenant's keyspace below, so
			// we can skip the ADD state and go straight to an ACTIVE state.
			State: descpb.TenantInfo_ACTIVE,
			Name:  name,
		},
	}

	initialTenantZoneConfig, err := GetHydratedZoneConfigForTenantsRange(ctx, p.Txn(), p.Descriptors())
	if err != nil {
		return tid, err
	}

	// Create the record. This also auto-allocates an ID if the
	// tenantID was zero.
	if _, err := CreateTenantRecord(ctx, p.ExecCfg(), p.Txn(), info, initialTenantZoneConfig); err != nil {
		return tid, err
	}
	// Retrieve the possibly auto-generated ID.
	tenantID = info.ID
	tid = roachpb.MustMakeTenantID(tenantID)

	// Initialize the tenant's keyspace.
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(tenantID))
	schema := bootstrap.MakeMetadataSchema(
		codec,
		initialTenantZoneConfig, /* defaultZoneConfig */
		initialTenantZoneConfig, /* defaultSystemZoneConfig */
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
			return tid, err
		}
		kvs = append(kvs, tenantSettingKV)
	}

	b := p.Txn().NewBatch()
	for _, kv := range kvs {
		b.CPut(kv.Key, &kv.Value, nil)
	}
	if err := p.Txn().Run(ctx, b); err != nil {
		if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
			return tid, errors.Wrap(err, "programming error: "+
				"tenant already exists but was not in system.tenants table")
		}
		return tid, err
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
	// updated system.tenants table in the gossipped SystemConfig, or if using
	// the span configs infrastructure, in `system.span_configurations`.
	expTime := p.ExecCfg().Clock.Now().Add(time.Hour.Nanoseconds(), 0)
	for _, key := range splits {
		if err := p.ExecCfg().DB.AdminSplit(ctx, key, expTime); err != nil {
			return tid, err
		}
	}

	return tid, nil
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
	ts, err := tree.MakeDTimestamp(timeutil.Now(), time.Microsecond)
	if err != nil {
		return roachpb.KeyValue{}, errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to represent the current time")
	}
	kvs, err := rowenc.EncodePrimaryIndex(
		codec,
		systemschema.SettingsTable,
		systemschema.SettingsTable.GetPrimaryIndex(),
		catalog.ColumnIDToOrdinalMap(systemschema.SettingsTable.PublicColumns()),
		[]tree.Datum{
			tree.NewDString(clusterversion.KeyVersionSetting), // name
			tree.NewDString(string(encoded)),                  // value
			ts,                                                // lastUpdated
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
//
// The caller is responsible for checking that the user is authorized
// to take this action.
func ActivateTenant(ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, tenID uint64) error {
	const op = "activate"
	if err := rejectIfCantCoordinateMultiTenancy(execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	// Retrieve the tenant's info.
	info, err := GetTenantRecordByID(ctx, execCfg, txn, roachpb.MustMakeTenantID(tenID))
	if err != nil {
		return errors.Wrap(err, "activating tenant")
	}

	// Mark the tenant as active.
	info.State = descpb.TenantInfo_ACTIVE
	if err := UpdateTenantRecord(ctx, execCfg, txn, info); err != nil {
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

	prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(info.ID))
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

// DestroyTenantByID implements the tree.TenantOperator interface.
func (p *planner) DestroyTenantByID(
	ctx context.Context, tenID uint64, synchronousImmediateDrop bool,
) error {
	if err := p.validateDestroyTenant(ctx); err != nil {
		return err
	}

	info, err := GetTenantRecordByID(ctx, p.execCfg, p.txn, roachpb.MustMakeTenantID(tenID))
	if err != nil {
		return errors.Wrap(err, "destroying tenant")
	}
	return destroyTenantInternal(ctx, p.txn, p.execCfg, &p.extendedEvalCtx, p.User(), info, synchronousImmediateDrop)
}

func (p *planner) validateDestroyTenant(ctx context.Context) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("destroy_tenant()")
	}

	const op = "destroy"
	if err := p.RequireAdminRole(ctx, "destroy tenant"); err != nil {
		return err
	}
	return rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op)
}

func destroyTenantInternal(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	extendedEvalCtx *extendedEvalContext,
	user username.SQLUsername,
	info *descpb.TenantInfo,
	synchronousImmediateDrop bool,
) error {
	const op = "destroy"
	tenID := info.ID
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	if info.State == descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is already in state DROP", tenID)
	}

	// Mark the tenant as dropping.
	//
	// Cancel any running replication job on this tenant record.
	// The GCJob will wait for this job to enter a terminal state.
	if info.TenantReplicationJobID != 0 {
		if err := execCfg.JobRegistry.CancelRequested(ctx, txn, info.TenantReplicationJobID); err != nil {
			return errors.Wrapf(err, "canceling tenant replication job %d", info.TenantReplicationJobID)
		}
	}

	// TODO(ssd): We may want to implement a job that waits out
	// any running sql pods before enqueing the GC job.
	info.State = descpb.TenantInfo_DROP
	info.DroppedName = info.Name
	info.Name = ""
	if err := UpdateTenantRecord(ctx, execCfg, txn, info); err != nil {
		return errors.Wrap(err, "destroying tenant")
	}

	jobID, err := gcTenantJob(ctx, execCfg, txn, user, tenID, synchronousImmediateDrop)
	if err != nil {
		return errors.Wrap(err, "scheduling gc job")
	}
	if synchronousImmediateDrop {
		extendedEvalCtx.Jobs.add(jobID)
	}
	return nil
}

// GCTenantSync clears the tenant's data and removes its record.
//
// The caller is responsible for checking that the user is authorized
// to take this action.
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

		if _, err := execCfg.InternalExecutor.ExecEx(
			ctx, "delete-tenant-settings", txn, sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.tenant_settings WHERE tenant_id = $1`, info.ID,
		); err != nil {
			return errors.Wrapf(err, "deleting tenant %d settings", info.ID)
		}

		// Clear out all span config records left over by the tenant.
		tenID := roachpb.MustMakeTenantID(info.ID)
		tenantPrefix := keys.MakeTenantPrefix(tenID)
		tenantSpan := roachpb.Span{
			Key:    tenantPrefix,
			EndKey: tenantPrefix.PrefixEnd(),
		}

		systemTarget, err := spanconfig.MakeTenantKeyspaceTarget(tenID, tenID)
		if err != nil {
			return err
		}
		scKVAccessor := execCfg.SpanConfigKVAccessor.WithTxn(ctx, txn)
		records, err := scKVAccessor.GetSpanConfigRecords(
			ctx, []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(tenantSpan),
				spanconfig.MakeTargetFromSystemTarget(systemTarget),
			},
		)
		if err != nil {
			return err
		}

		toDelete := make([]spanconfig.Target, len(records))
		for i, record := range records {
			toDelete[i] = record.GetTarget()
		}
		return scKVAccessor.UpdateSpanConfigRecords(
			ctx, toDelete, nil, hlc.MinTimestamp, hlc.MaxTimestamp,
		)
	})
	return errors.Wrapf(err, "deleting tenant %d record", info.ID)
}

// gcTenantJob clears the tenant's data and removes its record using a GC job.
func gcTenantJob(
	ctx context.Context,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	user username.SQLUsername,
	tenID uint64,
	dropImmediately bool,
) (jobspb.JobID, error) {
	// Queue a GC job that will delete the tenant data and finally remove the
	// row from `system.tenants`.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	gcDetails.Tenant = &jobspb.SchemaChangeGCDetails_DroppedTenant{
		ID:       tenID,
		DropTime: timeutil.Now().UnixNano(),
	}
	progress := jobspb.SchemaChangeGCProgress{}
	if dropImmediately {
		progress.Tenant = &jobspb.SchemaChangeGCProgress_TenantProgress{
			Status: jobspb.SchemaChangeGCProgress_CLEARING,
		}
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for tenant %d", tenID),
		Username:      user,
		Details:       gcDetails,
		Progress:      progress,
		NonCancelable: true,
	}
	jobID := execCfg.JobRegistry.MakeJobID()
	if _, err := execCfg.JobRegistry.CreateJobWithTxn(
		ctx, gcJobRecord, jobID, txn,
	); err != nil {
		return 0, err
	}
	return jobID, nil
}

// GCTenant implements the tree.TenantOperator interface.
//
// TODO(jeffswenson): Delete internal_crdb.gc_tenant after the DestroyTenant
// changes are deployed to all Cockroach Cloud serverless hosts.
func (p *planner) GCTenant(ctx context.Context, tenID uint64) error {
	if !p.extendedEvalCtx.TxnIsSingleStmt {
		return errors.Errorf("gc_tenant cannot be used inside a multi-statement transaction")
	}
	if err := p.RequireAdminRole(ctx, "gc tenant"); err != nil {
		return err
	}
	var info *descpb.TenantInfo
	if txnErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		info, err = GetTenantRecordByID(ctx, p.execCfg, p.txn, roachpb.MustMakeTenantID(tenID))
		return err
	}); txnErr != nil {
		return errors.Wrapf(txnErr, "retrieving tenant %d", tenID)
	}

	// Confirm tenant is ready to be cleared.
	if info.State != descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is not in state DROP", info.ID)
	}

	_, err := gcTenantJob(
		ctx, p.ExecCfg(), p.Txn(), p.User(), tenID, false, /* synchronous */
	)
	return err
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
	if err := p.RequireAdminRole(ctx, "update tenant resource limits"); err != nil {
		return err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, op); err != nil {
		return err
	}
	return p.WithInternalExecutor(ctx, func(
		ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor,
	) error {
		return p.ExecCfg().TenantUsageServer.ReconfigureTokenBucket(
			ctx, p.Txn(), ie, roachpb.MustMakeTenantID(tenantID), availableRU, refillRate,
			maxBurstRU, asOf, asOfConsumedRequestUnits,
		)
	})
}

// GetTenantInfo implements the tree.TenantOperator interface.
func (p *planner) GetTenantInfo(
	ctx context.Context, tenantName roachpb.TenantName,
) (*descpb.TenantInfo, error) {
	const op = "get-tenant-info"
	if err := p.RequireAdminRole(ctx, op); err != nil {
		return nil, err
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op); err != nil {
		return nil, err
	}

	return GetTenantRecordByName(ctx, p.execCfg, p.Txn(), tenantName)
}

// TestingUpdateTenantRecord is a public wrapper around updateTenantRecord
// intended for testing purposes.
func TestingUpdateTenantRecord(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, info *descpb.TenantInfo,
) error {
	return UpdateTenantRecord(ctx, execCfg, txn, info)
}

// RenameTenant implements the tree.TenantOperator interface.
func (p *planner) RenameTenant(
	ctx context.Context, tenantID uint64, tenantName roachpb.TenantName,
) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("rename_tenant()")
	}

	if err := p.RequireAdminRole(ctx, "rename tenant"); err != nil {
		return err
	}
	if err := rejectIfSystemTenant(tenantID, "rename"); err != nil {
		return err
	}

	if tenantName != "" {
		if err := tenantName.IsValid(); err != nil {
			return pgerror.WithCandidateCode(err, pgcode.Syntax)
		}

		if !p.EvalContext().Settings.Version.IsActive(ctx, clusterversion.V23_1TenantNames) {
			return pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
	}

	if num, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx, "rename-tenant", p.txn, sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.public.tenants
SET info =
crdb_internal.json_to_pb('cockroach.sql.sqlbase.TenantInfo',
  crdb_internal.pb_to_json('cockroach.sql.sqlbase.TenantInfo', info) ||
  json_build_object('name', $2))
WHERE id = $1`, tenantID, tenantName); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "name %q is already taken", tenantName)
		}
		return errors.Wrap(err, "renaming tenant")
	} else if num != 1 {
		return pgerror.Newf(pgcode.UndefinedObject, "tenant %d not found", tenantID)
	}

	return nil
}
