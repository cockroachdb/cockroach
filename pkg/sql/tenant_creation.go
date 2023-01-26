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
	gojson "encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CreateTenant implements the tree.TenantOperator interface.
func (p *planner) CreateTenant(
	ctx context.Context, parameters string,
) (tid roachpb.TenantID, err error) {
	var ctcfg createTenantConfig
	if parameters != "" {
		d := gojson.NewDecoder(strings.NewReader(parameters))
		d.DisallowUnknownFields()
		if err := d.Decode(&ctcfg); err != nil {
			return tid, pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
	}
	return p.createTenantInternal(ctx, ctcfg)
}

type createTenantConfig struct {
	ID          *uint64 `json:"id,omitempty"`
	Name        *string `json:"name,omitempty"`
	ServiceMode *string `json:"service_mode,omitempty"`
}

func (p *planner) createTenantInternal(
	ctx context.Context, ctcfg createTenantConfig,
) (tid roachpb.TenantID, err error) {
	var tenantID uint64
	if ctcfg.ID != nil {
		tenantID = *ctcfg.ID
	}
	var name roachpb.TenantName
	if ctcfg.Name != nil {
		name = roachpb.TenantName(*ctcfg.Name)
	}
	serviceMode := mtinfopb.ServiceModeNone
	if ctcfg.ServiceMode != nil {
		v, ok := mtinfopb.TenantServiceModeValues[strings.ToLower(*ctcfg.ServiceMode)]
		if !ok {
			return tid, pgerror.Newf(pgcode.Syntax, "unknown service mode: %q", *ctcfg.ServiceMode)
		}
		serviceMode = v
	}

	// tenantID uint64, name roachpb.TenantName,
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

	info := &mtinfopb.TenantInfoWithUsage{
		SQLInfo: mtinfopb.SQLInfo{
			ID: tenantID,
			// We synchronously initialize the tenant's keyspace below, so
			// we can skip the ADD state and go straight to the READY state.
			DataState:   mtinfopb.DataStateReady,
			Name:        name,
			ServiceMode: serviceMode,
		},
	}

	initialTenantZoneConfig, err := GetHydratedZoneConfigForTenantsRange(ctx, p.Txn(), p.Descriptors())
	if err != nil {
		return tid, err
	}

	// Create the record. This also auto-allocates an ID if the
	// tenantID was zero.
	if _, err := CreateTenantRecord(
		ctx,
		p.ExecCfg().Codec,
		p.ExecCfg().Settings,
		p.InternalSQLTxn(),
		p.ExecCfg().SpanConfigKVAccessor.WithTxn(ctx, p.Txn()),
		info,
		initialTenantZoneConfig,
	); err != nil {
		return tid, err
	}
	// Retrieve the possibly auto-generated ID.
	tenantID = info.ID
	tid = roachpb.MustMakeTenantID(tenantID)

	// Initialize the tenant's keyspace.
	var tenantVersion clusterversion.ClusterVersion
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(tenantID))
	var kvs []roachpb.KeyValue
	var splits []roachpb.RKey
	const minVersion = clusterversion.V22_2
	curVersion := clusterversion.V23_1
	if p.EvalContext().Settings.Version.IsActive(ctx, curVersion) {
		// The cluster is running the latest version.
		// Use this version to create the tenant and bootstrap it using the host
		// cluster's bootstrapping logic.
		tenantVersion.Version = clusterversion.ByKey(curVersion)
		schema := bootstrap.MakeMetadataSchema(
			codec,
			initialTenantZoneConfig, /* defaultZoneConfig */
			initialTenantZoneConfig, /* defaultSystemZoneConfig */
		)
		kvs, splits = schema.GetInitialValues()
	} else {
		// The cluster is not running the latest version.
		// Use the previous major version to create the tenant and bootstrap it
		// just like the previous major version binary would, using hardcoded
		// initial values.
		tenantVersion.Version = clusterversion.ByKey(minVersion)
		kvs, splits, err = bootstrap.InitialValuesForTenantV222(
			codec,
			initialTenantZoneConfig, /* defaultZoneConfig */
			initialTenantZoneConfig, /* defaultSystemZoneConfig */
		)
		if err != nil {
			return tid, err
		}
	}

	{
		// Populate the version setting for the tenant. This will allow the tenant
		// to know what migrations need to be run in the future. The choice to use
		// the active cluster version here is intentional; it allows tenants
		// created during the mixed-version state in the host cluster to avoid
		// using code which may be too new. The expectation is that the tenant
		// clusters will be updated to a version only after the system tenant has
		// been upgraded.
		tenantSettingKV, err := generateTenantClusterSettingKV(codec, tenantVersion)
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

// CreateTenantRecord creates a tenant in system.tenants and installs an initial
// span config (in system.span_configurations) for it. It also initializes the
// usage data in system.tenant_usage if info.Usage is set.
//
// The caller is responsible for ensuring the current user has the
// admin role.
//
// If the passed in `info` has the `TenantID` field unset (= 0),
// CreateTenantRecord will assign the tenant the next available ID after
// consulting the system.tenants table.
func CreateTenantRecord(
	ctx context.Context,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	txn isql.Txn,
	spanConfigs spanconfig.KVAccessor,
	info *mtinfopb.TenantInfoWithUsage,
	initialTenantZoneConfig *zonepb.ZoneConfig,
) (roachpb.TenantID, error) {
	const op = "create"
	if err := rejectIfCantCoordinateMultiTenancy(codec, op); err != nil {
		return roachpb.TenantID{}, err
	}
	if err := rejectIfSystemTenant(info.ID, op); err != nil {
		return roachpb.TenantID{}, err
	}
	if info.Name != "" {
		if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
			return roachpb.TenantID{}, pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
		if err := info.Name.IsValid(); err != nil {
			return roachpb.TenantID{}, pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
	}

	tenID := info.ID
	if tenID == 0 {
		tenantID, err := getAvailableTenantID(ctx, info.Name, txn)
		if err != nil {
			return roachpb.TenantID{}, err
		}
		tenID = tenantID.ToUint64()
		info.ID = tenID
	}

	if info.Name == "" {
		// No name: generate one if we are at the appropriate version.
		if settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
			info.Name = roachpb.TenantName(fmt.Sprintf("tenant-%d", info.ID))
		}
	}

	// Populate the deprecated DataState field for compatibility
	// with pre-v23.1 servers.
	switch info.DataState {
	case mtinfopb.DataStateReady:
		info.DeprecatedDataState = mtinfopb.ProtoInfo_READY
	case mtinfopb.DataStateAdd:
		info.DeprecatedDataState = mtinfopb.ProtoInfo_ADD
	case mtinfopb.DataStateDrop:
		info.DeprecatedDataState = mtinfopb.ProtoInfo_DROP
	default:
		return roachpb.TenantID{}, errors.AssertionFailedf("unhandled: %d", info.DataState)
	}
	// DeprecatedID is populated for the benefit of pre-v23.1 servers.
	info.DeprecatedID = info.ID

	// active is an obsolete column preserved for compatibility with
	// pre-v23.1 servers.
	active := info.DataState == mtinfopb.DataStateReady

	infoBytes, err := protoutil.Marshal(&info.ProtoInfo)
	if err != nil {
		return roachpb.TenantID{}, err
	}

	// Insert into the tenant table and detect collisions.
	var name tree.Datum
	if info.Name != "" {
		if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
			return roachpb.TenantID{}, pgerror.Newf(pgcode.FeatureNotSupported, "cannot use tenant names")
		}
		name = tree.NewDString(string(info.Name))
	} else {
		name = tree.DNull
	}

	query := `INSERT INTO system.tenants (id, active, info, name, data_state, service_mode) VALUES ($1, $2, $3, $4, $5, $6)`
	args := []interface{}{tenID, active, infoBytes, name, info.DataState, info.ServiceMode}
	if !settings.Version.IsActive(ctx, clusterversion.V23_1TenantNamesStateAndServiceMode) {
		// Ensure the insert can succeed if the upgrade is not finalized yet.
		query = `INSERT INTO system.tenants (id, active, info) VALUES ($1, $2, $3)`
		args = args[:3]
	}

	if num, err := txn.ExecEx(
		ctx, "create-tenant", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		query, args...,
	); err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			extra := redact.RedactableString("")
			if info.Name != "" {
				extra = redact.Sprintf(" or with name %q", info.Name)
			}
			return roachpb.TenantID{}, pgerror.Newf(pgcode.DuplicateObject, "a tenant with ID %d%s already exists", tenID, extra)
		}
		return roachpb.TenantID{}, errors.Wrap(err, "inserting new tenant")
	} else if num != 1 {
		logcrash.ReportOrPanic(ctx, &settings.SV, "inserting tenant %+v: unexpected number of rows affected: %d", info, num)
	}

	if u := info.Usage; u != nil {
		consumption, err := protoutil.Marshal(&u.Consumption)
		if err != nil {
			return roachpb.TenantID{}, errors.Wrap(err, "marshaling tenant usage data")
		}
		if num, err := txn.ExecEx(
			ctx, "create-tenant-usage", txn.KV(), sessiondata.NodeUserSessionDataOverride,
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
			logcrash.ReportOrPanic(ctx, &settings.SV, "inserting usage %+v for %v: unexpected number of rows affected: %d", u, tenID, num)
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

	tenantID := roachpb.MustMakeTenantID(tenID)

	// This adds a split at the start of the tenant keyspace.
	tenantPrefix := keys.MakeTenantPrefix(tenantID)
	startRecordTarget := spanconfig.MakeTargetFromSpan(roachpb.Span{
		Key:    tenantPrefix,
		EndKey: tenantPrefix.Next(),
	})
	startRecord, err := spanconfig.MakeRecord(startRecordTarget, tenantSpanConfig)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	toUpsert := []spanconfig.Record{startRecord}

	// Ensure we have a split at the current tenant's endKey in one of two ways:
	// 1) The next tenant (tenant whose startKey equals this tenants endKey)
	//    already exists and created this split at its own startKey when it was
	//    created.
	// 2) The next tenant does not exist so split now at the endKey.
	//
	// If this endKey split is not created via (1) or (2), the current tenant's
	// last range will have an endKey of /Max which is outside of it's keyspace.
	// See: https://github.com/cockroachdb/cockroach/issues/92072
	tenantPrefixEnd := tenantPrefix.PrefixEnd()
	endRecordTarget := spanconfig.MakeTargetFromSpan(roachpb.Span{
		Key:    tenantPrefixEnd,
		EndKey: tenantPrefixEnd.Next(),
	})

	// Check if a record exists for the next tenant's startKey from when the next
	// tenant was created. The current tenant's endRecordTarget is the same as
	// the next tenant's startRecordTarget.
	records, err := spanConfigs.GetSpanConfigRecords(ctx, []spanconfig.Target{endRecordTarget})
	if err != nil {
		return roachpb.TenantID{}, err
	}

	// If the next tenant's startKey record exists then do not split at the
	// current tenant's endKey. Doing will incorrectly overwrite the next
	// tenant's first span config.
	// See: https://github.com/cockroachdb/cockroach/issues/95882
	if len(records) == 0 {
		endRecord, err := spanconfig.MakeRecord(endRecordTarget, tenantSpanConfig)
		if err != nil {
			return roachpb.TenantID{}, err
		}
		toUpsert = append(toUpsert, endRecord)
	}

	return tenantID, spanConfigs.UpdateSpanConfigRecords(
		ctx, nil, toUpsert, hlc.MinTimestamp, hlc.MaxTimestamp,
	)
}

// GetAvailableTenantID is part of the PlanHook interface.
func (p *planner) GetAvailableTenantID(
	ctx context.Context, tenantName roachpb.TenantName,
) (roachpb.TenantID, error) {
	return getAvailableTenantID(ctx, tenantName, p.InternalSQLTxn())
}

// getAvailableTenantID returns the first available ID that can be assigned to
// the created tenant. Note, this ID could have previously belonged to another
// tenant that has since been dropped and gc'ed.
func getAvailableTenantID(
	ctx context.Context, tenantName roachpb.TenantName, txn isql.Txn,
) (roachpb.TenantID, error) {
	// Find the first available ID that can be assigned to the created tenant.
	// Note, this ID could have previously belonged to another tenant that has
	// since been dropped and gc'ed.
	row, err := txn.QueryRowEx(ctx, "next-tenant-id", txn.KV(),
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
