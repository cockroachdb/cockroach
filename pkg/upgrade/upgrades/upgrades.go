// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrades contains the implementation of upgrades. It is imported
// by the server library.
//
// This package registers the upgrades with the upgrade package.
package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// SettingsDefaultOverrides documents the effect of several migrations that add
// an explicit value for a setting, effectively changing the "default value"
// from what was defined in code.
var SettingsDefaultOverrides = map[string]string{
	"diagnostics.reporting.enabled": "true",
	"cluster.secret":                "<random>",
}

// GetUpgrade returns the upgrade corresponding to this version if
// one exists.
func GetUpgrade(key roachpb.Version) (upgradebase.Upgrade, bool) {
	m, ok := registry[key]
	if !ok {
		return nil, false
	}
	return m, ok
}

// NoTenantUpgradeFunc is a TenantUpgradeFunc that doesn't do anything.
func NoTenantUpgradeFunc(context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated upgrade. The upgrade is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[roachpb.Version]upgradebase.Upgrade)

var upgrades = []upgradebase.Upgrade{
	upgrade.NewPermanentTenantUpgrade(
		"add users and roles",
		toCV(clusterversion.VPrimordial1),
		addRootUser,
	),
	upgrade.NewPermanentTenantUpgrade(
		"enable diagnostics reporting",
		toCV(clusterversion.VPrimordial2),
		optInToDiagnosticsStatReporting,
	),
	upgrade.NewPermanentSystemUpgrade(
		"populate initial version cluster setting table entry",
		toCV(clusterversion.VPrimordial3),
		populateVersionSetting,
	),
	upgrade.NewPermanentTenantUpgrade(
		"initialize the cluster.secret setting",
		toCV(clusterversion.VPrimordial4),
		initializeClusterSecret,
	),
	// Introduced in v19.1.
	// TODO(knz): bake this migration into v19.2.
	upgrade.NewPermanentSystemUpgrade(
		"propagate the ts purge interval to the new setting names",
		toCV(clusterversion.VPrimordial5),
		retireOldTsPurgeIntervalSettings,
	),
	upgrade.NewPermanentTenantUpgrade(
		"update system.locations with default location data",
		toCV(clusterversion.VPrimordial6),
		updateSystemLocationData,
	),
	// Introduced in v2.1.
	// TODO(mberhault): bake into v19.1.
	upgrade.NewPermanentTenantUpgrade(
		"disallow public user or role name",
		toCV(clusterversion.VPrimordial7),
		disallowPublicUserOrRole,
	),
	// Introduced in v2.1.
	// TODO(knz): bake this migration into v19.1.
	upgrade.NewPermanentTenantUpgrade(
		"create default databases",
		toCV(clusterversion.VPrimordial8),
		createDefaultDbs,
	),
	upgrade.NewTenantUpgrade(
		"ensure preconditions are met before starting upgrading to v22.2",
		toCV(clusterversion.V22_2Start),
		preconditionBeforeStartingAnUpgrade,
		NoTenantUpgradeFunc,
	),
	upgrade.NewTenantUpgrade(
		"upgrade sequences to be referenced by ID",
		toCV(clusterversion.V22_2UpgradeSequenceToBeReferencedByID),
		upgrade.NoPrecondition,
		upgradeSequenceToBeReferencedByID,
	),
	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support sampling probabilities",
		toCV(clusterversion.V22_2SampledStmtDiagReqs),
		upgrade.NoPrecondition,
		sampledStmtDiagReqsMigration,
	),
	upgrade.NewTenantUpgrade(
		"add the system.privileges table",
		toCV(clusterversion.V22_2SystemPrivilegesTable),
		upgrade.NoPrecondition,
		systemPrivilegesTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column locality to table system.sql_instances",
		toCV(clusterversion.V22_2AlterSystemSQLInstancesAddLocality),
		upgrade.NoPrecondition,
		alterSystemSQLInstancesAddLocality,
	),
	upgrade.NewTenantUpgrade(
		"add the system.external_connections table",
		toCV(clusterversion.V22_2SystemExternalConnectionsTable),
		upgrade.NoPrecondition,
		systemExternalConnectionsTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column index_recommendations to table system.statement_statistics",
		toCV(clusterversion.V22_2AlterSystemStatementStatisticsAddIndexRecommendations),
		upgrade.NoPrecondition,
		alterSystemStatementStatisticsAddIndexRecommendations,
	),
	upgrade.NewTenantUpgrade("add system.role_id_sequence",
		toCV(clusterversion.V22_2RoleIDSequence),
		upgrade.NoPrecondition,
		roleIDSequenceMigration,
	),
	// Add user_id column, the column will not be backfilled.
	// However, new users created from this point forward will be created
	// with an ID. We cannot start using the IDs in this version
	// as old users are not backfilled. The key here is that we have a cut
	// off point where we know which users need to be backfilled and no
	// more users can be created without ids.
	upgrade.NewTenantUpgrade("alter system.users to include user_id column",
		toCV(clusterversion.V22_2AddSystemUserIDColumn),
		upgrade.NoPrecondition,
		alterSystemUsersAddUserIDColumnWithIndex,
	),
	upgrade.NewTenantUpgrade("backfill users with ids and add an index on the id column",
		toCV(clusterversion.V22_2SystemUsersIDColumnIsBackfilled),
		upgrade.NoPrecondition,
		backfillSystemUsersIDColumn,
	),
	upgrade.NewTenantUpgrade("set user_id column to not null",
		toCV(clusterversion.V22_2SetSystemUsersUserIDColumnNotNull),
		upgrade.NoPrecondition,
		setUserIDNotNull,
	),
	upgrade.NewTenantUpgrade(
		"add default SQL schema telemetry schedule",
		toCV(clusterversion.V22_2SQLSchemaTelemetryScheduledJobs),
		upgrade.NoPrecondition,
		ensureSQLSchemaTelemetrySchedule,
	),
	upgrade.NewTenantUpgrade("alter system.role_options to include user_id column",
		toCV(clusterversion.V22_2RoleOptionsTableHasIDColumn),
		upgrade.NoPrecondition,
		alterSystemRoleOptionsAddUserIDColumnWithIndex,
	),
	upgrade.NewTenantUpgrade("backfill entries in system.role_options to include IDs",
		toCV(clusterversion.V22_2RoleOptionsIDColumnIsBackfilled),
		upgrade.NoPrecondition,
		backfillSystemRoleOptionsIDColumn,
	),
	upgrade.NewTenantUpgrade("set system.role_options user_id column to not null",
		toCV(clusterversion.V22_2SetRoleOptionsUserIDColumnNotNull),
		upgrade.NoPrecondition,
		setSystemRoleOptionsUserIDColumnNotNull,
	),
	upgrade.NewTenantUpgrade("ensure all GC jobs send DeleteRange requests",
		toCV(clusterversion.V22_2WaitedForDelRangeInGCJob),
		checkForPausedGCJobs,
		waitForDelRangeInGCJob,
	),
	upgrade.NewTenantUpgrade(
		"wait for all in-flight schema changes",
		toCV(clusterversion.V22_2NoNonMVCCAddSSTable),
		upgrade.NoPrecondition,
		waitForAllSchemaChanges,
	),
	upgrade.NewTenantUpgrade("update invalid column IDs in sequence back references",
		toCV(clusterversion.V22_2UpdateInvalidColumnIDsInSequenceBackReferences),
		upgrade.NoPrecondition,
		updateInvalidColumnIDsInSequenceBackReferences,
	),
	upgrade.NewTenantUpgrade("fix corrupt user-file related table descriptors",
		toCV(clusterversion.V22_2FixUserfileRelatedDescriptorCorruption),
		upgrade.NoPrecondition,
		fixInvalidObjectsThatLookLikeBadUserfileConstraint,
	),
	upgrade.NewTenantUpgrade("add a name column to system.tenants and populate a system tenant entry",
		toCV(clusterversion.V23_1TenantNames),
		upgrade.NoPrecondition,
		addTenantNameColumnAndSystemTenantEntry,
	),
	upgrade.NewTenantUpgrade("set the value or system.descriptor_id_seq for the system tenant",
		toCV(clusterversion.V23_1DescIDSequenceForSystemTenant),
		upgrade.NoPrecondition,
		descIDSequenceForSystemTenant,
	),
	upgrade.NewTenantUpgrade("add a partial predicate column to system.table_statistics",
		toCV(clusterversion.V23_1AddPartialStatisticsPredicateCol),
		upgrade.NoPrecondition,
		alterSystemTableStatisticsAddPartialPredicate,
	),
}

func init() {
	for _, m := range upgrades {
		if _, exists := registry[m.Version()]; exists {
			panic(errors.AssertionFailedf("duplicate upgrade registration for %v", m.Version()))
		}
		registry[m.Version()] = m
	}
}

func toCV(key clusterversion.Key) roachpb.Version {
	return clusterversion.ByKey(key)
}

func initializeClusterSecret(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	_, err := deps.InternalExecutor.Exec(
		ctx, "initializeClusterSecret", nil, /* txn */
		`SET CLUSTER SETTING cluster.secret = gen_random_uuid()::STRING`,
	)
	return err
}

func optInToDiagnosticsStatReporting(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// We're opting-out of the automatic opt-in. See discussion in updates.go.
	if cluster.TelemetryOptOut() {
		return nil
	}
	_, err := deps.InternalExecutor.Exec(
		ctx, "optInToDiagnosticsStatReporting", nil, /* txn */
		`SET CLUSTER SETTING diagnostics.reporting.enabled = true`)
	return err
}

func populateVersionSetting(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.SystemDeps,
) error {
	var v roachpb.Version
	if err := deps.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.GetProto(ctx, keys.BootstrapVersionKey, &v)
	}); err != nil {
		return err
	}
	if v == (roachpb.Version{}) {
		// The cluster was bootstrapped at v1.0 (or even earlier), so just use
		// the TestingBinaryMinSupportedVersion of the binary.
		v = clusterversion.TestingBinaryMinSupportedVersion
	}

	b, err := protoutil.Marshal(&clusterversion.ClusterVersion{Version: v})
	if err != nil {
		return errors.Wrap(err, "while marshaling version")
	}

	// Add a ON CONFLICT DO NOTHING to avoid changing an existing version.
	// Again, this can happen if the migration doesn't run to completion
	// (overwriting also seems reasonable, but what for).
	// We don't allow users to perform version changes until we have run
	// the insert below.
	_, err = deps.InternalExecutor.Exec(
		ctx, "insert-setting", nil, /* txn */
		fmt.Sprintf(`INSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ('version', x'%x', now(), 'm') ON CONFLICT(name) DO NOTHING`, b),
	)
	if err != nil {
		return err
	}

	// Tenant ID 0 indicates that we're overriding the value for all
	// tenants.
	tenantID := tree.NewDInt(0)
	_, err = deps.InternalExecutor.Exec(
		ctx,
		"insert-setting", nil, /* txn */
		fmt.Sprintf(`INSERT INTO system.tenant_settings (tenant_id, name, value, "last_updated", "value_type") VALUES (%d, 'version', x'%x', now(), 'm') ON CONFLICT(tenant_id, name) DO NOTHING`, tenantID, b),
	)
	if err != nil {
		return err
	}

	return nil
}

func addRootUser(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Upsert the root user into the table. We intentionally override any existing entry.
	const upsertRootStmt = `
	        UPSERT INTO system.users (username, "hashedPassword", "isRole", "user_id") VALUES ($1, '', false,  1)
	        `
	_, err := deps.InternalExecutor.Exec(ctx, "addRootUser", nil /* txn */, upsertRootStmt, username.RootUser)
	if err != nil {
		return err
	}

	// Upsert the admin role into the table. We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.users (username, "hashedPassword", "isRole", "user_id") VALUES ($1, '', true,  2)
          `
	_, err = deps.InternalExecutor.Exec(ctx, "addAdminRole", nil /* txn */, upsertAdminStmt, username.AdminRole)
	if err != nil {
		return err
	}

	// Upsert the role membership into the table. We intentionally override any existing entry.
	const upsertMembership = `
          UPSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, true)
          `
	_, err = deps.InternalExecutor.Exec(
		ctx, "addRootToAdminRole", nil /* txn */, upsertMembership, username.AdminRole, username.RootUser)
	if err != nil {
		return err
	}

	// Add the CREATELOGIN option to roles that already have CREATEROLE.
	const upsertCreateRoleStmt = `
     UPSERT INTO system.role_options (username, option, value)
        SELECT username, 'CREATELOGIN', NULL
          FROM system.role_options
         WHERE option = 'CREATEROLE'
     `
	_, err = deps.InternalExecutor.Exec(
		ctx, "add CREATELOGIN where a role already has CREATEROLE", nil, /* txn */
		upsertCreateRoleStmt)
	return err
}

func retireOldTsPurgeIntervalSettings(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.SystemDeps,
) error {
	// We are going to deprecate `timeseries.storage.10s_resolution_ttl`
	// into `timeseries.storage.resolution_10s.ttl` if the latter is not
	// defined.
	//
	// Ditto for the `30m` resolution.

	// Copy 'timeseries.storage.10s_resolution_ttl' into
	// 'timeseries.storage.resolution_10s.ttl' if the former is defined
	// and the latter is not defined yet.
	//
	// We rely on the SELECT returning no row if the original setting
	// was not defined, and INSERT ON CONFLICT DO NOTHING to ignore the
	// insert if the new name was already set.
	_, err := deps.InternalExecutor.Exec(ctx, "copy-setting", nil, /* txn */
		`
INSERT INTO system.settings (name, value, "lastUpdated", "valueType")
   SELECT 'timeseries.storage.resolution_10s.ttl', value, "lastUpdated", "valueType"
     FROM system.settings WHERE name = 'timeseries.storage.10s_resolution_ttl'
ON CONFLICT (name) DO NOTHING`,
	)
	if err != nil {
		return err
	}

	// Ditto 30m.
	_, err = deps.InternalExecutor.Exec(ctx, "copy-setting", nil, /* txn */
		`
INSERT INTO system.settings (name, value, "lastUpdated", "valueType")
   SELECT 'timeseries.storage.resolution_30m.ttl', value, "lastUpdated", "valueType"
     FROM system.settings WHERE name = 'timeseries.storage.30m_resolution_ttl'
ON CONFLICT (name) DO NOTHING`,
	)
	if err != nil {
		return err
	}

	return nil
}

func updateSystemLocationData(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// See if the system.locations table already has data in it.
	// If so, we don't want to do anything.
	row, err := deps.InternalExecutor.QueryRowEx(ctx, "update-system-locations",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		`SELECT count(*) FROM system.locations`)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.New("failed to update system locations")
	}
	count := int(tree.MustBeDInt(row[0]))
	if count != 0 {
		return nil
	}

	for _, loc := range roachpb.DefaultLocationInformation {
		stmt := `UPSERT INTO system.locations VALUES ($1, $2, $3, $4)`
		tier := loc.Locality.Tiers[0]
		_, err := deps.InternalExecutor.Exec(ctx, "update-system-locations", nil, /* txn */
			stmt, tier.Key, tier.Value, loc.Latitude, loc.Longitude,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func disallowPublicUserOrRole(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Check whether a user or role named "public" exists.
	const selectPublicStmt = `
          SELECT username, "isRole" from system.users WHERE username = $1
          `

	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		row, err := deps.InternalExecutor.QueryRowEx(
			ctx, "disallowPublicUserOrRole", nil, /* txn */
			sessiondata.InternalExecutorOverride{
				User: username.RootUserName(),
			},
			selectPublicStmt, username.PublicRole,
		)
		if err != nil {
			continue
		}
		if row == nil {
			// No such user.
			return nil
		}

		isRole, ok := tree.AsDBool(row[1])
		if !ok {
			log.Fatalf(ctx, "expected 'isRole' column of system.users to be of type bool, got %v", row)
		}

		if isRole {
			return fmt.Errorf(`found a role named %s which is now a reserved name. Please drop the role `+
				`(DROP ROLE %s) using a previous version of CockroachDB and try again`,
				username.PublicRole, username.PublicRole)
		}
		return fmt.Errorf(`found a user named %s which is now a reserved name. Please drop the role `+
			`(DROP USER %s) using a previous version of CockroachDB and try again`,
			username.PublicRole, username.PublicRole)
	}
	return nil
}

func createDefaultDbs(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Create the default databases. These are plain databases with
	// default permissions. Nothing special happens if they exist
	// already.
	const createDbStmt = `CREATE DATABASE IF NOT EXISTS "%s"`

	var err error
	for _, dbName := range []string{catalogkeys.DefaultDatabaseName, catalogkeys.PgDatabaseName} {
		stmt := fmt.Sprintf(createDbStmt, dbName)
		_, err = deps.InternalExecutor.Exec(ctx, "create-default-DB", nil /* txn */, stmt)
		if err != nil {
			log.Warningf(ctx, "failed attempt to add database %q: %s", dbName, err)
			return err
		}
	}
	return nil
}
