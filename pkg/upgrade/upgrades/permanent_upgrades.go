// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// bootstrapSystem runs a series of steps required to bootstrap a new cluster's
// system tenant. These are run before the rest of the initialization steps in
// bootstrapCluster. The steps are run when, and only when, a new cluster is
// created, so typically when a step is added here it is also invoked in a
// separate upgrade migration so that existing clusters will run it as well.
func bootstrapSystem(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.SystemDeps,
) error {
	for _, u := range []struct {
		name string
		fn   upgrade.SystemUpgradeFunc
	}{
		{"initialize cluster version", populateVersionSetting},
		{"configure key visualizer", keyVisualizerTablesMigration},
		{"configure sql activity table TTLs", sqlStatsTTLChange},
	} {
		log.Infof(ctx, "executing system bootstrap step %q", u.name)
		if err := u.fn(ctx, cv, deps); err != nil {
			return errors.Wrapf(err, "system bootstrap step %q failed", u.name)
		}
	}
	return nil
}

// bootstrapCluster runs a series of steps required to bootstrap a new cluster,
// i.e. those things which run once when a new cluster is initialized, including
// when a virtual cluster is created. The steps are run when, and only when, a
// new cluster is created, so typically when a step is added here it is also
// invoked in a separate upgrade migration so that existing clusters will run it
// as well.
func bootstrapCluster(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	for _, u := range []struct {
		name string
		fn   upgrade.TenantUpgradeFunc
	}{
		{"add users and roles", addRootUser},
		{"enable diagnostics reporting", optInToDiagnosticsStatReporting},
		{"initialize the cluster.secret setting", initializeClusterSecret},
		{"update system.locations with default location data", updateSystemLocationData},
		{"create default databases", createDefaultDbs},
		{"create jobs metrics polling job", createJobsMetricsPollingJob},
		{"create sql activity updater job", createActivityUpdateJobMigration},
		{"create mvcc stats job", createMVCCStatisticsJob},
		{"create update cached table metadata job", createUpdateTableMetadataCacheJob},
		{"maybe initialize replication standby read-only catalog", maybeSetupPCRStandbyReader},
		{"create sql activity flush job", createSqlActivityFlushJob},
	} {
		log.Infof(ctx, "executing bootstrap step %q", u.name)
		if err := u.fn(ctx, cv, deps); err != nil {
			return errors.Wrapf(err, "bootstrap step %q failed", u.name)
		}
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

	// Upsert the role membership into the table.
	// We intentionally override any existing entry.
	return deps.DB.Txn(ctx, addRootToAdminRole)
}

func addRootToAdminRole(ctx context.Context, txn isql.Txn) error {
	var upsertStmt string
	var upsertVals []interface{}
	{
		// We query the pg_attribute to determine whether the role_id and member_id
		// columns are present because we can't rely on version gates here.
		const pgAttributeStmt = `
			SELECT * FROM system.pg_catalog.pg_attribute
			         WHERE attrelid = 'system.public.role_members'::REGCLASS
			         AND attname IN ('role_id', 'member_id')
			         LIMIT 1
			         `
		if row, err := txn.QueryRow(ctx, "roleMembersColumnsGet", txn.KV(), pgAttributeStmt); err != nil {
			return err
		} else if row == nil {
			upsertStmt = `
          UPSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, true)
          `
			upsertVals = []interface{}{username.AdminRole, username.RootUser}
		} else {
			upsertStmt = `
          UPSERT INTO system.role_members ("role", "member", "isAdmin", role_id, member_id) VALUES ($1, $2, true, $3, $4)
	        `
			upsertVals = []interface{}{username.AdminRole, username.RootUser, username.AdminRoleID, username.RootUserID}
		}
	}
	_, err := txn.Exec(ctx, "addRootToAdminRole", txn.KV(), upsertStmt, upsertVals...)
	return err
}

func optInToDiagnosticsStatReporting(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// We're opting-out of the automatic opt-in. See discussion in updates.go.
	if cluster.TelemetryOptOut {
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
	if err := deps.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.GetProto(ctx, keys.BootstrapVersionKey, &v)
	}); err != nil {
		return err
	}
	if v == (roachpb.Version{}) {
		// The cluster was bootstrapped at v1.0 (or even earlier), so just use
		// the TestingBinaryMinSupportedVersion of the binary.
		v = clusterversion.MinSupported.Version()
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
	ie := deps.DB.Executor()
	_, err = ie.Exec(
		ctx, "insert-setting", nil, /* txn */
		fmt.Sprintf(`INSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ('version', x'%x', now(), 'm') ON CONFLICT(name) DO NOTHING`, b),
	)
	return err
}

func initializeClusterSecret(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	_, err := deps.InternalExecutor.Exec(
		ctx, "initializeClusterSecret", nil, /* txn */
		`INSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ('cluster.secret', gen_random_uuid()::STRING, now(), 's') ON CONFLICT(name) DO NOTHING`,
	)
	return err
}

func updateSystemLocationData(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// See if the system.locations table already has data in it.
	// If so, we don't want to do anything.
	row, err := deps.InternalExecutor.QueryRowEx(ctx, "update-system-locations",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
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

func createDefaultDbs(
	ctx context.Context, _ clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	// Create the default databases. These are plain databases with
	// default permissions. Nothing special happens if they exist
	// already.
	const createDbStmt = `CREATE DATABASE IF NOT EXISTS "%s" WITH OWNER root`

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
