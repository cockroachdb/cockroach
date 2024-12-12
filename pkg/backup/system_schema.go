// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	fmt "fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	descpb "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// clusterBackupInclusion is an enum that specifies whether a system table
// should be included in a cluster backup.
type clusterBackupInclusion int

const (
	// invalidBackupInclusion indicates that no preference for cluster backup inclusion has been
	// set. This is to ensure that newly added system tables have to explicitly
	// opt-in or out of cluster backups.
	invalidBackupInclusion = iota
	// optInToClusterBackup indicates that the system table
	// should be included in the cluster backup.
	optInToClusterBackup
	// optOutOfClusterBackup indicates that the system table
	// should not be included in the cluster backup.
	optOutOfClusterBackup
)

// systemBackupConfiguration holds any configuration related to backing up
// system tables. System tables differ from normal tables with respect to backup
// for 2 reasons:
//
//  1. For some tables, their contents are read during the restore, so it is non-
//     trivial to restore this data without affecting the restore job itself.
//
//  2. It may reference system data which could be rewritten. This is particularly
//     problematic for data that references tables. At time of writing, cluster
//     restore creates descriptors with the same ID as they had in the backing up
//     cluster so there is no need to rewrite system table data.
type systemBackupConfiguration struct {
	shouldIncludeInClusterBackup clusterBackupInclusion
	// restoreBeforeData indicates that this system table should be fully restored
	// before restoring the user data. If a system table is restored before the
	// user data, the restore will see this system table during the restore.
	// The default is `false` because most of the system tables should be set up
	// to support the restore (e.g. users that can run the restore, cluster settings
	// that control how the restore runs, etc...).
	restoreBeforeData bool
	// restoreInOrder indicates that this system table should be restored in a
	// particular order relative to other system tables. Restore will sort all
	// system tables based on this value, and restore them in that order. All
	// tables with the same value can be restored in any order among themselves.
	// The default value is `0`.
	restoreInOrder int32
	// migrationFunc performs the necessary migrations on the system table data in
	// the crdb_temp staging table before it is loaded into the actual system
	// table.
	migrationFunc func(ctx context.Context, txn isql.Txn, tempTableName string, rekeys jobspb.DescRewriteMap) error
	// customRestoreFunc is responsible for restoring the data from a table that
	// holds the restore system table data into the given system table. If none
	// is provided then `defaultRestoreFunc` is used.
	customRestoreFunc func(ctx context.Context, deps customRestoreFuncDeps, txn isql.Txn, systemTableName, tempTableName string) error

	// The following fields are for testing.

	// expectMissingInSystemTenant is true for tables that only exist in secondary tenants.
	expectMissingInSystemTenant bool
	// expectMissingInSystemTenant is true for tables that only exist in the system tenant.
	expectMissingInSecondaryTenant bool
}

type customRestoreFuncDeps struct {
	settings *cluster.Settings
	codec    keys.SQLCodec
}

// roleIDSequenceRestoreOrder is set to 1 since it must be after system.users
// which has the default 0.
const roleIDSequenceRestoreOrder = 1

// defaultSystemTableRestoreFunc is how system table data is restored. This can
// be overwritten with the system table's
// systemBackupConfiguration.customRestoreFunc.
func defaultSystemTableRestoreFunc(
	ctx context.Context, _ customRestoreFuncDeps, txn isql.Txn, systemTableName, tempTableName string,
) error {
	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	opName := redact.Sprintf("%s-data-deletion", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := txn.Exec(ctx, opName, txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s);",
		systemTableName, tempTableName)
	opName = redact.Sprintf("%s-data-insert", systemTableName)
	if _, err := txn.Exec(ctx, opName, txn.KV(), restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}

	return nil
}

// Custom restore functions for different system tables.

// tenantSettingsTableRestoreFunc restores the system.tenant_settings table. It
// returns an error when trying to restore a non-empty tenant_settings table
// into a non-system tenant.
func tenantSettingsTableRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	if deps.codec.ForSystemTenant() {
		return systemTenantSettingsTableRestoreFunc(ctx, deps, txn, systemTableName, tempTableName)
	}

	if count, err := queryTableRowCount(ctx, txn, tempTableName); err == nil && count > 0 {
		log.Warningf(ctx, "skipping restore of %d entries in system.tenant_settings table", count)
	} else if err != nil {
		log.Warningf(ctx, "skipping restore of entries in system.tenant_settings table (count failed: %s)", err.Error())
	}
	return nil
}

func queryTableRowCount(ctx context.Context, txn isql.Txn, tableName string) (int64, error) {
	countQuery := fmt.Sprintf("SELECT count(1) FROM %s", tableName)
	row, err := txn.QueryRow(ctx, redact.Sprintf("count-%s", tableName), txn.KV(), countQuery)
	if err != nil {
		return 0, errors.Wrapf(err, "counting rows in %q", tableName)
	}

	count, ok := row[0].(*tree.DInt)
	if !ok {
		return 0, errors.AssertionFailedf("failed to read count as DInt (was %T)", row[0])
	}
	return int64(*count), nil
}

func usersRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) (retErr error) {
	hasIDColumn, err := tableHasColumnName(ctx, txn, tempTableName, "user_id")
	if err != nil {
		return err
	}
	if hasIDColumn {
		return defaultSystemTableRestoreFunc(
			ctx, deps, txn, systemTableName, tempTableName,
		)
	}

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	opName := redact.Sprintf("%s-data-deletion", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err = txn.Exec(ctx, opName, txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	it, err := txn.QueryIteratorEx(ctx, "query-system-users-in-backup",
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT * FROM %s`, tempTableName))
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		username := tree.MustBeDString(it.Cur()[0])
		password := it.Cur()[1]
		isRole := tree.MustBeDBool(it.Cur()[2])

		var id catid.RoleID
		if username == "root" {
			id = 1
		} else if username == "admin" {
			id = 2
		} else {
			id, err = descidgen.GenerateUniqueRoleIDInTxn(ctx, txn.KV(), deps.codec)
			if err != nil {
				return err
			}
		}

		restoreQuery := fmt.Sprintf("INSERT INTO system.%s VALUES ($1, $2, $3, $4)",
			systemTableName)
		opName = redact.Sprintf("%s-data-insert", systemTableName)
		if _, err := txn.Exec(ctx, opName, txn.KV(), restoreQuery, username, password, isRole, id); err != nil {
			return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
		}
	}
	return nil
}

func roleMembersRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	// It's enough to just check if role_id exists since member_id was added at
	// the same time.
	hasIDColumns, err := tableHasNotNullColumn(ctx, txn, tempTableName, "role_id")
	if err != nil {
		return err
	}
	if hasIDColumns {
		return defaultSystemTableRestoreFunc(ctx, deps, txn, systemTableName, tempTableName)
	}

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q", systemTableName, deleteQuery)

	_, err = txn.Exec(ctx, redact.Sprintf("%s-data-deletion", systemTableName), txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	roleMembers, err := txn.QueryBufferedEx(ctx, redact.Sprintf("%s-query-all-rows", systemTableName),
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT * FROM %s`, tempTableName),
	)
	if err != nil {
		return err
	}

	restoreQuery := fmt.Sprintf(`
INSERT INTO system.%s ("role", "member", "isAdmin", role_id, member_id)
VALUES ($1, $2, $3, (SELECT user_id FROM system.users WHERE username = $1), (SELECT user_id FROM system.users WHERE username = $2))`, systemTableName)
	for _, roleMember := range roleMembers {
		role := tree.MustBeDString(roleMember[0])
		member := tree.MustBeDString(roleMember[1])
		isAdmin := tree.MustBeDBool(roleMember[2])
		if _, err := txn.ExecEx(ctx, redact.Sprintf("%s-data-insert", systemTableName),
			txn.KV(), sessiondata.NodeUserSessionDataOverride,
			restoreQuery, role, member, isAdmin,
		); err != nil {
			return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
		}
	}

	return nil
}

func roleOptionsRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) (retErr error) {
	hasIDColumn, err := tableHasColumnName(ctx, txn, tempTableName, "user_id")
	if err != nil {
		return err
	}
	if hasIDColumn {
		return defaultSystemTableRestoreFunc(
			ctx, deps, txn, systemTableName, tempTableName,
		)
	}

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	opName := redact.Sprintf("%s-data-deletion", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err = txn.Exec(ctx, opName, txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	it, err := txn.QueryIteratorEx(ctx, "query-system-users-in-backup",
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT * FROM %s`, tempTableName))
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		username := tree.MustBeDString(it.Cur()[0])
		option := tree.MustBeDString(it.Cur()[1])
		val := it.Cur()[2]

		var id int64
		if username == "root" {
			id = 1
		} else if username == "admin" {
			id = 2
		} else {
			row, err := txn.QueryRow(ctx, `get-user-id`, txn.KV(), `SELECT user_id FROM system.users WHERE username = $1`, username)
			if err != nil {
				return err
			}
			oid := tree.MustBeDOid(row[0])
			id = int64(oid.Oid)
		}

		restoreQuery := fmt.Sprintf("INSERT INTO system.%s VALUES ($1, $2, $3, $4)",
			systemTableName)
		opName = redact.Sprintf("%s-data-insert", systemTableName)
		if _, err := txn.Exec(ctx, opName, txn.KV(), restoreQuery, username, option, val, id); err != nil {
			return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
		}
	}
	return nil
}

func systemPrivilegesRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	hasUserIDColumn, err := tableHasNotNullColumn(ctx, txn, tempTableName, "user_id")
	if err != nil {
		return err
	}
	if hasUserIDColumn {
		return defaultSystemTableRestoreFunc(ctx, deps, txn, systemTableName, tempTableName)
	}

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q", systemTableName, deleteQuery)

	_, err = txn.Exec(ctx, redact.Sprintf("%s-data-deletion", systemTableName), txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	systemPrivilegesRows, err := txn.QueryBufferedEx(ctx, redact.Sprintf("%s-query-all-rows", systemTableName),
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT * FROM %s`, tempTableName),
	)
	if err != nil {
		return err
	}

	restoreQuery := fmt.Sprintf(`
INSERT INTO system.%s (username, path, privileges, grant_options, user_id)
VALUES ($1, $2, $3, $4, (
    SELECT CASE $1
		WHEN '%s' THEN %d
		ELSE (SELECT user_id FROM system.users WHERE username = $1)
	END
))`,
		systemTableName, username.PublicRole, username.PublicRoleID)
	for _, row := range systemPrivilegesRows {
		if _, err := txn.ExecEx(ctx, redact.Sprintf("%s-data-insert", systemTableName),
			txn.KV(), sessiondata.NodeUserSessionDataOverride,
			restoreQuery, row[0], row[1], row[2], row[3],
		); err != nil {
			return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
		}
	}

	return nil
}

func systemDatabaseRoleSettingsRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	hasRoleIDColumn, err := tableHasNotNullColumn(ctx, txn, tempTableName, "role_id")
	if err != nil {
		return err
	}
	if hasRoleIDColumn {
		return defaultSystemTableRestoreFunc(ctx, deps, txn, systemTableName, tempTableName)
	}

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q", systemTableName, deleteQuery)

	_, err = txn.Exec(ctx, redact.Sprintf("%s-data-deletion", systemTableName), txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	databaseRoleSettingsRows, err := txn.QueryBufferedEx(ctx, redact.Sprintf("%s-query-all-rows", systemTableName),
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT * FROM %s`, tempTableName),
	)
	if err != nil {
		return err
	}

	restoreQuery := fmt.Sprintf(`
INSERT INTO system.%s (database_id, role_name, settings, role_id)
VALUES ($1, $2, $3, (
	SELECT CASE $2
		WHEN '%s' THEN %d
		ELSE (SELECT user_id FROM system.users WHERE username = $2)
	END
))`,
		systemTableName, username.EmptyRole, username.EmptyRoleID)
	for _, row := range databaseRoleSettingsRows {
		if _, err := txn.ExecEx(ctx, redact.Sprintf("%s-data-insert", systemTableName),
			txn.KV(), sessiondata.NodeUserSessionDataOverride,
			restoreQuery, row[0], row[1], row[2],
		); err != nil {
			return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
		}
	}

	return nil
}

func systemExternalConnectionsRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	hasOwnerIDColumn, err := tableHasNotNullColumn(ctx, txn, tempTableName, "owner_id")
	if err != nil {
		return err
	}
	if hasOwnerIDColumn {
		return defaultSystemTableRestoreFunc(ctx, deps, txn, systemTableName, tempTableName)
	}

	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q", systemTableName, deleteQuery)

	_, err = txn.Exec(ctx, redact.Sprintf("%s-data-deletion", systemTableName), txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	externalConnectionsRows, err := txn.QueryBufferedEx(ctx, redact.Sprintf("%s-query-all-rows", systemTableName),
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT * FROM %s`, tempTableName),
	)
	if err != nil {
		return err
	}

	restoreQuery := fmt.Sprintf(`
INSERT INTO system.%s (connection_name, created, updated, connection_type, connection_details, owner, owner_id)
VALUES ($1, $2, $3, $4, $5, $6, (SELECT user_id FROM system.users WHERE username = $6))`, systemTableName)
	for _, row := range externalConnectionsRows {
		if _, err := txn.ExecEx(ctx, redact.Sprintf("%s-data-insert", systemTableName),
			txn.KV(), sessiondata.NodeUserSessionDataOverride,
			restoreQuery, row[0], row[1], row[2], row[3], row[4], row[5],
		); err != nil {
			return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
		}
	}

	return nil
}

func tableHasColumnName(
	ctx context.Context, txn isql.Txn, tableName string, columnName string,
) (bool, error) {
	hasColumnQuery := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM [SHOW COLUMNS FROM %s] WHERE column_name = '%s')`, tableName, columnName)
	row, err := txn.QueryRow(ctx, "has-column", txn.KV(), hasColumnQuery)
	if err != nil {
		return false, err
	}
	hasColumn := tree.MustBeDBool(row[0])
	return bool(hasColumn), nil
}

func tableHasNotNullColumn(
	ctx context.Context, txn isql.Txn, tableName string, columnName string,
) (bool, error) {
	hasNotNullColumnQuery := fmt.Sprintf(
		`SELECT IFNULL((SELECT NOT is_nullable FROM [SHOW COLUMNS FROM %s] WHERE column_name = '%s'), false)`,
		tableName, columnName,
	)
	row, err := txn.QueryRowEx(ctx, "has-not-null-column", txn.KV(),
		sessiondata.NodeUserSessionDataOverride, hasNotNullColumnQuery)
	if err != nil {
		return false, err
	}
	hasNotNullColumn := bool(tree.MustBeDBool(row[0]))
	return hasNotNullColumn, nil
}

// systemTenantSettingsTableRestoreFunc implements custom logic when
// restoring the `system.tenant_settings` table on the system
// tenant. Specifically, it does not restore a row for the `version`
// setting that applies to all-tenants (tenant_id = 0). See issue
// #125702 for more details.
//
// TODO(multitenant): revert back to using the default restore func
// for system.tenant_settings when the MinSupportedVersion is 24.2+.
func systemTenantSettingsTableRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE true", systemTableName)
	opName := redact.Sprintf("%s-data-deletion", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := txn.Exec(ctx, opName, txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	restoreQuery := fmt.Sprintf(
		"INSERT INTO system.%s (SELECT * FROM %s WHERE NOT (tenant_id = 0 AND name = 'version'));",
		systemTableName, tempTableName)
	opName = redact.Sprintf("%s-data-insert", systemTableName)
	if _, err := txn.Exec(ctx, opName, txn.KV(), restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}

	return nil
}

// When restoring the settings table, we want to make sure to not override the
// version.
func settingsRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	deleteQuery := fmt.Sprintf("DELETE FROM system.%s WHERE name <> 'version'", systemTableName)
	opName := redact.Sprintf("%s-data-deletion", systemTableName)
	log.Eventf(ctx, "clearing data from system table %s with query %q",
		systemTableName, deleteQuery)

	_, err := txn.Exec(ctx, opName, txn.KV(), deleteQuery)
	if err != nil {
		return errors.Wrapf(err, "deleting data from system.%s", systemTableName)
	}

	restoreQuery := fmt.Sprintf("INSERT INTO system.%s (SELECT * FROM %s WHERE name <> 'version');",
		systemTableName, tempTableName)
	opName = redact.Sprintf("%s-data-insert", systemTableName)
	if _, err := txn.Exec(ctx, opName, txn.KV(), restoreQuery); err != nil {
		return errors.Wrapf(err, "inserting data to system.%s", systemTableName)
	}
	return nil
}

func roleIDSeqRestoreFunc(
	ctx context.Context,
	deps customRestoreFuncDeps,
	txn isql.Txn,
	systemTableName, tempTableName string,
) error {
	datums, err := txn.QueryRowEx(
		ctx, "role-id-seq-custom-restore", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT max(user_id) FROM system.users`,
	)
	if err != nil {
		return err
	}
	max := tree.MustBeDOid(datums[0])
	return txn.KV().Put(ctx, deps.codec.SequenceKey(keys.RoleIDSequenceID), max.Oid+1)
}

// systemTableBackupConfiguration is a map from every systemTable present in the
// cluster to a configuration struct which specifies how it should be treated by
// backup. Every system table should have a specification defined here, enforced
// by TestAllSystemTablesHaveBackupConfig.
var systemTableBackupConfiguration = map[string]systemBackupConfiguration{
	systemschema.UsersTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:            usersRestoreFunc,
	},
	systemschema.ZonesTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // ID in "id".
		// The zones table should be restored before the user data so that the range
		// allocator properly distributes ranges during the restore.
		migrationFunc:     rekeySystemTable("id"),
		restoreBeforeData: true,
	},
	systemschema.SettingsTable.GetName(): {
		// The settings table should be restored after all other system tables have
		// been restored. This is because we do not want to overwrite the clusters'
		// settings before all other user and system data has been restored.
		restoreInOrder:               math.MaxInt32,
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:            settingsRestoreFunc,
	},
	systemschema.LocationsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
	},
	systemschema.RoleMembersTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:            roleMembersRestoreFunc,
		restoreInOrder:               1, // Restore after system.users.
	},
	systemschema.RoleOptionsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:            roleOptionsRestoreFunc,
		restoreInOrder:               1, // Restore after system.users.
	},
	systemschema.UITable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
	},
	systemschema.CommentsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // ID in "object_id".
		migrationFunc:                rekeySystemTable("object_id"),
	},
	systemschema.JobsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ScheduledJobsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // Desc IDs in some rows.
		// Some rows, specifically those which are schedules for row-ttl, have IDs
		// baked into their values, making the restored rows invalid. Rewriting them
		// would be tricky since the ID is in a binary proto field, but we already
		// have code to synthesize new schedules from the table being restored that
		// runs during descriptor creation. We can leverage these by leaving the
		// synthesized schedule rows in the real schedule table when we otherwise
		// clean it out, and skipping TTL rows when we copy from the restored
		// schedule table.
		customRestoreFunc: func(ctx context.Context, _ customRestoreFuncDeps, txn isql.Txn, _, tempTableName string) error {
			execType := tree.ScheduledRowLevelTTLExecutor.InternalName()

			const deleteQuery = "DELETE FROM system.scheduled_jobs WHERE executor_type <> $1"
			if _, err := txn.Exec(
				ctx, "restore-scheduled_jobs-delete", txn.KV(), deleteQuery, execType,
			); err != nil {
				return errors.Wrapf(err, "deleting existing scheduled_jobs")
			}

			restoreQuery := fmt.Sprintf(
				"INSERT INTO system.scheduled_jobs (SELECT * FROM %s WHERE executor_type <> $1);",
				tempTableName,
			)

			if _, err := txn.Exec(
				ctx, "restore-scheduled_jobs-insert", txn.KV(), restoreQuery, execType,
			); err != nil {
				return err
			}
			return nil
		},
	},
	systemschema.TableStatisticsTable.GetName(): {
		// Table statistics are backed up in the backup descriptor for now.
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DescriptorTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.EventLogTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.LeaseTable().GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.NamespaceTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsMetaTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ProtectedTimestampsRecordsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.RangeEventTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationConstraintStatsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationCriticalLocalitiesTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReportsMetaTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.ReplicationStatsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SqllivenessTable().GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementBundleChunksTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementDiagnosticsRequestsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TenantsTable.GetName(): {
		shouldIncludeInClusterBackup:   optOutOfClusterBackup,
		expectMissingInSecondaryTenant: true,
	},
	systemschema.WebSessionsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.MigrationsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.JoinTokensTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementStatisticsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TransactionStatisticsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.DatabaseRoleSettingsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // ID in "database_id".
		migrationFunc:                rekeySystemTable("database_id"),
		customRestoreFunc:            systemDatabaseRoleSettingsRestoreFunc,
		restoreInOrder:               1, // Restore after system.users.
	},
	systemschema.TenantUsageTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemTenantTasksTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemTaskPayloadsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemJobProgressTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemJobProgressHistoryTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemJobStatusTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemJobMessageTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SQLInstancesTable().GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SpanConfigurationsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TenantSettingsTable.GetName(): {
		shouldIncludeInClusterBackup:   optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:              tenantSettingsTableRestoreFunc,
		expectMissingInSecondaryTenant: true,
	},
	systemschema.SpanCountTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
		expectMissingInSystemTenant:  true,
	},
	systemschema.SystemPrivilegeTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:            systemPrivilegesRestoreFunc,
		restoreInOrder:               1, // Restore after system.users.
	},
	systemschema.SystemExternalConnectionsTable.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup, // No desc ID columns.
		customRestoreFunc:            systemExternalConnectionsRestoreFunc,
		restoreInOrder:               1, // Restore after system.users.
	},
	systemschema.RoleIDSequence.GetName(): {
		shouldIncludeInClusterBackup: optInToClusterBackup,
		customRestoreFunc:            roleIDSeqRestoreFunc,
		restoreInOrder:               roleIDSequenceRestoreOrder,
	},
	systemschema.DescIDSequence.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TenantIDSequence.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemJobInfoTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SpanStatsUniqueKeysTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SpanStatsBucketsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SpanStatsSamplesTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SpanStatsTenantBoundariesTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementActivityTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TransactionActivityTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.RegionLivenessTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.SystemMVCCStatisticsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.StatementExecInsightsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TransactionExecInsightsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.TableMetadata.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
	systemschema.PreparedTransactionsTable.GetName(): {
		shouldIncludeInClusterBackup: optOutOfClusterBackup,
	},
}

func rekeySystemTable(
	colName string,
) func(context.Context, isql.Txn, string, jobspb.DescRewriteMap) error {
	return func(ctx context.Context, txn isql.Txn, tempTableName string, rekeys jobspb.DescRewriteMap) error {
		toRekey := make(descpb.IDs, 0, len(rekeys))
		for i := range rekeys {
			toRekey = append(toRekey, i)
		}
		sort.Sort(toRekey)

		// We will update every ID in the table from an old value to a new value
		// below, but as we do so there could be yet-to-be-updated rows with old-IDs
		// at the new IDs which would cause a uniqueness violation before we proceed
		// to update the row that is in the way. Instead, we will initially offset
		// the new ID to be +2B, to be up above existing IDs, then when we're done
		// remapping all of them to their new but offset IDs, we'll slide everything
		// down to remove the offset at once. We could query the current max, but
		// just guessing that 2^31 is above it works just as well. In the event this
		// guess doesn't hold, i.e. a cluster with more than 2^31 descriptors, then
		// we don't have room in a uint32 OUD for the offset IDs anyway before it
		// overflows anyway.
		const offset = 1 << 31

		// Some of the tables use oid columns to store desc IDs rather than ints; in
		// these tables we will need to cast to int to do the addition/subtraction
		// of the offset, and then cast back to the desired type determined here.
		typ := "int"
		switch tempTableName {
		case "crdb_temp_system.database_role_settings":
			typ = "oid"
		}

		// We'll build one big UPDATE that moves all remapped the IDs temporary IDs
		// consisting of new+fixed-offset; later we can slide everything down by
		// fixed-offset to put them in their correct, final places.
		q := strings.Builder{}
		fmt.Fprintf(&q, "UPDATE %s SET %s = (CASE\n", tempTableName, colName)
		for _, old := range toRekey {
			fmt.Fprintf(&q, "WHEN %s = %d THEN %d\n", colName, old, rekeys[old].ID+offset)
		}
		fmt.Fprintf(&q, "ELSE %s END)::%s", colName, typ)
		if _, err := txn.Exec(
			ctx, redact.Sprintf("remap-%s", tempTableName), txn.KV(), q.String(),
		); err != nil {
			return errors.Wrapf(err, "remapping IDs %s", tempTableName)
		}

		// Now that the rows mentioning remapped IDs are remapped to be shifted to
		// be above offset, we can clean out anything that did not get remapped and
		// might be in the way when we shift back down such as a zone config for a
		// dropped table that isn't being restored and thus has no remapping. Rows
		// that mention IDs below 50 can stay though: these are mentioning old fixed
		// ID system tables that we do not restore directly, and thus have no entry
		// in our remapping, but the configuration of them (comments, zones, etc) is
		// expected to be restored.
		if _, err := txn.Exec(ctx, redact.Sprintf("remap-remove-%s", tempTableName), txn.KV(),
			fmt.Sprintf("DELETE FROM %s WHERE %s >= 50 AND %s < %d", tempTableName, colName, colName, offset),
		); err != nil {
			return errors.Wrapf(err, "remapping IDs %s", tempTableName)
		}

		// Now slide remapped the IDs back down by offset, to their intended values.
		if _, err := txn.Exec(ctx,
			redact.Sprintf("remap-%s-deoffset", tempTableName),
			txn.KV(),
			fmt.Sprintf("UPDATE %s SET %s = (%s::int - %d)::%s WHERE %s::int >= %d", tempTableName, colName, colName, offset, typ, colName, offset),
		); err != nil {
			return errors.Wrapf(err, "remapping %s; removing offset", tempTableName)
		}

		return nil
	}
}

// GetSystemTablesToIncludeInClusterBackup returns a set of system table names that
// should be included in a cluster backup.
func GetSystemTablesToIncludeInClusterBackup() map[string]struct{} {
	systemTablesToInclude := make(map[string]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.shouldIncludeInClusterBackup == optInToClusterBackup {
			systemTablesToInclude[systemTableName] = struct{}{}
		}
	}

	return systemTablesToInclude
}

// GetSystemTableIDsToExcludeFromClusterBackup returns a set of system table ids
// that should be excluded from a cluster backup.
func GetSystemTableIDsToExcludeFromClusterBackup(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) (map[descpb.ID]struct{}, error) {
	systemTableIDsToExclude := make(map[descpb.ID]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.shouldIncludeInClusterBackup == optOutOfClusterBackup {
			err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
				tn := tree.MakeTableNameWithSchema("system", catconstants.PublicSchemaName, tree.Name(systemTableName))
				_, desc, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).MaybeGet(), &tn)
				isNotFoundErr := errors.Is(err, catalog.ErrDescriptorNotFound)
				if err != nil && !isNotFoundErr {
					return err
				}

				// Some system tables are not present when running inside a secondary
				// tenant egs: `systemschema.TenantsTable`. In such situations we are
				// print a warning and move on.
				if desc == nil || isNotFoundErr {
					log.Warningf(ctx, "could not find system table descriptor %q", systemTableName)
					return nil
				}
				systemTableIDsToExclude[desc.GetID()] = struct{}{}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return systemTableIDsToExclude, nil
}

// getSystemTablesToRestoreBeforeData returns the set of system tables that
// should be restored before the user data.
func getSystemTablesToRestoreBeforeData() map[string]struct{} {
	systemTablesToRestoreBeforeData := make(map[string]struct{})
	for systemTableName, backupConfig := range systemTableBackupConfiguration {
		if backupConfig.shouldIncludeInClusterBackup == optInToClusterBackup && backupConfig.restoreBeforeData {
			systemTablesToRestoreBeforeData[systemTableName] = struct{}{}
		}
	}

	return systemTablesToRestoreBeforeData
}
