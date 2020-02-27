// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlmigrations

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

var (
	leaseDuration        = time.Minute
	leaseRefreshInterval = leaseDuration / 5
)

// MigrationManagerTestingKnobs contains testing knobs.
type MigrationManagerTestingKnobs struct {
	// DisableBackfillMigrations stops applying migrations once
	// a migration with 'doesBackfill == true' is encountered.
	// TODO(mberhault): we could skip only backfill migrations and dependencies
	// if we had some concept of migration dependencies.
	DisableBackfillMigrations bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*MigrationManagerTestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = &MigrationManagerTestingKnobs{}

// backwardCompatibleMigrations is a hard-coded list of migrations to be run on
// startup. They will always be run from top-to-bottom, and because they are
// assumed to be backward-compatible, they will be run regardless of what other
// node versions are currently running within the cluster.
// Migrations must be idempotent: a migration may run successfully but not be
// recorded as completed, causing a second run.
//
// Attention: If a migration is creating new tables, it should also be added to
// the metadata schema written by bootstrap (see addSystemDatabaseToSchema())
// and it should have the includedInBootstrap field set (see comments on that
// field too).
var backwardCompatibleMigrations = []migrationDescriptor{
	{
		// Introduced in v1.0. Baked into v2.0.
		name: "default UniqueID to uuid_v4 in system.eventlog",
	},
	{
		// Introduced in v1.0. Baked into v2.0.
		name: "create system.jobs table",
	},
	{
		// Introduced in v1.0. Baked into v2.0.
		name: "create system.settings table",
	},
	{
		// Introduced in v1.0. Permanent migration.
		name:   "enable diagnostics reporting",
		workFn: optInToDiagnosticsStatReporting,
	},
	{
		// Introduced in v1.1. Baked into v2.0.
		name: "establish conservative dependencies for views #17280 #17269 #17306",
	},
	{
		// Introduced in v1.1. Baked into v2.0.
		name: "create system.sessions table",
	},
	{
		// Introduced in v1.1. Permanent migration.
		name:   "populate initial version cluster setting table entry",
		workFn: populateVersionSetting,
	},
	{
		// Introduced in v2.0. Baked into v2.1.
		name:             "create system.table_statistics table",
		newDescriptorIDs: staticIDs(keys.TableStatisticsTableID),
	},
	{
		// Introduced in v2.0. Permanent migration.
		name:   "add root user",
		workFn: addRootUser,
	},
	{
		// Introduced in v2.0. Baked into v2.1.
		name:             "create system.locations table",
		newDescriptorIDs: staticIDs(keys.LocationsTableID),
	},
	{
		// Introduced in v2.0. Baked into v2.1.
		name: "add default .meta and .liveness zone configs",
	},
	{
		// Introduced in v2.0. Baked into v2.1.
		name:             "create system.role_members table",
		newDescriptorIDs: staticIDs(keys.RoleMembersTableID),
	},
	{
		// Introduced in v2.0. Permanent migration.
		name:   "add system.users isRole column and create admin role",
		workFn: addAdminRole,
	},
	{
		// Introduced in v2.0, replaced by "ensure admin role privileges in all descriptors"
		name: "grant superuser privileges on all objects to the admin role",
	},
	{
		// Introduced in v2.0. Permanent migration.
		name:   "make root a member of the admin role",
		workFn: addRootToAdminRole,
	},
	{
		// Introduced in v2.0. Baked into v2.1.
		name: "upgrade table descs to interleaved format version",
	},
	{
		// Introduced in v2.0 alphas then folded into `retiredSettings`.
		name: "remove cluster setting `kv.gc.batch_size`",
	},
	{
		// Introduced in v2.0 alphas then folded into `retiredSettings`.
		name: "remove cluster setting `kv.transaction.max_intents`",
	},
	{
		// Introduced in v2.0. Baked into v2.1.
		name: "add default system.jobs zone config",
	},
	{
		// Introduced in v2.0. Permanent migration.
		name:   "initialize cluster.secret",
		workFn: initializeClusterSecret,
	},
	{
		// Introduced in v2.0. Repeated in v2.1 below.
		name: "ensure admin role privileges in all descriptors",
	},
	{
		// Introduced in v2.1, repeat of 2.0 migration to catch mixed-version issues.
		// TODO(mberhault): bake into v19.1.
		name: "repeat: ensure admin role privileges in all descriptors",
	},
	{
		// Introduced in v2.1.
		// TODO(mberhault): bake into v19.1.
		name:   "disallow public user or role name",
		workFn: disallowPublicUserOrRole,
	},
	{
		// Introduced in v2.1.
		// TODO(knz): bake this migration into v19.1.
		name:             "create default databases",
		workFn:           createDefaultDbs,
		newDescriptorIDs: databaseIDs(sessiondata.DefaultDatabaseName, sessiondata.PgDatabaseName),
	},
	{
		// Introduced in v2.1.
		// TODO(dt): Bake into v19.1.
		name:   "add progress to system.jobs",
		workFn: addJobsProgress,
	},
	{
		// Introduced in v19.1.
		// TODO(knz): bake this migration into v19.2
		name:   "create system.comment table",
		workFn: createCommentTable,
		// This migration has been introduced some time before 19.2.
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
		newDescriptorIDs:    staticIDs(keys.CommentsTableID),
	},
	{
		name:   "create system.replication_constraint_stats table",
		workFn: createReplicationConstraintStatsTable,
		// This migration has been introduced some time before 19.2.
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
		newDescriptorIDs:    staticIDs(keys.ReplicationConstraintStatsTableID),
	},
	{
		name:   "create system.replication_critical_localities table",
		workFn: createReplicationCriticalLocalitiesTable,
		// This migration has been introduced some time before 19.2.
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
		newDescriptorIDs:    staticIDs(keys.ReplicationCriticalLocalitiesTableID),
	},
	{
		name:   "create system.reports_meta table",
		workFn: createReportsMetaTable,
		// This migration has been introduced some time before 19.2.
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
		newDescriptorIDs:    staticIDs(keys.ReportsMetaTableID),
	},
	{
		name:   "create system.replication_stats table",
		workFn: createReplicationStatsTable,
		// This migration has been introduced some time before 19.2.
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
		newDescriptorIDs:    staticIDs(keys.ReplicationStatsTableID),
	},
	{
		// Introduced in v19.1.
		// TODO(knz): bake this migration into v19.2.
		name:   "propagate the ts purge interval to the new setting names",
		workFn: retireOldTsPurgeIntervalSettings,
	},
	{
		// Introduced in v19.2.
		name:   "update system.locations with default location data",
		workFn: updateSystemLocationData,
	},
	{
		// Introduced in v19.2.
		name:                "change reports fields from timestamp to timestamptz",
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
		workFn: func(ctx context.Context, r runner) error {
			// Note that these particular schema changes are idempotent.
			if _, err := r.sqlExecutor.ExecEx(ctx, "update-reports-meta-generated", nil, /* txn */
				sqlbase.InternalExecutorSessionDataOverride{
					User: security.NodeUser,
				},
				`ALTER TABLE system.reports_meta ALTER generated TYPE TIMESTAMP WITH TIME ZONE`,
			); err != nil {
				return err
			}
			if _, err := r.sqlExecutor.ExecEx(ctx, "update-reports-meta-generated", nil, /* txn */
				sqlbase.InternalExecutorSessionDataOverride{
					User: security.NodeUser,
				},
				"ALTER TABLE system.replication_constraint_stats ALTER violation_start "+
					"TYPE TIMESTAMP WITH TIME ZONE",
			); err != nil {
				return err
			}
			return nil
		},
	},
	{
		// Introduced in v20.1.
		// TODO(ajwerner): Bake this migration into v20.2.
		name:                "create system.protected_ts_meta table",
		workFn:              createProtectedTimestampsMetaTable,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionProtectedTimestamps),
		newDescriptorIDs:    staticIDs(keys.ProtectedTimestampsMetaTableID),
	},
	{
		// Introduced in v20.1.
		// TODO(ajwerner): Bake this migration into v20.2.
		name:                "create system.protected_ts_records table",
		workFn:              createProtectedTimestampsRecordsTable,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionProtectedTimestamps),
		newDescriptorIDs:    staticIDs(keys.ProtectedTimestampsRecordsTableID),
	},
	{
		// Introduced in v20.1.
		name:                "create new system.namespace table",
		workFn:              createNewSystemNamespaceDescriptor,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionNamespaceTableWithSchemas),
		newDescriptorIDs:    staticIDs(keys.NamespaceTableID),
	},
	{
		// Introduced in v20.1.
		name:                "migrate system.namespace_deprecated entries into system.namespace",
		workFn:              migrateSystemNamespace,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionNamespaceTableWithSchemas),
	},
	{
		// Introduced in v20.1.,
		name:                "create system.role_options table with an entry for (admin, CREATEROLE)",
		workFn:              createRoleOptionsTable,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionCreateRolePrivilege),
		newDescriptorIDs:    staticIDs(keys.RoleOptionsTableID),
	},
	{
		// Introduced in v20.1.
		// TODO(andrei): Bake this migration into v20.2.
		name: "create statement_diagnostics_requests, statement_diagnostics and " +
			"system.statement_bundle_chunks tables",
		workFn:              createStatementInfoSystemTables,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionStatementDiagnosticsSystemTables),
		newDescriptorIDs: staticIDs(keys.StatementBundleChunksTableID,
			keys.StatementDiagnosticsRequestsTableID, keys.StatementDiagnosticsTableID),
	},
}

func staticIDs(ids ...sqlbase.ID) func(ctx context.Context, db db) ([]sqlbase.ID, error) {
	return func(ctx context.Context, db db) ([]sqlbase.ID, error) { return ids, nil }
}

func databaseIDs(names ...string) func(ctx context.Context, db db) ([]sqlbase.ID, error) {
	return func(ctx context.Context, db db) ([]sqlbase.ID, error) {
		var ids []sqlbase.ID
		for _, name := range names {
			// This runs as part of an older migration (introduced in 2.1). We use
			// the DeprecatedDatabaseKey, and let the 20.1 migration handle moving
			// from the old namespace table into the new one.
			kv, err := db.Get(ctx, sqlbase.NewDeprecatedDatabaseKey(name).Key())
			if err != nil {
				return nil, err
			}
			ids = append(ids, sqlbase.ID(kv.ValueInt()))
		}
		return ids, nil
	}
}

// migrationDescriptor describes a single migration hook that's used to modify
// some part of the cluster state when the CockroachDB version is upgraded.
// See docs/RFCs/cluster_upgrade_tool.md for details.
type migrationDescriptor struct {
	// name must be unique amongst all hard-coded migrations.
	// ATTENTION: A migration's name can never be changed. It is included in a key
	// marking a migration as completed.
	name string
	// workFn must be idempotent so that we can safely re-run it if a node failed
	// while running it. nil if the migration has been "backed in" and is no
	// longer to be performed at cluster startup.
	workFn func(context.Context, runner) error
	// includedInBootstrap is set for migrations that need to be performed for
	// updating old clusters, but are also covered by the MetadataSchema that gets
	// created by hand for a new cluster when it bootstraps itself. This kind of
	// duplication between a migration and the MetadataSchema is useful for
	// migrations that create system descriptor - for new clusters (particularly
	// for tests) we want to create these tables by hand so that a corresponding
	// range is created at bootstrap time. Otherwise, we'd have the split queue
	// asynchronously creating some ranges which is annoying for tests.
	//
	// Generally when setting this field you'll want to introduce a new cluster
	// version.
	includedInBootstrap roachpb.Version
	// doesBackfill should be set to true if the migration triggers a backfill.
	doesBackfill bool
	// newDescriptorIDs is a function that returns the IDs of any additional
	// descriptors that were added by this migration. This is needed to automate
	// certain tests, which check the number of ranges/descriptors present on
	// server bootup.
	newDescriptorIDs func(ctx context.Context, db db) ([]sqlbase.ID, error)
}

func init() {
	// Ensure that all migrations have unique names.
	names := make(map[string]struct{}, len(backwardCompatibleMigrations))
	for _, migration := range backwardCompatibleMigrations {
		name := migration.name
		if _, ok := names[name]; ok {
			log.Fatalf(context.Background(), "duplicate sql migration %q", name)
		}
		names[name] = struct{}{}
	}
}

type runner struct {
	db          db
	sqlExecutor *sql.InternalExecutor
	settings    *cluster.Settings
}

func (r runner) execAsRoot(ctx context.Context, opName, stmt string, qargs ...interface{}) error {
	_, err := r.sqlExecutor.ExecEx(ctx, opName, nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{
			User: security.RootUser,
		},
		stmt, qargs...)
	return err
}

func (r runner) execAsRootWithRetry(
	ctx context.Context, opName string, stmt string, qargs ...interface{},
) error {
	// Retry a limited number of times because returning an error and letting
	// the node kill itself is better than holding the migration lease for an
	// arbitrarily long time.
	var err error
	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		err := r.execAsRoot(ctx, opName, stmt, qargs...)
		if err == nil {
			break
		}
		log.Warningf(ctx, "failed to run %s: %v", stmt, err)
	}
	return err
}

// leaseManager is defined just to allow us to use a fake client.LeaseManager
// when testing this package.
type leaseManager interface {
	AcquireLease(ctx context.Context, key roachpb.Key) (*leasemanager.Lease, error)
	ExtendLease(ctx context.Context, l *leasemanager.Lease) error
	ReleaseLease(ctx context.Context, l *leasemanager.Lease) error
	TimeRemaining(l *leasemanager.Lease) time.Duration
}

// db is defined just to allow us to use a fake client.DB when testing this
// package.
type db interface {
	Scan(ctx context.Context, begin, end interface{}, maxRows int64) ([]client.KeyValue, error)
	Get(ctx context.Context, key interface{}) (client.KeyValue, error)
	Put(ctx context.Context, key, value interface{}) error
	Txn(ctx context.Context, retryable func(ctx context.Context, txn *client.Txn) error) error
}

// Manager encapsulates the necessary functionality for handling migrations
// of data in the cluster.
type Manager struct {
	stopper      *stop.Stopper
	leaseManager leaseManager
	db           db
	sqlExecutor  *sql.InternalExecutor
	testingKnobs MigrationManagerTestingKnobs
	settings     *cluster.Settings
}

// NewManager initializes and returns a new Manager object.
func NewManager(
	stopper *stop.Stopper,
	db *client.DB,
	executor *sql.InternalExecutor,
	clock *hlc.Clock,
	testingKnobs MigrationManagerTestingKnobs,
	clientID string,
	settings *cluster.Settings,
) *Manager {
	opts := leasemanager.Options{
		ClientID:      clientID,
		LeaseDuration: leaseDuration,
	}
	return &Manager{
		stopper:      stopper,
		leaseManager: leasemanager.New(db, clock, opts),
		db:           db,
		sqlExecutor:  executor,
		testingKnobs: testingKnobs,
		settings:     settings,
	}
}

// ExpectedDescriptorIDs returns the list of all expected system descriptor IDs,
// including those added by completed migrations. This is needed for certain
// tests, which check the number of ranges and system tables at node startup.
//
// NOTE: This value may be out-of-date if another node is actively running
// migrations, and so should only be used in test code where the migration
// lifecycle is tightly controlled.
func ExpectedDescriptorIDs(
	ctx context.Context,
	db db,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) (sqlbase.IDs, error) {
	completedMigrations, err := getCompletedMigrations(ctx, db)
	if err != nil {
		return nil, err
	}
	descriptorIDs := sqlbase.MakeMetadataSchema(defaultZoneConfig, defaultSystemZoneConfig).DescriptorIDs()
	for _, migration := range backwardCompatibleMigrations {
		// Is the migration not creating descriptors?
		if migration.newDescriptorIDs == nil ||
			// Is the migration included in the metadata schema considered above?
			(migration.includedInBootstrap != roachpb.Version{}) {
			continue
		}
		if _, ok := completedMigrations[string(migrationKey(migration))]; ok {
			newIDs, err := migration.newDescriptorIDs(ctx, db)
			if err != nil {
				return nil, err
			}
			descriptorIDs = append(descriptorIDs, newIDs...)
		}
	}
	sort.Sort(descriptorIDs)
	return descriptorIDs, nil
}

// EnsureMigrations should be run during node startup to ensure that all
// required migrations have been run (and running all those that are definitely
// safe to run).
func (m *Manager) EnsureMigrations(ctx context.Context, bootstrapVersion roachpb.Version) error {
	// First, check whether there are any migrations that need to be run.
	completedMigrations, err := getCompletedMigrations(ctx, m.db)
	if err != nil {
		return err
	}
	allMigrationsCompleted := true
	for _, migration := range backwardCompatibleMigrations {
		minVersion := migration.includedInBootstrap
		if migration.workFn == nil || // has the migration been baked in?
			// is the migration unnecessary? It's not if it's been baked in.
			(minVersion != roachpb.Version{} && !bootstrapVersion.Less(minVersion)) {
			continue
		}
		if m.testingKnobs.DisableBackfillMigrations && migration.doesBackfill {
			log.Infof(ctx, "ignoring migrations after (and including) %s due to testing knob",
				migration.name)
			break
		}
		key := migrationKey(migration)
		if _, ok := completedMigrations[string(key)]; !ok {
			allMigrationsCompleted = false
		}
	}
	if allMigrationsCompleted {
		return nil
	}

	// If there are any, grab the migration lease to ensure that only one
	// node is ever doing migrations at a time.
	// Note that we shouldn't ever let client.LeaseNotAvailableErrors cause us
	// to stop trying, because if we return an error the server will be shut down,
	// and this server being down may prevent the leaseholder from finishing.
	var lease *leasemanager.Lease
	if log.V(1) {
		log.Info(ctx, "trying to acquire lease")
	}
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		lease, err = m.leaseManager.AcquireLease(ctx, keys.MigrationLease)
		if err == nil {
			break
		}
		log.Infof(ctx, "failed attempt to acquire migration lease: %s", err)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to acquire lease for running necessary migrations")
	}

	// Ensure that we hold the lease throughout the migration process and release
	// it when we're done.
	done := make(chan interface{}, 1)
	defer func() {
		done <- nil
		if log.V(1) {
			log.Info(ctx, "trying to release the lease")
		}
		if err := m.leaseManager.ReleaseLease(ctx, lease); err != nil {
			log.Errorf(ctx, "failed to release migration lease: %s", err)
		}
	}()
	if err := m.stopper.RunAsyncTask(ctx, "migrations.Manager: lease watcher",
		func(ctx context.Context) {
			select {
			case <-done:
				return
			case <-time.After(leaseRefreshInterval):
				if err := m.leaseManager.ExtendLease(ctx, lease); err != nil {
					log.Warningf(ctx, "unable to extend ownership of expiration lease: %s", err)
				}
				if m.leaseManager.TimeRemaining(lease) < leaseRefreshInterval {
					// Do one last final check of whether we're done - it's possible that
					// ReleaseLease can sneak in and execute ahead of ExtendLease even if
					// the ExtendLease started first (making for an unexpected value error),
					// and doing this final check can avoid unintended shutdowns.
					select {
					case <-done:
						return
					default:
						// Note that we may be able to do better than this by influencing the
						// deadline of migrations' transactions based on the lease expiration
						// time, but simply kill the process for now for the sake of simplicity.
						log.Fatal(ctx, "not enough time left on migration lease, terminating for safety")
					}
				}
			}
		}); err != nil {
		return err
	}

	// Re-get the list of migrations in case any of them were completed between
	// our initial check and our grabbing of the lease.
	completedMigrations, err = getCompletedMigrations(ctx, m.db)
	if err != nil {
		return err
	}

	startTime := timeutil.Now().String()
	r := runner{
		db:          m.db,
		sqlExecutor: m.sqlExecutor,
		settings:    m.settings,
	}
	for _, migration := range backwardCompatibleMigrations {
		minVersion := migration.includedInBootstrap
		if migration.workFn == nil || // has the migration been baked in?
			// is the migration unnecessary? It's not if it's been baked in.
			(minVersion != roachpb.Version{} && !bootstrapVersion.Less(minVersion)) {
			continue
		}

		key := migrationKey(migration)
		if _, ok := completedMigrations[string(key)]; ok {
			continue
		}

		if m.testingKnobs.DisableBackfillMigrations && migration.doesBackfill {
			log.Infof(ctx, "ignoring migrations after (and including) %s due to testing knob",
				migration.name)
			break
		}

		if log.V(1) {
			log.Infof(ctx, "running migration %q", migration.name)
		}
		if err := migration.workFn(ctx, r); err != nil {
			return errors.Wrapf(err, "failed to run migration %q", migration.name)
		}

		log.VEventf(ctx, 1, "persisting record of completing migration %s", migration.name)
		if err := m.db.Put(ctx, key, startTime); err != nil {
			return errors.Wrapf(err, "failed to persist record of completing migration %q",
				migration.name)
		}
	}

	return nil
}

func getCompletedMigrations(ctx context.Context, db db) (map[string]struct{}, error) {
	if log.V(1) {
		log.Info(ctx, "trying to get the list of completed migrations")
	}
	keyvals, err := db.Scan(ctx, keys.MigrationPrefix, keys.MigrationKeyMax, 0 /* maxRows */)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get list of completed migrations")
	}
	completedMigrations := make(map[string]struct{})
	for _, keyval := range keyvals {
		completedMigrations[string(keyval.Key)] = struct{}{}
	}
	return completedMigrations, nil
}

func migrationKey(migration migrationDescriptor) roachpb.Key {
	return append(keys.MigrationPrefix, roachpb.RKey(migration.name)...)
}

func createSystemTable(ctx context.Context, r runner, desc sqlbase.TableDescriptor) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		tKey := sqlbase.MakePublicTableNameKey(ctx, r.settings, desc.GetParentID(), desc.GetName())
		b.CPut(tKey.Key(), desc.GetID(), nil)
		b.CPut(sqlbase.MakeDescMetadataKey(desc.GetID()), sqlbase.WrapDescriptor(&desc), nil)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	// CPuts only provide idempotent inserts if we ignore the errors that arise
	// when the condition isn't met.
	if _, ok := err.(*roachpb.ConditionFailedError); ok {
		return nil
	}
	return err
}

func createCommentTable(ctx context.Context, r runner) error {
	return createSystemTable(ctx, r, sqlbase.CommentsTable)
}

func createReplicationConstraintStatsTable(ctx context.Context, r runner) error {
	if err := createSystemTable(ctx, r, sqlbase.ReplicationConstraintStatsTable); err != nil {
		return err
	}
	err := r.execAsRoot(ctx, "add-constraints-ttl",
		fmt.Sprintf(
			"ALTER TABLE system.replication_constraint_stats CONFIGURE ZONE USING gc.ttlseconds = %d",
			int(sqlbase.ReplicationConstraintStatsTableTTL.Seconds())))
	return errors.Wrapf(err, "failed to set TTL on %s", sqlbase.ReplicationConstraintStatsTable.Name)
}

func createReplicationCriticalLocalitiesTable(ctx context.Context, r runner) error {
	return createSystemTable(ctx, r, sqlbase.ReplicationCriticalLocalitiesTable)
}

func createReplicationStatsTable(ctx context.Context, r runner) error {
	if err := createSystemTable(ctx, r, sqlbase.ReplicationStatsTable); err != nil {
		return err
	}
	err := r.execAsRoot(ctx, "add-replication-status-ttl",
		fmt.Sprintf("ALTER TABLE system.replication_stats CONFIGURE ZONE USING gc.ttlseconds = %d",
			int(sqlbase.ReplicationStatsTableTTL.Seconds())))
	return errors.Wrapf(err, "failed to set TTL on %s", sqlbase.ReplicationStatsTable.Name)
}

func createProtectedTimestampsMetaTable(ctx context.Context, r runner) error {
	return errors.Wrap(createSystemTable(ctx, r, sqlbase.ProtectedTimestampsMetaTable),
		"failed to create system.protected_ts_meta")
}

func createProtectedTimestampsRecordsTable(ctx context.Context, r runner) error {
	return errors.Wrap(createSystemTable(ctx, r, sqlbase.ProtectedTimestampsRecordsTable),
		"failed to create system.protected_ts_records")
}

func createNewSystemNamespaceDescriptor(ctx context.Context, r runner) error {

	return r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()

		// Retrieve the existing namespace table's descriptor and change its name to
		// "namespace_deprecated". This prevents name collisions with the new
		// namespace table, for instance during backup.
		deprecatedKey := sqlbase.MakeDescMetadataKey(keys.DeprecatedNamespaceTableID)
		deprecatedDesc := &sqlbase.Descriptor{}
		ts, err := txn.GetProtoTs(ctx, deprecatedKey, deprecatedDesc)
		if err != nil {
			return err
		}
		deprecatedDesc.Table(ts).Name = sqlbase.DeprecatedNamespaceTable.Name
		b.Put(deprecatedKey, deprecatedDesc)

		// The 19.2 namespace table contains an entry for "namespace" which maps to
		// the deprecated namespace tables ID. Even though the cluster version at
		// this point is 19.2, we construct a metadata name key in the 20.1 format.
		// This is for two reasons:
		// 1. We do not want to change the mapping in namespace_deprecated for
		//    "namespace", as for the purpose of namespace_deprecated, namespace
		//    refers to the correct ID.
		// 2. By adding the ID mapping in the new system.namespace table, the
		//    idempotent semantics of the migration ensure that "namespace" maps to
		//    the correct ID in the new system.namespace table after all tables are
		//    copied over.
		nameKey := sqlbase.NewPublicTableKey(
			sqlbase.NamespaceTable.GetParentID(), sqlbase.NamespaceTable.GetName())
		b.Put(nameKey.Key(), sqlbase.NamespaceTable.GetID())
		b.Put(sqlbase.MakeDescMetadataKey(
			sqlbase.NamespaceTable.GetID()), sqlbase.WrapDescriptor(&sqlbase.NamespaceTable))
		return txn.Run(ctx, b)
	})
}

func createRoleOptionsTable(ctx context.Context, r runner) error {
	// Create system.role_options table with an entry for (admin, CREATEROLE).
	err := createSystemTable(ctx, r, sqlbase.RoleOptionsTable)
	if err != nil {
		return errors.Wrap(err, "failed to create system.role_options")
	}

	// Upsert the admin role with CreateRole privilege into the table.
	// We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.role_options (username, option, value) VALUES ($1, 'CREATEROLE', NULL)
          `
	return r.execAsRootWithRetry(ctx,
		"add role options table and upsert admin with CREATEROLE",
		upsertAdminStmt,
		sqlbase.AdminRole)
}

// migrateSystemNamespace migrates entries from the deprecated system.namespace
// table to the new one, which includes a parentSchemaID column. Each database
// entry is copied to the new table along with a corresponding entry for the
// 'public' schema. Each table entry is copied over with the public schema as
// as its parentSchemaID.
//
// New database and table entries continue to be written to the deprecated
// namespace table until VersionNamespaceTableWithSchemas is active. This means
// that an additional migration will be necessary in 20.2 to catch any new
// entries which may have been missed by this one. In the meantime, namespace
// lookups fall back to the deprecated table if a name is not found in the new
// one.
func migrateSystemNamespace(ctx context.Context, r runner) error {
	q := fmt.Sprintf(
		`SELECT "parentID", name, id FROM [%d AS namespace_deprecated]`,
		sqlbase.DeprecatedNamespaceTable.ID)
	rows, err := r.sqlExecutor.QueryEx(
		ctx, "read-deprecated-namespace-table", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{
			User: security.RootUser,
		},
		q)
	if err != nil {
		return err
	}
	for _, row := range rows {
		parentID := sqlbase.ID(tree.MustBeDInt(row[0]))
		name := string(tree.MustBeDString(row[1]))
		id := sqlbase.ID(tree.MustBeDInt(row[2]))
		if parentID == keys.RootNamespaceID {
			// This row represents a database. Add it to the new namespace table.
			databaseKey := sqlbase.NewDatabaseKey(name)
			if err := r.db.Put(ctx, databaseKey.Key(), id); err != nil {
				return err
			}
			// Also create a 'public' schema for this database.
			schemaKey := sqlbase.NewSchemaKey(id, "public")
			if err := r.db.Put(ctx, schemaKey.Key(), keys.PublicSchemaID); err != nil {
				return err
			}
		} else {
			// This row represents a table. Add it to the new namespace table with the
			// schema set to 'public'.
			if id == keys.DeprecatedNamespaceTableID {
				// The namespace table itself was already handled in
				// createNewSystemNamespaceDescriptor. Do not overwrite it with the
				// deprecated ID.
				continue
			}
			tableKey := sqlbase.NewTableKey(parentID, keys.PublicSchemaID, name)
			if err := r.db.Put(ctx, tableKey.Key(), id); err != nil {
				return err
			}
		}
	}
	return nil
}

func createReportsMetaTable(ctx context.Context, r runner) error {
	return createSystemTable(ctx, r, sqlbase.ReportsMetaTable)
}

func createStatementInfoSystemTables(ctx context.Context, r runner) error {
	if err := createSystemTable(ctx, r, sqlbase.StatementBundleChunksTable); err != nil {
		return errors.Wrap(err, "failed to create system.statement_bundle_chunks")
	}
	if err := createSystemTable(ctx, r, sqlbase.StatementDiagnosticsRequestsTable); err != nil {
		return errors.Wrap(err, "failed to create system.statement_diagnostics_requests")
	}
	if err := createSystemTable(ctx, r, sqlbase.StatementDiagnosticsTable); err != nil {
		return errors.Wrap(err, "failed to create system.statement_diagnostics")
	}
	return nil
}

// SettingsDefaultOverrides documents the effect of several migrations that add
// an explicit value for a setting, effectively changing the "default value"
// from what was defined in code.
var SettingsDefaultOverrides = map[string]string{
	"diagnostics.reporting.enabled": "true",
	"cluster.secret":                "<random>",
}

func optInToDiagnosticsStatReporting(ctx context.Context, r runner) error {
	// We're opting-out of the automatic opt-in. See discussion in updates.go.
	if cluster.TelemetryOptOut() {
		return nil
	}
	return r.execAsRootWithRetry(ctx, "optInToDiagnosticsStatReporting",
		`SET CLUSTER SETTING diagnostics.reporting.enabled = true`)
}

func initializeClusterSecret(ctx context.Context, r runner) error {
	return r.execAsRootWithRetry(
		ctx, "initializeClusterSecret",
		`SET CLUSTER SETTING cluster.secret = gen_random_uuid()::STRING`,
	)
}

func populateVersionSetting(ctx context.Context, r runner) error {
	var v roachpb.Version
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
	if err := r.execAsRoot(
		ctx,
		"insert-setting",
		fmt.Sprintf(`INSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ('version', x'%x', now(), 'm') ON CONFLICT(name) DO NOTHING`, b),
	); err != nil {
		return err
	}

	if err := r.execAsRoot(
		ctx, "set-setting", "SET CLUSTER SETTING version = $1", v.String(),
	); err != nil {
		return err
	}
	return nil
}

func addRootUser(ctx context.Context, r runner) error {
	// Upsert the root user into the table. We intentionally override any existing entry.
	const upsertRootStmt = `
	        UPSERT INTO system.users (username, "hashedPassword", "isRole") VALUES ($1, '', false)
	        `
	return r.execAsRootWithRetry(ctx, "addRootUser", upsertRootStmt, security.RootUser)
}

func addAdminRole(ctx context.Context, r runner) error {
	// Upsert the admin role into the table. We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.users (username, "hashedPassword", "isRole") VALUES ($1, '', true)
          `
	return r.execAsRootWithRetry(ctx, "addAdminRole", upsertAdminStmt, sqlbase.AdminRole)
}

func addRootToAdminRole(ctx context.Context, r runner) error {
	// Upsert the role membership into the table. We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, true)
          `
	return r.execAsRootWithRetry(
		ctx, "addRootToAdminRole", upsertAdminStmt, sqlbase.AdminRole, security.RootUser)
}

func disallowPublicUserOrRole(ctx context.Context, r runner) error {
	// Check whether a user or role named "public" exists.
	const selectPublicStmt = `
          SELECT username, "isRole" from system.users WHERE username = $1
          `

	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		row, err := r.sqlExecutor.QueryRowEx(
			ctx, "disallowPublicUserOrRole", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{
				User: security.RootUser,
			},
			selectPublicStmt, sqlbase.PublicRole,
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
				sqlbase.PublicRole, sqlbase.PublicRole)
		}
		return fmt.Errorf(`found a user named %s which is now a reserved name. Please drop the role `+
			`(DROP USER %s) using a previous version of CockroachDB and try again`,
			sqlbase.PublicRole, sqlbase.PublicRole)
	}
	return nil
}

func createDefaultDbs(ctx context.Context, r runner) error {
	// Create the default databases. These are plain databases with
	// default permissions. Nothing special happens if they exist
	// already.
	const createDbStmt = `CREATE DATABASE IF NOT EXISTS "%s"`

	var err error
	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		for _, dbName := range []string{sessiondata.DefaultDatabaseName, sessiondata.PgDatabaseName} {
			stmt := fmt.Sprintf(createDbStmt, dbName)
			err = r.execAsRoot(ctx, "create-default-db", stmt)
			if err != nil {
				log.Warningf(ctx, "failed attempt to add database %q: %s", dbName, err)
				break
			}
		}
		if err == nil {
			break
		}
	}
	return err
}

func addJobsProgress(ctx context.Context, r runner) error {
	// Ideally to add a column progress, we'd just run a query like:
	//  ALTER TABLE system.jobs ADD COLUMN progress BYTES CREATE FAMLIY progress;
	// However SQL-managed schema changes use jobs tracking internally, which will
	// fail if the jobs table's schema has not been migrated. Rather than add very
	// significant complexity to the jobs code to try to handle pre- and post-
	// migration schemas, we can dodge the chicken-and-egg problem by doing our
	// schema change manually.
	return r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		desc, err := sqlbase.GetMutableTableDescFromID(ctx, txn, keys.JobsTableID)
		if err != nil {
			return err
		}
		if _, err := desc.FindActiveColumnByName("progress"); err == nil {
			return nil
		}
		desc.AddColumn(&sqlbase.ColumnDescriptor{
			Name:     "progress",
			Type:     *types.Bytes,
			Nullable: true,
		})
		if err := desc.AddColumnToFamilyMaybeCreate("progress", "progress", true, false); err != nil {
			return err
		}
		if err := desc.AllocateIDs(); err != nil {
			return err
		}
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(desc.ID), sqlbase.WrapDescriptor(desc))
	})
}

func retireOldTsPurgeIntervalSettings(ctx context.Context, r runner) error {
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
	if err := r.execAsRoot(ctx, "copy-setting", `
INSERT INTO system.settings (name, value, "lastUpdated", "valueType")
   SELECT 'timeseries.storage.resolution_10s.ttl', value, "lastUpdated", "valueType"
     FROM system.settings WHERE name = 'timeseries.storage.10s_resolution_ttl'
ON CONFLICT (name) DO NOTHING`,
	); err != nil {
		return err
	}

	// Ditto 30m.
	if err := r.execAsRoot(ctx, "copy-setting", `
INSERT INTO system.settings (name, value, "lastUpdated", "valueType")
   SELECT 'timeseries.storage.resolution_30m.ttl', value, "lastUpdated", "valueType"
     FROM system.settings WHERE name = 'timeseries.storage.30m_resolution_ttl'
ON CONFLICT (name) DO NOTHING`,
	); err != nil {
		return err
	}

	return nil
}

func updateSystemLocationData(ctx context.Context, r runner) error {
	// See if the system.locations table already has data in it.
	// If so, we don't want to do anything.
	row, err := r.sqlExecutor.QueryRowEx(ctx, "update-system-locations",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT count(*) FROM system.locations`)
	if err != nil {
		return err
	}
	count := int(tree.MustBeDInt(row[0]))
	if count != 0 {
		return nil
	}

	for _, loc := range roachpb.DefaultLocationInformation {
		stmt := `UPSERT INTO system.locations VALUES ($1, $2, $3, $4)`
		tier := loc.Locality.Tiers[0]
		if err := r.execAsRoot(ctx, "update-system-locations",
			stmt, tier.Key, tier.Value, loc.Latitude, loc.Longitude,
		); err != nil {
			return err
		}
	}
	return nil
}
