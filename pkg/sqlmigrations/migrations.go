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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
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
	AfterJobMigration         func()
	// AlwaysRunJobMigration controls whether to always run the schema change job
	// migration regardless of whether it has been marked as complete.
	AlwaysRunJobMigration bool
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
		// Introduced in v2.1. Baked into 20.1.
		name: "add progress to system.jobs",
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
		// Introduced in v19.2, baked into v20.1.
		name:                "change reports fields from timestamp to timestamptz",
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.Version19_2),
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
		// Introduced in v20.1. Note that this migration
		// has v2 appended to it because in 20.1 betas, the migration edited the old
		// system.namespace descriptor to change its Name. This wrought havoc,
		// causing #47167, which caused 19.2 nodes to fail to be able to read
		// system.namespace from SQL queries. However, without changing the old
		// descriptor's Name, backup would fail, since backup requires that no two
		// descriptors have the same Name. So, in v2 of this migration, we edit
		// the name of the new table's Descriptor, calling it
		// namespace2, and re-edit the old descriptor's Name to
		// be just "namespace" again, to try to help clusters that might have
		// upgraded to the 20.1 betas with the problem.
		name:                "create new system.namespace table v2",
		workFn:              createNewSystemNamespaceDescriptor,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionNamespaceTableWithSchemas),
		newDescriptorIDs:    staticIDs(keys.NamespaceTableID),
	},
	{
		// Introduced in v20.10. Replaced in v20.1.1 and v20.2 by the
		// StartSystemNamespaceMigration post-finalization-style migration.
		name: "migrate system.namespace_deprecated entries into system.namespace",
		// workFn:              migrateSystemNamespace,
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionNamespaceTableWithSchemas),
	},
	{
		// Introduced in v20.1.
		name:                "create system.role_options table",
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
	{
		// Introduced in v20.1.
		name:                "remove public permissions on system.comments",
		includedInBootstrap: clusterversion.VersionByKey(clusterversion.VersionSchemaChangeJob),
		workFn:              depublicizeSystemComments,
	},
	{
		// Introduced in v20.1.
		name:   "add CREATEROLE privilege to admin/root",
		workFn: addCreateRoleToAdminAndRoot,
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
	Scan(ctx context.Context, begin, end interface{}, maxRows int64) ([]kv.KeyValue, error)
	Get(ctx context.Context, key interface{}) (kv.KeyValue, error)
	Put(ctx context.Context, key, value interface{}) error
	Txn(ctx context.Context, retryable func(ctx context.Context, txn *kv.Txn) error) error
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
	jobRegistry  *jobs.Registry
}

// NewManager initializes and returns a new Manager object.
func NewManager(
	stopper *stop.Stopper,
	db *kv.DB,
	executor *sql.InternalExecutor,
	clock *hlc.Clock,
	testingKnobs MigrationManagerTestingKnobs,
	clientID string,
	settings *cluster.Settings,
	registry *jobs.Registry,
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
		jobRegistry:  registry,
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

var (
	schemaChangeJobMigrationName = "upgrade schema change job format"
	// schemaChangeJobMigrationKey is used for storing the completion status of
	// the schema change job migration.
	schemaChangeJobMigrationKey = append(keys.MigrationPrefix, roachpb.RKey(schemaChangeJobMigrationName)...)

	systemNamespaceMigrationName = "upgrade system.namespace post-20.1-finalization"
	systemNamespaceMigrationKey  = append(keys.MigrationPrefix, roachpb.RKey(systemNamespaceMigrationName)...)
)

// StartSchemaChangeJobMigration starts an async task to run the migration that
// upgrades 19.2-style jobs to the 20.1 job format, so that the jobs can be
// adopted by the job registry. The task first waits until the upgrade to 20.1
// is finalized before running the migration. The migration is retried until
// it succeeds (on any node).
func (m *Manager) StartSchemaChangeJobMigration(ctx context.Context) error {
	return m.stopper.RunAsyncTask(ctx, "run-schema-change-job-migration", func(ctx context.Context) {
		log.Info(ctx, "starting wait for upgrade finalization before schema change job migration")
		// First wait for the cluster to finalize the upgrade to 20.1. These values
		// were chosen to be similar to the retry loop for finalizing the cluster
		// upgrade.
		waitRetryOpts := retry.Options{
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     10 * time.Second,
			Closer:         m.stopper.ShouldQuiesce(),
		}
		for retry := retry.StartWithCtx(ctx, waitRetryOpts); retry.Next(); {
			if m.settings.Version.IsActive(ctx, clusterversion.VersionSchemaChangeJob) {
				break
			}
		}
		select {
		case <-m.stopper.ShouldQuiesce():
			return
		default:
		}
		log.VEventf(ctx, 2, "detected upgrade finalization")

		if !m.testingKnobs.AlwaysRunJobMigration {
			// Check whether this migration has already been completed.
			if kv, err := m.db.Get(ctx, schemaChangeJobMigrationKey); err != nil {
				log.Infof(ctx, "error getting record of schema change job migration: %s", err.Error())
			} else if kv.Exists() {
				log.Infof(ctx, "schema change job migration already complete")
				return
			}
		}

		// Now run the migration. This is retried indefinitely until it finishes.
		log.Infof(ctx, "starting schema change job migration")
		r := runner{
			db:          m.db,
			sqlExecutor: m.sqlExecutor,
			settings:    m.settings,
		}
		migrationRetryOpts := retry.Options{
			InitialBackoff: 1 * time.Minute,
			MaxBackoff:     10 * time.Minute,
			Closer:         m.stopper.ShouldQuiesce(),
		}
		startTime := timeutil.Now().String()
		for migRetry := retry.Start(migrationRetryOpts); migRetry.Next(); {
			migrateCtx, _ := m.stopper.WithCancelOnQuiesce(context.Background())
			migrateCtx = logtags.AddTag(migrateCtx, "schema-change-job-migration", nil)
			if err := migrateSchemaChangeJobs(migrateCtx, r, m.jobRegistry); err != nil {
				log.Errorf(ctx, "error attempting running schema change job migration, will retry: %s %s", err.Error(), startTime)
				continue
			}
			log.Infof(ctx, "schema change job migration completed")
			if err := m.db.Put(ctx, schemaChangeJobMigrationKey, startTime); err != nil {
				log.Warningf(ctx, "error persisting record of schema change job migration, will retry: %s", err.Error())
			}
			break
		}
		if fn := m.testingKnobs.AfterJobMigration; fn != nil {
			fn()
		}
	})
}

// schemaChangeMigrationKeyForTable returns a key prefixed with
// schemaChangeJobMigrationKey for a specific table, to store the completion
// status for adding a new job if the table was being added or needed to drain
// names.
func schemaChangeMigrationKeyForTable(tableID sqlbase.ID) roachpb.Key {
	return encoding.EncodeUint32Ascending(schemaChangeJobMigrationKey, uint32(tableID))
}

var systemNamespaceMigrationEnabled = settings.RegisterBoolSetting(
	"testing.system_namespace_migration.enabled",
	"internal testing only: disable the system namespace migration",
	true,
)

// StartSystemNamespaceMigration starts an async task to run the migration that
// migrates entries from system.namespace (descriptor 2) to system.namespace2
// (descriptor 30). The task first waits until the upgrade to 20.1 is finalized
// before running the migration. The migration is retried until it succeeds (on
// any node).
func (m *Manager) StartSystemNamespaceMigration(
	ctx context.Context, bootstrapVersion roachpb.Version,
) error {
	if !bootstrapVersion.Less(clusterversion.VersionByKey(clusterversion.VersionNamespaceTableWithSchemas)) {
		// Our bootstrap version is equal to or greater than 20.1, where no old
		// namespace table is created: we can skip this migration.
		return nil
	}
	return m.stopper.RunAsyncTask(ctx, "run-system-namespace-migration", func(ctx context.Context) {
		log.Info(ctx, "starting wait for upgrade finalization before system.namespace migration")
		// First wait for the cluster to finalize the upgrade to 20.1. These values
		// were chosen to be similar to the retry loop for finalizing the cluster
		// upgrade.
		waitRetryOpts := retry.Options{
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     10 * time.Second,
			Closer:         m.stopper.ShouldQuiesce(),
		}
		for retry := retry.StartWithCtx(ctx, waitRetryOpts); retry.Next(); {
			if !systemNamespaceMigrationEnabled.Get(&m.settings.SV) {
				continue
			}
			if m.settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) {
				break
			}
		}
		select {
		case <-m.stopper.ShouldQuiesce():
			return
		default:
		}
		log.VEventf(ctx, 2, "detected upgrade finalization for system.namespace migration")

		// Check whether this migration has already been completed.
		if kv, err := m.db.Get(ctx, systemNamespaceMigrationKey); err != nil {
			log.Infof(ctx, "error getting record of system.namespace migration: %s", err.Error())
		} else if kv.Exists() {
			log.Infof(ctx, "system.namespace migration already complete")
			return
		}

		// Now run the migration. This is retried indefinitely until it finishes.
		log.Infof(ctx, "starting system.namespace migration")
		r := runner{
			db:          m.db,
			sqlExecutor: m.sqlExecutor,
			settings:    m.settings,
		}
		migrationRetryOpts := retry.Options{
			InitialBackoff: 1 * time.Minute,
			MaxBackoff:     10 * time.Minute,
			Closer:         m.stopper.ShouldQuiesce(),
		}
		startTime := timeutil.Now().String()
		for migRetry := retry.Start(migrationRetryOpts); migRetry.Next(); {
			if err := m.migrateSystemNamespace(ctx, systemNamespaceMigrationKey, r, startTime); err != nil {
				log.Errorf(ctx, "error attempting running system.namespace migration, will retry: %s %s", err.Error(),
					startTime)
				continue
			}
			break
		}
	})
}

// migrateSystemNamespace migrates entries from the deprecated system.namespace
// table to the new one, which includes a parentSchemaID column. Each database
// entry is copied to the new table along with a corresponding entry for the
// 'public' schema. Each table entry is copied over with the public schema as
// as its parentSchemaID.
//
// Only entries that do not exist in the new table are copied.
//
// New database and table entries continue to be written to the deprecated
// namespace table until VersionNamespaceTableWithSchemas is active. This means
// that an additional migration will be necessary in 20.2 to catch any new
// entries which may have been missed by this one. In the meantime, namespace
// lookups fall back to the deprecated table if a name is not found in the new
// one.
func (m *Manager) migrateSystemNamespace(
	ctx context.Context, migrationKey roachpb.Key, r runner, startTime string,
) error {
	migrateCtx, cancel := m.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	migrateCtx = logtags.AddTag(migrateCtx, "system-namespace-migration", nil)
	// Loop until there's no more work to be done.
	workLeft := true
	for workLeft {
		if err := m.db.Txn(migrateCtx, func(ctx context.Context, txn *kv.Txn) error {
			// Check again to see if someone else wrote the migration key.
			if kv, err := txn.Get(ctx, migrationKey); err != nil {
				log.Infof(ctx, "error getting record of system.namespace migration: %s", err.Error())
				// Retry the migration.
				return err
			} else if kv.Exists() {
				// Give up, no work to be done.
				log.Infof(ctx, "system.namespace migration already complete")
				return nil
			}
			// Fetch all entries that are not present in the new namespace table. Each
			// of these entries will be copied to the new table.
			//
			// Note that we are very careful to always delete from both namespace tables
			// in 20.1, so there's no possibility that we'll be overwriting a deleted
			// table that existed in the old table and the new table but was deleted
			// from only the new table.
			const batchSize = 1000
			q := fmt.Sprintf(
				`SELECT "parentID", name, id FROM [%d AS namespace_deprecated]
              WHERE id NOT IN (SELECT id FROM [%d AS namespace]) LIMIT %d`,
				sqlbase.DeprecatedNamespaceTable.ID, sqlbase.NamespaceTable.ID, batchSize+1)
			rows, err := r.sqlExecutor.QueryEx(
				ctx, "read-deprecated-namespace-table", txn,
				sqlbase.InternalExecutorSessionDataOverride{
					User: security.RootUser,
				},
				q)
			if err != nil {
				return err
			}
			log.Infof(ctx, "Migrating system.namespace chunk with %d rows", len(rows))
			for i, row := range rows {
				workLeft = false
				// We found some rows from the query, which means that we can't quit
				// just yet.
				if i >= batchSize {
					workLeft = true
					// Just process 1000 rows at a time.
					break
				}
				parentID := sqlbase.ID(tree.MustBeDInt(row[0]))
				name := string(tree.MustBeDString(row[1]))
				id := sqlbase.ID(tree.MustBeDInt(row[2]))
				if parentID == keys.RootNamespaceID {
					// This row represents a database. Add it to the new namespace table.
					databaseKey := sqlbase.NewDatabaseKey(name)
					if err := txn.Put(ctx, databaseKey.Key(), id); err != nil {
						return err
					}
					// Also create a 'public' schema for this database.
					schemaKey := sqlbase.NewSchemaKey(id, "public")

					log.VEventf(ctx, 2, "Migrating system.namespace entry for database %s", name)
					if err := txn.Put(ctx, schemaKey.Key(), keys.PublicSchemaID); err != nil {
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
					log.VEventf(ctx, 2, "Migrating system.namespace entry for table %s", name)
					if err := txn.Put(ctx, tableKey.Key(), id); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// No more work to be done.
	log.Infof(migrateCtx, "system.namespace migration completed")
	if err := m.db.Put(migrateCtx, migrationKey, startTime); err != nil {
		log.Warningf(migrateCtx, "error persisting record of system.namespace migration, will retry: %s", err.Error())
		return err
	}
	return nil
}

// migrateSchemaChangeJobs runs the schema change job migration. The migration
// has two steps. In the first step, we scan the jobs table for all
// non-Succeeded jobs; for each job, it looks up the associated table and uses
// the table descriptor state to update the job payload appropriately. For jobs
// that are waiting for GC for dropped tables, indexes, etc., we mark the
// existing job as completed and create a new GC job. In the second step, we
// get all the descriptors and all running jobs, and create a new job for all
// tables that are either in the ADD state or have draining names but which
// have no running jobs, since tables in those states in 19.2 would have been
// processed by the schema changer.
func migrateSchemaChangeJobs(ctx context.Context, r runner, registry *jobs.Registry) error {
	// Get all jobs that aren't Succeeded and evaluate whether they need a
	// migration. (Jobs that are canceled in 19.2 could still have in-progress
	// schema changes.)
	rows, err := r.sqlExecutor.QueryEx(
		ctx, "jobs-for-migration", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"SELECT id, payload FROM system.jobs WHERE status != $1", jobs.StatusSucceeded,
	)
	if err != nil {
		return err
	}
	for _, row := range rows {
		jobID := int64(tree.MustBeDInt(row[0]))
		log.VEventf(ctx, 2, "job %d: evaluating for schema change job migration", jobID)

		payload, err := jobs.UnmarshalPayload(row[1])
		if err != nil {
			return err
		}
		if details := payload.GetSchemaChange(); details == nil ||
			details.FormatVersion > jobspb.BaseFormatVersion {
			continue
		}

		log.Infof(ctx, "job %d: undergoing schema change job migration", jobID)

		if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Read the job again inside the transaction. If the job was already
			// upgraded, we don't have to do anything else.
			job, err := registry.LoadJobWithTxn(ctx, jobID, txn)
			if err != nil {
				// The job could have been GC'ed in the meantime.
				if jobs.HasJobNotFoundError(err) {
					return nil
				}
				return err
			}
			payload := job.Payload()
			details := payload.GetSchemaChange()
			if details.FormatVersion > jobspb.BaseFormatVersion {
				return nil
			}

			// Determine whether the job is for dropping a database/table. Note that
			// DroppedTables is always populated in 19.2 for all jobs that drop
			// tables.
			if len(details.DroppedTables) > 0 {
				return migrateDropTablesOrDatabaseJob(ctx, txn, registry, job)
			}

			descIDs := job.Payload().DescriptorIDs
			// All other jobs have exactly 1 associated descriptor ID (for a table),
			// and correspond to a schema change with a mutation.
			if len(descIDs) != 1 {
				return errors.AssertionFailedf(
					"job %d: could not be migrated due to unexpected descriptor IDs %v", *job.ID(), descIDs)
			}
			descID := descIDs[0]
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, descID)
			if err != nil {
				if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
					// If the table descriptor doesn't exist, we call Registry.Failed() to
					// mark the job as failed and return the (hopefully nil) error from
					// that.
					log.Warningf(ctx, "job %d: expected descriptor %d not found, marking job as failed", *job.ID(), descID)
					return registry.Failed(ctx, txn, *job.ID(), err)
				}
				return err
			}
			return migrateMutationJobForTable(ctx, txn, registry, job, tableDesc)
		}); err != nil {
			return err
		}
		log.Infof(ctx, "job %d: completed schema change job migration", jobID)
	}

	// Finally, we iterate through all table descriptors and jobs, and create jobs
	// for any tables in the ADD state or that have draining names that don't
	// already have jobs. We also create a GC job for all tables in the DROP state
	// with no associated schema change or GC job, which can result from failed
	// IMPORT and RESTORE jobs whose table data wasn't fully GC'ed.
	//
	// We start by getting all descriptors and all running jobs in a single
	// transaction. Each eligible table then gets a job created for it, each in a
	// separate transaction; in each of those transactions, we write a table-
	// specific KV with a key prefixed by schemaChangeJobMigrationKey to try to
	// prevent more than one such job from being created for the table.
	//
	// This process ensures that every table that entered into one of these
	// intermediate states (being added/dropped, or having draining names) in 19.2
	// will have a job created for it in 20.1, so that the table can finish being
	// processed. It's not essential for only one job to be created for each
	// table, since a redundant schema change job is a no-op, but we make an
	// effort to do that anyway.
	//
	// There are probably more efficient ways to do this part of the migration,
	// but the current approach seemed like the most straightforward.
	var allDescs []sqlbase.DescriptorProto
	schemaChangeJobsForDesc := make(map[sqlbase.ID][]int64)
	gcJobsForDesc := make(map[sqlbase.ID][]int64)
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		descs, err := sql.GetAllDescriptors(ctx, txn)
		if err != nil {
			return err
		}
		allDescs = descs

		// Get all running schema change jobs.
		rows, err := r.sqlExecutor.QueryEx(
			ctx, "preexisting-jobs", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"SELECT id, payload FROM system.jobs WHERE status = $1", jobs.StatusRunning,
		)
		if err != nil {
			return err
		}
		for _, row := range rows {
			jobID := int64(tree.MustBeDInt(row[0]))
			payload, err := jobs.UnmarshalPayload(row[1])
			if err != nil {
				return err
			}
			if details := payload.GetSchemaChange(); details != nil {
				if details.FormatVersion < jobspb.JobResumerFormatVersion {
					continue
				}
				if details.TableID != sqlbase.InvalidID {
					schemaChangeJobsForDesc[details.TableID] = append(schemaChangeJobsForDesc[details.TableID], jobID)
				} else {
					for _, t := range details.DroppedTables {
						schemaChangeJobsForDesc[t.ID] = append(schemaChangeJobsForDesc[t.ID], jobID)
					}
				}
			} else if details := payload.GetSchemaChangeGC(); details != nil {
				for _, t := range details.Tables {
					gcJobsForDesc[t.ID] = append(gcJobsForDesc[t.ID], jobID)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	createSchemaChangeJobForTable := func(txn *kv.Txn, desc *sqlbase.TableDescriptor) error {
		var description string
		if desc.Adding() {
			description = fmt.Sprintf("adding table %d", desc.ID)
		} else if desc.HasDrainingNames() {
			description = fmt.Sprintf("draining names for table %d", desc.ID)
		} else {
			// This shouldn't be possible, but if it happens, it would be
			// appropriate to do nothing without returning an error.
			log.Warningf(
				ctx,
				"tried to add schema change job for table %d which is neither being added nor has draining names",
				desc.ID,
			)
			return nil
		}
		record := jobs.Record{
			Description:   description,
			Username:      security.NodeUser,
			DescriptorIDs: sqlbase.IDs{desc.ID},
			Details: jobspb.SchemaChangeDetails{
				TableID:       desc.ID,
				FormatVersion: jobspb.JobResumerFormatVersion,
			},
			Progress:      jobspb.SchemaChangeProgress{},
			NonCancelable: true,
		}
		job, err := registry.CreateJobWithTxn(ctx, record, txn)
		if err != nil {
			return err
		}
		log.Infof(ctx, "migration created new schema change job %d: %s", *job.ID(), description)
		return nil
	}

	createGCJobForTable := func(txn *kv.Txn, desc *sqlbase.TableDescriptor) error {
		record := sql.CreateGCJobRecord(
			fmt.Sprintf("table %d", desc.ID),
			security.NodeUser,
			jobspb.SchemaChangeGCDetails{
				Tables: []jobspb.SchemaChangeGCDetails_DroppedID{{ID: desc.ID, DropTime: desc.DropTime}},
			})
		job, err := registry.CreateJobWithTxn(ctx, record, txn)
		if err != nil {
			return err
		}
		log.Infof(ctx, "migration created new GC job %d for table %d", *job.ID(), desc.ID)
		return nil
	}

	log.Infof(ctx, "evaluating tables for creating jobs")
	for _, desc := range allDescs {
		switch desc := desc.(type) {
		case *sqlbase.TableDescriptor:
			if scJobs := schemaChangeJobsForDesc[desc.ID]; len(scJobs) > 0 {
				log.VEventf(ctx, 3, "table %d has running schema change jobs %v, skipping", desc.ID, scJobs)
				continue
			} else if gcJobs := gcJobsForDesc[desc.ID]; len(gcJobs) > 0 {
				log.VEventf(ctx, 3, "table %d has running GC jobs %v, skipping", desc.ID, gcJobs)
				continue
			}
			if !desc.Adding() && !desc.Dropped() && !desc.HasDrainingNames() {
				log.VEventf(ctx, 3,
					"table %d is not being added or dropped and does not have draining names, skipping",
					desc.ID,
				)
				continue
			}

			if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				key := schemaChangeMigrationKeyForTable(desc.ID)
				startTime := timeutil.Now().String()
				if kv, err := txn.Get(ctx, key); err != nil {
					return err
				} else if kv.Exists() {
					log.VEventf(ctx, 3, "table %d already processed in migration", desc.ID)
					return nil
				}
				if desc.Adding() || desc.HasDrainingNames() {
					if err := createSchemaChangeJobForTable(txn, desc); err != nil {
						return err
					}
				} else if desc.Dropped() {
					// Note that a table can be both in the DROP state and have draining
					// names. In that case it was enough to just create a schema change
					// job, as in the case above, because that job will itself create a
					// GC job.
					if err := createGCJobForTable(txn, desc); err != nil {
						return err
					}
				}
				if err := txn.Put(ctx, key, startTime); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		case *sqlbase.DatabaseDescriptor:
			// Do nothing.
		}
	}

	return nil
}

// migrateMutationJobForTable handles migrating jobs associated with mutations
// on a table, each of which is stored in MutationJobs. (This includes adding
// and dropping columns, indexes, and constraints, as well as primary key
// changes.) This function also handles jobs for indexes waiting for GC,
// stored in GCMutations.
func migrateMutationJobForTable(
	ctx context.Context,
	txn *kv.Txn,
	registry *jobs.Registry,
	job *jobs.Job,
	tableDesc *sql.TableDescriptor,
) error {
	log.VEventf(ctx, 2, "job %d: undergoing migration as mutation job for table %d", *job.ID(), tableDesc.ID)

	// Check whether the job is for a mutation. There can be multiple mutations
	// with the same ID that correspond to the same job, but we just need to
	// look at one, since all the mutations with the same ID get state updates
	// in the same transaction.
	for i := range tableDesc.MutationJobs {
		mutationJob := &tableDesc.MutationJobs[i]
		if mutationJob.JobID != *job.ID() {
			continue
		}
		log.VEventf(
			ctx, 2, "job %d: found corresponding MutationJob %d on table %d",
			*job.ID(), mutationJob.MutationID, tableDesc.ID,
		)
		var mutation *sqlbase.DescriptorMutation
		for i := range tableDesc.Mutations {
			if tableDesc.Mutations[i].MutationID == mutationJob.MutationID {
				mutation = &tableDesc.Mutations[i]
				break
			}
		}
		if mutation == nil {
			// In theory, MutationJobs[i] corresponds to Mutations[i] in 19.2 and
			// earlier versions, so this should never happen. However, we've seen this
			// happen (#48786), so we have to be defensive.
			mutationNotFoundError := errors.AssertionFailedf("mutation %d not found for MutationJob %d",
				mutationJob.MutationID, mutationJob.JobID)
			log.Errorf(ctx, "%v", mutationNotFoundError)
			return registry.Failed(ctx, txn, *job.ID(), mutationNotFoundError)
		}

		// Update the job details and status based on the table descriptor
		// state.
		if err := job.WithTxn(txn).Update(ctx, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			// Update the job details with the table and mutation IDs.
			details := md.Payload.GetSchemaChange()
			details.TableID = tableDesc.ID
			details.MutationID = mutationJob.MutationID
			details.FormatVersion = jobspb.JobResumerFormatVersion
			md.Payload.Details = jobspb.WrapPayloadDetails(*details)

			log.VEventf(ctx, 2, "job %d: updating details to %+v", *job.ID(), details)

			// Also give the job a non-nil expired lease to indicate that the job
			// is adoptable.
			md.Payload.Lease = &jobspb.Lease{}
			ju.UpdatePayload(md.Payload)

			// If the mutation exists on the table descriptor, then the schema
			// change isn't actually in a terminal state, regardless of what the
			// job status is. So we force the status to Reverting if there's any
			// indication that it failed or was canceled, and otherwise force the
			// state to Running.
			shouldRevert := md.Status == jobs.StatusFailed || md.Status == jobs.StatusCanceled ||
				md.Status == jobs.StatusReverting || md.Status == jobs.StatusCancelRequested
			previousStatus := md.Status
			if mutation.Rollback || shouldRevert {
				md.Status = jobs.StatusReverting
			} else {
				md.Status = jobs.StatusRunning
			}
			log.VEventf(ctx, 2, "job %d: updating status from %s to %s", *job.ID(), previousStatus, md.Status)
			ju.UpdateStatus(md.Status)
			return nil
		}); err != nil {
			return err
		}
		log.Infof(
			ctx, "job %d: successfully migrated for table %d, mutation %d",
			*job.ID(), tableDesc.ID, mutationJob.MutationID,
		)
		return nil
	}

	// If not a mutation, check whether the job corresponds to a GCMutation.
	// This indicates that the job must be in the "waiting for GC TTL" state.
	// In that case, we mark the job as succeeded and create a new job for GC.
	for i := range tableDesc.GCMutations {
		gcMutation := &tableDesc.GCMutations[i]
		// JobID and dropTime are populated only in 19.2 and earlier versions.
		if gcMutation.JobID != *job.ID() {
			continue
		}
		log.VEventf(
			ctx, 2, "job %d: found corresponding index GC mutation for index %d on table %d",
			*job.ID(), gcMutation.IndexID, tableDesc.ID,
		)
		if err := registry.Succeeded(ctx, txn, *job.ID()); err != nil {
			return err
		}
		log.VEventf(ctx, 2, "job %d: marked as succeeded", *job.ID())

		indexGCJobRecord := sql.CreateGCJobRecord(
			job.Payload().Description,
			job.Payload().Username,
			jobspb.SchemaChangeGCDetails{
				Indexes: []jobspb.SchemaChangeGCDetails_DroppedIndex{
					{
						IndexID:  gcMutation.IndexID,
						DropTime: gcMutation.DropTime,
					},
				},
				ParentID: tableDesc.GetID(),
			},
		)
		// The new job ID won't be written to GCMutations, which is fine because
		// we don't read the job ID in 20.1 for anything except this migration.
		newJob, err := registry.CreateJobWithTxn(ctx, indexGCJobRecord, txn)
		if err != nil {
			return err
		}
		log.Infof(ctx,
			"migration marked drop table job %d as successful, created GC job %d",
			*job.ID(), *newJob.ID(),
		)
		return nil
	}

	// If the job isn't in MutationJobs or GCMutations, it's likely just a
	// failed or canceled job that was successfully cleaned up. Check for this,
	// and return an error if this is not the case.
	status, err := job.WithTxn(txn).CurrentStatus(ctx)
	if err != nil {
		return err
	}
	if status == jobs.StatusCanceled || status == jobs.StatusFailed {
		return nil
	}
	return errors.Newf(
		"job %d: no corresponding mutation found on table %d during migration", *job.ID(), tableDesc.ID)
}

// migrateDropTablesOrDatabaseJob handles migrating any jobs that require
// dropping a table, including dropping tables, views, sequences, and
// databases, as well as truncating tables.
func migrateDropTablesOrDatabaseJob(
	ctx context.Context, txn *kv.Txn, registry *jobs.Registry, job *jobs.Job,
) error {
	payload := job.Payload()
	details := payload.GetSchemaChange()
	log.VEventf(ctx, 2,
		"job %d: undergoing migration as drop table/database job for tables %+v",
		*job.ID(), details.DroppedTables,
	)

	if job.Progress().RunningStatus == string(sql.RunningStatusDrainingNames) {
		// If the job is draining names, the schema change job resumer will handle
		// it. Just update the job details.
		if err := job.WithTxn(txn).Update(ctx, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			if len(details.DroppedTables) == 1 {
				details.TableID = details.DroppedTables[0].ID
			}
			details.FormatVersion = jobspb.JobResumerFormatVersion
			md.Payload.Details = jobspb.WrapPayloadDetails(*details)

			log.VEventf(ctx, 2, "job %d: updating details to %+v", *job.ID(), details)

			// Also give the job a non-nil expired lease to indicate that the job
			// is adoptable.
			md.Payload.Lease = &jobspb.Lease{}
			ju.UpdatePayload(md.Payload)
			return nil
		}); err != nil {
			return err
		}
		log.Infof(ctx, "job %d: successfully migrated in draining names state", *job.ID())
		return nil
	}

	// Otherwise, the job is in the "waiting for GC TTL" phase. In this case, we
	// mark the present job as Succeeded and create a new GC job.
	if err := registry.Succeeded(ctx, txn, *job.ID()); err != nil {
		return err
	}
	log.VEventf(ctx, 2, "job %d: marked as succeeded", *job.ID())

	var tablesToDrop []jobspb.SchemaChangeGCDetails_DroppedID
	for i := range details.DroppedTables {
		tableID := details.DroppedTables[i].ID
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
		if err != nil {
			if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
				// This table was already GC'ed, so exclude it from the job. This is
				// possible when different tables in the dropped database, or in the set
				// of tables to be truncated, etc., have different zone configs.
				log.Infof(ctx, "table %d does not exist, skipping table in GC job", tableID)
				continue
			}
			return err
		}
		tablesToDrop = append(tablesToDrop, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       desc.ID,
			DropTime: desc.DropTime,
		})
	}
	gcJobRecord := sql.CreateGCJobRecord(
		job.Payload().Description,
		job.Payload().Username,
		jobspb.SchemaChangeGCDetails{
			Tables:   tablesToDrop,
			ParentID: details.DroppedDatabaseID,
		},
	)
	// The new job ID won't be written to DropJobID on the table descriptor(s),
	// which is fine because we don't read the job ID in 20.1 for anything
	// except this migration.
	// TODO (lucy): The above is true except for the cleanup loop for orphaned
	// jobs in the registry, which should be fixed.
	newJob, err := registry.CreateJobWithTxn(ctx, gcJobRecord, txn)
	if err != nil {
		return err
	}
	log.Infof(ctx,
		"migration marked drop database/table job %d as successful, created GC job %d",
		*job.ID(), *newJob.ID(),
	)
	return err
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
	err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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

	return r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()

		// Retrieve the existing namespace table's descriptor and change its name to
		// "namespace". This corrects the behavior of this migration as it existed
		// in 20.1 betas. The old namespace table cannot be edited without breaking
		// explicit selects from system.namespace in 19.2.
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
			sqlbase.NamespaceTable.GetParentID(), sqlbase.NamespaceTableName)
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

	return nil
}

func addCreateRoleToAdminAndRoot(ctx context.Context, r runner) error {
	// Upsert the admin/root roles with CreateRole privilege into the table.
	// We intentionally override any existing entry.
	const upsertCreateRoleStmt = `
          UPSERT INTO system.role_options (username, option, value) VALUES ($1, 'CREATEROLE', NULL)
          `
	err := r.execAsRootWithRetry(ctx,
		"add role options table and upsert admin with CREATEROLE",
		upsertCreateRoleStmt,
		sqlbase.AdminRole)

	if err != nil {
		return err
	}

	return r.execAsRootWithRetry(ctx,
		"add role options table and upsert admin with CREATEROLE",
		upsertCreateRoleStmt,
		security.RootUser)
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
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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

func depublicizeSystemComments(ctx context.Context, r runner) error {
	// At some point in time, system.comments was mistakenly created
	// with all privileges granted to the "public" role (i.e. everyone).
	// This migration cleans this up.

	// Schema changes are normally banned in the mixed-version 19.2/20.1 state, so
	// we override the ban and force the schema change to run anyway. This is safe
	// because updating privileges on a table only requires an update to the table
	// descriptor, and the job created will only wait for leases to expire. We
	// don't expect any other schema changes to happen on the comments table that
	// the 20.1 job could interfere with. The update to the table descriptor would
	// cause a 19.2 SchemaChangeManager to attempt a schema change, but it would
	// be a no-op.
	ctx = sql.MigrationSchemaChangeRequiredContext(ctx)

	for _, priv := range []string{"GRANT", "INSERT", "DELETE", "UPDATE"} {
		stmt := fmt.Sprintf(`REVOKE %s ON TABLE system.comments FROM public`, priv)
		// REVOKE should never fail here -- it's always possible for root
		// to revoke a privilege even if it's not currently granted.
		if err := r.execAsRoot(ctx, "depublicize-system-comments", stmt); err != nil {
			return err
		}
	}
	return nil
}
