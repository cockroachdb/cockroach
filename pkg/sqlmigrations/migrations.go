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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

	// AfterEnsureMigrations is called after each call to EnsureMigrations.
	AfterEnsureMigrations func()
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
		name:        "enable diagnostics reporting",
		workFn:      optInToDiagnosticsStatReporting,
		clusterWide: true,
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
		name:        "populate initial version cluster setting table entry",
		workFn:      populateVersionSetting,
		clusterWide: true,
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
		name:        "initialize cluster.secret",
		workFn:      initializeClusterSecret,
		clusterWide: true,
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
		newDescriptorIDs: databaseIDs(catalogkeys.DefaultDatabaseName, catalogkeys.PgDatabaseName),
	},
	{
		// Introduced in v2.1. Baked into 20.1.
		name: "add progress to system.jobs",
	},
	{
		// Introduced in v19.1.
		name: "create system.comment table",
	},
	{
		// This migration has been introduced some time before 19.2.
		name: "create system.replication_constraint_stats table",
	},
	{
		// This migration has been introduced some time before 19.2.
		name: "create system.replication_critical_localities table",
	},
	{
		// This migration has been introduced some time before 19.2.
		name: "create system.reports_meta table",
	},
	{
		// This migration has been introduced some time before 19.2.
		name: "create system.replication_stats table",
	},
	{
		// Introduced in v19.1.
		// TODO(knz): bake this migration into v19.2.
		name:        "propagate the ts purge interval to the new setting names",
		workFn:      retireOldTsPurgeIntervalSettings,
		clusterWide: true,
	},
	{
		// Introduced in v19.2.
		name:   "update system.locations with default location data",
		workFn: updateSystemLocationData,
	},
	{
		// Introduced in v19.2, baked into v20.1.
		name: "change reports fields from timestamp to timestamptz",
	},
	{
		// Introduced in v20.1, baked into v20.2.
		name: "create system.protected_ts_meta table",
	},
	{
		// Introduced in v20.1, baked into v20.2.
		name: "create system.protected_ts_records table",
	},
	{
		// Introduced in v20.1, baked into v21.2.
		name: "create new system.namespace table v2",
	},
	{
		// Introduced in v20.10. Replaced in v20.1.1 and v20.2 by the
		// StartSystemNamespaceMigration post-finalization-style migration.
		name: "migrate system.namespace_deprecated entries into system.namespace",
		// workFn:              migrateSystemNamespace,
	},
	{
		// Introduced in v20.1, baked into v20.2.
		name: "create system.role_options table",
	},
	{
		// Introduced in v20.1, baked into v20.2.
		name: "create statement_diagnostics_requests, statement_diagnostics and " +
			"system.statement_bundle_chunks tables",
	},
	{
		// Introduced in v20.1. Baked into v20.2.
		name:   "add CREATEROLE privilege to admin/root",
		workFn: nil,
	},
	{
		// Introduced in v20.2. Baked into v21.1.
		name: "add created_by columns to system.jobs",
	},
	{
		// Introduced in v20.2. Baked into v21.1.
		name:             "create new system.scheduled_jobs table",
		newDescriptorIDs: staticIDs(keys.ScheduledJobsTableID),
	},
	{
		// Introduced in v20.2. Baked into v21.1.
		name: "add new sqlliveness table and claim columns to system.jobs",
	},
	{
		// Introduced in v20.2.
		name:   "create new system.tenants table",
		workFn: createTenantsTable,
		// NB: no dedicated cluster version was introduced for this table at the
		// time (4272248e573cbaa4fac436b0ea07195fcd648845). The below is the first
		// cluster version that was added after the system.tenants table.
		includedInBootstrap: clusterversion.ByKey(clusterversion.AlterColumnTypeGeneral),
		newDescriptorIDs:    staticIDs(keys.TenantsTableID),
	},
	{
		// Introduced in v20.2. Baked into v21.1.
		name: "alter scheduled jobs",
	},
	{
		// Introduced in v20.2.
		name:   "add CREATELOGIN privilege to roles with CREATEROLE",
		workFn: extendCreateRoleWithCreateLogin,
	},
	{
		// Introduced in v20.2.
		name: "mark non-terminal schema change jobs with a pre-20.1 format version as failed",
	},
}

func staticIDs(
	ids ...descpb.ID,
) func(ctx context.Context, db DB, codec keys.SQLCodec) ([]descpb.ID, error) {
	return func(ctx context.Context, db DB, codec keys.SQLCodec) ([]descpb.ID, error) { return ids, nil }
}

func databaseIDs(
	names ...string,
) func(ctx context.Context, db DB, codec keys.SQLCodec) ([]descpb.ID, error) {
	return func(ctx context.Context, db DB, codec keys.SQLCodec) ([]descpb.ID, error) {
		var ids []descpb.ID
		for _, name := range names {
			kv, err := db.Get(ctx, catalogkeys.MakeDatabaseNameKey(codec, name))
			if err != nil {
				return nil, err
			}
			ids = append(ids, descpb.ID(kv.ValueInt()))
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
	// clusterWide migrations are only run by the system tenant. All other
	// migrations are run by each individual tenant. clusterWide migrations
	// typically have to do with cluster settings, which is a cluster-wide
	// concept.
	clusterWide bool
	// newDescriptorIDs is a function that returns the IDs of any additional
	// descriptors that were added by this migration. This is needed to automate
	// certain tests, which check the number of ranges/descriptors present on
	// server bootup.
	newDescriptorIDs func(ctx context.Context, db DB, codec keys.SQLCodec) ([]descpb.ID, error)
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
	db          DB
	codec       keys.SQLCodec
	sqlExecutor *sql.InternalExecutor
	settings    *cluster.Settings
}

func (r runner) execAsRoot(ctx context.Context, opName, stmt string, qargs ...interface{}) error {
	_, err := r.sqlExecutor.ExecEx(ctx, opName, nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.RootUserName(),
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

// DB is defined just to allow us to use a fake client.DB when testing this
// package.
type DB interface {
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
	db           DB
	codec        keys.SQLCodec
	sqlExecutor  *sql.InternalExecutor
	testingKnobs MigrationManagerTestingKnobs
	settings     *cluster.Settings
	jobRegistry  *jobs.Registry
}

// NewManager initializes and returns a new Manager object.
func NewManager(
	stopper *stop.Stopper,
	db *kv.DB,
	codec keys.SQLCodec,
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
		codec:        codec,
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
	db DB,
	codec keys.SQLCodec,
	defaultZoneConfig *zonepb.ZoneConfig,
	defaultSystemZoneConfig *zonepb.ZoneConfig,
) (descpb.IDs, error) {
	completedMigrations, err := getCompletedMigrations(ctx, db, codec)
	if err != nil {
		return nil, err
	}
	descriptorIDs := bootstrap.MakeMetadataSchema(codec, defaultZoneConfig, defaultSystemZoneConfig).DescriptorIDs()
	for _, migration := range backwardCompatibleMigrations {
		// Is the migration not creating descriptors?
		if migration.newDescriptorIDs == nil ||
			// Is the migration included in the metadata schema considered above?
			(migration.includedInBootstrap != roachpb.Version{}) {
			continue
		}
		if _, ok := completedMigrations[string(migrationKey(codec, migration))]; ok {
			newIDs, err := migration.newDescriptorIDs(ctx, db, codec)
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
	if m.testingKnobs.AfterEnsureMigrations != nil {
		defer m.testingKnobs.AfterEnsureMigrations()
	}
	// First, check whether there are any migrations that need to be run.
	completedMigrations, err := getCompletedMigrations(ctx, m.db, m.codec)
	if err != nil {
		return err
	}
	allMigrationsCompleted := true
	for _, migration := range backwardCompatibleMigrations {
		if !m.shouldRunMigration(migration, bootstrapVersion) {
			continue
		}
		if m.testingKnobs.DisableBackfillMigrations && migration.doesBackfill {
			log.Infof(ctx, "ignoring migrations after (and including) %s due to testing knob",
				migration.name)
			break
		}
		key := migrationKey(m.codec, migration)
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
		lease, err = m.leaseManager.AcquireLease(ctx, m.codec.MigrationLeaseKey())
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
	completedMigrations, err = getCompletedMigrations(ctx, m.db, m.codec)
	if err != nil {
		return err
	}

	startTime := timeutil.Now().String()
	r := runner{
		db:          m.db,
		codec:       m.codec,
		sqlExecutor: m.sqlExecutor,
		settings:    m.settings,
	}
	for _, migration := range backwardCompatibleMigrations {
		if !m.shouldRunMigration(migration, bootstrapVersion) {
			continue
		}

		key := migrationKey(m.codec, migration)
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

func (m *Manager) shouldRunMigration(
	migration migrationDescriptor, bootstrapVersion roachpb.Version,
) bool {
	if migration.workFn == nil {
		// The migration has been baked in.
		return false
	}
	minVersion := migration.includedInBootstrap
	if minVersion != (roachpb.Version{}) && !bootstrapVersion.Less(minVersion) {
		// The migration is unnecessary.
		return false
	}
	if migration.clusterWide && !m.codec.ForSystemTenant() {
		// The migration is a cluster-wide migration and we are not the
		// system tenant.
		return false
	}
	return true
}

func getCompletedMigrations(
	ctx context.Context, db DB, codec keys.SQLCodec,
) (map[string]struct{}, error) {
	if log.V(1) {
		log.Info(ctx, "trying to get the list of completed migrations")
	}
	prefix := codec.MigrationKeyPrefix()
	keyvals, err := db.Scan(ctx, prefix, prefix.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get list of completed migrations")
	}
	completedMigrations := make(map[string]struct{})
	for _, keyval := range keyvals {
		completedMigrations[string(keyval.Key)] = struct{}{}
	}
	return completedMigrations, nil
}

func migrationKey(codec keys.SQLCodec, migration migrationDescriptor) roachpb.Key {
	return append(codec.MigrationKeyPrefix(), roachpb.RKey(migration.name)...)
}

func createSystemTable(ctx context.Context, r runner, desc catalog.TableDescriptor) error {
	return CreateSystemTable(ctx, r.db, r.codec, r.settings, desc)
}

// CreateSystemTable is a function to inject a new system table. If the table
// already exists, ths function is a no-op.
func CreateSystemTable(
	ctx context.Context,
	db DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	desc catalog.TableDescriptor,
) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		tKey := catalogkeys.MakePublicObjectNameKey(codec, desc.GetParentID(), desc.GetName())
		b.CPut(tKey, desc.GetID(), nil)
		b.CPut(catalogkeys.MakeDescMetadataKey(codec, desc.GetID()), desc.DescriptorProto(), nil)
		if err := txn.SetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	// CPuts only provide idempotent inserts if we ignore the errors that arise
	// when the condition isn't met.
	if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
		return nil
	}
	return err
}

func extendCreateRoleWithCreateLogin(ctx context.Context, r runner) error {
	// Add the CREATELOGIN option to roles that already have CREATEROLE.
	const upsertCreateRoleStmt = `
     UPSERT INTO system.role_options (username, option, value)
        SELECT username, 'CREATELOGIN', NULL
          FROM system.role_options
         WHERE option = 'CREATEROLE'
     `
	return r.execAsRootWithRetry(ctx,
		"add CREATELOGIN where a role already has CREATEROLE",
		upsertCreateRoleStmt)
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

	// NB: We have to run with retry here due to the following "race" condition:
	// - We're attempting to the set the cluster version at startup.
	// - Setting the cluster version requires all nodes to be up and running, in
	//   order to push out all relevant version gates.
	// - This list of "all nodes" is gathered by looking at all the liveness
	//   records in KV.
	// - When starting a multi-node cluster all at once, nodes other than the
	//   one being bootstrapped join the cluster using the join RPC.
	// - The join RPC results in the creation of a liveness record for the
	//   joining node, except it starts off in an expired state (leaving it to
	//   the joining node to heartbeat it for the very first time).
	//
	// Attempting to set the cluster version at startup, while there also may be
	// other nodes trying to join, could then result in failures where the
	// migration infrastructure find expired liveness records and gives up. To
	// that end we'll simply retry, expecting the joining nodes to "come live"
	// before long.
	if err := r.execAsRootWithRetry(
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
	return r.execAsRootWithRetry(ctx, "addAdminRole", upsertAdminStmt, security.AdminRole)
}

func addRootToAdminRole(ctx context.Context, r runner) error {
	// Upsert the role membership into the table. We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, true)
          `
	return r.execAsRootWithRetry(
		ctx, "addRootToAdminRole", upsertAdminStmt, security.AdminRole, security.RootUser)
}

func disallowPublicUserOrRole(ctx context.Context, r runner) error {
	// Check whether a user or role named "public" exists.
	const selectPublicStmt = `
          SELECT username, "isRole" from system.users WHERE username = $1
          `

	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		row, err := r.sqlExecutor.QueryRowEx(
			ctx, "disallowPublicUserOrRole", nil, /* txn */
			sessiondata.InternalExecutorOverride{
				User: security.RootUserName(),
			},
			selectPublicStmt, security.PublicRole,
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
				security.PublicRole, security.PublicRole)
		}
		return fmt.Errorf(`found a user named %s which is now a reserved name. Please drop the role `+
			`(DROP USER %s) using a previous version of CockroachDB and try again`,
			security.PublicRole, security.PublicRole)
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
		for _, dbName := range []string{catalogkeys.DefaultDatabaseName, catalogkeys.PgDatabaseName} {
			stmt := fmt.Sprintf(createDbStmt, dbName)
			err = r.execAsRoot(ctx, "create-default-DB", stmt)
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
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
		if err := r.execAsRoot(ctx, "update-system-locations",
			stmt, tier.Key, tier.Value, loc.Latitude, loc.Longitude,
		); err != nil {
			return err
		}
	}
	return nil
}

func createTenantsTable(ctx context.Context, r runner) error {
	return createSystemTable(ctx, r, systemschema.TenantsTable)
}
