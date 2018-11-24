// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlmigrations

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
// Migrations must be idempotent: a migration may run successfully but not be recorded
// as completed, causing a second run.
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
		// Introduced in v1.1. Permanent migration.
		name:   "persist trace.debug.enable = 'false'",
		workFn: disableNetTrace,
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
		// TODO(mberhault): bake into v2.2.
		name:   "repeat: ensure admin role privileges in all descriptors",
		workFn: ensureMaxPrivileges,
	},
	{
		// Introduced in v2.1.
		// TODO(mberhault): bake into v2.2.
		name:   "disallow public user or role name",
		workFn: disallowPublicUserOrRole,
	},
	{
		// Introduced in v2.1.
		// TODO(knz): bake this migration into v2.2.
		name:             "create default databases",
		workFn:           createDefaultDbs,
		newDescriptorIDs: databaseIDs(sessiondata.DefaultDatabaseName, sessiondata.PgDatabaseName),
	},
	{
		// Introduced in v2.1.
		// TODO(dt): Bake into v2.2.
		name:   "add progress to system.jobs",
		workFn: addJobsProgress,
	},
	{
		// Introduced in v2.1.
		// TODO(hueypark): bake this migration into v2.2.
		name:             "create system.comment table",
		workFn:           createCommentTable,
		newDescriptorIDs: staticIDs(keys.CommentsTableID),
	},
}

func staticIDs(ids ...sqlbase.ID) func(ctx context.Context, db db) ([]sqlbase.ID, error) {
	return func(ctx context.Context, db db) ([]sqlbase.ID, error) { return ids, nil }
}

func databaseIDs(names ...string) func(ctx context.Context, db db) ([]sqlbase.ID, error) {
	return func(ctx context.Context, db db) ([]sqlbase.ID, error) {
		var ids []sqlbase.ID
		for _, name := range names {
			kv, err := db.Get(ctx, sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, name))
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
	name string
	// workFn must be idempotent so that we can safely re-run it if a node failed
	// while running it.
	workFn func(context.Context, runner) error
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
}

// leaseManager is defined just to allow us to use a fake client.LeaseManager
// when testing this package.
type leaseManager interface {
	AcquireLease(ctx context.Context, key roachpb.Key) (*client.Lease, error)
	ExtendLease(ctx context.Context, l *client.Lease) error
	ReleaseLease(ctx context.Context, l *client.Lease) error
	TimeRemaining(l *client.Lease) time.Duration
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
}

// NewManager initializes and returns a new Manager object.
func NewManager(
	stopper *stop.Stopper,
	db *client.DB,
	executor *sql.InternalExecutor,
	clock *hlc.Clock,
	testingKnobs MigrationManagerTestingKnobs,
	clientID string,
) *Manager {
	opts := client.LeaseManagerOptions{
		ClientID:      clientID,
		LeaseDuration: leaseDuration,
	}
	return &Manager{
		stopper:      stopper,
		leaseManager: client.NewLeaseManager(db, clock, opts),
		db:           db,
		sqlExecutor:  executor,
		testingKnobs: testingKnobs,
	}
}

// ExpectedDescriptorIDs returns the list of all expected system descriptor IDs,
// including those added by completed migrations. This is needed for certain
// tests, which check the number of ranges and system tables at node startup.
//
// NOTE: This value may be out-of-date if another node is actively running
// migrations, and so should only be used in test code where the migration
// lifecycle is tightly controlled.
func ExpectedDescriptorIDs(ctx context.Context, db db) (sqlbase.IDs, error) {
	completedMigrations, err := getCompletedMigrations(ctx, db)
	if err != nil {
		return nil, err
	}
	descriptorIDs := sqlbase.MakeMetadataSchema().DescriptorIDs()
	for _, migration := range backwardCompatibleMigrations {
		if migration.newDescriptorIDs == nil {
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
func (m *Manager) EnsureMigrations(ctx context.Context) error {
	// First, check whether there are any migrations that need to be run.
	completedMigrations, err := getCompletedMigrations(ctx, m.db)
	if err != nil {
		return err
	}
	allMigrationsCompleted := true
	for _, migration := range backwardCompatibleMigrations {
		if migration.workFn == nil {
			// Migration has been baked in. Ignore it.
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
	var lease *client.Lease
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
	}
	for _, migration := range backwardCompatibleMigrations {
		if migration.workFn == nil {
			// Migration has been baked in. Ignore it.
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

		if log.V(1) {
			log.Infof(ctx, "trying to persist record of completing migration %s", migration.name)
		}
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
		b.CPut(sqlbase.MakeNameMetadataKey(desc.GetParentID(), desc.GetName()), desc.GetID(), nil)
		b.CPut(sqlbase.MakeDescMetadataKey(desc.GetID()), sqlbase.WrapDescriptor(&desc), nil)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	if err != nil {
		// CPuts only provide idempotent inserts if we ignore the errors that arise
		// when the condition isn't met.
		if _, ok := err.(*roachpb.ConditionFailedError); ok {
			return nil
		}
	}
	return err
}

func createCommentTable(ctx context.Context, r runner) error {
	return createSystemTable(ctx, r, sqlbase.CommentsTable)
}

var reportingOptOut = envutil.EnvOrDefaultBool("COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", false)

func runStmtAsRootWithRetry(
	ctx context.Context, r runner, opName string, stmt string, qargs ...interface{},
) error {
	// Retry a limited number of times because returning an error and letting
	// the node kill itself is better than holding the migration lease for an
	// arbitrarily long time.
	var err error
	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		_, err := r.sqlExecutor.Exec(ctx, opName, nil /* txn */, stmt, qargs...)
		if err == nil {
			break
		}
		log.Warningf(ctx, "failed to run %s: %v", stmt, err)
	}
	return err
}

// SettingsDefaultOverrides documents the effect of several migrations that add
// an explicit value for a setting, effectively changing the "default value"
// from what was defined in code.
var SettingsDefaultOverrides = map[string]string{
	"diagnostics.reporting.enabled": "true",
	"trace.debug.enable":            "false",
	"cluster.secret":                "<random>",
}

func optInToDiagnosticsStatReporting(ctx context.Context, r runner) error {
	// We're opting-out of the automatic opt-in. See discussion in updates.go.
	if reportingOptOut {
		return nil
	}
	return runStmtAsRootWithRetry(
		ctx, r, "optInToDiagnosticsStatReporting", `SET CLUSTER SETTING diagnostics.reporting.enabled = true`)
}

func disableNetTrace(ctx context.Context, r runner) error {
	return runStmtAsRootWithRetry(
		ctx, r, "disableNetTrace", `SET CLUSTER SETTING trace.debug.enable = false`)
}

func initializeClusterSecret(ctx context.Context, r runner) error {
	return runStmtAsRootWithRetry(
		ctx, r, "initializeClusterSecret",
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
		// The cluster was bootstrapped at v1.0 (or even earlier), so make that
		// the version.
		v = cluster.VersionByKey(cluster.VersionBase)
	}

	b, err := protoutil.Marshal(&cluster.ClusterVersion{
		MinimumVersion: v,
		UseVersion:     v,
	})
	if err != nil {
		return errors.Wrap(err, "while marshaling version")
	}

	// Add a ON CONFLICT DO NOTHING to avoid changing an existing version.
	// Again, this can happen if the migration doesn't run to completion
	// (overwriting also seems reasonable, but what for).
	// We don't allow users to perform version changes until we have run
	// the insert below.
	if _, err := r.sqlExecutor.Exec(
		ctx,
		"insert-setting",
		nil, /* txn */
		fmt.Sprintf(`INSERT INTO system.settings (name, value, "lastUpdated", "valueType") VALUES ('version', x'%x', now(), 'm') ON CONFLICT(name) DO NOTHING`, b),
	); err != nil {
		return err
	}

	if _, err := r.sqlExecutor.Exec(
		ctx, "set-setting", nil /* txn */, "SET CLUSTER SETTING version = $1", v.String(),
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
	return runStmtAsRootWithRetry(ctx, r, "addRootUser", upsertRootStmt, security.RootUser)
}

func addAdminRole(ctx context.Context, r runner) error {
	// Upsert the admin role into the table. We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.users (username, "hashedPassword", "isRole") VALUES ($1, '', true)
          `
	return runStmtAsRootWithRetry(ctx, r, "addAdminRole", upsertAdminStmt, sqlbase.AdminRole)
}

func addRootToAdminRole(ctx context.Context, r runner) error {
	// Upsert the role membership into the table. We intentionally override any existing entry.
	const upsertAdminStmt = `
          UPSERT INTO system.role_members ("role", "member", "isAdmin") VALUES ($1, $2, true)
          `
	return runStmtAsRootWithRetry(
		ctx, r, "addRootToAdminRole", upsertAdminStmt, sqlbase.AdminRole, security.RootUser)
}

// ensureMaxPrivileges ensures that all descriptors have privileges
// for the admin role, and that root and user privileges do not exceed
// the allowed privileges.
//
// TODO(mberhault): Remove this migration in v2.1.
func ensureMaxPrivileges(ctx context.Context, r runner) error {
	tableDescFn := func(desc *sqlbase.TableDescriptor) (bool, error) {
		return desc.Privileges.MaybeFixPrivileges(desc.ID), nil
	}
	databaseDescFn := func(desc *sqlbase.DatabaseDescriptor) (bool, error) {
		return desc.Privileges.MaybeFixPrivileges(desc.ID), nil
	}
	return upgradeDescsWithFn(ctx, r, tableDescFn, databaseDescFn)
}

var upgradeDescBatchSize int64 = 10

// upgradeTableDescsWithFn runs the provided upgrade functions on each table
// and database descriptor, persisting any upgrades if the function indicates that the
// descriptor was changed.
// Upgrade functions may be nil to perform nothing for the corresponding descriptor type.
// If it returns an error some descriptors could have been upgraded.
func upgradeDescsWithFn(
	ctx context.Context,
	r runner,
	upgradeTableDescFn func(desc *sqlbase.TableDescriptor) (upgraded bool, err error),
	upgradeDatabaseDescFn func(desc *sqlbase.DatabaseDescriptor) (upgraded bool, err error),
) error {
	// use multiple transactions to prevent blocking reads on the
	// table descriptors while running this upgrade process.
	startKey := sqlbase.MakeAllDescsMetadataKey()
	span := roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()}
	for resumeSpan := (roachpb.Span{}); span.Key != nil; span = resumeSpan {
		// It's safe to use multiple transactions here because it is assumed
		// that a new table created will be created upgraded.
		if err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			// Scan a limited batch of keys.
			b := txn.NewBatch()
			b.Header.MaxSpanRequestKeys = upgradeDescBatchSize
			b.Scan(span.Key, span.EndKey)
			if err := txn.Run(ctx, b); err != nil {
				return err
			}
			result := b.Results[0]
			kvs := result.Rows
			// Store away the span for the next batch.
			resumeSpan = result.ResumeSpan

			var idVersions []sql.IDVersion
			var now hlc.Timestamp
			b = txn.NewBatch()
			for _, kv := range kvs {
				var sqlDesc sqlbase.Descriptor
				if err := kv.ValueProto(&sqlDesc); err != nil {
					return err
				}
				switch t := sqlDesc.Union.(type) {
				case *sqlbase.Descriptor_Table:
					if table := sqlDesc.GetTable(); table != nil && upgradeTableDescFn != nil {
						if upgraded, err := upgradeTableDescFn(table); err != nil {
							return err
						} else if upgraded {
							// It's safe to ignore the DROP state here and
							// unconditionally increment the version. For proof, see
							// TestDropTableWhileUpgradingFormat.
							//
							// In fact, it's of the utmost importance that this migration
							// upgrades every last old-format table descriptor, including those
							// that are dropping. Otherwise, the user could upgrade to a version
							// without support for reading the old format before the drop
							// completes, leaving a broken table descriptor and the table's
							// remaining data around forever. This isn't just a theoretical
							// concern: consider that dropping a large table can take several
							// days, while upgrading to a new version can take as little as a
							// few minutes.
							table.Version++
							now = txn.CommitTimestamp()
							idVersions = append(idVersions,
								sql.NewIDVersion(
									table.Name, table.ID, table.Version-2,
								))
							// Use ValidateTable() instead of Validate()
							// because of #26422. We still do not know why
							// a table can reference a dropped database.
							if err := table.ValidateTable(nil); err != nil {
								return err
							}

							b.Put(kv.Key, sqlbase.WrapDescriptor(table))
						}
					}
				case *sqlbase.Descriptor_Database:
					if database := sqlDesc.GetDatabase(); database != nil && upgradeDatabaseDescFn != nil {
						if upgraded, err := upgradeDatabaseDescFn(database); err != nil {
							return err
						} else if upgraded {
							if err := database.Validate(); err != nil {
								return err
							}

							b.Put(kv.Key, sqlbase.WrapDescriptor(database))
						}
					}

				default:
					return errors.Errorf("Descriptor.Union has unexpected type %T", t)
				}
			}
			if err := txn.SetSystemConfigTrigger(); err != nil {
				return err
			}
			if idVersions != nil {
				count, err := sql.CountLeases(ctx, r.sqlExecutor, idVersions, now)
				if err != nil {
					return err
				}
				if count > 0 {
					return errors.Errorf(
						`penultimate schema version is leased, upgrade again with no outstanding schema changes`,
					)
				}
			}
			return txn.Run(ctx, b)
		}); err != nil {
			return err
		}
	}
	return nil
}

func disallowPublicUserOrRole(ctx context.Context, r runner) error {
	// Check whether a user or role named "public" exists.
	const selectPublicStmt = `
          SELECT username, "isRole" from system.users WHERE username = $1
          `

	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		row, err := r.sqlExecutor.QueryRow(
			ctx, "disallowPublicUserOrRole", nil /* txn */, selectPublicStmt, sqlbase.PublicRole,
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
			_, err = r.sqlExecutor.Exec(ctx, "create-default-db", nil /* txn */, stmt)
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
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, keys.JobsTableID)
		if err != nil {
			return err
		}
		if _, err := desc.FindActiveColumnByName("progress"); err == nil {
			return nil
		}
		desc.AddColumn(sqlbase.ColumnDescriptor{
			Name:     "progress",
			Type:     sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES},
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
