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

package migrations

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

var (
	leaseDuration        = time.Minute
	leaseRefreshInterval = leaseDuration / 5
)

// backwardCompatibleMigrations is a hard-coded list of migrations to be run on
// startup. They will always be run from top-to-bottom, and because they are
// assumed to be backward-compatible, they will be run regardless of what other
// node versions are currently running within the cluster.
var backwardCompatibleMigrations = []migrationDescriptor{
	{
		name:   "default UniqueID to uuid_v4 in system.eventlog",
		workFn: eventlogUniqueIDDefault,
	},
	{
		name:           "create system.jobs table",
		workFn:         createJobsTable,
		newDescriptors: 1,
		newRanges:      1,
	},
	{
		name:           "create system.settings table",
		workFn:         createSettingsTable,
		newDescriptors: 1,
		newRanges:      0, // it lives in gossip range.
	},
	{
		name:   "enable diagnostics reporting",
		workFn: optIntToDiagnosticsStatReporting,
	},
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
	// newRanges and descriptors are the number of additional ranges/descriptors
	// that would be added by this migration in a fresh cluster. This is needed to
	// automate certain tests, which check the number of ranges/descriptors
	// present on server bootup.
	newRanges, newDescriptors int
}

type runner struct {
	db          db
	sqlExecutor *sql.Executor
	memMetrics  *sql.MemoryMetrics
}

func (r *runner) newRootSession(ctx context.Context) *sql.Session {
	args := sql.SessionArgs{User: security.NodeUser}
	s := sql.NewSession(ctx, args, r.sqlExecutor, nil, r.memMetrics)
	s.StartUnlimitedMonitor()
	return s
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
	Put(ctx context.Context, key, value interface{}) error
	Txn(ctx context.Context, retryable func(ctx context.Context, txn *client.Txn) error) error
}

// Manager encapsulates the necessary functionality for handling migrations
// of data in the cluster.
type Manager struct {
	stopper      *stop.Stopper
	leaseManager leaseManager
	db           db
	sqlExecutor  *sql.Executor
	memMetrics   *sql.MemoryMetrics
}

// NewManager initializes and returns a new Manager object.
func NewManager(
	stopper *stop.Stopper,
	db *client.DB,
	executor *sql.Executor,
	clock *hlc.Clock,
	memMetrics *sql.MemoryMetrics,
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
		memMetrics:   memMetrics,
	}
}

// AdditionalInitialDescriptors returns the number of system descriptors and
// ranges that have been added by migrations. This is needed for certain tests,
// which check the number of ranges at node startup.
//
// NOTE: This value may be out-of-date if another node is actively running
// migrations, and so should only be used in test code where the migration
// lifecycle is tightly controlled.
func AdditionalInitialDescriptors(
	ctx context.Context, db db,
) (descriptors int, ranges int, _ error) {
	completedMigrations, err := getCompletedMigrations(ctx, db)
	if err != nil {
		return 0, 0, err
	}
	for _, migration := range backwardCompatibleMigrations {
		key := migrationKey(migration)
		if _, ok := completedMigrations[string(key)]; ok {
			descriptors += migration.newDescriptors
			ranges += migration.newRanges
		}
	}
	return descriptors, ranges, nil
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
		log.Errorf(ctx, "failed attempt to acquire migration lease: %s", err)
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
	if err := m.stopper.RunAsyncTask(ctx, func(ctx context.Context) {
		select {
		case <-done:
			return
		case <-time.After(leaseRefreshInterval):
			if err := m.leaseManager.ExtendLease(ctx, lease); err != nil {
				log.Warningf(ctx, "unable to extend ownership of expiration lease: %s", err)
			}
			if m.leaseManager.TimeRemaining(lease) < leaseRefreshInterval {
				// Note that we may be able to do better than this by influencing the
				// deadline of migrations' transactions based on the least expiration
				// time, but simply kill the process for now for the sake of simplicity.
				log.Fatal(ctx, "not enough time left on migration lease, terminating for safety")
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
		memMetrics:  m.memMetrics,
	}
	for _, migration := range backwardCompatibleMigrations {
		key := migrationKey(migration)
		if _, ok := completedMigrations[string(key)]; ok {
			continue
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

func checkQueryResults(results []sql.Result, numResults int) error {
	for _, result := range results {
		if result.Err != nil {
			return result.Err
		}
	}
	if a, e := len(results), numResults; a != e {
		return errors.Errorf("number of results %d != expected %d", a, e)
	}
	return nil
}

func eventlogUniqueIDDefault(ctx context.Context, r runner) error {
	const alterStmt = "ALTER TABLE system.eventlog ALTER COLUMN uniqueID SET DEFAULT uuid_v4();"

	// System tables can only be modified by a privileged internal user.
	session := r.newRootSession(ctx)
	defer session.Finish(r.sqlExecutor)

	// Retry a limited number of times because returning an error and letting
	// the node kill itself is better than holding the migration lease for an
	// arbitrarily long time.
	var err error
	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		res := r.sqlExecutor.ExecuteStatements(session, alterStmt, nil)
		err = checkQueryResults(res.ResultList, 1)
		if err == nil {
			break
		}
		log.Warningf(ctx, "failed attempt to update system.eventlog schema: %s", err)
	}
	return err
}

// TODO(a-robinson): Write unit test for this.
func createJobsTable(ctx context.Context, r runner) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	return r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		desc := sqlbase.JobsTable
		b.CPut(sqlbase.MakeNameMetadataKey(desc.GetParentID(), desc.GetName()), desc.GetID(), nil)
		b.CPut(sqlbase.MakeDescMetadataKey(desc.GetID()), sqlbase.WrapDescriptor(&desc), nil)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
}

// TODO(a-robinson): Write unit test for this.
func createSettingsTable(ctx context.Context, r runner) error {
	// We install the table at the KV layer so that we can choose a known ID in
	// the reserved ID space. (The SQL layer doesn't allow this.)
	return r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		desc := sqlbase.SettingsTable
		b.CPut(sqlbase.MakeNameMetadataKey(desc.GetParentID(), desc.GetName()), desc.GetID(), nil)
		b.CPut(sqlbase.MakeDescMetadataKey(desc.GetID()), sqlbase.WrapDescriptor(&desc), nil)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
}

var reportingOptOut = envutil.EnvOrDefaultBool("COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING", false)

func optIntToDiagnosticsStatReporting(ctx context.Context, r runner) error {
	const setStmt = "SET CLUSTER SETTING diagnostics.reporting.enabled = true"

	// We're opting-out of the automatic opt-in. See discussion in updates.go.
	if reportingOptOut {
		return nil
	}

	// System tables can only be modified by a privileged internal user.
	session := r.newRootSession(ctx)
	defer session.Finish(r.sqlExecutor)
	// Retry a limited number of times because returning an error and letting
	// the node kill itself is better than holding the migration lease for an
	// arbitrarily long time.
	var err error
	for retry := retry.Start(retry.Options{MaxRetries: 5}); retry.Next(); {
		res := r.sqlExecutor.ExecuteStatements(session, setStmt, nil)
		err = checkQueryResults(res.ResultList, 1)
		if err == nil {
			break
		}
		log.Warningf(ctx, "failed attempt to update setting: %s", err)
	}
	return err
}
