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
	"bytes"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
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
		name:   "example migration",
		workFn: exampleNoopMigration,
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
}

type runner struct {
	db          db
	sqlExecutor *sql.Executor
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
}

// Manager encapsulates the necessary functionality for handling migrations
// of data in the cluster.
type Manager struct {
	stopper      *stop.Stopper
	leaseManager leaseManager
	db           db
	sqlExecutor  *sql.Executor
}

// NewManager initializes and returns a new Manager object.
func NewManager(
	stopper *stop.Stopper, db *client.DB, executor *sql.Executor, clock *hlc.Clock, clientID string,
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
	}
}

// InitNewCluster should be run while the first node of a new cluster is being
// initialized in order to mark all known migrations as complete.
func (m *Manager) InitNewCluster(ctx context.Context) error {
	return m.migrations(ctx, false /* runWorkFns */)
}

// EnsureMigrations should be run during node startup to ensure that all
// required migrations have been run (and running all those that are definitely
// safe to run).
func (m *Manager) EnsureMigrations(ctx context.Context) error {
	return m.migrations(ctx, true /* runWorkFns */)
}

func (m *Manager) migrations(ctx context.Context, runWorkFns bool) error {
	// Before doing anything, grab the migration lease to ensure that only one
	// node is ever doing migrations at a time.
	var lease *client.Lease
	var err error
	retryOpts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     10,
	}
	if log.V(1) {
		log.Info(ctx, "trying to acquire lease")
	}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
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
				log.Fatal(ctx, "not enough time left on migration lease, terminating for safety")
			}
		}
	}); err != nil {
		return err
	}

	// Get the set of all completed migrations at once to avoid having to look
	// them up one-by-one, which could potentially add a noticeable delay to
	// process startup if we eventually have a lot of migrations to check for.
	var keyvals []client.KeyValue
	if log.V(1) {
		log.Info(ctx, "trying to get the list of completed migrations")
	}
	for r := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 3}); r.Next(); {
		keyvals, err = m.db.Scan(ctx, keys.MigrationPrefix, keys.MigrationKeyMax, 10000 /* maxRows */)
		if err == nil {
			break
		}
		log.Errorf(ctx, "failed attempt to get list of completed migrations: %s", err)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to get list of completed migrations")
	}
	completedMigrations := make(map[string]bool)
	for _, keyval := range keyvals {
		completedMigrations[string(keyval.Key)] = true
	}

	startTime := timeutil.Now().String()
	r := runner{
		db:          m.db,
		sqlExecutor: m.sqlExecutor,
	}
	for _, migration := range backwardCompatibleMigrations {
		key := migrationKey(migration)
		if completedMigrations[string(key)] {
			continue
		}

		if runWorkFns {
			if log.V(1) {
				log.Infof(ctx, "running migration %q", migration.name)
			}
			if err := migration.workFn(ctx, r); err != nil {
				return errors.Wrapf(err, "failed to run migration %q", migration.name)
			}
		}

		if log.V(1) {
			log.Infof(ctx, "trying to persist record of completing migration %s", migration.name)
		}
		var err error
		for r := retry.StartWithCtx(ctx, retry.Options{MaxRetries: 5}); r.Next(); {
			err = m.db.Put(ctx, key, startTime)
			if err == nil {
				log.Infof(ctx, "successfully completed migration %q", migration.name)
				break
			}
			log.Errorf(ctx, "failed attempt to persist record of completing migration %q", migration.name)
		}
		if err != nil {
			return errors.Wrapf(err, "failed to persist record of completing migration %q",
				migration.name)
		}
	}

	return nil
}

func migrationKey(migration migrationDescriptor) roachpb.Key {
	return bytes.Join([][]byte{keys.MigrationPrefix, roachpb.RKey(migration.name)}, nil)
}

func exampleNoopMigration(ctx context.Context, r runner) error {
	return nil
}
