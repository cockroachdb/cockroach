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
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	noopMigration1 = migrationDescriptor{
		name:   "noop 1",
		workFn: func(_ context.Context, _ runner) error { return nil },
	}
	noopMigration2 = migrationDescriptor{
		name:   "noop 2",
		workFn: func(_ context.Context, _ runner) error { return nil },
	}
	addOneDescriptorMigration = migrationDescriptor{
		name:           "add one descriptor",
		workFn:         func(_ context.Context, _ runner) error { return nil },
		newDescriptors: 1,
		newRanges:      1,
	}
	addTwoDescriptorsMigration = migrationDescriptor{
		name:           "add two descriptors",
		workFn:         func(_ context.Context, _ runner) error { return nil },
		newDescriptors: 2,
		newRanges:      2,
	}
	addRangelessDescMigration = migrationDescriptor{
		name:           "add rangeless descriptor",
		workFn:         func(_ context.Context, _ runner) error { return nil },
		newDescriptors: 2,
		newRanges:      1,
	}
	errorMigration = migrationDescriptor{
		name:   "error",
		workFn: func(_ context.Context, _ runner) error { return errors.New("errorMigration") },
	}
	panicMigration = migrationDescriptor{
		name:   "panic",
		workFn: func(_ context.Context, _ runner) error { panic("panicMigration") },
	}
)

type fakeLeaseManager struct {
	extendErr          error
	releaseErr         error
	leaseTimeRemaining time.Duration
}

func (f *fakeLeaseManager) AcquireLease(
	ctx context.Context, key roachpb.Key,
) (*client.Lease, error) {
	return &client.Lease{}, nil
}

func (f *fakeLeaseManager) ExtendLease(ctx context.Context, l *client.Lease) error {
	return f.extendErr
}

func (f *fakeLeaseManager) ReleaseLease(ctx context.Context, l *client.Lease) error {
	return f.releaseErr
}

func (f *fakeLeaseManager) TimeRemaining(l *client.Lease) time.Duration {
	// Default to a reasonable amount of time left if the field wasn't set.
	if f.leaseTimeRemaining == 0 {
		return leaseRefreshInterval * 2
	}
	return f.leaseTimeRemaining
}

type fakeDB struct {
	kvs     map[string][]byte
	scanErr error
	putErr  error
}

func (f *fakeDB) Scan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]client.KeyValue, error) {
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	if !bytes.Equal(begin.(roachpb.Key), keys.MigrationPrefix) {
		return nil, errors.Errorf("expected begin key %q, got %q", keys.MigrationPrefix, begin)
	}
	if !bytes.Equal(end.(roachpb.Key), keys.MigrationKeyMax) {
		return nil, errors.Errorf("expected end key %q, got %q", keys.MigrationKeyMax, end)
	}
	var results []client.KeyValue
	for k, v := range f.kvs {
		results = append(results, client.KeyValue{
			Key:   []byte(k),
			Value: &roachpb.Value{RawBytes: v},
		})
	}
	return results, nil
}

func (f *fakeDB) Put(ctx context.Context, key, value interface{}) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.kvs[string(key.(roachpb.Key))] = []byte(value.(string))
	return nil
}

func (f *fakeDB) Txn(context.Context, func(context.Context, *client.Txn) error) error {
	return errors.New("unimplemented")
}

func TestEnsureMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
	}
	defer mgr.stopper.Stop(context.TODO())

	fnGotCalled := false
	fnGotCalledDescriptor := migrationDescriptor{
		name: "got-called-verifier",
		workFn: func(context.Context, runner) error {
			fnGotCalled = true
			return nil
		},
	}
	testCases := []struct {
		preCompleted                                    []migrationDescriptor
		migrations                                      []migrationDescriptor
		expectedErr                                     string
		expectedPreDescriptors, expectedPostDescriptors int
		expectedPreRanges, expectedPostRanges           int
	}{
		{
			nil,
			nil,
			"",
			0, 0, 0, 0,
		},
		{
			nil,
			[]migrationDescriptor{noopMigration1},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{},
			[]migrationDescriptor{noopMigration1, noopMigration2},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1, noopMigration2},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2, panicMigration},
			[]migrationDescriptor{noopMigration1, noopMigration2, panicMigration},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2},
			[]migrationDescriptor{noopMigration1, noopMigration2, fnGotCalledDescriptor},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2},
			[]migrationDescriptor{noopMigration1, noopMigration2, errorMigration},
			fmt.Sprintf("failed to run migration %q", errorMigration.name),
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1, addOneDescriptorMigration},
			"",
			0, 1, 0, 1,
		},
		{
			[]migrationDescriptor{addOneDescriptorMigration},
			[]migrationDescriptor{addOneDescriptorMigration, addTwoDescriptorsMigration},
			"",
			1, 3, 1, 3,
		},
		{
			[]migrationDescriptor{addOneDescriptorMigration},
			[]migrationDescriptor{addOneDescriptorMigration, addRangelessDescMigration},
			"",
			1, 3, 1, 2,
		},
		{
			[]migrationDescriptor{},
			[]migrationDescriptor{noopMigration1, addOneDescriptorMigration, noopMigration2, addTwoDescriptorsMigration},
			"",
			0, 3, 0, 3,
		},
	}

	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			db.kvs = make(map[string][]byte)
			for _, name := range tc.preCompleted {
				db.kvs[string(migrationKey(name))] = []byte{}
			}
			backwardCompatibleMigrations = tc.migrations

			preDescriptors, preRanges, err := AdditionalInitialDescriptors(context.Background(), mgr.db)
			if err != nil {
				t.Fatal(err)
			}
			if preDescriptors != tc.expectedPreDescriptors {
				t.Errorf("expected %d additional initial descriptors before migration, got %d",
					tc.expectedPreDescriptors, preDescriptors)
			}
			if preRanges != tc.expectedPreRanges {
				t.Errorf("expected %d additional initial ranges before migration, got %d",
					tc.expectedPreRanges, preRanges)
			}

			err = mgr.EnsureMigrations(context.Background())
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
			}

			postDescriptors, postRanges, err := AdditionalInitialDescriptors(context.Background(), mgr.db)
			if err != nil {
				t.Fatal(err)
			}
			if postDescriptors != tc.expectedPostDescriptors {
				t.Errorf("expected %d additional initial descriptors after migration, got %d",
					tc.expectedPostDescriptors, postDescriptors)
			}
			if postRanges != tc.expectedPostRanges {
				t.Errorf("expected %d additional initial ranges after migration, got %d",
					tc.expectedPostRanges, postRanges)
			}

			for _, migration := range tc.migrations {
				if _, ok := db.kvs[string(migrationKey(migration))]; !ok {
					t.Errorf("expected key %s to be written, but it wasn't", migrationKey(migration))
				}
			}
			if len(db.kvs) != len(tc.migrations) {
				t.Errorf("expected %d key to be written, but %d were",
					len(tc.migrations), len(db.kvs))
			}
		})
	}
	if !fnGotCalled {
		t.Errorf("expected fnGotCalledDescriptor to be run by the migration coordinator, but it wasn't")
	}
}

func TestDBErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
	}
	defer mgr.stopper.Stop(context.TODO())

	migration := noopMigration1
	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)
	backwardCompatibleMigrations = []migrationDescriptor{migration}
	testCases := []struct {
		scanErr     error
		putErr      error
		expectedErr string
	}{
		{
			nil,
			nil,
			"",
		},
		{
			fmt.Errorf("context deadline exceeded"),
			nil,
			"failed to get list of completed migrations.*context deadline exceeded",
		},
		{
			nil,
			fmt.Errorf("context deadline exceeded"),
			"failed to persist record of completing migration.*context deadline exceeded",
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			db.scanErr = tc.scanErr
			db.putErr = tc.putErr
			db.kvs = make(map[string][]byte)
			err := mgr.EnsureMigrations(context.Background())
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
			}
			if _, ok := db.kvs[string(migrationKey(migration))]; !ok {
				t.Errorf("expected key %s to be written, but it wasn't", migrationKey(migration))
			}
			if len(db.kvs) != len(backwardCompatibleMigrations) {
				t.Errorf("expected %d key to be written, but %d were",
					len(backwardCompatibleMigrations), len(db.kvs))
			}
		})
	}
}

// ExtendLease and ReleaseLease errors should not, by themselves, cause the
// migration process to fail. Not being able to acquire the lease should, but
// we don't test that here due to the added code that would be needed to change
// its retry settings to allow for testing it in a reasonable amount of time.
func TestLeaseErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{kvs: make(map[string][]byte)}
	mgr := Manager{
		stopper: stop.NewStopper(),
		leaseManager: &fakeLeaseManager{
			extendErr:  fmt.Errorf("context deadline exceeded"),
			releaseErr: fmt.Errorf("context deadline exceeded"),
		},
		db: db,
	}
	defer mgr.stopper.Stop(context.TODO())

	migration := noopMigration1
	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)
	backwardCompatibleMigrations = []migrationDescriptor{migration}
	if err := mgr.EnsureMigrations(context.Background()); err != nil {
		t.Error(err)
	}
	if _, ok := db.kvs[string(migrationKey(migration))]; !ok {
		t.Errorf("expected key %s to be written, but it wasn't", migrationKey(migration))
	}
	if len(db.kvs) != len(backwardCompatibleMigrations) {
		t.Errorf("expected %d key to be written, but %d were",
			len(backwardCompatibleMigrations), len(db.kvs))
	}
}

// The lease not having enough time left on it to finish migrations should
// cause the process to exit via a call to log.Fatal.
func TestLeaseExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{kvs: make(map[string][]byte)}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{leaseTimeRemaining: time.Nanosecond},
		db:           db,
	}
	defer mgr.stopper.Stop(context.TODO())

	oldLeaseRefreshInterval := leaseRefreshInterval
	leaseRefreshInterval = time.Microsecond
	defer func() { leaseRefreshInterval = oldLeaseRefreshInterval }()

	exitCalled := make(chan bool)
	log.SetExitFunc(func(int) { exitCalled <- true })
	defer log.SetExitFunc(os.Exit)
	// Disable stack traces to make the test output in teamcity less deceiving.
	defer log.DisableTracebacks()()

	waitForExitMigration := migrationDescriptor{
		name: "wait for exit to be called",
		workFn: func(context.Context, runner) error {
			select {
			case <-exitCalled:
				return nil
			case <-time.After(10 * time.Second):
				return fmt.Errorf("timed out waiting for exit to be called")
			}
		},
	}
	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)
	backwardCompatibleMigrations = []migrationDescriptor{waitForExitMigration}
	if err := mgr.EnsureMigrations(context.Background()); err != nil {
		t.Error(err)
	}
}

func TestCreateSystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	jobsDesc := sqlbase.JobsTable
	jobsNameKey := sqlbase.MakeNameMetadataKey(jobsDesc.GetParentID(), jobsDesc.GetName())
	jobsDescKey := sqlbase.MakeDescMetadataKey(jobsDesc.GetID())
	jobsDescVal := sqlbase.WrapDescriptor(&jobsDesc)
	settingsDesc := sqlbase.SettingsTable
	settingsNameKey := sqlbase.MakeNameMetadataKey(settingsDesc.GetParentID(), settingsDesc.GetName())
	settingsDescKey := sqlbase.MakeDescMetadataKey(settingsDesc.GetID())
	settingsDescVal := sqlbase.WrapDescriptor(&settingsDesc)

	// Start up a test server without running the system.jobs migration.
	newMigrations := append([]migrationDescriptor(nil), backwardCompatibleMigrations...)
	for i := range newMigrations {
		if strings.HasPrefix(newMigrations[i].name, "create system.jobs") {
			// Disable.
			//
			// We merely replace the workFn and newDescriptors here instead
			// of completely removing the migration step, because
			// AdditionalInitialDescriptors needs to see the newRanges
			// field. This is because another migration adds the web
			// sessions table with larger ID 19. The split queue always
			// splits at every table boundary, up to the maximum present
			// table ID, even if the tables in-between are missing. So when
			// table 19 is created, table ID 15 (the jobs table) will get a
			// range, even though the table doesn't exist.
			newMigrations[i].newDescriptors = 0
			newMigrations[i].workFn = func(context.Context, runner) error { return nil }
			// Change the name so that mgr.EnsureMigrations below finds
			// something to do.
			newMigrations[i].name = "disabled"
			break
		}
	}
	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)
	backwardCompatibleMigrations = newMigrations

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Verify that the system.jobs keys were not written, but the system.settings
	// keys were. This verifies that the migration system table migrations work.
	if kv, err := kvDB.Get(ctx, jobsNameKey); err != nil {
		t.Error(err)
	} else if kv.Exists() {
		t.Errorf("expected %q not to exist, got %v", jobsNameKey, kv)
	}
	if kv, err := kvDB.Get(ctx, jobsDescKey); err != nil {
		t.Error(err)
	} else if kv.Exists() {
		t.Errorf("expected %q not to exist, got %v", jobsDescKey, kv)
	}
	if kv, err := kvDB.Get(ctx, settingsNameKey); err != nil {
		t.Error(err)
	} else if !kv.Exists() {
		t.Errorf("expected %q to exist, got that it doesn't exist", settingsNameKey)
	}
	var descriptor sqlbase.Descriptor
	if err := kvDB.GetProto(ctx, settingsDescKey, &descriptor); err != nil {
		t.Error(err)
	} else if !proto.Equal(settingsDescVal, &descriptor) {
		t.Errorf("expected %v for key %q, got %v", settingsDescVal, settingsDescKey, descriptor)
	}

	// Run the system.jobs migration outside the context of a migration manager
	// such that its work gets done but the key indicating it's been completed
	// doesn't get written.
	if err := createJobsTable(ctx, runner{db: kvDB}); err != nil {
		t.Fatal(err)
	}

	// Verify that the appropriate keys were written.
	if kv, err := kvDB.Get(ctx, jobsNameKey); err != nil {
		t.Error(err)
	} else if !kv.Exists() {
		t.Errorf("expected %q to exist, got that it doesn't exist", jobsNameKey)
	}
	if err := kvDB.GetProto(ctx, jobsDescKey, &descriptor); err != nil {
		t.Error(err)
	} else if !proto.Equal(jobsDescVal, &descriptor) {
		t.Errorf("expected %v for key %q, got %v", jobsDescVal, jobsDescKey, descriptor)
	}

	// Finally, try running both migrations and make sure they still succeed.
	// This verifies the idempotency of the migration, since the system.jobs
	// migration will get rerun here.
	mgr := NewManager(s.Stopper(), kvDB, nil, s.Clock(), nil, "clientID")
	backwardCompatibleMigrations = append(backwardCompatibleMigrations, migrationDescriptor{
		name:           "create system.jobs table",
		workFn:         createJobsTable,
		newDescriptors: 1,
		newRanges:      1,
	})
	if err := mgr.EnsureMigrations(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateViewDependenciesMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// remove the view update migration so we can test its effects.
	newMigrations := make([]migrationDescriptor, 0, len(backwardCompatibleMigrations))
	for _, m := range backwardCompatibleMigrations {
		if strings.HasPrefix(m.name, "establish conservative dependencies for views") {
			// Remove this migration.
			// This is safe because it neither adds descriptors nor ranges.
			continue
		}
		newMigrations = append(newMigrations, m)
	}

	// We also hijack the migration process to capture the SQL memory
	// metric object, needed below.
	var memMetrics *sql.MemoryMetrics
	newMigrations = append(newMigrations,
		migrationDescriptor{
			name: "capture mem metrics",
			workFn: func(ctx context.Context, r runner) error {
				memMetrics = r.memMetrics
				return nil
			},
		})

	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)
	backwardCompatibleMigrations = newMigrations

	t.Log("starting server")

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	t.Log("create test tables")

	const createStmts = `
CREATE DATABASE test;
CREATE DATABASE test2;
SET DATABASE=test;

CREATE TABLE t(x INT, y INT);
CREATE VIEW v1 AS SELECT x FROM t WHERE false;
CREATE VIEW v2 AS SELECT x FROM v1;

CREATE TABLE u(x INT, y INT);
CREATE VIEW v3 AS SELECT x FROM (SELECT x, y FROM u);

CREATE VIEW v4 AS SELECT id from system.descriptor;

CREATE TABLE w(x INT);
CREATE VIEW test2.v5 AS SELECT x FROM w;

CREATE TABLE x(x INT);
CREATE INDEX y ON x(x);
CREATE VIEW v6 AS SELECT x FROM x@y;
`
	if _, err := sqlDB.Exec(createStmts); err != nil {
		t.Fatal(err)
	}

	testDesc := []struct {
		dbName parser.Name
		tname  parser.Name
		desc   *sqlbase.TableDescriptor
	}{
		{"test", "t", nil},
		{"test", "v1", nil},
		{"test", "u", nil},
		{"test", "w", nil},
		{"test", "x", nil},
		{"system", "descriptor", nil},
	}

	t.Log("fetch descriptors")

	e := s.Executor().(*sql.Executor)
	vt := e.GetVirtualTabler()

	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		for i := range testDesc {
			desc, err := sql.MustGetTableOrViewDesc(ctx, txn, vt,
				&parser.TableName{DatabaseName: testDesc[i].dbName, TableName: testDesc[i].tname}, true)
			if err != nil {
				return err
			}
			testDesc[i].desc = desc
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Now, corrupt the descriptors by breaking their dependency information.
	t.Log("break descriptors")
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		for _, t := range testDesc {
			t.desc.UpVersion = true
			t.desc.DependedOnBy = nil
			t.desc.DependedOnBy = nil

			descKey := sqlbase.MakeDescMetadataKey(t.desc.GetID())
			descVal := sqlbase.WrapDescriptor(t.desc)
			if err := txn.Put(ctx, descKey, descVal); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Break further by deleting the referenced tables. This has become possible
	// because the dependency links have been broken above.
	t.Log("delete tables")

	if _, err := sqlDB.Exec(`
DROP TABLE test.t;
DROP VIEW test.v1;
DROP TABLE test.u;
DROP TABLE test.w;
DROP INDEX test.x@y;
`); err != nil {
		t.Fatal(err)
	}

	// Check the views are effectively broken.
	t.Log("check views are broken")

	for _, vname := range []string{"test.v2", "test.v3", "test2.v5", "test.v6"} {
		_, err := sqlDB.Exec(fmt.Sprintf(`TABLE %s`, vname))
		if !testutils.IsError(err,
			`relation ".*" does not exist|index ".*" not found|table is being dropped`) {
			t.Fatalf("%s: unexpected error: %v", vname, err)
		}
	}

	// Restore missing dependencies for the rest of the test.
	t.Log("restore dependencies")

	if _, err := sqlDB.Exec(`
CREATE TABLE test.t(x INT, y INT);
CREATE TABLE test.u(x INT, y INT);
CREATE TABLE test.w(x INT);
CREATE VIEW test.v1 AS SELECT x FROM test.t WHERE false;
CREATE INDEX y ON test.x(x);
`); err != nil {
		t.Fatal(err)
	}

	// Run the migration outside the context of a migration manager
	// such that its work gets done but the key indicating it's been completed
	// doesn't get written.
	t.Log("fix view deps manually")
	if err := repopulateViewDeps(ctx, runner{db: kvDB, sqlExecutor: e}); err != nil {
		t.Fatal(err)
	}

	// Check that the views are fixed now.
	t.Log("check views working")

	// Check the views can be queried.
	for _, vname := range []string{"test.v1", "test.v2", "test.v3", "test.v4", "test2.v5", "test.v6"} {
		if _, err := sqlDB.Exec(fmt.Sprintf("TABLE %s", vname)); err != nil {
			t.Fatal(err)
		}
	}

	// Check that the tables cannot be dropped any more.
	for _, tn := range []string{"TABLE test.t", "TABLE test.u", "TABLE test.w", "INDEX test.x@y"} {
		_, err := sqlDB.Exec(fmt.Sprintf(`DROP %s`, tn))
		if !testutils.IsError(err,
			`cannot drop (relation|index) .* because view .* depends on it`) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Finally, try running the migration and make sure it still succeeds.
	// This verifies the idempotency of the migration.
	t.Log("run migration")

	mgr := NewManager(s.Stopper(), kvDB, e, s.Clock(), memMetrics, "clientID")
	backwardCompatibleMigrations = append(backwardCompatibleMigrations, migrationDescriptor{
		name:   "repopulate view dependencies",
		workFn: repopulateViewDeps,
	})
	if err := mgr.EnsureMigrations(ctx); err != nil {
		t.Fatal(err)
	}
}
