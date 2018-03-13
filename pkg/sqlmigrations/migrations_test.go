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
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
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
		preCompleted []migrationDescriptor
		migrations   []migrationDescriptor
		expectedErr  string
	}{
		{
			nil,
			nil,
			"",
		},
		{
			nil,
			[]migrationDescriptor{noopMigration1},
			"",
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1},
			"",
		},
		{
			[]migrationDescriptor{},
			[]migrationDescriptor{noopMigration1, noopMigration2},
			"",
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1, noopMigration2},
			"",
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2, panicMigration},
			[]migrationDescriptor{noopMigration1, noopMigration2, panicMigration},
			"",
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2},
			[]migrationDescriptor{noopMigration1, noopMigration2, fnGotCalledDescriptor},
			"",
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2},
			[]migrationDescriptor{noopMigration1, noopMigration2, errorMigration},
			fmt.Sprintf("failed to run migration %q", errorMigration.name),
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

			err := mgr.EnsureMigrations(context.Background())
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
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

// migrationTest assists in testing the effects of a migration. It provides
// methods to edit the list of migrations run at server startup, start a test
// server, and run an individual migration. The test server's KV and SQL
// interfaces are intended to be accessed directly to verify the effect of the
// migration. Any mutations to the list of migrations run at server startup are
// reverted at the end of the test.
type migrationTest struct {
	oldMigrations []migrationDescriptor
	server        serverutils.TestServerInterface
	sqlDB         *sqlutils.SQLRunner
	kvDB          *client.DB
	memMetrics    *sql.MemoryMetrics
}

// makeMigrationTest creates a new migrationTest.
//
// The caller is responsible for calling the test's close method at the end of
// the test.
func makeMigrationTest(ctx context.Context, t testing.TB) migrationTest {
	t.Helper()

	oldMigrations := append([]migrationDescriptor(nil), backwardCompatibleMigrations...)
	memMetrics := sql.MakeMemMetrics("migration-test-internal", time.Minute)
	return migrationTest{
		oldMigrations: oldMigrations,
		memMetrics:    &memMetrics,
	}
}

// pop removes the migration whose name starts with namePrefix from the list of
// migrations run at server startup. It fails the test if the number of
// migrations that match namePrefix is not exactly one.
//
// You must not call pop after calling start.
func (mt *migrationTest) pop(t testing.TB, namePrefix string) migrationDescriptor {
	t.Helper()

	if mt.server != nil {
		t.Fatal("migrationTest.pop must be called before mt.start")
	}

	var migration migrationDescriptor
	var newMigrations []migrationDescriptor
	for _, m := range backwardCompatibleMigrations {
		if strings.HasPrefix(m.name, namePrefix) {
			migration = m
			continue
		}
		newMigrations = append(newMigrations, m)
	}
	if n := len(backwardCompatibleMigrations) - len(newMigrations); n != 1 {
		t.Fatalf("expected prefix %q to match exactly one migration, but matched %d", namePrefix, n)
	}
	backwardCompatibleMigrations = newMigrations
	return migration
}

// start starts a test server with the given serverArgs.
func (mt *migrationTest) start(t testing.TB, serverArgs base.TestServerArgs) {
	server, sqlDB, kvDB := serverutils.StartServer(t, serverArgs)
	mt.server = server
	mt.sqlDB = sqlutils.MakeSQLRunner(sqlDB)
	mt.kvDB = kvDB
}

// runMigration triggers a manual run of migration. It does not mark migration
// as completed, so subsequent calls will cause migration to be re-executed.
// This is useful for verifying idempotency.
//
// You must call start before calling runMigration.
func (mt *migrationTest) runMigration(ctx context.Context, m migrationDescriptor) error {
	if m.workFn == nil {
		// Migration has been baked in. Ignore it.
		return nil
	}
	return m.workFn(ctx, runner{
		db:          mt.kvDB,
		memMetrics:  mt.memMetrics,
		sqlExecutor: mt.server.Executor().(*sql.Executor),
	})
}

// close stops the test server and restores package-global state.
func (mt *migrationTest) close(ctx context.Context) {
	if mt.server != nil {
		mt.server.Stopper().Stop(ctx)
	}
	backwardCompatibleMigrations = mt.oldMigrations
}

func TestCreateSystemTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	table := sqlbase.NamespaceTable
	table.ID = keys.MaxReservedDescID

	prevPrivileges, ok := sqlbase.SystemAllowedPrivileges[table.ID]
	defer func() {
		if ok {
			// Restore value of privileges.
			sqlbase.SystemAllowedPrivileges[table.ID] = prevPrivileges
		} else {
			delete(sqlbase.SystemAllowedPrivileges, table.ID)
		}
	}()
	sqlbase.SystemAllowedPrivileges[table.ID] = sqlbase.SystemAllowedPrivileges[keys.NamespaceTableID]

	table.Name = "dummy"
	nameKey := sqlbase.MakeNameMetadataKey(table.ParentID, table.Name)
	descKey := sqlbase.MakeDescMetadataKey(table.ID)
	descVal := sqlbase.WrapDescriptor(&table)

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	mt.start(t, base.TestServerArgs{})

	// Verify that the keys were not written.
	if kv, err := mt.kvDB.Get(ctx, nameKey); err != nil {
		t.Error(err)
	} else if kv.Exists() {
		t.Errorf("expected %q not to exist, got %v", nameKey, kv)
	}
	if kv, err := mt.kvDB.Get(ctx, descKey); err != nil {
		t.Error(err)
	} else if kv.Exists() {
		t.Errorf("expected %q not to exist, got %v", descKey, kv)
	}

	migration := migrationDescriptor{
		name: "add system.dummy table",
		workFn: func(ctx context.Context, r runner) error {
			return createSystemTable(ctx, r, table)
		},
	}
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}

	// Verify that the appropriate keys were written.
	if kv, err := mt.kvDB.Get(ctx, nameKey); err != nil {
		t.Error(err)
	} else if !kv.Exists() {
		t.Errorf("expected %q to exist, got that it doesn't exist", nameKey)
	}
	var descriptor sqlbase.Descriptor
	if err := mt.kvDB.GetProto(ctx, descKey, &descriptor); err != nil {
		t.Error(err)
	} else if !proto.Equal(descVal, &descriptor) {
		t.Errorf("expected %v for key %q, got %v", descVal, descKey, descriptor)
	}

	// Verify the idempotency of the migration.
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
}

func checkNoZoneConfig(t *testing.T, sqlDB *sqlutils.SQLRunner, id int) {
	t.Helper()

	var s string
	err := sqlDB.DB.QueryRow(`SELECT config FROM system.zones WHERE id = $1`, id).Scan(&s)
	if err != gosql.ErrNoRows {
		t.Fatalf("expected no rows, but found %v", err)
	}
}

func checkZoneConfig(t *testing.T, sqlDB *sqlutils.SQLRunner, id int, expected config.ZoneConfig) {
	t.Helper()

	var s string
	sqlDB.QueryRow(t, `SELECT config FROM system.zones WHERE id = $1`, id).Scan(&s)
	var zone config.ZoneConfig
	if err := protoutil.Unmarshal([]byte(s), &zone); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, zone) {
		t.Fatalf("unexpected zone config: %s", pretty.Diff(expected, zone))
	}
}

func deleteZoneConfig(t *testing.T, sqlDB *sqlutils.SQLRunner, id int) {
	sqlDB.Exec(t, `DELETE FROM system.zones WHERE id=$1`, id)
}

func setZoneConfig(t *testing.T, sqlDB *sqlutils.SQLRunner, id int, zone config.ZoneConfig) {
	buf, err := protoutil.Marshal(&zone)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `UPSERT INTO system.zones (id, config) VALUES ($1, $2)`, id, buf)
}

func TestAddDefaultMetaZoneConfigMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "add default .meta and .liveness zone configs")
	mt.start(t, base.TestServerArgs{})

	checkNoZoneConfig(t, mt.sqlDB, keys.MetaRangesID)
	checkNoZoneConfig(t, mt.sqlDB, keys.LivenessRangesID)

	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	expectedMeta := config.DefaultZoneConfig()
	expectedMeta.GC.TTLSeconds = 60 * 60
	checkZoneConfig(t, mt.sqlDB, keys.MetaRangesID, expectedMeta)
	expectedLiveness := config.DefaultZoneConfig()
	expectedLiveness.GC.TTLSeconds = 10 * 60
	checkZoneConfig(t, mt.sqlDB, keys.LivenessRangesID, expectedLiveness)

	// Set configs for the .meta and .system zones and clear the zone config for
	// .liveness.
	testZone := config.DefaultZoneConfig()
	testZone.GC.TTLSeconds = 819
	setZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
	setZoneConfig(t, mt.sqlDB, keys.SystemRangesID, testZone)
	deleteZoneConfig(t, mt.sqlDB, keys.LivenessRangesID)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	checkZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
	checkZoneConfig(t, mt.sqlDB, keys.LivenessRangesID, testZone)
	checkZoneConfig(t, mt.sqlDB, keys.SystemRangesID, testZone)

	// Set configs for the .meta and .liveness zones and clear the zone config
	// for .system.
	testZone.GC.TTLSeconds = 834
	setZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
	setZoneConfig(t, mt.sqlDB, keys.SystemRangesID, testZone)
	deleteZoneConfig(t, mt.sqlDB, keys.LivenessRangesID)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	checkZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
	checkZoneConfig(t, mt.sqlDB, keys.LivenessRangesID, testZone)
	// NB: The .system zone config still doesn't exist.

	// Verify that we'll update the meta/liveness zone config TTLs by migrating
	// from the default/system zone configs.
	testZone.RangeMaxBytes *= 2
	testZone.GC.TTLSeconds = config.DefaultZoneConfig().GC.TTLSeconds
	setZoneConfig(t, mt.sqlDB, keys.RootNamespaceID, testZone)
	setZoneConfig(t, mt.sqlDB, keys.SystemRangesID, testZone)
	deleteZoneConfig(t, mt.sqlDB, keys.MetaRangesID)
	deleteZoneConfig(t, mt.sqlDB, keys.LivenessRangesID)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	testZone.GC.TTLSeconds = 60 * 60
	checkZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
	testZone.GC.TTLSeconds = 10 * 60
	checkZoneConfig(t, mt.sqlDB, keys.LivenessRangesID, testZone)

	// Verify that we'll update the meta zone config even if it already exists as
	// long as it has the default TTL.
	testZone.RangeMaxBytes = 863
	testZone.GC.TTLSeconds = config.DefaultZoneConfig().GC.TTLSeconds
	setZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	testZone.GC.TTLSeconds = 60 * 60
	checkZoneConfig(t, mt.sqlDB, keys.MetaRangesID, testZone)
}

func TestAddDefaultSystemJobsZoneConfigMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "add default system.jobs zone config")
	mt.start(t, base.TestServerArgs{})

	checkNoZoneConfig(t, mt.sqlDB, keys.JobsTableID)

	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	expected := config.DefaultZoneConfig()
	expected.GC.TTLSeconds = 10 * 60
	checkZoneConfig(t, mt.sqlDB, keys.JobsTableID, expected)

	// Verify we migrate from the .default zone config.
	testZone := config.DefaultZoneConfig()
	testZone.RangeMaxBytes = 264
	setZoneConfig(t, mt.sqlDB, keys.RootNamespaceID, testZone)
	deleteZoneConfig(t, mt.sqlDB, keys.JobsTableID)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	testZone.GC.TTLSeconds = 10 * 60
	checkZoneConfig(t, mt.sqlDB, keys.JobsTableID, testZone)

	// Verify that we'll update even if it already exists as long as it has the
	// default TTL.
	testZone.RangeMaxBytes = 863
	testZone.GC.TTLSeconds = config.DefaultZoneConfig().GC.TTLSeconds
	setZoneConfig(t, mt.sqlDB, keys.JobsTableID, testZone)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	testZone.GC.TTLSeconds = 10 * 60
	checkZoneConfig(t, mt.sqlDB, keys.JobsTableID, testZone)
}

func TestAdminUserExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "add system.users isRole column and create admin role")
	mt.start(t, base.TestServerArgs{})

	// Create a user named "admin". We have to do a manual insert as "CREATE USER"
	// knows about "isRole", but the migration hasn't run yet.
	mt.sqlDB.Exec(t, `INSERT INTO system.users (username, "hashedPassword") VALUES ($1, '')`,
		sqlbase.AdminRole)

	e := `cannot create role "admin", a user with that name exists.`
	if err := mt.runMigration(ctx, migration); !testutils.IsError(err, e) {
		t.Errorf("expected error %q, got %q", err, e)
	}
}

func TestReplayMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	mt.start(t, base.TestServerArgs{})

	// Test all migrations again. Starting the server did the first round.
	for _, m := range backwardCompatibleMigrations {
		if err := mt.runMigration(ctx, m); err != nil {
			t.Error(err)
		}
	}
}

func TestUpgradeTableDescsToInterleavedFormatVersionMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "upgrade table descs to interleaved format version")
	mt.start(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				// Block schema changes to ensure that our migration sees some table
				// descriptors in the DROP state. The schema changer might otherwise
				// erase them before our migration runs.
				AsyncExecNotification: func() error { return errors.New("schema changes disabled") },
			},
		},
	})

	defer func(prev int64) { upgradeDescBatchSize = prev }(upgradeDescBatchSize)
	upgradeDescBatchSize = 5
	n := int(upgradeDescBatchSize) * 3

	// Create n tables.
	mt.sqlDB.Exec(t, `CREATE DATABASE db`)
	for i := 0; i < n; i++ {
		mt.sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE db.t%d ()`, i))
	}

	// Corrupt every second table's format version, and mark every third table as
	// dropping.
	for i := 0; i < n; i++ {
		tableDesc := sqlbase.GetTableDescriptor(mt.kvDB, "db", fmt.Sprintf("t%d", i))
		if i%2 == 0 {
			tableDesc.FormatVersion = sqlbase.FamilyFormatVersion
		}
		if i%3 == 0 {
			tableDesc.State = sqlbase.TableDescriptor_DROP
		}
		tableKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
		if err := mt.kvDB.Put(ctx, tableKey, sqlbase.WrapDescriptor(tableDesc)); err != nil {
			t.Fatal(err)
		}
	}
	// Ensure the migration upgrades the format of all tables, even those that
	// are dropping.
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		tableDesc := sqlbase.GetTableDescriptor(mt.kvDB, "db", fmt.Sprintf("t%d", i))
		if e, a := sqlbase.InterleavedFormatVersion, tableDesc.FormatVersion; e != a {
			t.Errorf("t%d: expected format version %s, but got %s", i, e, a)
		}
	}

	// Verify idempotency.
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Fatal(err)
	}
}

func TestExpectedInitialRangeCount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	testutils.SucceedsSoon(t, func() error {
		lastMigration := backwardCompatibleMigrations[len(backwardCompatibleMigrations)-1]
		if _, err := kvDB.Get(ctx, migrationKey(lastMigration)); err != nil {
			return errors.New("last migration has not completed")
		}

		sysCfg, ok := s.Gossip().GetSystemConfig()
		if !ok {
			return errors.New("gossipped system config not available")
		}

		rows, err := sqlDB.Query(`SELECT range_id, start_key, end_key FROM crdb_internal.ranges`)
		if err != nil {
			return err
		}
		defer rows.Close()
		nranges := 0
		for rows.Next() {
			var rangeID int
			var startKey, endKey []byte
			if err := rows.Scan(&rangeID, &startKey, &endKey); err != nil {
				return err
			}
			if sysCfg.NeedsSplit(startKey, endKey) {
				return fmt.Errorf("range %d needs split", rangeID)
			}
			nranges++
		}
		if rows.Err() != nil {
			return err
		}

		expectedRanges, err := s.ExpectedInitialRangeCount()
		if err != nil {
			return err
		}
		if expectedRanges != nranges {
			return fmt.Errorf("expected %d ranges but got %d", expectedRanges, nranges)
		}

		return nil
	})
}
