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
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
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
) (*leasemanager.Lease, error) {
	return &leasemanager.Lease{}, nil
}

func (f *fakeLeaseManager) ExtendLease(ctx context.Context, l *leasemanager.Lease) error {
	return f.extendErr
}

func (f *fakeLeaseManager) ReleaseLease(ctx context.Context, l *leasemanager.Lease) error {
	return f.releaseErr
}

func (f *fakeLeaseManager) TimeRemaining(l *leasemanager.Lease) time.Duration {
	// Default to a reasonable amount of time left if the field wasn't set.
	if f.leaseTimeRemaining == 0 {
		return leaseRefreshInterval * 2
	}
	return f.leaseTimeRemaining
}

type fakeDB struct {
	codec   keys.SQLCodec
	kvs     map[string][]byte
	scanErr error
	putErr  error
}

func (f *fakeDB) Scan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]kv.KeyValue, error) {
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	min := f.codec.MigrationKeyPrefix()
	max := min.PrefixEnd()
	if !bytes.Equal(begin.(roachpb.Key), min) {
		return nil, errors.Errorf("expected begin key %q, got %q", min, begin)
	}
	if !bytes.Equal(end.(roachpb.Key), max) {
		return nil, errors.Errorf("expected end key %q, got %q", max, end)
	}
	var results []kv.KeyValue
	for k, v := range f.kvs {
		results = append(results, kv.KeyValue{
			Key:   []byte(k),
			Value: &roachpb.Value{RawBytes: v},
		})
	}
	return results, nil
}

func (f *fakeDB) Get(ctx context.Context, key interface{}) (kv.KeyValue, error) {
	return kv.KeyValue{}, errors.New("unimplemented")
}

func (f *fakeDB) Put(ctx context.Context, key, value interface{}) error {
	if f.putErr != nil {
		return f.putErr
	}
	if f.kvs != nil {
		f.kvs[string(key.(roachpb.Key))] = []byte(value.(string))
	}
	return nil
}

func (f *fakeDB) Txn(context.Context, func(context.Context, *kv.Txn) error) error {
	return errors.New("unimplemented")
}

func TestEnsureMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	codec := keys.SystemSQLCodec
	db := &fakeDB{codec: codec}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
		codec:        codec,
	}
	defer mgr.stopper.Stop(context.Background())

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
				db.kvs[string(migrationKey(codec, name))] = []byte{}
			}
			backwardCompatibleMigrations = tc.migrations

			err := mgr.EnsureMigrations(context.Background(), roachpb.Version{} /* bootstrapVersion */)
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
			}

			for _, migration := range tc.migrations {
				if _, ok := db.kvs[string(migrationKey(codec, migration))]; !ok {
					t.Errorf("expected key %s to be written, but it wasn't", migrationKey(codec, migration))
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

func TestSkipMigrationsIncludedInBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	codec := keys.SystemSQLCodec
	db := &fakeDB{codec: codec}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
		codec:        codec,
	}
	defer mgr.stopper.Stop(ctx)
	defer func(prev []migrationDescriptor) {
		backwardCompatibleMigrations = prev
	}(backwardCompatibleMigrations)

	v := roachpb.MustParseVersion("19.1")
	fnGotCalled := false
	backwardCompatibleMigrations = []migrationDescriptor{{
		name:                "got-called-verifier",
		includedInBootstrap: v,
		workFn: func(context.Context, runner) error {
			fnGotCalled = true
			return nil
		},
	}}
	// If the cluster has been bootstrapped at an old version, the migration should run.
	require.NoError(t, mgr.EnsureMigrations(ctx, roachpb.Version{} /* bootstrapVersion */))
	require.True(t, fnGotCalled)
	fnGotCalled = false
	// If the cluster has been bootstrapped at a new version, the migration should
	// not run.
	require.NoError(t, mgr.EnsureMigrations(ctx, v /* bootstrapVersion */))
	require.False(t, fnGotCalled)
}

func TestClusterWideMigrationOnlyRunBySystemTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "system tenant", func(t *testing.T, systemTenant bool) {
		var codec keys.SQLCodec
		if systemTenant {
			codec = keys.SystemSQLCodec
		} else {
			codec = keys.MakeSQLCodec(roachpb.MakeTenantID(5))
		}

		ctx := context.Background()
		db := &fakeDB{codec: codec}
		mgr := Manager{
			stopper:      stop.NewStopper(),
			leaseManager: &fakeLeaseManager{},
			db:           db,
			codec:        codec,
		}
		defer mgr.stopper.Stop(ctx)
		defer func(prev []migrationDescriptor) {
			backwardCompatibleMigrations = prev
		}(backwardCompatibleMigrations)

		fnGotCalled := false
		backwardCompatibleMigrations = []migrationDescriptor{{
			name:        "got-called-verifier",
			clusterWide: true,
			workFn: func(context.Context, runner) error {
				fnGotCalled = true
				return nil
			},
		}}
		// The migration should only be run by the system tenant.
		require.NoError(t, mgr.EnsureMigrations(ctx, roachpb.Version{} /* bootstrapVersion */))
		require.Equal(t, systemTenant, fnGotCalled)
	})
}

func TestDBErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	codec := keys.SystemSQLCodec
	db := &fakeDB{codec: codec}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
		codec:        codec,
	}
	defer mgr.stopper.Stop(context.Background())

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
			err := mgr.EnsureMigrations(context.Background(), roachpb.Version{} /* bootstrapVersion */)
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
			}
			if _, ok := db.kvs[string(migrationKey(codec, migration))]; !ok {
				t.Errorf("expected key %s to be written, but it wasn't", migrationKey(codec, migration))
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
	codec := keys.SystemSQLCodec
	db := &fakeDB{codec: codec, kvs: make(map[string][]byte)}
	mgr := Manager{
		stopper: stop.NewStopper(),
		leaseManager: &fakeLeaseManager{
			extendErr:  fmt.Errorf("context deadline exceeded"),
			releaseErr: fmt.Errorf("context deadline exceeded"),
		},
		db:    db,
		codec: codec,
	}
	defer mgr.stopper.Stop(context.Background())

	migration := noopMigration1
	defer func(prev []migrationDescriptor) { backwardCompatibleMigrations = prev }(backwardCompatibleMigrations)
	backwardCompatibleMigrations = []migrationDescriptor{migration}
	if err := mgr.EnsureMigrations(context.Background(), roachpb.Version{} /* bootstrapVersion */); err != nil {
		t.Error(err)
	}
	if _, ok := db.kvs[string(migrationKey(codec, migration))]; !ok {
		t.Errorf("expected key %s to be written, but it wasn't", migrationKey(codec, migration))
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
	codec := keys.SystemSQLCodec
	db := &fakeDB{codec: codec, kvs: make(map[string][]byte)}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{leaseTimeRemaining: time.Nanosecond},
		db:           db,
		codec:        codec,
	}
	defer mgr.stopper.Stop(context.Background())

	oldLeaseRefreshInterval := leaseRefreshInterval
	leaseRefreshInterval = time.Microsecond
	defer func() { leaseRefreshInterval = oldLeaseRefreshInterval }()

	exitCalled := make(chan bool)
	log.SetExitFunc(true /* hideStack */, func(int) { exitCalled <- true })
	defer log.ResetExitFunc()
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
	if err := mgr.EnsureMigrations(context.Background(), roachpb.Version{} /* bootstrapVersion */); err != nil {
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
	kvDB          *kv.DB
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
		settings:    mt.server.ClusterSettings(),
		codec:       keys.SystemSQLCodec,
		db:          mt.kvDB,
		sqlExecutor: mt.server.InternalExecutor().(*sql.InternalExecutor),
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

	table := sqlbase.NewMutableExistingTableDescriptor(sqlbase.NamespaceTable.TableDescriptor)
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
	nameKey := sqlbase.NewPublicTableKey(table.ParentID, table.Name).Key(keys.SystemSQLCodec)
	descKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, table.ID)
	descVal := table.DescriptorProto()

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

	// The revised migration in v2.1 upserts the admin user, so this should succeed.
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Errorf("expected success, got %q", err)
	}
}

func TestPublicRoleExists(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "disallow public user or role name")
	mt.start(t, base.TestServerArgs{})

	// Create a user (we check for user or role) named "public".
	// We have to do a manual insert as "CREATE USER" knows to disallow "public".
	mt.sqlDB.Exec(t, `INSERT INTO system.users (username, "hashedPassword", "isRole") VALUES ($1, '', false)`,
		sqlbase.PublicRole)

	e := `found a user named public which is now a reserved name.`
	// The revised migration in v2.1 upserts the admin user, so this should succeed.
	if err := mt.runMigration(ctx, migration); !testutils.IsError(err, e) {
		t.Errorf("expected error %q got %q", e, err)
	}

	// Now try with a role instead of a user.
	mt.sqlDB.Exec(t, `DELETE FROM system.users WHERE username = $1`, sqlbase.PublicRole)
	mt.sqlDB.Exec(t, `INSERT INTO system.users (username, "hashedPassword", "isRole") VALUES ($1, '', true)`,
		sqlbase.PublicRole)

	e = `found a role named public which is now a reserved name.`
	// The revised migration in v2.1 upserts the admin user, so this should succeed.
	if err := mt.runMigration(ctx, migration); !testutils.IsError(err, e) {
		t.Errorf("expected error %q got %q", e, err)
	}

	// Drop it and try again.
	mt.sqlDB.Exec(t, `DELETE FROM system.users WHERE username = $1`, sqlbase.PublicRole)
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Errorf("expected success, got %q", err)
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

func TestExpectedInitialRangeCount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	testutils.SucceedsSoon(t, func() error {
		lastMigration := backwardCompatibleMigrations[len(backwardCompatibleMigrations)-1]
		if _, err := kvDB.Get(ctx, migrationKey(keys.SystemSQLCodec, lastMigration)); err != nil {
			return errors.New("last migration has not completed")
		}

		sysCfg := s.GossipI().(*gossip.Gossip).GetSystemConfig()
		if sysCfg == nil {
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
			if sysCfg.NeedsSplit(ctx, startKey, endKey) {
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

func TestUpdateSystemLocationData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "update system.locations with default location data")
	mt.start(t, base.TestServerArgs{})

	// Check that we don't have any data in the system.locations table without the migration.
	var count int
	mt.sqlDB.QueryRow(t, `SELECT count(*) FROM system.locations`).Scan(&count)
	if count != 0 {
		t.Fatalf("Exected to find 0 rows in system.locations. Found  %d instead", count)
	}

	// Run the migration to insert locations.
	if err := mt.runMigration(ctx, migration); err != nil {
		t.Errorf("expected success, got %q", err)
	}

	// Check that we have all of the expected locations.
	mt.sqlDB.QueryRow(t, `SELECT count(*) FROM system.locations`).Scan(&count)
	if count != len(roachpb.DefaultLocationInformation) {
		t.Fatalf("Exected to find 0 rows in system.locations. Found  %d instead", count)
	}
}

func TestMigrateNamespaceTableDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "create new system.namespace table")
	mt.start(t, base.TestServerArgs{})

	// Since we're already on 20.1, mimic the beginning state by deleting the
	// new namespace descriptor.
	key := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, keys.NamespaceTableID)
	require.NoError(t, mt.kvDB.Del(ctx, key))

	deprecatedKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, keys.DeprecatedNamespaceTableID)
	desc := &sqlbase.Descriptor{}
	require.NoError(t, mt.kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := txn.GetProtoTs(ctx, deprecatedKey, desc)
		return err
	}))

	// Run the migration.
	require.NoError(t, mt.runMigration(ctx, migration))

	require.NoError(t, mt.kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Check that the persisted descriptors now match our in-memory versions,
		// ignoring create and modification times.
		{
			ts, err := txn.GetProtoTs(ctx, key, desc)
			require.NoError(t, err)
			table := desc.Table(ts)
			table.CreateAsOfTime = sqlbase.NamespaceTable.CreateAsOfTime
			table.ModificationTime = sqlbase.NamespaceTable.ModificationTime
			require.True(t, table.Equal(sqlbase.NamespaceTable.TableDesc()))
		}
		{
			ts, err := txn.GetProtoTs(ctx, deprecatedKey, desc)
			require.NoError(t, err)
			table := desc.Table(ts)
			table.CreateAsOfTime = sqlbase.DeprecatedNamespaceTable.CreateAsOfTime
			table.ModificationTime = sqlbase.DeprecatedNamespaceTable.ModificationTime
			require.True(t, table.Equal(sqlbase.DeprecatedNamespaceTable.TableDesc()))
		}
		return nil
	}))
}

func TestAlterSystemJobsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// We need to use "old" jobs table descriptor without newly added columns
	// in order to test migration.
	// oldJobsTableSchema is system.jobs definition prior to 20.2
	oldJobsTableSchema := `
CREATE TABLE system.jobs (
	id                INT8      DEFAULT unique_rowid() PRIMARY KEY,
	status            STRING    NOT NULL,
	created           TIMESTAMP NOT NULL DEFAULT now(),
	payload           BYTES     NOT NULL,
	progress          BYTES,
	INDEX (status, created),

	FAMILY (id, status, created, payload),
	FAMILY progress (progress)
)
`
	oldJobsTable, err := sql.CreateTestTableDescriptor(
		context.Background(),
		keys.SystemDatabaseID,
		keys.JobsTableID,
		oldJobsTableSchema,
		sqlbase.JobsTable.Privileges,
	)
	require.NoError(t, err)

	const primaryFamilyName = "fam_0_id_status_created_payload"
	oldPrimaryFamilyColumns := []string{"id", "status", "created", "payload"}
	newPrimaryFamilyColumns := append(
		oldPrimaryFamilyColumns, "created_by_type", "created_by_id")

	// Sanity check oldJobsTable does not have new columns.
	require.Equal(t, 5, len(oldJobsTable.Columns))
	require.Equal(t, 2, len(oldJobsTable.Families))
	require.Equal(t, primaryFamilyName, oldJobsTable.Families[0].Name)
	require.Equal(t, oldPrimaryFamilyColumns, oldJobsTable.Families[0].ColumnNames)

	jobsTable := sqlbase.JobsTable
	sqlbase.JobsTable = sqlbase.NewImmutableTableDescriptor(*oldJobsTable.TableDesc())
	defer func() {
		sqlbase.JobsTable = jobsTable
	}()

	mt := makeMigrationTest(ctx, t)
	defer mt.close(ctx)

	migration := mt.pop(t, "add created_by columns to system.jobs")
	mt.start(t, base.TestServerArgs{})

	// Run migration and verify we have added columns and renamed column family.
	require.NoError(t, mt.runMigration(ctx, migration))

	newJobsTable := sqlbase.TestingGetTableDescriptor(
		mt.kvDB, keys.SystemSQLCodec, "system", "jobs")
	require.Equal(t, 7, len(newJobsTable.Columns))
	require.Equal(t, "created_by_type", newJobsTable.Columns[5].Name)
	require.Equal(t, "created_by_id", newJobsTable.Columns[6].Name)
	require.Equal(t, 2, len(newJobsTable.Families))
	// Ensure we keep old family name.
	require.Equal(t, primaryFamilyName, newJobsTable.Families[0].Name)
	// Make sure our primary family has new columns added to it.
	require.Equal(t, newPrimaryFamilyColumns, newJobsTable.Families[0].ColumnNames)

	// Run the migration again -- it should be a no-op.
	require.NoError(t, mt.runMigration(ctx, migration))
	newJobsTableAgain := sqlbase.TestingGetTableDescriptor(
		mt.kvDB, keys.SystemSQLCodec, "system", "jobs")
	require.True(t, proto.Equal(newJobsTable, newJobsTableAgain))
}
