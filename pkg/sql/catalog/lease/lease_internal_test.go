// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Note that there's also a lease_test.go, in package sql_test.

package lease

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/sqllivenesstestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestTableSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	type data struct {
		version    descpb.DescriptorVersion
		expiration int64
	}
	type insert data
	type remove data

	type newest struct {
		version descpb.DescriptorVersion
	}

	testData := []struct {
		op       interface{}
		expected string
	}{
		{newest{0}, "<nil>"},
		{insert{2, 3}, "2:3"},
		{newest{0}, "2:3"},
		{newest{2}, "2:3"},
		{newest{3}, "<nil>"},
		{remove{2, 3}, ""},
		{insert{2, 4}, "2:4"},
		{newest{0}, "2:4"},
		{newest{2}, "2:4"},
		{newest{3}, "<nil>"},
		{insert{3, 1}, "2:4 3:1"},
		{newest{0}, "3:1"},
		{newest{1}, "<nil>"},
		{newest{2}, "2:4"},
		{newest{3}, "3:1"},
		{newest{4}, "<nil>"},
		{insert{1, 1}, "1:1 2:4 3:1"},
		{newest{0}, "3:1"},
		{newest{1}, "1:1"},
		{newest{2}, "2:4"},
		{newest{3}, "3:1"},
		{newest{4}, "<nil>"},
		{remove{3, 1}, "1:1 2:4"},
		{remove{1, 1}, "2:4"},
		{remove{2, 4}, ""},
	}

	set := &descriptorSet{}
	for i, d := range testData {
		switch op := d.op.(type) {
		case insert:
			s := &descriptorVersionState{
				Descriptor: tabledesc.NewBuilder(&descpb.TableDescriptor{Version: op.version}).BuildImmutable(),
			}
			s.mu.expiration = hlc.Timestamp{WallTime: op.expiration}
			set.insert(s)

		case remove:
			s := &descriptorVersionState{
				Descriptor: tabledesc.NewBuilder(&descpb.TableDescriptor{Version: op.version}).BuildImmutable(),
			}
			s.mu.expiration = hlc.Timestamp{WallTime: op.expiration}
			set.remove(s)

		case newest:
			n := set.findNewest()
			if op.version != 0 {
				n = set.findVersion(op.version)
			}
			s := "<nil>"
			if n != nil {
				s = fmt.Sprintf("%d:%d", n.GetVersion(), n.getExpiration(ctx).WallTime)
			}
			if d.expected != s {
				t.Fatalf("%d: expected %s, but found %s", i, d.expected, s)
			}
			continue
		}
		if s := set.String(); d.expected != s {
			t.Fatalf("%d: expected %s, but found %s", i, d.expected, s)
		}
	}
}

func getNumVersions(ds *descriptorState) int {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return len(ds.mu.active.data)
}

func TestPurgeOldVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to block gossip so it doesn't come randomly and clear up the
	// leases we're artificially setting up.
	gossipSem := make(chan struct{}, 1)
	serverParams := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &ManagerTestingKnobs{
				TestingDescriptorUpdateEvent: func(_ *descpb.Descriptor) error {
					gossipSem <- struct{}{}
					<-gossipSem
					return nil
				},
			},
		},
	}
	srv, db, kvDB := serverutils.StartServer(t, serverParams)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	// Block gossip.
	gossipSem <- struct{}{}
	defer func() {
		// Unblock gossip.
		<-gossipSem
	}()

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	var tables []catalog.TableDescriptor
	var expiration hlc.Timestamp
	getLeases := func() {
		for i := 0; i < 3; i++ {
			if err := leaseManager.AcquireFreshestFromStore(context.Background(), tableDesc.GetID()); err != nil {
				t.Fatal(err)
			}
			table, err := leaseManager.Acquire(context.Background(), s.Clock().Now(), tableDesc.GetID())
			if err != nil {
				t.Fatal(err)
			}
			tables = append(tables, table.Underlying().(catalog.TableDescriptor))
			expiration = table.Expiration(context.Background())
			table.Release(context.Background())
		}
	}
	getLeases()
	ts := leaseManager.findDescriptorState(tableDesc.GetID(), false)
	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}

	// Publish a new version for the table
	if _, err := leaseManager.Publish(context.Background(), tableDesc.GetID(), func(catalog.MutableDescriptor) error {
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}

	getLeases()
	ts = leaseManager.findDescriptorState(tableDesc.GetID(), false)
	if numLeases := getNumVersions(ts); numLeases != 2 {
		t.Fatalf("found %d versions instead of 2", numLeases)
	}
	ctx := context.Background()
	if err := purgeOldVersions(
		ctx, kvDB, tableDesc.GetID(), false, 2 /* minVersion */, leaseManager); err != nil {
		t.Fatal(err)
	}

	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
	ts.mu.Lock()
	correctLease := ts.mu.active.data[0].GetID() == tables[5].GetID() &&
		ts.mu.active.data[0].GetVersion() == tables[5].GetVersion()
	correctExpiration := ts.mu.active.data[0].getExpiration(ctx) == expiration
	ts.mu.Unlock()
	if !correctLease {
		t.Fatalf("wrong lease survived purge")
	}
	if !correctExpiration {
		t.Fatalf("wrong lease expiration survived purge")
	}

	// Test that purgeOldVersions correctly removes a table version
	// without a lease.
	ts.mu.Lock()
	tableVersion := &descriptorVersionState{
		Descriptor: tables[0],
	}
	tableVersion.mu.expiration = tables[5].GetModificationTime()
	ts.mu.active.insert(tableVersion)
	ts.mu.Unlock()
	if numLeases := getNumVersions(ts); numLeases != 2 {
		t.Fatalf("found %d versions instead of 2", numLeases)
	}
	if err := purgeOldVersions(
		context.Background(), kvDB, tableDesc.GetID(), false, 2 /* minVersion */, leaseManager); err != nil {
		t.Fatal(err)
	}
	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
}

// TestPurgeOldVersionsRetainsDescriptorWithFutureModificationTime tests the
// behavior of purgeOldVersions when the descriptorSet contains a descriptor
// version with a modification time in advance of the current HLC clock, as can
// be the case if the descriptor was updated in a transaction that wrote to a
// global_reads range. In such cases, the descriptor with the newest
// modification time should still be retained.
func TestPurgeOldVersionsRetainsDescriptorWithFutureModificationTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// We're going to block gossip so it doesn't come randomly and clear up the
	// leases we're artificially setting up.
	gossipSem := make(chan struct{}, 1)
	serverParams := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &ManagerTestingKnobs{
				TestingDescriptorUpdateEvent: func(_ *descpb.Descriptor) error {
					gossipSem <- struct{}{}
					<-gossipSem
					return nil
				},
			},
		},
	}
	ctx := context.Background()
	srv, db, kvDB := serverutils.StartServer(t, serverParams)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	// Block gossip.
	gossipSem <- struct{}{}
	defer func() {
		// Unblock gossip.
		<-gossipSem
	}()

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")
	futureTime := s.Clock().Now().Add(500*time.Millisecond.Nanoseconds(), 0)

	getLatestDesc := func() catalog.TableDescriptor {
		if err := leaseManager.AcquireFreshestFromStore(ctx, tableDesc.GetID()); err != nil {
			t.Fatal(err)
		}
		table, err := leaseManager.Acquire(ctx, futureTime, tableDesc.GetID())
		if err != nil {
			t.Fatal(err)
		}
		latestDesc := table.Underlying().(catalog.TableDescriptor)
		table.Release(ctx)
		return latestDesc
	}
	origDesc := getLatestDesc()
	ts := leaseManager.findDescriptorState(tableDesc.GetID(), false)
	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}

	// Publish a new version for the table with a modification time slightly in
	// the future of present time. We dictate this modification time by creating
	// a read-write conflict that forces the publishing transaction to bump its
	// commit timestamp.
	update := func(catalog.MutableDescriptor) error { return nil }
	logEvent := func(txn *kv.Txn) error {
		txn2 := kvDB.NewTxn(ctx, "future-read")
		if err := txn2.SetFixedTimestamp(ctx, futureTime.Prev()); err != nil {
			return err
		}
		key := append(s.Codec().TenantPrefix(), "key"...)
		if _, err := txn2.Get(ctx, key); err != nil {
			return errors.Wrap(err, "read from other txn in future")
		}

		return txn.Put(ctx, key, "value")
	}
	if _, err := leaseManager.Publish(ctx, tableDesc.GetID(), update, logEvent); err != nil {
		t.Fatal(err)
	}

	// The leaseManager should be able to acquire the new version.
	latestDesc := getLatestDesc()
	if latestDesc.GetVersion() <= origDesc.GetVersion() {
		t.Fatalf("expected new version, found %v after %v", latestDesc, origDesc)
	}
	ts = leaseManager.findDescriptorState(tableDesc.GetID(), false)
	if numLeases := getNumVersions(ts); numLeases != 2 {
		t.Fatalf("found %d versions instead of 2", numLeases)
	}

	// Purge old versions and make sure that the newest lease survives the
	// purge.
	if err := purgeOldVersions(ctx, kvDB, tableDesc.GetID(), false, 2 /* minVersion */, leaseManager); err != nil {
		t.Fatal(err)
	}
	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
	ts.mu.Lock()
	correctLease := ts.mu.active.data[0].GetID() == latestDesc.GetID() &&
		ts.mu.active.data[0].GetVersion() == latestDesc.GetVersion()
	ts.mu.Unlock()
	if !correctLease {
		t.Fatalf("wrong lease survived purge")
	}
}

// Test that a database with conflicting table names under different schemas
// do not cause issues.
func TestNameCacheDBConflictingTableNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := db.Exec(`SET experimental_enable_temp_tables = true`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
CREATE TABLE t (public int);
CREATE TEMP TABLE t (temp int);
CREATE TABLE t2 (public int);
CREATE TEMP TABLE t2 (temp int);
`); err != nil {
		t.Fatal(err)
	}

	// Select in different orders, and make sure the right one is returned.
	if _, err := db.Exec("SELECT * FROM pg_temp.t;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT * FROM public.t;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT * FROM public.t2;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT * FROM pg_temp.t2;"); err != nil {
		t.Fatal(err)
	}

	for _, tableName := range []string{"t", "t2"} {
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "defaultdb", tableName)
		lease, _ := leaseManager.names.get(
			context.Background(),
			tableDesc.GetParentID(),
			tableDesc.GetParentSchemaID(),
			tableName,
			s.Clock().Now(),
		)
		if lease.GetID() != tableDesc.GetID() {
			t.Fatalf("lease has wrong ID: %d (expected: %d)", lease.GetID(), tableDesc.GetID())
		}
	}
}

// Test that changing a descriptor's name updates the name cache.
func TestNameCacheIsUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE DATABASE t1;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	// Rename.
	if _, err := db.Exec("ALTER TABLE t.test RENAME TO t.test2;"); err != nil {
		t.Fatal(err)
	}

	// Check that the cache has been updated.
	if lease, _ := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"test",
		s.Clock().Now(),
	); lease != nil {
		t.Fatalf("old name still in cache")
	}

	lease, _ := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"test2",
		s.Clock().Now(),
	)
	if lease == nil {
		t.Fatalf("new name not found in cache")
	}
	if lease.GetID() != tableDesc.GetID() {
		t.Fatalf("new name has wrong ID: %d (expected: %d)", lease.GetID(), tableDesc.GetID())
	}
	lease.Release(context.Background())
}

// Tests that a name cache entry with by an expired lease is not returned.
func TestNameCacheEntryDoesntReturnExpiredLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	const tableName = "test"

	if _, err := db.Exec(fmt.Sprintf(`
CREATE DATABASE t;
CREATE TABLE t.%s (k CHAR PRIMARY KEY, v CHAR);
`, tableName)); err != nil {
		t.Fatal(err)
	}

	// Disable stats collection so that the descriptor isn't modified.
	if _, err := db.Exec("SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false"); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", tableName)

	// Check the assumptions this tests makes: that there is a cache entry
	// (with a valid lease).
	if lease, _ := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		tableName,
		s.Clock().Now(),
	); lease == nil {
		t.Fatalf("name cache has no unexpired entry for (%d, %s)", tableDesc.GetParentID(), tableName)
	} else {
		lease.Release(context.Background())
	}

	leaseManager.ExpireLeases(s.Clock())

	// Check the name no longer resolves.
	if lease, _ := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		tableName,
		s.Clock().Now(),
	); lease != nil {
		t.Fatalf("name cache has unexpired entry for (%d, %s): %s", tableDesc.GetParentID(), tableName, lease)
	}
}

// Tests that a name cache entry always exists for the latest lease and
// the lease expiration time is monotonically increasing.
func TestNameCacheContainsLatestLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	const tableName = "test"

	if _, err := db.Exec(fmt.Sprintf(`
CREATE DATABASE t;
CREATE TABLE t.%s (k CHAR PRIMARY KEY, v CHAR);
`, tableName)); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", tableName)

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.test;"); err != nil {
		t.Fatal(err)
	}

	// There is a cache entry.
	lease, _ := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		tableName,
		s.Clock().Now(),
	)
	if lease == nil {
		t.Fatalf("name cache has no unexpired entry for (%d, %s)", tableDesc.GetParentID(), tableName)
	}
	expiration := lease.Expiration(context.Background())
	// Acquire another lease.
	if _, err := acquireNodeLease(
		context.Background(), leaseManager, tableDesc.GetID(), AcquireBlock,
	); err != nil {
		t.Fatal(err)
	}

	// Check the name resolves to the new lease.
	newLease, _ := leaseManager.names.get(context.Background(), tableDesc.GetParentID(), tableDesc.GetParentSchemaID(), tableName, s.Clock().Now())
	if newLease == nil {
		t.Fatalf("name cache doesn't contain entry for (%d, %s)", tableDesc.GetParentID(), tableName)
	}

	if newLease.Expiration(context.Background()).Less(expiration) {
		t.Fatalf("unexpected lease extension %s %s", expiration.GoTime(), newLease.Expiration(context.Background()).GoTime())
	}

	// TODO(ajwerner): does this matter?
	lease.Release(context.Background())
	newLease.Release(context.Background())
}

// Test that table names are treated as case sensitive by the name cache.
func TestTableNameCaseSensitive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	// Check that we cannot get the table by a different name.
	if lease, _ := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"tEsT",
		s.Clock().Now(),
	); lease != nil {
		t.Fatalf("lease manager incorrectly found table with different case")
	}
}

// Test that there's no deadlock between AcquireByName and Release.
// We used to have one due to lock inversion between the nameCache lock and
// the descriptorVersionState lock, triggered when the same lease was Release()d after the
// table had been dropped (which means it's removed from the nameCache) and
// AcquireByName()d at the same time.
func TestReleaseAcquireByNameDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	removalTracker := NewLeaseRemovalTracker()
	testingKnobs := base.TestingKnobs{
		SQLLeaseManager: &ManagerTestingKnobs{
			LeaseStoreTestingKnobs: StorageTestingKnobs{
				LeaseReleasedEvent:     removalTracker.LeaseRemovedNotification,
				RemoveOnceDereferenced: true,
			},
		},
	}
	srv, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{Knobs: testingKnobs})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	// Populate the name cache.
	ctx := context.Background()
	table, err := leaseManager.AcquireByName(
		ctx,
		leaseManager.storage.clock.Now(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"test",
	)
	if err != nil {
		t.Fatal(err)
	}
	table.Release(ctx)

	// Try to trigger the race repeatedly: race an AcquireByName against a
	// Release.
	// tableChan acts as a barrier, synchronizing the two routines at every
	// iteration.
	tableChan := make(chan LeasedDescriptor)
	releaseChan := make(chan struct{})
	go func() {
		for table := range tableChan {
			// Move errors to the main goroutine.
			table.Release(ctx)
			releaseChan <- struct{}{}
		}
	}()

	for i := 0; i < 1; i++ {
		timestamp := leaseManager.storage.clock.Now()
		ctx := context.Background()
		desc, err := leaseManager.AcquireByName(
			ctx,
			timestamp,
			tableDesc.GetParentID(),
			tableDesc.GetParentSchemaID(),
			"test",
		)
		if err != nil {
			t.Fatal(err)
		}
		table := desc.Underlying().(catalog.TableDescriptor)
		// This test will need to wait until leases are removed from the store
		// before creating new leases because the jitter used in the leases'
		// expiration causes duplicate key errors when trying to create new
		// leases. This is not a problem in production, since leases are not
		// removed from the store until they expire, and the jitter is small
		// compared to their lifetime, but it is a problem in this test because
		// we churn through leases quickly.
		tracker := removalTracker.TrackRemoval(table)
		// Start the race: signal the other guy to release, and we do another
		// acquire at the same time.
		tableChan <- desc
		tableByName, err := leaseManager.AcquireByName(
			ctx,
			timestamp,
			tableDesc.GetParentID(),
			tableDesc.GetParentSchemaID(),
			"test",
		)
		if err != nil {
			t.Fatal(err)
		}

		// See if there was an error releasing lease.
		<-releaseChan

		// Release the lease for the last time.
		tableByName.Release(ctx)

		// There are 2 possible results of the race above: Either we acquired before
		// releasing (causing us to acquire the same lease, incrementing and then
		// decrementing the refCount), or we released before acquiring (causing the
		// lease to be removed before another new lease is acquired). In the latter
		// case, there are actually two different lease removals, but we still only
		// track one.
		//
		// An earlier version of this test tracked both lease removals, but it used
		// reference equality to determine whether we were reacquiring the same
		// lease, which is no longer feasible after the 20.2 descriptor interface
		// changes. This is mostly fine because async lease removal doesn't require
		// the descriptorState lock anyway, so it's not that relevant to this test.
		if err := tracker.WaitForRemoval(); err != nil {
			t.Fatal(err)
		}
	}
	close(tableChan)
}

// TestAcquireFreshestFromStoreRaces runs
// Manager.acquireFreshestFromStore() in parallel to test for races.
func TestAcquireFreshestFromStoreRaces(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	var wg sync.WaitGroup
	numRoutines := 10
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func() {
			defer wg.Done()
			if err := leaseManager.AcquireFreshestFromStore(context.Background(), tableDesc.GetID()); err != nil {
				t.Error(err)
			}
			table, err := leaseManager.Acquire(context.Background(), s.Clock().Now(), tableDesc.GetID())
			if err != nil {
				t.Error(err)
			}
			table.Release(context.Background())
		}()
	}
	wg.Wait()
}

// This test checks that multiple threads can simultaneously acquire the
// latest table version with a lease. When multiple threads
// wait on a particular thread acquiring a lease for the latest table version,
// they are able to check after waiting that the lease they were waiting on
// is still valid. They are able to reacquire a lease if needed.
func TestParallelLeaseAcquireWithImmediateRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testingKnobs := base.TestingKnobs{
		SQLLeaseManager: &ManagerTestingKnobs{
			LeaseStoreTestingKnobs: StorageTestingKnobs{
				// Immediate remove descriptorVersionState and release its
				// lease when it is dereferenced. This forces threads
				// waiting on a lease to reacquire the lease.
				RemoveOnceDereferenced: true,
			},
		},
	}
	srv, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{Knobs: testingKnobs})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	var wg sync.WaitGroup
	numRoutines := 10
	now := s.Clock().Now()
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func() {
			defer wg.Done()
			table, err := leaseManager.Acquire(context.Background(), now, tableDesc.GetID())
			if err != nil {
				t.Error(err)
			}
			table.Release(context.Background())
		}()
	}

	wg.Wait()
}

// Test one possible outcome of a race between a lease acquisition (the first
// case through descriptorState.acquire(), the second through
// descriptorState.acquireFreshestFromStore()) and a release of the lease that was
// just acquired. Precisely:
//  1. Thread 1 calls either acquireFreshestFromStore() or acquire().
//  2. Thread 1 releases the lock on descriptorState and starts acquisition of a lease
//     from the store, blocking until it's finished.
//  3. Thread 2 calls acquire(). The lease has not been acquired yet, so it
//     also enters the acquisition code path (calling DoChan).
//  4. Thread 2 proceeds to release the lock on descriptorState waiting for the
//     in-flight acquisition.
//  4. The lease is acquired from the store and the waiting routines are
//     unblocked.
//  5. Thread 2 unblocks first, and releases the new lease, for whatever reason.
//  5. Thread 1 wakes up. At this point, a naive implementation would use the
//     newly acquired lease, which would be incorrect. The test checks that
//     acquireFreshestFromStore() or acquire() notices, after re-acquiring the
//     descriptorState lock, that the new lease has been released and acquires a new
//     one.
func TestLeaseAcquireAndReleaseConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Result is a struct for moving results to the main result routine.
	type Result struct {
		table LeasedDescriptor
		exp   hlc.Timestamp
		err   error
	}
	mkResult := func(table LeasedDescriptor, err error) Result {
		res := Result{err: err}
		if table != nil {
			res.table = table
			res.exp = table.Expiration(context.Background())
		}
		return res
	}

	var descID atomic.Value
	descID.Store(descpb.ID(0))
	getDescID := func() descpb.ID { return descID.Load().(descpb.ID) }

	// acquireBlock calls Acquire.
	acquireBlock := func(
		ctx context.Context,
		m *Manager,
		acquireChan chan Result,
	) {
		acquireChan <- mkResult(m.Acquire(ctx, m.storage.clock.Now(), getDescID()))
	}

	testCases := []struct {
		name string
		// Whether the second routine is a call to Manager.acquireFreshest or
		// not. This determines which channel we unblock.
		isSecondCallAcquireFreshest bool
	}{

		// Checks what happens when the race between between acquire() and
		// lease release occurs.
		{
			name:                        "CallAcquireConcurrently",
			isSecondCallAcquireFreshest: false,
		},
		// Checks what happens when the race between
		// acquireFreshestFromStore() and lease release occurs.
		{
			name:                        "CallAcquireFreshestAndAcquireConcurrently",
			isSecondCallAcquireFreshest: true,
		},
	}

	for _, test := range testCases {
		ctx := logtags.AddTag(context.Background(), "test: Lease", nil)

		t.Run(test.name, func(t *testing.T) {
			// blockChan and freshestBlockChan is used to set up the race condition.
			blockChan := make(chan struct{})
			freshestBlockChan := make(chan struct{})
			// acquisitionBlock is used to prevent acquireNodeLease from
			// completing, to force a lease to delay its acquisition.
			acquisitionBlock := make(chan struct{})

			// preblock is used for the main routine to wait for all acquisition
			// routines to catch up.
			var preblock sync.WaitGroup
			// acquireArrivals and acquireFreshestArrivals tracks how many times
			// we've arrived at the knob codepath for the corresponding functions.
			// This is needed because the fix to the race condition hits the knob more
			// than once in a single routine, so we need to ignore any extra passes.
			var acquireArrivals int32
			var acquireFreshestArrivals int32
			// leasesAcquiredCount counts how many leases were acquired in total.
			var leasesAcquiredCount int32

			removalTracker := NewLeaseRemovalTracker()
			testingKnobs := base.TestingKnobs{
				SQLLeaseManager: &ManagerTestingKnobs{
					TestingDescriptorUpdateEvent: func(descriptor *descpb.Descriptor) error {
						// Ignore the update to the table in question.
						if id, _, _, _, _ := descpb.GetDescriptorMetadata(descriptor); id == getDescID() {
							return errors.New("boom")
						}
						return nil
					},
					LeaseStoreTestingKnobs: StorageTestingKnobs{
						RemoveOnceDereferenced: true,
						LeaseReleasedEvent:     removalTracker.LeaseRemovedNotification,
						LeaseAcquireResultBlockEvent: func(leaseBlockType AcquireType, id descpb.ID) {
							if id != getDescID() {
								return
							}
							if leaseBlockType == AcquireBlock {
								if count := atomic.LoadInt32(&acquireArrivals); (count < 1 && test.isSecondCallAcquireFreshest) ||
									(count < 2 && !test.isSecondCallAcquireFreshest) {
									atomic.AddInt32(&acquireArrivals, 1)
									preblock.Done()
									<-blockChan
								}
							} else if leaseBlockType == AcquireFreshestBlock {
								if atomic.LoadInt32(&acquireFreshestArrivals) < 1 {
									atomic.AddInt32(&acquireFreshestArrivals, 1)
									preblock.Done()
									<-freshestBlockChan
								}
							}
						},
						LeaseAcquiredEvent: func(desc catalog.Descriptor, err error) {
							if desc.GetID() != getDescID() {
								return
							}
							atomic.AddInt32(&leasesAcquiredCount, 1)
							<-acquisitionBlock
						},
					},
				},
			}

			serverArgs := base.TestServerArgs{
				Knobs:    testingKnobs,
				Settings: cluster.MakeTestingClusterSettings(),
			}

			// The LeaseJitterFraction is zero so leases will have
			// monotonically increasing expiration. This prevents two leases
			// from having the same expiration due to randomness, as the
			// leases are checked for having a different expiration.
			LeaseJitterFraction.Override(ctx, &serverArgs.Settings.SV, 0)

			srv, sqlDB, _ := serverutils.StartServer(t, serverArgs)
			defer srv.Stopper().Stop(context.Background())
			s := srv.ApplicationLayer()
			tdb := sqlutils.MakeSQLRunner(sqlDB)
			tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
			{
				var tableID descpb.ID
				tdb.QueryRow(
					t, "SELECT id FROM system.namespace WHERE name = $1", "t",
				).Scan(&tableID)
				descID.Store(tableID)
			}
			leaseManager := s.LeaseManager().(*Manager)

			acquireResultChan := make(chan Result)

			// Start two routines to acquire and release.
			preblock.Add(2)
			go acquireBlock(ctx, leaseManager, acquireResultChan)
			if test.isSecondCallAcquireFreshest {
				go func(ctx context.Context, m *Manager, acquireChan chan Result) {
					if err := m.AcquireFreshestFromStore(ctx, getDescID()); err != nil {
						acquireChan <- mkResult(nil, err)
						return
					}
					acquireChan <- mkResult(m.Acquire(ctx, s.Clock().Now(), getDescID()))
				}(ctx, leaseManager, acquireResultChan)

			} else {
				go acquireBlock(ctx, leaseManager, acquireResultChan)
			}

			// Wait until both routines arrive.
			preblock.Wait()

			// Allow the acquisition to finish. By delaying it until now, we guarantee
			// both routines will receive the same lease.
			acquisitionBlock <- struct{}{}

			// Allow the first routine to finish acquisition. In the case where both
			// routines are calling Acquire(), first refers to whichever routine
			// continues, order does not matter.
			blockChan <- struct{}{}
			// Wait for the first routine's results.
			result1 := <-acquireResultChan
			if result1.err != nil {
				t.Fatal(result1.err)
			}

			// Release the lease. This also causes it to get removed as the
			// knob RemoveOnceDereferenced is set.
			tracker := removalTracker.TrackRemoval(result1.table.Underlying())
			result1.table.Release(ctx)
			// Wait until the lease is removed.
			if err := tracker.WaitForRemoval(); err != nil {
				t.Fatal(err)
			}

			// Allow the second routine to proceed.
			if test.isSecondCallAcquireFreshest {
				freshestBlockChan <- struct{}{}
			} else {
				blockChan <- struct{}{}
			}

			// Allow all future acquisitions to complete.
			close(acquisitionBlock)

			// Get the acquisition results of the second routine.
			result2 := <-acquireResultChan
			if result2.err != nil {
				t.Fatal(result2.err)
			}

			if result1.table == result2.table && result1.exp == result2.exp {
				t.Fatalf("Expected the leases to be different. TableDescriptor pointers are equal and both the same expiration")
			}
			if count := atomic.LoadInt32(&leasesAcquiredCount); count != 2 {
				t.Fatalf("Expected to acquire 2 leases, instead got %d", count)
			}
		})
	}
}

// Tests retrieving older versions within a given start and end timestamp of a
// table descriptor from store through an ExportRequest.
func TestReadOlderVersionForTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	serverParams := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &ManagerTestingKnobs{
				TestingDescriptorUpdateEvent: func(_ *descpb.Descriptor) error {
					return errors.New("Caught race between resetting state and refreshing leases")
				},
			},
		},
	}
	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, serverParams)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	// Prevent non-explicit Acquire to leases for testing purposes.
	tdb.Exec(t, "SET CLUSTER SETTING sql.tablecache.lease.refresh_limit = 0")
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	var tableID descpb.ID
	tdb.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 'foo'").Scan(&tableID)

	manager := s.LeaseManager().(*Manager)
	const numHistoricalVersions = 5
	const maxVersion = numHistoricalVersions + 1
	descs := make([]catalog.Descriptor, maxVersion)

	// Create numHistoricalVersions versions of table descriptor
	for i := 0; i < numHistoricalVersions; i++ {
		_, err := manager.Publish(ctx, tableID, func(desc catalog.MutableDescriptor) error {
			descs[i] = desc.ImmutableCopy()
			return nil
		}, nil)
		require.NoError(t, err)
	}
	{
		last, err := manager.Acquire(ctx, s.Clock().Now(), tableID)
		require.NoError(t, err)
		descs[numHistoricalVersions] = last.Underlying()
		last.Release(ctx)
	}

	type version int
	type testCase struct {
		before   []version
		ts       hlc.Timestamp
		tsStr    string
		expected []version
	}
	versionTS := func(v version) hlc.Timestamp {
		return descs[v-1].GetModificationTime()
	}
	versionDesc := func(v version) catalog.Descriptor {
		return descs[v-1]
	}
	resetDescriptorState := func(
		manager *Manager, tableID descpb.ID, tc testCase,
	) {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		descStates := manager.mu.descriptors
		descStates[tableID] = &descriptorState{m: manager, id: tableID}
		for _, v := range tc.before {
			addedDescVState := &descriptorVersionState{
				t:          descStates[tableID],
				Descriptor: versionDesc(v),
			}
			addedDescVState.mu.Lock()
			if v < maxVersion {
				addedDescVState.mu.expiration = versionTS(v + 1)
			} else {
				addedDescVState.mu.expiration = hlc.MaxTimestamp
			}
			addedDescVState.mu.Unlock()
			descStates[tableID].mu.active.insert(addedDescVState)
		}
	}

	// Test historical read for descriptors as of specific timestamps and confirm
	// expected data.
	// [v1 ---)[v2 --)[v3 ---)[v4 ----)[v5 -----)[v6 ------)
	for _, tc := range []testCase{

		// The following cases represent having no existing descriptor state, or
		// as importantly, having some descriptor state but searching for a
		// timestamp after the known state. The code, as stands, assumes that
		// when this happens, we'll rely on an existing lease to provide the end
		// timestamp, and when no such lease exists, we'll go get one. If the
		// attempt to get a lease fails, then we'll propagate that error up. This
		// fact is the source of the known limitation described on
		// Acquire and AcquireByName.
		{
			before:   []version{},
			ts:       versionTS(1),
			tsStr:    "ts1",
			expected: []version{},
		},
		{
			before:   []version{},
			ts:       versionTS(4),
			tsStr:    "ts4",
			expected: []version{},
		},
		{
			before:   []version{},
			ts:       versionTS(6),
			tsStr:    "ts6",
			expected: []version{},
		},
		{
			before:   []version{1, 2, 3},
			ts:       versionTS(4).Next(),
			tsStr:    "ts4.Next",
			expected: []version{},
		},
		{
			before:   []version{},
			ts:       versionTS(6).Prev(),
			tsStr:    "ts6.Prev",
			expected: []version{},
		},

		{
			before:   []version{6},
			ts:       versionTS(4).Prev(),
			tsStr:    "ts4.Prev",
			expected: []version{3, 4, 5},
		},
		{
			before:   []version{6},
			ts:       versionTS(5),
			tsStr:    "ts5",
			expected: []version{5},
		},
		{
			before:   []version{5, 6},
			ts:       versionTS(3).Prev(),
			tsStr:    "ts3.Prev",
			expected: []version{2, 3, 4},
		},
		{
			before:   []version{1, 2, 3, 4, 5, 6},
			ts:       versionTS(4),
			tsStr:    "ts4",
			expected: []version{},
		},
		{
			before:   []version{1, 4, 5, 6},
			ts:       versionTS(4).Prev(),
			tsStr:    "ts4.Prev",
			expected: []version{3},
		},
		{
			before:   []version{1, 4, 5, 6},
			ts:       versionTS(3).Prev(),
			tsStr:    "ts3.Prev",
			expected: []version{2, 3},
		},
		{
			before:   []version{1, 4, 5, 6},
			ts:       versionTS(2),
			tsStr:    "ts2",
			expected: []version{2, 3},
		},
		{
			before:   []version{1, 4, 5, 6},
			ts:       versionTS(2).Prev(),
			tsStr:    "ts2.Prev",
			expected: []version{},
		},
	} {
		t.Run(fmt.Sprintf("%v@%v->%v", tc.before, tc.tsStr, tc.expected), func(t *testing.T) {
			// Reset the descriptor state to before versions.
			resetDescriptorState(manager, tableID, tc)

			// Retrieve historicalDescriptors modification times.
			retrieved, err := manager.readOlderVersionForTimestamp(ctx, tableID, tc.ts)
			require.NoError(t, err)

			// Validate retrieved descriptors match expected versions.
			retrievedVersions := make([]version, 0)
			for _, desc := range retrieved {
				ver := version(desc.desc.GetVersion())
				retrievedVersions = append([]version{ver}, retrievedVersions...)
			}
			require.Equal(t, tc.expected, retrievedVersions)
		})
	}
}

// TestDescriptorByteSizeOrder inserts different amount of data in each
// descriptor and guarantees the relative order of the Descriptor sizes are
// correct.
func TestDescriptorByteSizeOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}
	srv, db, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)

	manager := s.LeaseManager().(*Manager)
	for _, tc := range []struct {
		sqlExprs map[string]string
		expOrder []string
		name     string
	}{
		// Confirm order of TableDescriptors with varying name.
		{
			sqlExprs: map[string]string{
				"s":             "CREATE TABLE s (col1 INT)",
				"medxx":         "CREATE TABLE medxx (col1 INT)",
				"largexxxx":     "CREATE TABLE largexxxx (col1 INT)",
				"largestxxxxxx": "CREATE TABLE largestxxxxxx (col1 INT)",
			},
			expOrder: []string{
				"s",
				"medxx",
				"largexxxx",
				"largestxxxxxx",
			},
			name: "tables with varying name",
		},
		// Confirm order of TableDescriptors with varying col count.
		{
			sqlExprs: map[string]string{
				"smallxxcol": "CREATE TABLE smallxxcol (col1 INT)",
				"mediumxcol": "CREATE TABLE mediumxcol (col1 INT, col2 INT)",
				"largexxcol": "CREATE TABLE largexxcol (col1 INT, col2 INT, col3 UUID)",
				"largestcol": "CREATE TABLE largestcol (col1 INT, col2 INT, col3 UUID, col4 UUID)",
			},
			expOrder: []string{
				"smallxxcol",
				"mediumxcol",
				"largexxcol",
				"largestcol",
			},
			name: "tables with varying col",
		},
		// Confirm order of TableDescriptors with varying idx count.
		{
			sqlExprs: map[string]string{
				"smallxx": "CREATE TABLE smallxx (col1 INT, col2 INT, col3 STRING, col4 BOOL, INDEX (col1))",
				"mediumx": "CREATE TABLE mediumx (col1 INT, col2 INT, col3 STRING, col4 BOOL, INDEX (col1, col2))",
				"largexx": "CREATE TABLE largexx (col1 INT, col2 INT, col3 STRING, col4 BOOL, INDEX (col1, col2, col3))",
				"largest": "CREATE TABLE largest (col1 INT, col2 INT, col3 STRING, col4 BOOL, INDEX (col1, col2, col3, col4))",
			},
			expOrder: []string{
				"smallxx",
				"mediumx",
				"largexx",
				"largest",
			},
			name: "tables with varying idx",
		},
		// Confirm order of DatabaseDescriptors with varying name.
		{
			sqlExprs: map[string]string{
				"sdb":          "CREATE DATABASE sdb",
				"meddbx":       "CREATE DATABASE meddbx",
				"largedbxx":    "CREATE DATABASE largedbxx",
				"largestdbxxx": "CREATE DATABASE largestdbxxx",
			},
			expOrder: []string{
				"sdb",
				"meddbx",
				"largedbxx",
				"largestdbxxx",
			},
			name: "databases with varying name",
		},
		// Confirm order of SchemaDescriptors with varying name.
		{
			sqlExprs: map[string]string{
				"sschema":          "CREATE SCHEMA sschema",
				"medschemax":       "CREATE SCHEMA medschemax",
				"largeschemaxx":    "CREATE SCHEMA largeschemaxx",
				"largestschemaxxx": "CREATE SCHEMA largestschemaxxx",
			},
			expOrder: []string{
				"sschema",
				"medschemax",
				"largeschemaxx",
				"largestschemaxxx",
			},
			name: "schemas with varying name",
		},
		// Confirm order of TypeDescriptors with varying name.
		{
			sqlExprs: map[string]string{
				"stype":       "CREATE TYPE stype AS ENUM ('open', 'closed')",
				"medtype":     "CREATE TYPE medtype AS ENUM ('open', 'closed', 'inactive')",
				"largetype":   "CREATE TYPE largetype AS ENUM ('open', 'closed', 'inactive', 'active')",
				"largesttype": "CREATE TYPE largesttype AS ENUM ('open', 'closed', 'inactive', 'active', 'starting')",
			},
			expOrder: []string{
				"stype",
				"medtype",
				"largetype",
				"largesttype",
			},
			name: "types with varying name",
		},
	} {
		t.Run(fmt.Sprint(tc.name), func(t *testing.T) {
			// Create tables and retrieve TableDescriptors.
			descs := make([]LeasedDescriptor, 0, len(tc.sqlExprs))
			for size, expr := range tc.sqlExprs {
				tdb.Exec(t, expr)
				var tableID descpb.ID
				tdb.QueryRow(t, "SELECT id FROM system.namespace WHERE name = "+"'"+size+"'").Scan(&tableID)
				desc, err := manager.Acquire(ctx, s.Clock().Now(), tableID)
				require.NoError(t, err)
				descs = append(descs, desc)
			}

			// Sort the descriptors and confirm is same as expected.
			sort.Slice(descs, func(i, j int) bool {
				return descs[i].Underlying().ByteSize() < descs[j].Underlying().ByteSize()
			})
			actual := make([]string, len(descs))
			for i, desc := range descs {
				actual[i] = desc.GetName()
			}
			require.Equalf(t, tc.expOrder, actual, "expected: %v, got: %v", tc.expOrder, actual)
		})
	}
}

// TestLeasedDescriptorByteSizeBaseline ensures each descriptor type has a byte
// size of at least the baseline approximation, which is the number of bytes
// from the binary encoded in the descriptor table.
func TestLeasedDescriptorByteSizeBaseline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)
	manager := s.LeaseManager().(*Manager)

	getApproxByteSize := func(t *testing.T, descID descpb.ID) int64 {
		var descBytes *[]byte
		rows := tdb.Query(t, "SELECT descriptor FROM system.descriptor WHERE id = $1", descID)
		if rows.Next() {
			err := rows.Scan(&descBytes)
			require.NoError(t, err, "error retrieving descriptor from system table")
		}
		return int64(len(*descBytes))
	}

	for _, tc := range []struct {
		sqlExpr  []string
		descType string
		name     string
	}{
		{
			sqlExpr: []string{
				"CREATE TABLE s (col1 INT)",
			},
			descType: "table",
			name:     "s",
		},
		{
			sqlExpr: []string{
				"CREATE TABLE ss (col1 INT)",
				"INSERT INTO s VALUES (1)",
				"INSERT INTO s VALUES (10)",
			},
			descType: "table",
			name:     "ss",
		},
		{
			sqlExpr: []string{
				"CREATE DATABASE randomDB",
			},
			descType: "db",
			name:     "randomdb",
		},
		{
			sqlExpr: []string{
				"CREATE SCHEMA schema_one",
			},
			descType: "schema",
			name:     "schema_one",
		},
		{
			sqlExpr: []string{
				"CREATE USER max WITH PASSWORD 'roach'",
				"CREATE SCHEMA schema_two AUTHORIZATION max",
			},
			descType: "schema",
			name:     "schema_two",
		},
		{
			sqlExpr: []string{
				"CREATE TYPE type_one AS ENUM ('open', 'closed')",
			},
			descType: "type",
			name:     "type_one",
		},
		{
			sqlExpr: []string{
				"CREATE TYPE type_two AS ENUM ('open', 'closed', 'inactive', 'active', 'starting')",
			},
			descType: "type",
			name:     "type_two",
		},
	} {
		t.Run(fmt.Sprintf("%s descriptor %s", tc.descType, tc.name), func(t *testing.T) {
			// Execute SQL commands and acquire leases on descriptors.
			for _, expr := range tc.sqlExpr {
				tdb.Exec(t, expr)
			}
			var descID descpb.ID
			tdb.QueryRow(t, "SELECT id FROM system.namespace WHERE name ="+
				"'"+tc.name+"'").Scan(&descID)
			desc, err := manager.Acquire(ctx, s.Clock().Now(), descID)
			require.NoError(t, err)

			// Confirm each descriptor byte size is at least the baseline's.
			approx := getApproxByteSize(t, descID)
			require.Greaterf(t, desc.Underlying().ByteSize(), approx, "ByteSize of desc "+
				"%d does not exceed the baseline", desc.GetID())
		})
	}
}

// TODO(adityamaru): We do not set SplitMidKey to true for ExportRequests sent
// in getDescriptorsFromStoreForInterval. This disallows the elastic CPU limiter
// from preempting the ExportRequest unless we are on a key boundary. In this
// test we are only exporting revisions of the same key so we should never be
// allowed to paginate because of exhausted CPU tokens. Once we do add support
// for SplitMidKey, we should change the test to verify the correctness of our
// pagination logic.
//
// For now, assert that all revisions are fetched in a single ExportRequest even
// though we are always OverLimit according to the elastic CPU limiter.
func TestGetDescriptorsFromStoreForIntervalCPULimiterPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var numRequests int
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
				for _, ru := range request.Requests {
					if _, ok := ru.GetInner().(*kvpb.ExportRequest); ok {
						numRequests++
						h := admission.ElasticCPUWorkHandleFromContext(ctx)
						if h == nil {
							t.Fatalf("expected context to have CPU work handle")
						}
						h.TestingOverrideOverLimit(func() (bool, time.Duration) {
							return true, 0
						})
					}
				}
				return nil
			},
		}},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	beforeCreate := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE foo RENAME TO bar`)
	sqlDB.Exec(t, `ALTER TABLE bar RENAME TO baz`)
	afterCreate := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	var tableID int
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'baz'`).Scan(&tableID)
	descs, err := getDescriptorsFromStoreForInterval(ctx, kvDB, s.Codec(), descpb.ID(tableID),
		beforeCreate, afterCreate)
	require.NoError(t, err)
	require.Len(t, descs, 3)
	require.Equal(t, numRequests, 1)
}

// TestLeaseCountDetailSessionBased will test out the extra debugging info that
// comes from countLeasesWithDetail. This version targets session based leasing.
func TestLeaseCountDetailSessionBased(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings:          st,
		DefaultTestTenant: base.TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs,
		Knobs:             base.TestingKnobs{},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	idb := srv.InternalDB().(isql.DB)
	executor := idb.Executor()

	descID := 890 // Descriptor ID that we will insert and query in this test
	session, err := srv.SQLLivenessProvider().(sqlliveness.Provider).Session(ctx)
	require.NoError(t, err)
	err = executor.WithSyntheticDescriptors(catalog.Descriptors{systemschema.LeaseTable()}, func() error {
		nodeID := "0" // Hard code the node rather than getting it from srv to avoid import cycle
		version := 1
		region := enum.One
		_, err := executor.Exec(ctx, "add-rows-for-test", nil,
			fmt.Sprintf("INSERT INTO system.lease VALUES (%d, %d, %s, '\\x%x', '\\x%x')",
				descID, version, nodeID, session.ID().UnsafeBytes(), region))
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	codec := s.Codec()
	now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	detail, err := countLeasesWithDetail(ctx, idb, codec, nil, st,
		[]IDVersion{{ID: descpb.ID(descID), Version: 1}}, now, true)
	require.NoError(t, err)
	require.Equal(t, 1, detail.count)
	require.Equal(t, 1, detail.numSQLInstances)
	require.Equal(t, 0, detail.sampleSQLInstanceID)
}

// fakeSessionProvider session provider that only overloads the Session function
// with a callback.
type fakeSessionProvider struct {
	syncutil.Mutex
	sqlliveness.Provider
	getSession func() *sqllivenesstestutils.FakeSession
}

var _ sqlliveness.Provider = &fakeSessionProvider{}

// Session implements sqlliveness.Provider
func (p *fakeSessionProvider) Session(ctx context.Context) (sqlliveness.Session, error) {
	p.Lock()
	defer p.Unlock()
	if f := p.getSession(); f != nil {
		return f, nil
	}
	return p.Provider.Session(ctx)
}

// TestLeaseManagerSessionIDChanges validates that the lease manager can acquire
// and release leases properly even if the SessionID changes. This can happen
// during a fail over scenario, where a new SessionID could be picked up. Which,
// should cause us to reacquire leases.
func TestLeaseManagerSessionIDChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{
			// Avoid using tenants since async tenant migration steps can acquire
			// leases on our user tables.
			DefaultTestTenant: base.TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs,
		})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	leaseManager := s.LeaseManager().(*Manager)

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	runner.Exec(t, `SET CLUSTER SETTING sql.catalog.descriptor_wait_for_initial_version.enabled = false`)

	runner.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")

	getLatestLeasedDesc := func() *descriptorVersionState {
		state := leaseManager.findDescriptorState(tableDesc.GetID(), false)
		state.mu.Lock()
		defer state.mu.Unlock()
		descState := state.mu.active.findNewest()
		return descState
	}

	// Set up a fake session provider that will keep changing IDs and every session
	// will instantly expire. This is like having a zero duration lease in the expiry
	// model.
	var nextSessionID atomic.Int64
	var enableHook atomic.Bool

	fs := fakeSessionProvider{
		Provider: leaseManager.storage.livenessProvider,
		getSession: func() *sqllivenesstestutils.FakeSession {
			if !enableHook.Load() {
				return nil
			}
			now := s.Clock().Now()
			return &sqllivenesstestutils.FakeSession{
				SessionID: sqlliveness.SessionID(fmt.Sprintf("session-%d", nextSessionID.Load())),
				ExpTS:     now,
				StartTS:   now,
			}
		},
	}

	// Replace the session provider which only returns expired leases.
	leaseManager.storage.livenessProvider = &fs
	defer func() {
		// Restore the original provider so valid session IDs
		// are assigned.
		leaseManager.storage.livenessProvider = fs.Provider
	}()

	// Repeatedly lease the same descriptor, with the session ID continuously changing
	// and each one always being expired. This validates that in fail over scenarios,
	// nothing bad happens if the session is expired.
	ctx := context.Background()
	var previousSessionID sqlliveness.SessionID
	var previousExpiry hlc.Timestamp
	for count := 0; count < 10; count++ {
		enableHook.Swap(true)
		nextSessionID.Add(1)
		now := s.Clock().Now()
		desc, err := leaseManager.Acquire(ctx, now, tableDesc.GetID())
		require.NoError(t, err)
		// We expect a new session ID each time, and the descriptor
		// to be expired.
		newSessionID := getLatestLeasedDesc().getSessionID()
		newExpiry := getLatestLeasedDesc().getExpiration(ctx)
		require.NotEqualf(t, previousSessionID, newSessionID, "session ID should not match")
		require.Truef(t, previousExpiry.Less(newExpiry), "session expiry should be later.")
		// Disable the hook before the lease query.
		enableHook.Swap(false)
		// Sanity: Validate that system.lease only has a single lease with new
		// id.
		runner.CheckQueryResults(t, fmt.Sprintf("SELECT session_id FROM system.lease WHERE desc_id=%d", tableDesc.GetID()),
			[][]string{{string(newSessionID.UnsafeBytes())}})
		previousExpiry = newExpiry
		previousSessionID = newSessionID
		desc.Release(ctx)
	}

}
