// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Note that there's also a lease_test.go, in package sql_test.

package lease

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

func TestTableSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
				s = fmt.Sprintf("%d:%d", n.GetVersion(), n.getExpiration().WallTime)
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
	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.Background())
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

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

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
			expiration = table.Expiration()
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
	if err := purgeOldVersions(
		context.Background(), kvDB, tableDesc.GetID(), false, 2 /* minVersion */, leaseManager); err != nil {
		t.Fatal(err)
	}

	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
	ts.mu.Lock()
	correctLease := ts.mu.active.data[0].GetID() == tables[5].GetID() &&
		ts.mu.active.data[0].GetVersion() == tables[5].GetVersion()
	correctExpiration := ts.mu.active.data[0].getExpiration() == expiration
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
	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(ctx)
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

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	futureTime := s.Clock().Now().Add(500*time.Millisecond.Nanoseconds(), 0).WithSynthetic(true)

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
		txn2.SetFixedTimestamp(ctx, futureTime.Prev())
		if _, err := txn2.Get(ctx, "key"); err != nil {
			return errors.Wrap(err, "read from other txn in future")
		}

		return txn.Put(ctx, "key", "value")
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

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
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
		tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "defaultdb", tableName)
		lease := leaseManager.names.get(
			context.Background(),
			tableDesc.GetParentID(),
			descpb.ID(keys.PublicSchemaID),
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
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
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

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Rename.
	if _, err := db.Exec("ALTER TABLE t.test RENAME TO t.test2;"); err != nil {
		t.Fatal(err)
	}

	// Check that the cache has been updated.
	if leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"test",
		s.Clock().Now(),
	) != nil {
		t.Fatalf("old name still in cache")
	}

	lease := leaseManager.names.get(
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

	// Rename to a different database.
	if _, err := db.Exec("ALTER TABLE t.test2 RENAME TO t1.test2;"); err != nil {
		t.Fatal(err)
	}

	// Re-read the descriptor, to get the new ParentID.
	newTableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t1", "test2")
	if tableDesc.GetParentID() == newTableDesc.GetParentID() {
		t.Fatalf("database didn't change")
	}

	// Check that the cache has been updated.
	if leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"test2",
		s.Clock().Now(),
	) != nil {
		t.Fatalf("old name still in cache")
	}

	lease = leaseManager.names.get(
		context.Background(),
		newTableDesc.GetParentID(),
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
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	leaseManager := s.LeaseManager().(*Manager)

	const tableName = "test"

	if _, err := db.Exec(fmt.Sprintf(`
CREATE DATABASE t;
CREATE TABLE t.%s (k CHAR PRIMARY KEY, v CHAR);
`, tableName)); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName)

	// Check the assumptions this tests makes: that there is a cache entry
	// (with a valid lease).
	if lease := leaseManager.names.get(
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
	if lease := leaseManager.names.get(
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
	removalTracker := NewLeaseRemovalTracker()
	testingKnobs := base.TestingKnobs{
		SQLLeaseManager: &ManagerTestingKnobs{
			LeaseStoreTestingKnobs: StorageTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{Knobs: testingKnobs})
	defer s.Stopper().Stop(context.Background())
	leaseManager := s.LeaseManager().(*Manager)

	const tableName = "test"

	if _, err := db.Exec(fmt.Sprintf(`
CREATE DATABASE t;
CREATE TABLE t.%s (k CHAR PRIMARY KEY, v CHAR);
`, tableName)); err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName)

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.test;"); err != nil {
		t.Fatal(err)
	}

	// There is a cache entry.
	lease := leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		tableName,
		s.Clock().Now(),
	)
	if lease == nil {
		t.Fatalf("name cache has no unexpired entry for (%d, %s)", tableDesc.GetParentID(), tableName)
	}
	expiration := lease.Expiration()
	tracker := removalTracker.TrackRemoval(lease.Descriptor)

	// Acquire another lease.
	if _, err := acquireNodeLease(context.Background(), leaseManager, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	// Check the name resolves to the new lease.
	newLease := leaseManager.names.get(context.Background(), tableDesc.GetParentID(), tableDesc.GetParentSchemaID(), tableName, s.Clock().Now())
	if newLease == nil {
		t.Fatalf("name cache doesn't contain entry for (%d, %s)", tableDesc.GetParentID(), tableName)
	}
	if newLease.Expiration() == expiration {
		t.Fatalf("same lease %s %s", expiration.GoTime(), newLease.Expiration().GoTime())
	}

	// TODO(ajwerner): does this matter?
	lease.Release(context.Background())

	// The first lease acquisition was released.
	if err := tracker.WaitForRemoval(); err != nil {
		t.Fatal(err)
	}

	newLease.Release(context.Background())
}

// Test that table names are treated as case sensitive by the name cache.
func TestTableNameCaseSensitive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
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

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Check that we cannot get the table by a different name.
	if leaseManager.names.get(
		context.Background(),
		tableDesc.GetParentID(),
		tableDesc.GetParentSchemaID(),
		"tEsT",
		s.Clock().Now(),
	) != nil {
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
	removalTracker := NewLeaseRemovalTracker()
	testingKnobs := base.TestingKnobs{
		SQLLeaseManager: &ManagerTestingKnobs{
			LeaseStoreTestingKnobs: StorageTestingKnobs{
				LeaseReleasedEvent:     removalTracker.LeaseRemovedNotification,
				RemoveOnceDereferenced: true,
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{Knobs: testingKnobs})
	defer s.Stopper().Stop(context.Background())
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

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
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

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
	s, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{Knobs: testingKnobs})
	defer s.Stopper().Stop(context.Background())
	leaseManager := s.LeaseManager().(*Manager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

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
// 1. Thread 1 calls either acquireFreshestFromStore() or acquire().
// 2. Thread 1 releases the lock on descriptorState and starts acquisition of a lease
//    from the store, blocking until it's finished.
// 3. Thread 2 calls acquire(). The lease has not been acquired yet, so it
//    also enters the acquisition code path (calling DoChan).
// 4. Thread 2 proceeds to release the lock on descriptorState waiting for the
//    in-flight acquisition.
// 4. The lease is acquired from the store and the waiting routines are
//    unblocked.
// 5. Thread 2 unblocks first, and releases the new lease, for whatever reason.
// 5. Thread 1 wakes up. At this point, a naive implementation would use the
//    newly acquired lease, which would be incorrect. The test checks that
//    acquireFreshestFromStore() or acquire() notices, after re-acquiring the
//    descriptorState lock, that the new lease has been released and acquires a new
//    one.
func TestLeaseAcquireAndReleaseConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.WithIssue(t, 51798, "fails in the presence of migrations requiring backfill, "+
		"but cannot import sqlmigrations")

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
			res.exp = table.Expiration()
		}
		return res
	}

	descID := descpb.ID(keys.LeaseTableID)

	// acquireBlock calls Acquire.
	acquireBlock := func(
		ctx context.Context,
		m *Manager,
		acquireChan chan Result,
	) {
		acquireChan <- mkResult(m.Acquire(ctx, m.storage.clock.Now(), descID))
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
					LeaseStoreTestingKnobs: StorageTestingKnobs{
						RemoveOnceDereferenced: true,
						LeaseReleasedEvent:     removalTracker.LeaseRemovedNotification,
						LeaseAcquireResultBlockEvent: func(leaseBlockType AcquireBlockType) {
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
						LeaseAcquiredEvent: func(_ catalog.Descriptor, _ error) {
							atomic.AddInt32(&leasesAcquiredCount, 1)
							<-acquisitionBlock
						},
					},
				},
			}

			serverArgs := base.TestServerArgs{Knobs: testingKnobs}

			// The LeaseJitterFraction is zero so leases will have
			// monotonically increasing expiration. This prevents two leases
			// from having the same expiration due to randomness, as the
			// leases are checked for having a different expiration.
			LeaseJitterFraction.Override(ctx, &serverArgs.SV, 0)

			s, _, _ := serverutils.StartServer(
				t, serverArgs)
			defer s.Stopper().Stop(context.Background())
			leaseManager := s.LeaseManager().(*Manager)

			acquireResultChan := make(chan Result)

			// Start two routines to acquire and release.
			preblock.Add(2)
			go acquireBlock(ctx, leaseManager, acquireResultChan)
			if test.isSecondCallAcquireFreshest {
				go func(ctx context.Context, m *Manager, acquireChan chan Result) {
					if err := m.AcquireFreshestFromStore(ctx, descID); err != nil {
						acquireChan <- mkResult(nil, err)
						return
					}
					acquireChan <- mkResult(m.Acquire(ctx, s.Clock().Now(), descID))
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
