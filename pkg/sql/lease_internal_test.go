// Copyright 2015 The Cockroach Authors.
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

// Note that there's also a lease_test.go, in package sql_test.

package sql

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTableSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type data struct {
		version    sqlbase.DescriptorVersion
		expiration int64
	}
	type insert data
	type remove data

	type newest struct {
		version sqlbase.DescriptorVersion
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

	set := &tableSet{}
	for i, d := range testData {
		switch op := d.op.(type) {
		case insert:
			s := &tableVersionState{}
			s.Version = op.version
			s.expiration = hlc.Timestamp{WallTime: op.expiration}
			set.insert(s)

		case remove:
			s := &tableVersionState{}
			s.Version = op.version
			s.expiration = hlc.Timestamp{WallTime: op.expiration}
			set.remove(s)

		case newest:
			n := set.findNewest()
			if op.version != 0 {
				n = set.findVersion(op.version)
			}
			s := "<nil>"
			if n != nil {
				s = fmt.Sprintf("%d:%d", n.Version, n.expiration.WallTime)
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

func getNumVersions(ts *tableState) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.mu.active.data)
}

func TestPurgeOldVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We're going to block gossip so it doesn't come randomly and clear up the
	// leases we're artificially setting up.
	gossipSem := make(chan struct{}, 1)
	serverParams := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &LeaseManagerTestingKnobs{
				GossipUpdateEvent: func(cfg config.SystemConfig) {
					gossipSem <- struct{}{}
					<-gossipSem
				},
			},
		},
	}
	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)
	// Block gossip.
	gossipSem <- struct{}{}
	defer func() {
		// Unblock gossip.
		<-gossipSem
	}()

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	var tables []sqlbase.TableDescriptor
	var expiration hlc.Timestamp
	getLeases := func() {
		for i := 0; i < 3; i++ {
			if err := leaseManager.acquireFreshestFromStore(context.TODO(), tableDesc.ID); err != nil {
				t.Fatal(err)
			}
			table, exp, err := leaseManager.Acquire(context.TODO(), s.Clock().Now(), tableDesc.ID)
			if err != nil {
				t.Fatal(err)
			}
			tables = append(tables, *table)
			expiration = exp
			if err := leaseManager.Release(table); err != nil {
				t.Fatal(err)
			}
		}
	}
	getLeases()
	ts := leaseManager.findTableState(tableDesc.ID, false)
	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
	// Publish a new version for the table
	if _, err := leaseManager.Publish(context.TODO(), tableDesc.ID, func(*sqlbase.TableDescriptor) error {
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}

	getLeases()
	ts = leaseManager.findTableState(tableDesc.ID, false)
	if numLeases := getNumVersions(ts); numLeases != 2 {
		t.Fatalf("found %d versions instead of 2", numLeases)
	}
	if err := ts.purgeOldVersions(
		context.TODO(), kvDB, false, 2 /* minVersion */, leaseManager); err != nil {
		t.Fatal(err)
	}

	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
	ts.mu.Lock()
	correctLease := ts.mu.active.data[0].TableDescriptor.ID == tables[5].ID &&
		ts.mu.active.data[0].TableDescriptor.Version == tables[5].Version
	correctExpiration := ts.mu.active.data[0].expiration == expiration
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
	tableVersion := &tableVersionState{
		TableDescriptor: tables[0],
		expiration:      tables[5].ModificationTime,
	}
	ts.mu.active.insert(tableVersion)
	ts.mu.Unlock()
	if numLeases := getNumVersions(ts); numLeases != 2 {
		t.Fatalf("found %d versions instead of 2", numLeases)
	}
	if err := ts.purgeOldVersions(
		context.TODO(), kvDB, false, 2 /* minVersion */, leaseManager); err != nil {
		t.Fatal(err)
	}
	if numLeases := getNumVersions(ts); numLeases != 1 {
		t.Fatalf("found %d versions instead of 1", numLeases)
	}
}

// Test that changing a descriptor's name updates the name cache.
func TestNameCacheIsUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE DATABASE t1;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.public.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Rename.
	if _, err := db.Exec("ALTER TABLE t.public.test RENAME TO t.public.test2;"); err != nil {
		t.Fatal(err)
	}

	// Check that the cache has been updated.
	if leaseManager.tableNames.get(tableDesc.ParentID, "test", s.Clock().Now()) != nil {
		t.Fatalf("old name still in cache")
	}

	lease := leaseManager.tableNames.get(tableDesc.ParentID, "test2", s.Clock().Now())
	if lease == nil {
		t.Fatalf("new name not found in cache")
	}
	if lease.ID != tableDesc.ID {
		t.Fatalf("new name has wrong ID: %d (expected: %d)", lease.ID, tableDesc.ID)
	}
	if err := leaseManager.Release(&lease.TableDescriptor); err != nil {
		t.Fatal(err)
	}

	// Rename to a different database.
	if _, err := db.Exec("ALTER TABLE t.public.test2 RENAME TO t1.public.test2;"); err != nil {
		t.Fatal(err)
	}

	// Re-read the descriptor, to get the new ParentID.
	newTableDesc := sqlbase.GetTableDescriptor(kvDB, "t1", "test2")
	if tableDesc.ParentID == newTableDesc.ParentID {
		t.Fatalf("database didn't change")
	}

	// Check that the cache has been updated.
	if leaseManager.tableNames.get(tableDesc.ParentID, "test2", s.Clock().Now()) != nil {
		t.Fatalf("old name still in cache")
	}

	lease = leaseManager.tableNames.get(newTableDesc.ParentID, "test2", s.Clock().Now())
	if lease == nil {
		t.Fatalf("new name not found in cache")
	}
	if lease.ID != tableDesc.ID {
		t.Fatalf("new name has wrong ID: %d (expected: %d)", lease.ID, tableDesc.ID)
	}
	if err := leaseManager.Release(&lease.TableDescriptor); err != nil {
		t.Fatal(err)
	}
}

// Tests that a name cache entry with by an expired lease is not returned.
func TestNameCacheEntryDoesntReturnExpiredLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)

	const tableName = "test"

	if _, err := db.Exec(fmt.Sprintf(`
CREATE DATABASE t;
CREATE TABLE t.public.%s (k CHAR PRIMARY KEY, v CHAR);
`, tableName)); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.public.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", tableName)

	// Check the assumptions this tests makes: that there is a cache entry
	// (with a valid lease).
	if lease := leaseManager.tableNames.get(tableDesc.ParentID, tableName, s.Clock().Now()); lease == nil {
		t.Fatalf("name cache has no unexpired entry for (%d, %s)", tableDesc.ParentID, tableName)
	} else {
		if err := leaseManager.Release(&lease.TableDescriptor); err != nil {
			t.Fatal(err)
		}
	}

	leaseManager.ExpireLeases(s.Clock())

	// Check the name no longer resolves.
	if lease := leaseManager.tableNames.get(tableDesc.ParentID, tableName, s.Clock().Now()); lease != nil {
		t.Fatalf("name cache has unexpired entry for (%d, %s): %s", tableDesc.ParentID, tableName, lease)
	}
}

// Test that table names are treated as case sensitive by the name cache.
func TestTableNameCaseSensitive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	// Populate the name cache.
	if _, err := db.Exec("SELECT * FROM t.public.test;"); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Check that we cannot get the table by a different name.
	if leaseManager.tableNames.get(tableDesc.ParentID, "tEsT", s.Clock().Now()) != nil {
		t.Fatalf("lease manager incorrectly found table with different case")
	}
}

// Test that there's no deadlock between AcquireByName and Release.
// We used to have one due to lock inversion between the tableNameCache lock and
// the tableVersionState lock, triggered when the same lease was Release()d after the
// table had been dropped (which means it's removed from the tableNameCache) and
// AcquireByName()d at the same time.
func TestReleaseAcquireByNameDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	removalTracker := NewLeaseRemovalTracker()
	testingKnobs := base.TestingKnobs{
		SQLLeaseManager: &LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: LeaseStoreTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{Knobs: testingKnobs})
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Populate the name cache.
	ctx := context.TODO()
	table, _, err := leaseManager.AcquireByName(
		ctx, leaseManager.execCfg.Clock.Now(), tableDesc.ParentID, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := leaseManager.Release(table); err != nil {
		t.Fatal(err)
	}

	// Pretend the table has been dropped, so that when we release leases on it,
	// they are removed from the tableNameCache too.
	tableState := leaseManager.findTableState(tableDesc.ID, true)
	tableState.mu.Lock()
	tableState.mu.dropped = true
	tableState.mu.Unlock()

	// Try to trigger the race repeatedly: race an AcquireByName against a
	// Release.
	// tableChan acts as a barrier, synchronizing the two routines at every
	// iteration.
	tableChan := make(chan *sqlbase.TableDescriptor)
	errChan := make(chan error)
	go func() {
		for table := range tableChan {
			// Move errors to the main goroutine.
			errChan <- leaseManager.Release(table)
		}
	}()

	for i := 0; i < 50; i++ {
		timestamp := leaseManager.execCfg.Clock.Now()
		ctx := context.TODO()
		table, _, err := leaseManager.AcquireByName(ctx, timestamp, tableDesc.ParentID, "test")
		if err != nil {
			t.Fatal(err)
		}
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
		tableChan <- table
		tableByName, _, err := leaseManager.AcquireByName(ctx, timestamp, tableDesc.ParentID, "test")
		if err != nil {
			t.Fatal(err)
		}
		tracker2 := removalTracker.TrackRemoval(tableByName)
		// See if there was an error releasing lease.
		err = <-errChan
		if err != nil {
			t.Fatal(err)
		}

		// Depending on how the race went, there are two cases - either the
		// AcquireByName ran first, and got the same lease as we already had,
		// or the Release ran first and so we got a new lease.
		if tableByName.ID == table.ID {
			if err := leaseManager.Release(table); err != nil {
				t.Fatal(err)
			}
			if err := tracker.WaitForRemoval(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := leaseManager.Release(tableByName); err != nil {
				t.Fatal(err)
			}
			if err := tracker2.WaitForRemoval(); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(tableChan)
}

// TestAcquireFreshestFromStoreRaces runs
// LeaseManager.acquireFreshestFromStore() in parallel to test for races.
func TestAcquireFreshestFromStoreRaces(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	var wg sync.WaitGroup
	numRoutines := 10
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func() {
			defer wg.Done()
			if err := leaseManager.acquireFreshestFromStore(context.TODO(), tableDesc.ID); err != nil {
				t.Error(err)
			}
			table, _, err := leaseManager.Acquire(context.TODO(), s.Clock().Now(), tableDesc.ID)
			if err != nil {
				t.Error(err)
			}
			if err := leaseManager.Release(table); err != nil {
				t.Error(err)
			}
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
		SQLLeaseManager: &LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: LeaseStoreTestingKnobs{
				// Immediate remove tableVersionState and release its
				// lease when it is dereferenced. This forces threads
				// waiting on a lease to reacquire the lease.
				RemoveOnceDereferenced: true,
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(
		t, base.TestServerArgs{Knobs: testingKnobs})
	defer s.Stopper().Stop(context.TODO())
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	var wg sync.WaitGroup
	numRoutines := 10
	now := s.Clock().Now()
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func() {
			defer wg.Done()
			table, _, err := leaseManager.Acquire(context.TODO(), now, tableDesc.ID)
			if err != nil {
				t.Error(err)
			}
			if err := leaseManager.Release(table); err != nil {
				t.Error(err)
			}
		}()
	}

	wg.Wait()
}

// Test one possible outcome of a race between a lease acquisition (the first
// case through tableState.acquire(), the second through
// tableState.acquireFreshestFromStore()) and a release of the lease that was
// just acquired. Precisely:
// 1. Thread 1 calls either acquireFreshestFromStore() or acquire().
// 2. Thread 1 releases the lock on tableState and starts acquisition of a lease
//    from the store, blocking until it's finished.
// 3. Thread 2 calls acquire(). The lease has not been acquired yet, so it
//    also enters the acquisition code path (calling DoChan).
// 4. Thread 2 proceeds to release the lock on tableState waiting for the
//    in-flight acquisition.
// 4. The lease is acquired from the store and the waiting routines are
//    unblocked.
// 5. Thread 2 unblocks first, and releases the new lease, for whatever reason.
// 5. Thread 1 wakes up. At this point, a naive implementation would use the
//    newly acquired lease, which would be incorrect. The test checks that
//    acquireFreshestFromStore() or acquire() notices, after re-acquiring the
//    tableState lock, that the new lease has been released and acquires a new
//    one.
func TestLeaseAcquireAndReleaseConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("fails in the presence of migrations requiring backfill, but cannot import sqlmigrations")

	// Result is a struct for moving results to the main result routine.
	type Result struct {
		table *sqlbase.TableDescriptor
		exp   hlc.Timestamp
		err   error
	}

	descID := sqlbase.ID(keys.LeaseTableID)

	// acquireBlock calls Acquire.
	acquireBlock := func(
		ctx context.Context,
		m *LeaseManager,
		acquireChan chan Result,
	) {
		table, e, err := m.Acquire(ctx, m.execCfg.Clock.Now(), descID)
		acquireChan <- Result{err: err, exp: e, table: table}
	}

	testCases := []struct {
		name string
		// Whether the second routine is a call to LeaseManager.acquireFreshest or
		// not. This determines which channel we unblock.
		isSecondCallAcquireFreshest bool
	}{

		// Checks what happens when the race between between acquire() and
		// lease release occurs.
		{
			name: "CallAcquireConcurrently",
			isSecondCallAcquireFreshest: false,
		},
		// Checks what happens when the race between
		// acquireFreshestFromStore() and lease release occurs.
		{
			name: "CallAcquireFreshestAndAcquireConcurrently",
			isSecondCallAcquireFreshest: true,
		},
	}

	for _, test := range testCases {
		ctx := log.WithLogTag(context.Background(), "test: Lease", nil)

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
				SQLLeaseManager: &LeaseManagerTestingKnobs{
					LeaseStoreTestingKnobs: LeaseStoreTestingKnobs{
						RemoveOnceDereferenced: true,
						LeaseReleasedEvent:     removalTracker.LeaseRemovedNotification,
						LeaseAcquireResultBlockEvent: func(leaseBlockType LeaseAcquireBlockType) {
							if leaseBlockType == LeaseAcquireBlock {
								if count := atomic.LoadInt32(&acquireArrivals); (count < 1 && test.isSecondCallAcquireFreshest) ||
									(count < 2 && !test.isSecondCallAcquireFreshest) {
									atomic.AddInt32(&acquireArrivals, 1)
									preblock.Done()
									<-blockChan
								}
							} else if leaseBlockType == LeaseAcquireFreshestBlock {
								if atomic.LoadInt32(&acquireFreshestArrivals) < 1 {
									atomic.AddInt32(&acquireFreshestArrivals, 1)
									preblock.Done()
									<-freshestBlockChan
								}
							}
						},
						LeaseAcquiredEvent: func(_ sqlbase.TableDescriptor, _ error) {
							atomic.AddInt32(&leasesAcquiredCount, 1)
							<-acquisitionBlock
						},
					},
				},
			}

			serverArgs := base.TestServerArgs{Knobs: testingKnobs}

			serverArgs.LeaseManagerConfig = base.NewLeaseManagerConfig()
			// The LeaseJitterFraction is zero so leases will have
			// monotonically increasing expiration. This prevents two leases
			// from having the same expiration due to randomness, as the
			// leases are checked for having a different expiration.
			serverArgs.LeaseManagerConfig.TableDescriptorLeaseJitterFraction = 0.0

			s, _, _ := serverutils.StartServer(
				t, serverArgs)
			defer s.Stopper().Stop(context.TODO())
			leaseManager := s.LeaseManager().(*LeaseManager)

			acquireResultChan := make(chan Result)

			// Start two routines to acquire and release.
			preblock.Add(2)
			go acquireBlock(ctx, leaseManager, acquireResultChan)
			if test.isSecondCallAcquireFreshest {
				go func(ctx context.Context, m *LeaseManager, acquireChan chan Result) {
					if err := m.acquireFreshestFromStore(ctx, descID); err != nil {
						acquireChan <- Result{err: err, exp: hlc.Timestamp{}, table: nil}
						return
					}
					table, e, err := m.Acquire(ctx, s.Clock().Now(), descID)
					acquireChan <- Result{err: err, exp: e, table: table}
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
			tracker := removalTracker.TrackRemoval(result1.table)
			if err := leaseManager.Release(result1.table); err != nil {
				t.Fatal(err)
			}
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
