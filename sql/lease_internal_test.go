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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// Note that there's also a lease_test.go, in package sql_test.

package sql

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestLeaseSet(t *testing.T) {
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
		{insert{2, 1}, "2:1 2:3"},
		{newest{0}, "2:3"},
		{newest{2}, "2:3"},
		{newest{3}, "<nil>"},
		{insert{2, 4}, "2:1 2:3 2:4"},
		{newest{0}, "2:4"},
		{newest{2}, "2:4"},
		{newest{3}, "<nil>"},
		{insert{2, 2}, "2:1 2:2 2:3 2:4"},
		{insert{3, 1}, "2:1 2:2 2:3 2:4 3:1"},
		{newest{0}, "3:1"},
		{newest{1}, "<nil>"},
		{newest{2}, "2:4"},
		{newest{3}, "3:1"},
		{newest{4}, "<nil>"},
		{insert{1, 1}, "1:1 2:1 2:2 2:3 2:4 3:1"},
		{newest{0}, "3:1"},
		{newest{1}, "1:1"},
		{newest{2}, "2:4"},
		{newest{3}, "3:1"},
		{newest{4}, "<nil>"},
		{remove{2, 4}, "1:1 2:1 2:2 2:3 3:1"},
		{remove{3, 1}, "1:1 2:1 2:2 2:3"},
		{remove{1, 1}, "2:1 2:2 2:3"},
		{remove{2, 2}, "2:1 2:3"},
		{remove{2, 3}, "2:1"},
		{remove{2, 1}, ""},
	}

	set := &leaseSet{}
	for i, d := range testData {
		switch op := d.op.(type) {
		case insert:
			s := &LeaseState{}
			s.Version = op.version
			s.expiration.Time = time.Unix(0, op.expiration)
			set.insert(s)
		case remove:
			s := &LeaseState{}
			s.Version = op.version
			s.expiration.Time = time.Unix(0, op.expiration)
			set.remove(s)
		case newest:
			n := set.findNewest(op.version)
			s := "<nil>"
			if n != nil {
				s = fmt.Sprintf("%d:%d", n.Version, n.Expiration().UnixNano())
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

func getNumLeases(ts *tableState) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.active.data)
}

func TestPurgeOldLeases(t *testing.T) {
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
	defer s.Stopper().Stop()
	leaseManager := s.LeaseManager().(*LeaseManager)
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

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	var leases []*LeaseState
	err := kvDB.Txn(func(txn *client.Txn) error {
		for i := 0; i < 3; i++ {
			lease, err := leaseManager.acquireFreshestFromStore(txn, tableDesc.ID)
			if err != nil {
				t.Fatal(err)
			}
			leases = append(leases, lease)
			if err := leaseManager.Release(lease); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := leaseManager.findTableState(tableDesc.ID, false)
	if numLeases := getNumLeases(ts); numLeases != 3 {
		t.Fatalf("found %d leases instead of 3", numLeases)
	}

	if err := ts.purgeOldLeases(
		kvDB, false, 1 /* minVersion */, leaseManager.LeaseStore); err != nil {
		t.Fatal(err)
	}

	if numLeases := getNumLeases(ts); numLeases != 1 {
		t.Fatalf("found %d leases instead of 1", numLeases)
	}
	ts.mu.Lock()
	correctLease := ts.active.data[0] == leases[2]
	ts.mu.Unlock()
	if !correctLease {
		t.Fatalf("wrong lease survived purge")
	}
}

// Test that changing a descriptor's name updates the name cache.
func TestNameCacheIsUpdated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	leaseManager := s.LeaseManager().(*LeaseManager)

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

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Rename.
	if _, err := db.Exec("ALTER TABLE t.test RENAME TO t.test2;"); err != nil {
		t.Fatal(err)
	}

	// Check that the cache has been updated.
	if leaseManager.tableNames.get(tableDesc.ParentID, "test", s.Clock()) != nil {
		t.Fatalf("old name still in cache")
	}

	lease := leaseManager.tableNames.get(tableDesc.ParentID, "test2", s.Clock())
	if lease == nil {
		t.Fatalf("new name not found in cache")
	}
	if lease.ID != tableDesc.ID {
		t.Fatalf("new name has wrong ID: %d (expected: %d)", lease.ID, tableDesc.ID)
	}
	if err := leaseManager.Release(lease); err != nil {
		t.Fatal(err)
	}

	// Rename to a different database.
	if _, err := db.Exec("ALTER TABLE t.test2 RENAME TO t1.test2;"); err != nil {
		t.Fatal(err)
	}

	// Re-read the descriptor, to get the new ParentID.
	newTableDesc := sqlbase.GetTableDescriptor(kvDB, "t1", "test2")
	if tableDesc.ParentID == newTableDesc.ParentID {
		t.Fatalf("database didn't change")
	}

	// Check that the cache has been updated.
	if leaseManager.tableNames.get(tableDesc.ParentID, "test2", s.Clock()) != nil {
		t.Fatalf("old name still in cache")
	}

	lease = leaseManager.tableNames.get(newTableDesc.ParentID, "test2", s.Clock())
	if lease == nil {
		t.Fatalf("new name not found in cache")
	}
	if lease.ID != tableDesc.ID {
		t.Fatalf("new name has wrong ID: %d (expected: %d)", lease.ID, tableDesc.ID)
	}
	if err := leaseManager.Release(lease); err != nil {
		t.Fatal(err)
	}
}

// Tests that a name cache entry with by an expired lease is not returned.
func TestNameCacheEntryDoesntReturnExpiredLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	leaseManager := s.LeaseManager().(*LeaseManager)

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

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Check the assumptions this tests makes: that there is a cache entry
	// (with a valid lease).
	lease := leaseManager.tableNames.get(tableDesc.ParentID, "test", s.Clock())
	if lease == nil {
		t.Fatalf("no name cache entry")
	}
	if err := leaseManager.Release(lease); err != nil {
		t.Fatal(err)
	}
	// Advance the clock to expire the lease.
	s.Clock().SetMaxOffset(10 * LeaseDuration)
	s.Clock().Update(s.Clock().Now().Add(int64(2*LeaseDuration), 0))

	// Check the name no longer resolves.
	if leaseManager.tableNames.get(tableDesc.ParentID, "test", s.Clock()) != nil {
		t.Fatalf("name resolves when it shouldn't")
	}
}

// Test that table names are not treated as case sensitive by the name cache.
func TestTableNameNotCaseSensitive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	leaseManager := s.LeaseManager().(*LeaseManager)

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

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Check that we can get the table by a different name.
	lease := leaseManager.tableNames.get(tableDesc.ParentID, "tEsT", s.Clock())
	if lease == nil {
		t.Fatalf("no name cache entry")
	}
	if err := leaseManager.Release(lease); err != nil {
		t.Fatal(err)
	}
}

// Test that there's no deadlock between AcquireByName and Release.
// We used to have one due to lock inversion between the tableNameCache lock and
// the leaseState lock, triggered when the same lease was Release()d after the
// table had been deleted (which means it's removed from the tableNameCache) and
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
	defer s.Stopper().Stop()
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Populate the name cache.
	var lease *LeaseState
	if err := kvDB.Txn(func(txn *client.Txn) error {
		var err error
		lease, err = leaseManager.AcquireByName(txn, tableDesc.ParentID, "test")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := leaseManager.Release(lease); err != nil {
		t.Fatal(err)
	}

	// Pretend the table has been deleted, so that when we release leases on it,
	// they are removed from the tableNameCache too.
	tableState := leaseManager.findTableState(tableDesc.ID, true)
	tableState.deleted = true

	// Try to trigger the race repeatedly: race an AcquireByName against a
	// Release.
	// leaseChan acts as a barrier, synchornizing the two routines at every
	// iteration.
	leaseChan := make(chan *LeaseState)
	errChan := make(chan error)
	go func() {
		for lease := range leaseChan {
			// Move errors to the main goroutine.
			errChan <- leaseManager.Release(lease)
		}
	}()

	for i := 0; i < 50; i++ {
		var leaseByName *LeaseState
		if err := kvDB.Txn(func(txn *client.Txn) error {
			var err error
			lease, err := leaseManager.AcquireByName(txn, tableDesc.ParentID, "test")
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
			tracker := removalTracker.TrackRemoval(lease)
			// Start the race: signal the other guy to release, and we do another
			// acquire at the same time.
			leaseChan <- lease
			leaseByName, err = leaseManager.AcquireByName(txn, tableDesc.ParentID, "test")
			if err != nil {
				t.Fatal(err)
			}
			tracker2 := removalTracker.TrackRemoval(leaseByName)
			// See if there was an error releasing lease.
			err = <-errChan
			if err != nil {
				t.Fatal(err)
			}

			// Depending on how the race went, there are two cases - either the
			// AcquireByName ran first, and got the same lease as we already had,
			// or the Release ran first and so we got a new lease.
			if leaseByName == lease {
				if lease.Refcount() != 1 {
					t.Fatalf("expected refcount 1, got %d", lease.Refcount())
				}
				if err := leaseManager.Release(lease); err != nil {
					t.Fatal(err)
				}
				if err := tracker.WaitForRemoval(); err != nil {
					t.Fatal(err)
				}
			} else {
				if lease.Refcount() != 0 {
					t.Fatalf("expected refcount 0, got %d", lease.Refcount())
				}
				if err := leaseManager.Release(leaseByName); err != nil {
					t.Fatal(err)
				}
				if err := tracker2.WaitForRemoval(); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	close(leaseChan)
}

// TestAcquireFreshestFromStoreRaces runs
// LeaseManager.acquireFreshestFromStore() in parallel to test for races.
func TestAcquireFreshestFromStoreRaces(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()
	leaseManager := s.LeaseManager().(*LeaseManager)

	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
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
			err := kvDB.Txn(func(txn *client.Txn) error {
				lease, err := leaseManager.acquireFreshestFromStore(txn, tableDesc.ID)
				if err != nil {
					return err
				}
				if err := leaseManager.Release(lease); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
