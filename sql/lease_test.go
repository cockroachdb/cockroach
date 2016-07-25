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

// Note that there's also lease_internal_test.go, in package sql.

package sql_test

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/server"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

type leaseTest struct {
	*testing.T
	server                   serverutils.TestServerInterface
	db                       *gosql.DB
	kvDB                     *client.DB
	nodes                    map[uint32]*csql.LeaseManager
	leaseManagerTestingKnobs csql.LeaseManagerTestingKnobs
}

func newLeaseTest(t *testing.T, params base.TestServerArgs) *leaseTest {
	s, db, kvDB := serverutils.StartServer(t, params)
	leaseTest := &leaseTest{
		T:      t,
		server: s,
		db:     db,
		kvDB:   kvDB,
		nodes:  map[uint32]*csql.LeaseManager{},
	}
	if params.Knobs.SQLLeaseManager != nil {
		leaseTest.leaseManagerTestingKnobs =
			*params.Knobs.SQLLeaseManager.(*csql.LeaseManagerTestingKnobs)
	}
	return leaseTest
}

func (t *leaseTest) cleanup() {
	t.server.Stopper().Stop()
}

func (t *leaseTest) getLeases(descID sqlbase.ID) string {
	sql := `
SELECT version, nodeID FROM system.lease WHERE descID = $1 ORDER BY version, nodeID
`
	rows, err := t.db.Query(sql, descID)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	var prefix string
	for rows.Next() {
		var (
			version int
			nodeID  int
		)
		if err := rows.Scan(&version, &nodeID); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&buf, "%s/%d/%d", prefix, version, nodeID)
		prefix = " "
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func (t *leaseTest) expectLeases(descID sqlbase.ID, expected string) {
	leases := t.getLeases(descID)
	if expected != leases {
		t.Fatalf("expected %s, but found %s", expected, leases)
	}
}

func (t *leaseTest) acquire(nodeID uint32, descID sqlbase.ID, version sqlbase.DescriptorVersion) (*csql.LeaseState, error) {
	var lease *csql.LeaseState
	err := t.kvDB.Txn(func(txn *client.Txn) error {
		var err error
		lease, err = t.node(nodeID).Acquire(txn, descID, version)
		return err
	})
	return lease, err
}

func (t *leaseTest) mustAcquire(nodeID uint32, descID sqlbase.ID, version sqlbase.DescriptorVersion) *csql.LeaseState {
	lease, err := t.acquire(nodeID, descID, version)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func (t *leaseTest) release(nodeID uint32, lease *csql.LeaseState) error {
	return t.node(nodeID).Release(lease)
}

// If leaseRemovalTracker is not nil, it will be used to block until the lease is
// released from the store. If the lease is not supposed to be released from the
// store (i.e. it's not expired and it's not for an old descriptor version),
// this shouldn't be set.
func (t *leaseTest) mustRelease(
	nodeID uint32,
	lease *csql.LeaseState,
	leaseRemovalTracker *csql.LeaseRemovalTracker,
) {
	var tracker csql.RemovalTracker
	if leaseRemovalTracker != nil {
		tracker = leaseRemovalTracker.TrackRemoval(lease)
	}
	if err := t.release(nodeID, lease); err != nil {
		t.Fatal(err)
	}
	if leaseRemovalTracker != nil {
		if err := tracker.WaitForRemoval(); err != nil {
			t.Fatal(err)
		}
	}
}

func (t *leaseTest) publish(nodeID uint32, descID sqlbase.ID) error {
	_, err := t.node(nodeID).Publish(descID, func(*sqlbase.TableDescriptor) error {
		return nil
	}, nil)
	return err
}

func (t *leaseTest) mustPublish(nodeID uint32, descID sqlbase.ID) {
	if err := t.publish(nodeID, descID); err != nil {
		t.Fatal(err)
	}
}

func (t *leaseTest) node(nodeID uint32) *csql.LeaseManager {
	mgr := t.nodes[nodeID]
	if mgr == nil {
		mgr = csql.NewLeaseManager(
			nodeID, *t.kvDB,
			t.server.Clock(),
			t.leaseManagerTestingKnobs,
			t.server.Stopper(),
		)
		t.nodes[nodeID] = mgr
	}
	return mgr
}

func TestLeaseManager(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	removalTracker := csql.NewLeaseRemovalTracker()
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &csql.LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: csql.LeaseStoreTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	// We can't acquire a lease on a non-existent table.
	expected := "descriptor not found"
	if _, err := t.acquire(1, 10000, 0); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	l1 := t.mustAcquire(1, descID, 0)
	t.expectLeases(descID, "/1/1")
	// Node 2 never acquired a lease on descID, so we should expect an error.
	if err := t.release(2, l1); err == nil {
		t.Fatalf("expected error, but found none")
	}
	t.mustRelease(1, l1, nil)
	t.expectLeases(descID, "/1/1")

	// It is an error to acquire a lease for a specific version that doesn't
	// exist yet.
	expected = "version 2 of table .* does not exist"
	if _, err := t.acquire(1, descID, 2); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	// Publish a new version and explicitly acquire it.
	l2 := t.mustAcquire(1, descID, 0)
	t.mustPublish(1, descID)
	l3 := t.mustAcquire(1, descID, 2)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the new version is released we don't
	// release the node lease.
	t.mustRelease(1, l3, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// We can still acquire a local reference on the old version since it hasn't
	// expired.
	l4 := t.mustAcquire(1, descID, 1)
	t.mustRelease(1, l4, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the old version is released the node
	// lease is also released.
	t.mustRelease(1, l2, removalTracker)
	t.expectLeases(descID, "/2/1")

	// It is an error to acquire a lease for an old version once a new version
	// exists and there are no local references for the old version.
	expected = `table \d+ unable to acquire lease on old version: 1 < 2`
	if _, err := t.acquire(1, descID, 1); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	// Acquire 2 node leases on version 2.
	l5 := t.mustAcquire(1, descID, 2)
	l6 := t.mustAcquire(2, descID, 2)
	// Publish version 3. This will succeed immediately.
	t.mustPublish(3, descID)

	// Start a goroutine to publish version 4 which will block until the version
	// 2 leases are released.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		t.mustPublish(3, descID)
		wg.Done()
	}()

	// Force both nodes ahead to version 3.
	l7 := t.mustAcquire(1, descID, 3)
	l8 := t.mustAcquire(2, descID, 3)
	t.expectLeases(descID, "/2/1 /2/2 /3/1 /3/2")

	t.mustRelease(1, l5, removalTracker)
	t.expectLeases(descID, "/2/2 /3/1 /3/2")
	t.mustRelease(2, l6, removalTracker)
	t.expectLeases(descID, "/3/1 /3/2")

	// Wait for version 4 to be published.
	wg.Wait()
	l9 := t.mustAcquire(1, descID, 4)
	t.mustRelease(1, l7, removalTracker)
	t.mustRelease(2, l8, nil)
	t.expectLeases(descID, "/3/2 /4/1")
	t.mustRelease(1, l9, nil)
	t.expectLeases(descID, "/3/2 /4/1")
}

func TestLeaseManagerReacquire(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := createTestServerParams()
	removalTracker := csql.NewLeaseRemovalTracker()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &csql.LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: csql.LeaseStoreTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	// Acquire 2 leases from the same node. They should point to the same lease
	// structure.
	l1 := t.mustAcquire(1, descID, 0)
	l2 := t.mustAcquire(1, descID, 0)
	if l1 != l2 {
		t.Fatalf("expected same lease, but found %p != %p", l1, l2)
	}
	if l1.Refcount() != 2 {
		t.Fatalf("expected refcount of 2, but found %d", l1.Refcount())
	}
	t.expectLeases(descID, "/1/1")

	// Set the minimum lease duration such that the next lease acquisition will
	// require the lease to be reacquired.
	savedLeaseDuration, savedMinLeaseDuration := csql.LeaseDuration, csql.MinLeaseDuration
	defer func() {
		csql.LeaseDuration, csql.MinLeaseDuration = savedLeaseDuration, savedMinLeaseDuration
	}()
	csql.MinLeaseDuration = l1.Expiration().Sub(timeutil.Now())
	csql.LeaseDuration = 2 * csql.MinLeaseDuration

	// Another lease acquisition from the same node will result in a new lease.
	l3 := t.mustAcquire(1, descID, 0)
	if l1 == l3 {
		t.Fatalf("expected different leases, but found %p", l1)
	}
	if l3.Refcount() != 1 {
		t.Fatalf("expected refcount of 1, but found %d", l3.Refcount())
	}
	if l3.Expiration().Before(l1.Expiration()) {
		t.Fatalf("expected new lease expiration (%s) to be after old lease expiration (%s)",
			l3.Expiration(), l1.Expiration())
	}
	t.expectLeases(descID, "/1/1 /1/1")

	t.mustRelease(1, l1, nil)
	t.mustRelease(1, l2, removalTracker)
	t.mustRelease(1, l3, nil)
}

func TestLeaseManagerPublishVersionChanged(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := createTestServerParams()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	// Start two goroutines that are concurrently trying to publish a new version
	// of the descriptor. The first goroutine progresses to the update function
	// and then signals the second goroutine to start which is allowed to proceed
	// through completion. The first goroutine is then signaled and when it
	// attempts to publish the new version it will encounter an update error and
	// retry the transaction. Upon retry it will see that the descriptor version
	// has changed and have to proceed to its outer retry loop and wait for the
	// number of leases on the previous version to drop to 0.

	n1 := t.node(1)
	n2 := t.node(2)

	n1update := make(chan struct{})
	n2start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func(n1update, n2start chan struct{}) {
		_, err := n1.Publish(descID, func(*sqlbase.TableDescriptor) error {
			if n2start != nil {
				// Signal node 2 to start.
				close(n2start)
				n2start = nil
			}
			// Wait for node 2 signal indicating that node 2 finished publication of
			// a new version.
			<-n1update
			return nil
		}, nil)
		if err != nil {
			panic(err)
		}
		wg.Done()
	}(n1update, n2start)

	go func(n1update, n2start chan struct{}) {
		// Wait for node 1 signal indicating that node 1 is in its update()
		// function.
		<-n2start
		_, err := n2.Publish(descID, func(*sqlbase.TableDescriptor) error {
			return nil
		}, nil)
		if err != nil {
			panic(err)
		}
		close(n1update)
		wg.Done()
	}(n1update, n2start)

	wg.Wait()

	t.mustAcquire(1, descID, 0)
	t.expectLeases(descID, "/3/1")
}

// Test that we fail to lease a table that was marked for deletion.
func TestCantLeaseDeletedTable(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	var mu syncutil.Mutex
	clearSchemaChangers := false

	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SyncSchemaChangersFilter: func(tscc csql.TestingSchemaChangerCollection) {
				mu.Lock()
				defer mu.Unlock()
				if clearSchemaChangers {
					tscc.ClearSchemaChangers()
				}
			},
		},
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
		},
	}

	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	_, err := t.db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	// Block schema changers so that the table we're about to DROP is not actually
	// dropped; it will be left in a "deleted" state.
	mu.Lock()
	clearSchemaChangers = true
	mu.Unlock()

	// DROP the table
	_, err = t.db.Exec(`DROP TABLE test.t`)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure we can't get a lease on the descriptor.
	tableDesc := sqlbase.GetTableDescriptor(t.kvDB, "test", "t")
	// try to acquire at a bogus version to make sure we don't get back a lease we
	// already had.
	_, err = t.acquire(1, tableDesc.ID, tableDesc.Version+1)
	if !testutils.IsError(err, "table is being deleted") {
		t.Fatalf("got a different error than expected: %s", err)
	}
}

func isDeleted(tableID sqlbase.ID, cfg config.SystemConfig) bool {
	descKey := sqlbase.MakeDescMetadataKey(tableID)
	val := cfg.GetValue(descKey)
	if val == nil {
		return false
	}
	var descriptor sqlbase.Descriptor
	if err := val.GetProto(&descriptor); err != nil {
		panic("unable to unmarshal table descriptor")
	}
	table := descriptor.GetTable()
	return table.Deleted()
}

func acquire(s *server.TestServer, descID sqlbase.ID, version sqlbase.DescriptorVersion) (*csql.LeaseState, error) {
	var lease *csql.LeaseState
	err := s.DB().Txn(func(txn *client.Txn) error {
		var err error
		lease, err = s.LeaseManager().(*csql.LeaseManager).Acquire(txn, descID, version)
		return err
	})
	return lease, err
}

// Test that once a table is marked as deleted, a lease's refcount dropping to 0
// means the lease is released immediately, as opposed to being released only
// when it expires.
func TestLeasesOnDeletedTableAreReleasedImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mu syncutil.Mutex
	clearSchemaChangers := false

	var waitTableID sqlbase.ID
	deleted := make(chan bool)

	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SyncSchemaChangersFilter: func(tscc csql.TestingSchemaChangerCollection) {
				mu.Lock()
				defer mu.Unlock()
				if clearSchemaChangers {
					tscc.ClearSchemaChangers()
				}
			},
		},
		SQLLeaseManager: &csql.LeaseManagerTestingKnobs{
			TestingLeasesRefreshedEvent: func(cfg config.SystemConfig) {
				mu.Lock()
				defer mu.Unlock()
				if waitTableID != 0 {
					if isDeleted(waitTableID, cfg) {
						close(deleted)
						waitTableID = 0
					}
				}
			},
		},
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	lease1, err := acquire(s.(*server.TestServer), tableDesc.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease2, err := acquire(s.(*server.TestServer), tableDesc.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Block schema changers so that the table we're about to DROP is not actually
	// dropped; it will be left in a "deleted" state.
	// Also install a way to wait for the config update to be processed.
	mu.Lock()
	clearSchemaChangers = true
	waitTableID = tableDesc.ID
	mu.Unlock()

	// DROP the table
	_, err = db.Exec(`DROP TABLE test.t`)
	if err != nil {
		t.Fatal(err)
	}

	// Block until the LeaseManager has processed the gossip update.
	<-deleted

	// We should still be able to acquire, because we have an active lease.
	lease3, err := acquire(s.(*server.TestServer), tableDesc.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Release everything.
	if err := s.LeaseManager().(*csql.LeaseManager).Release(lease1); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseManager().(*csql.LeaseManager).Release(lease2); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseManager().(*csql.LeaseManager).Release(lease3); err != nil {
		t.Fatal(err)
	}
	// Now we shouldn't be able to acquire any more.
	_, err = acquire(s.(*server.TestServer), tableDesc.ID, 0)
	if !testutils.IsError(err, "table is being deleted") {
		t.Fatalf("got a different error than expected: %s", err)
	}
}

// TestTxnObeysLeaseExpiration tests that a transaction is aborted when it tries
// to use a table descriptor with an expired lease.
func TestTxnObeysLeaseExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(vivek): #7031")
	// Set the lease duration such that it expires quickly.
	savedLeaseDuration, savedMinLeaseDuration := csql.LeaseDuration, csql.MinLeaseDuration
	defer func() {
		csql.LeaseDuration, csql.MinLeaseDuration = savedLeaseDuration, savedMinLeaseDuration
	}()
	csql.MinLeaseDuration = 100 * time.Millisecond
	csql.LeaseDuration = 2 * csql.MinLeaseDuration

	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	clock := s.Clock()

	// Increase the MaxOffset so that the clock can be updated to expire the
	// table leases.
	clock.SetMaxOffset(10 * csql.LeaseDuration)

	// Run a number of sql operations and expire the lease they acquire.
	runCommandAndExpireLease(t, clock, sqlDB, `INSERT INTO t.kv VALUES ('c', 'd')`)
	runCommandAndExpireLease(t, clock, sqlDB, `UPDATE t.kv SET v = 'd' WHERE k = 'a'`)
	runCommandAndExpireLease(t, clock, sqlDB, `DELETE FROM t.kv WHERE k = 'a'`)
	runCommandAndExpireLease(t, clock, sqlDB, `TRUNCATE TABLE t.kv`)
}

func runCommandAndExpireLease(t *testing.T, clock *hlc.Clock, sqlDB *gosql.DB, sql string) {
	// Run a transaction that lets its table lease expire.
	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	// Use snapshot isolation so that the transaction is pushed without being
	// restarted.
	if _, err := txn.Exec("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec(sql); err != nil {
		t.Fatal(err)
	}

	// Update the clock to expire the table lease.
	_ = clock.Update(clock.Now().Add(int64(2*csql.LeaseDuration), 0))

	// Run another transaction that pushes the above transaction.
	if _, err := sqlDB.Query("SELECT * FROM t.kv"); err != nil {
		t.Fatal(err)
	}

	// Commit and see the aborted txn.
	if err := txn.Commit(); !testutils.IsError(err, "pq: restart transaction: txn aborted") {
		t.Fatalf("%s, err = %s", sql, err)
	}
}
