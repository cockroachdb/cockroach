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
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type leaseTest struct {
	*testing.T
	server                   serverutils.TestServerInterface
	db                       *gosql.DB
	kvDB                     *client.DB
	nodes                    map[uint32]*sql.LeaseManager
	leaseManagerTestingKnobs sql.LeaseManagerTestingKnobs
}

func newLeaseTest(t *testing.T, params base.TestServerArgs) *leaseTest {
	s, db, kvDB := serverutils.StartServer(t, params)
	leaseTest := &leaseTest{
		T:      t,
		server: s,
		db:     db,
		kvDB:   kvDB,
		nodes:  map[uint32]*sql.LeaseManager{},
	}
	if params.Knobs.SQLLeaseManager != nil {
		leaseTest.leaseManagerTestingKnobs =
			*params.Knobs.SQLLeaseManager.(*sql.LeaseManagerTestingKnobs)
	}
	return leaseTest
}

func (t *leaseTest) cleanup() {
	t.server.Stopper().Stop(context.TODO())
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

func (t *leaseTest) acquire(
	nodeID uint32, descID sqlbase.ID, version sqlbase.DescriptorVersion,
) (sqlbase.TableDescriptor, hlc.Timestamp, error) {
	var table sqlbase.TableDescriptor
	var expiration hlc.Timestamp
	err := t.kvDB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		var err error
		table, expiration, err = t.node(nodeID).Acquire(ctx, txn, descID, version)
		return err
	})
	return table, expiration, err
}

func (t *leaseTest) mustAcquire(
	nodeID uint32, descID sqlbase.ID, version sqlbase.DescriptorVersion,
) (sqlbase.TableDescriptor, hlc.Timestamp) {
	table, expiration, err := t.acquire(nodeID, descID, version)
	if err != nil {
		t.Fatal(err)
	}
	return table, expiration
}

func (t *leaseTest) release(nodeID uint32, table sqlbase.TableDescriptor) error {
	return t.node(nodeID).Release(table)
}

// If leaseRemovalTracker is not nil, it will be used to block until the lease is
// released from the store. If the lease is not supposed to be released from the
// store (i.e. it's not expired and it's not for an old descriptor version),
// this shouldn't be set.
func (t *leaseTest) mustRelease(
	nodeID uint32, table sqlbase.TableDescriptor, leaseRemovalTracker *sql.LeaseRemovalTracker,
) {
	var tracker sql.RemovalTracker
	if leaseRemovalTracker != nil {
		tracker = leaseRemovalTracker.TrackRemoval(table)
	}
	if err := t.release(nodeID, table); err != nil {
		t.Fatal(err)
	}
	if leaseRemovalTracker != nil {
		if err := tracker.WaitForRemoval(); err != nil {
			t.Fatal(err)
		}
	}
}

func (t *leaseTest) publish(ctx context.Context, nodeID uint32, descID sqlbase.ID) error {
	_, err := t.node(nodeID).Publish(ctx, descID, func(*sqlbase.TableDescriptor) error {
		return nil
	}, nil)
	return err
}

func (t *leaseTest) mustPublish(ctx context.Context, nodeID uint32, descID sqlbase.ID) {
	if err := t.publish(ctx, nodeID, descID); err != nil {
		t.Fatal(err)
	}
}

func (t *leaseTest) node(nodeID uint32) *sql.LeaseManager {
	mgr := t.nodes[nodeID]
	if mgr == nil {
		nc := &base.NodeIDContainer{}
		nc.Set(context.TODO(), roachpb.NodeID(nodeID))
		mgr = sql.NewLeaseManager(
			nc, *t.kvDB,
			t.server.Clock(),
			t.leaseManagerTestingKnobs,
			t.server.Stopper(),
			&sql.MemoryMetrics{},
		)
		t.nodes[nodeID] = mgr
	}
	return mgr
}

func TestLeaseManager(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	removalTracker := sql.NewLeaseRemovalTracker()
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: sql.LeaseStoreTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID
	ctx := context.TODO()

	// We can't acquire a lease on a non-existent table.
	expected := "descriptor not found"
	if _, _, err := t.acquire(1, 10000, 0); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	l1, _ := t.mustAcquire(1, descID, 0)
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
	if _, _, err := t.acquire(1, descID, 2); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	// Publish a new version and explicitly acquire it.
	l2, _ := t.mustAcquire(1, descID, 0)
	t.mustPublish(ctx, 1, descID)
	l3, _ := t.mustAcquire(1, descID, 2)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the new version is released we don't
	// release the node lease.
	t.mustRelease(1, l3, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// We can still acquire a local reference on the old version since it hasn't
	// expired.
	l4, _ := t.mustAcquire(1, descID, 1)
	t.mustRelease(1, l4, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the old version is released the node
	// lease is also released.
	t.mustRelease(1, l2, removalTracker)
	t.expectLeases(descID, "/2/1")

	// It is an error to acquire a lease for an old version once a new version
	// exists and there are no local references for the old version.
	expected = `table \d+ unable to acquire lease on old version: 1 < 2`
	if _, _, err := t.acquire(1, descID, 1); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	// Acquire 2 node leases on version 2.
	l5, _ := t.mustAcquire(1, descID, 2)
	l6, _ := t.mustAcquire(2, descID, 2)
	// Publish version 3. This will succeed immediately.
	t.mustPublish(ctx, 3, descID)

	// Start a goroutine to publish version 4 which will block until the version
	// 2 leases are released.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		t.mustPublish(ctx, 3, descID)
		wg.Done()
	}()

	// Force both nodes ahead to version 3.
	l7, _ := t.mustAcquire(1, descID, 3)
	l8, _ := t.mustAcquire(2, descID, 3)
	t.expectLeases(descID, "/2/1 /2/2 /3/1 /3/2")

	t.mustRelease(1, l5, removalTracker)
	t.expectLeases(descID, "/2/2 /3/1 /3/2")
	t.mustRelease(2, l6, removalTracker)
	t.expectLeases(descID, "/3/1 /3/2")

	// Wait for version 4 to be published.
	wg.Wait()
	l9, _ := t.mustAcquire(1, descID, 4)
	t.mustRelease(1, l7, removalTracker)
	t.mustRelease(2, l8, nil)
	t.expectLeases(descID, "/3/2 /4/1")
	t.mustRelease(1, l9, nil)
	t.expectLeases(descID, "/3/2 /4/1")
}

func TestLeaseManagerReacquire(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := createTestServerParams()
	removalTracker := sql.NewLeaseRemovalTracker()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: sql.LeaseStoreTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	// Acquire 2 leases from the same node. They should return the same
	// table and expiration.
	l1, e1 := t.mustAcquire(1, descID, 0)
	l2, e2 := t.mustAcquire(1, descID, 0)
	if l1.ID != l2.ID || e1 != e2 {
		t.Fatalf("expected same lease, but found %v != %v", l1, l2)
	}

	t.expectLeases(descID, "/1/1")

	// Set the minimum lease duration such that the next lease acquisition will
	// require the lease to be reacquired.
	savedLeaseDuration, savedMinLeaseDuration := sql.LeaseDuration, sql.MinLeaseDuration
	defer func() {
		sql.LeaseDuration, sql.MinLeaseDuration = savedLeaseDuration, savedMinLeaseDuration
	}()

	sql.MinLeaseDuration = time.Unix(0, e1.WallTime).Sub(timeutil.Now())
	sql.LeaseDuration = 2 * sql.MinLeaseDuration

	// Another lease acquisition from the same node will result in a new lease.
	rt := removalTracker.TrackRemoval(l1)
	l3, e3 := t.mustAcquire(1, descID, 0)
	if l1.ID == l3.ID && e3.WallTime == e1.WallTime {
		t.Fatalf("expected different leases, but found %v", l1)
	}
	if e3.WallTime < e1.WallTime {
		t.Fatalf("expected new lease expiration (%s) to be after old lease expiration (%s)",
			e3, e1)
	}
	// In acquiring the new lease the older lease is released.
	if err := rt.WaitForRemoval(); err != nil {
		t.Fatal(err)
	}
	// Only one actual lease.
	t.expectLeases(descID, "/1/1")

	t.mustRelease(1, l1, nil)
	t.mustRelease(1, l2, nil)
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
		_, err := n1.Publish(context.TODO(), descID, func(*sqlbase.TableDescriptor) error {
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
		_, err := n2.Publish(context.TODO(), descID, func(*sqlbase.TableDescriptor) error {
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

func TestLeaseManagerPublishIllegalVersionChange(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := createTestServerParams()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.node(1).Publish(
		context.TODO(), keys.LeaseTableID, func(table *sqlbase.TableDescriptor) error {
			table.Version++
			return nil
		}, nil); !testutils.IsError(err, "updated version") {
		t.Fatalf("unexpected error: %+v", err)
	}
	if _, err := t.node(1).Publish(
		context.TODO(), keys.LeaseTableID, func(table *sqlbase.TableDescriptor) error {
			table.Version--
			return nil
		}, nil); !testutils.IsError(err, "updated version") {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func TestLeaseManagerDrain(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := createTestServerParams()
	leaseRemovalTracker := sql.NewLeaseRemovalTracker()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: sql.LeaseStoreTestingKnobs{
				LeaseReleasedEvent: leaseRemovalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	{
		l1, _ := t.mustAcquire(1, descID, 0)
		l2, _ := t.mustAcquire(2, descID, 0)
		t.mustRelease(1, l1, nil)
		t.expectLeases(descID, "/1/1 /1/2")

		// Removal tracker to track for node 1's lease removal once the node
		// starts draining.
		l1RemovalTracker := leaseRemovalTracker.TrackRemoval(l1)

		t.nodes[1].SetDraining(true)
		t.nodes[2].SetDraining(true)

		// Leases cannot be acquired when in draining mode.
		if _, _, err := t.acquire(1, descID, 0); !testutils.IsError(err, "cannot acquire lease when draining") {
			t.Fatalf("unexpected error: %v", err)
		}

		// Node 1's lease has a refcount of 0 and should therefore be removed from
		// the store.
		if err := l1RemovalTracker.WaitForRemoval(); err != nil {
			t.Fatal(err)
		}
		t.expectLeases(descID, "/1/2")

		// Once node 2's lease is released, the lease should be removed from the
		// store.
		t.mustRelease(2, l2, leaseRemovalTracker)
		t.expectLeases(descID, "")
	}

	{
		// Check that leases with a refcount of 0 are correctly kept in the
		// store once the drain mode has been exited.
		t.nodes[1].SetDraining(false)
		l1, _ := t.mustAcquire(1, descID, 0)
		t.mustRelease(1, l1, nil)
		t.expectLeases(descID, "/1/1")
	}
}

// Test that we fail to lease a table that was marked for deletion.
func TestCantLeaseDeletedTable(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	var mu syncutil.Mutex
	clearSchemaChangers := false

	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				mu.Lock()
				defer mu.Unlock()
				if clearSchemaChangers {
					tscc.ClearSchemaChangers()
				}
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
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
	_, _, err = t.acquire(1, tableDesc.ID, tableDesc.Version+1)
	if !testutils.IsError(err, "table is being dropped") {
		t.Fatalf("got a different error than expected: %v", err)
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
	return table.Dropped()
}

func acquire(
	ctx context.Context, s *server.TestServer, descID sqlbase.ID, version sqlbase.DescriptorVersion,
) (sqlbase.TableDescriptor, hlc.Timestamp, error) {
	var table sqlbase.TableDescriptor
	var expiration hlc.Timestamp
	err := s.DB().Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		var err error
		table, expiration, err = s.LeaseManager().(*sql.LeaseManager).Acquire(ctx, txn, descID, version)
		return err
	})
	return table, expiration, err
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
		SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
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
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				mu.Lock()
				defer mu.Unlock()
				if clearSchemaChangers {
					tscc.ClearSchemaChangers()
				}
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	stmt := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	_, err := db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	ctx := context.TODO()

	lease1, _, err := acquire(ctx, s.(*server.TestServer), tableDesc.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease2, _, err := acquire(ctx, s.(*server.TestServer), tableDesc.ID, 0)
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
	lease3, _, err := acquire(ctx, s.(*server.TestServer), tableDesc.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Release everything.
	if err := s.LeaseManager().(*sql.LeaseManager).Release(lease1); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseManager().(*sql.LeaseManager).Release(lease2); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseManager().(*sql.LeaseManager).Release(lease3); err != nil {
		t.Fatal(err)
	}
	// Now we shouldn't be able to acquire any more.
	_, _, err = acquire(ctx, s.(*server.TestServer), tableDesc.ID, 0)
	if !testutils.IsError(err, "table is being dropped") {
		t.Fatalf("got a different error than expected: %v", err)
	}
}

// TestSubqueryLeases tests that all leases acquired by a subquery are
// properly tracked and released.
func TestSubqueryLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()

	fooRelease := make(chan struct{}, 10)
	fooAcquiredCount := int32(0)
	fooReleaseCount := int32(0)

	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: sql.LeaseStoreTestingKnobs{
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(table sqlbase.TableDescriptor, _ error) {
					if table.Name == "foo" {
						atomic.AddInt32(&fooAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(table sqlbase.TableDescriptor, _ error) {
					if table.Name == "foo" {
						// Note: we don't use close(fooRelease) here because the
						// lease on "foo" may be re-acquired (and re-released)
						// multiple times, at least once for the first
						// CREATE/SELECT pair and one for the final DROP.
						atomic.AddInt32(&fooReleaseCount, 1)
						fooRelease <- struct{}{}
					}
				},
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.foo (v INT);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) > 0 {
		t.Fatalf("CREATE TABLE has acquired a lease: got %d, expected 0", atomic.LoadInt32(&fooAcquiredCount))
	}

	if _, err := sqlDB.Exec(`
SELECT EXISTS(SELECT * FROM t.foo);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) == 0 {
		t.Fatalf("subquery has not acquired a lease")
	}

	// Now wait for the release to happen. We use a local timer
	// to make the test fail faster if it needs to fail.
	timeout := time.After(5 * time.Second)
	select {
	case <-timeout:
		t.Fatal("lease from sub-query was not released")
	case <-fooRelease:
	}
}
