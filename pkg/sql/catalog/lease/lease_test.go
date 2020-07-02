// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Note that there's also lease_internal_test.go, in package sql.

package lease_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type leaseTest struct {
	testing.TB
	server                   serverutils.TestServerInterface
	db                       *gosql.DB
	kvDB                     *kv.DB
	nodes                    map[uint32]*lease.Manager
	leaseManagerTestingKnobs lease.ManagerTestingKnobs
	cfg                      *base.LeaseManagerConfig
}

func newLeaseTest(tb testing.TB, params base.TestServerArgs) *leaseTest {
	if params.LeaseManagerConfig == nil {
		params.LeaseManagerConfig = base.NewLeaseManagerConfig()
	}
	s, db, kvDB := serverutils.StartServer(tb, params)
	leaseTest := &leaseTest{
		TB:     tb,
		server: s,
		db:     db,
		kvDB:   kvDB,
		nodes:  map[uint32]*lease.Manager{},
		cfg:    params.LeaseManagerConfig,
	}
	if params.Knobs.SQLLeaseManager != nil {
		leaseTest.leaseManagerTestingKnobs =
			*params.Knobs.SQLLeaseManager.(*lease.ManagerTestingKnobs)
	}
	return leaseTest
}

func (t *leaseTest) cleanup() {
	t.server.Stopper().Stop(context.Background())
}

func (t *leaseTest) getLeases(descID sqlbase.ID) string {
	sql := `
SELECT version, "nodeID" FROM system.lease WHERE "descID" = $1 ORDER BY version, "nodeID"
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
	testutils.SucceedsSoon(t, func() error {
		leases := t.getLeases(descID)
		if expected != leases {
			return errors.Errorf("expected %s, but found %s", expected, leases)
		}
		return nil
	})
}

func (t *leaseTest) acquire(
	nodeID uint32, descID sqlbase.ID,
) (catalog.Descriptor, hlc.Timestamp, error) {
	return t.node(nodeID).Acquire(context.Background(), t.server.Clock().Now(), descID)
}

func (t *leaseTest) acquireMinVersion(
	nodeID uint32, descID sqlbase.ID, minVersion sqlbase.DescriptorVersion,
) (catalog.Descriptor, hlc.Timestamp, error) {
	return t.node(nodeID).TestingAcquireAndAssertMinVersion(
		context.Background(), t.server.Clock().Now(), descID, minVersion)
}

func (t *leaseTest) mustAcquire(
	nodeID uint32, descID sqlbase.ID,
) (catalog.Descriptor, hlc.Timestamp) {
	table, expiration, err := t.acquire(nodeID, descID)
	if err != nil {
		t.Fatal(err)
	}
	return table, expiration
}

func (t *leaseTest) mustAcquireMinVersion(
	nodeID uint32, descID sqlbase.ID, minVersion sqlbase.DescriptorVersion,
) (catalog.Descriptor, hlc.Timestamp) {
	desc, expiration, err := t.acquireMinVersion(nodeID, descID, minVersion)
	if err != nil {
		t.Fatal(err)
	}
	return desc, expiration
}

func (t *leaseTest) release(nodeID uint32, desc catalog.Descriptor) error {
	return t.node(nodeID).Release(desc)
}

// If leaseRemovalTracker is not nil, it will be used to block until the lease is
// released from the store. If the lease is not supposed to be released from the
// store (i.e. it's not expired and it's not for an old descriptor version),
// this shouldn't be set.
func (t *leaseTest) mustRelease(
	nodeID uint32, desc catalog.Descriptor, leaseRemovalTracker *lease.LeaseRemovalTracker,
) {
	var tracker lease.RemovalTracker
	if leaseRemovalTracker != nil {
		tracker = leaseRemovalTracker.TrackRemoval(desc)
	}
	if err := t.release(nodeID, desc); err != nil {
		t.Fatal(err)
	}
	if leaseRemovalTracker != nil {
		if err := tracker.WaitForRemoval(); err != nil {
			t.Fatal(err)
		}
	}
}

func (t *leaseTest) publish(ctx context.Context, nodeID uint32, descID sqlbase.ID) error {
	_, err := t.node(nodeID).Publish(ctx, descID, func(catalog.MutableDescriptor) error {
		return nil
	}, nil)
	return err
}

func (t *leaseTest) mustPublish(ctx context.Context, nodeID uint32, descID sqlbase.ID) {
	if err := t.publish(ctx, nodeID, descID); err != nil {
		t.Fatal(err)
	}
}

// node gets a Manager corresponding to a mock node. A new lease
// manager is initialized for each node. This allows for more complex
// inter-node lease testing.
func (t *leaseTest) node(nodeID uint32) *lease.Manager {
	mgr := t.nodes[nodeID]
	if mgr == nil {
		var c base.NodeIDContainer
		c.Set(context.Background(), roachpb.NodeID(nodeID))
		nc := base.NewSQLIDContainer(0, &c, true /* exposed*/)
		// Hack the ExecutorConfig that we pass to the Manager to have a
		// different node id.
		cfgCpy := t.server.ExecutorConfig().(sql.ExecutorConfig)
		cfgCpy.NodeInfo.NodeID = nc
		mgr = lease.NewLeaseManager(
			log.AmbientContext{Tracer: tracing.NewTracer()},
			nc,
			cfgCpy.DB,
			cfgCpy.Clock,
			cfgCpy.InternalExecutor,
			cfgCpy.Settings,
			cfgCpy.Codec,
			t.leaseManagerTestingKnobs,
			t.server.Stopper(),
			t.cfg,
		)
		ctx := logtags.AddTag(context.Background(), "leasemgr", nodeID)
		mgr.PeriodicallyRefreshSomeLeases(ctx)
		t.nodes[nodeID] = mgr
	}
	return mgr
}

func TestLeaseManager(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	removalTracker := lease.NewLeaseRemovalTracker()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID
	ctx := context.Background()

	// We can't acquire a lease on a non-existent table.
	expected := "descriptor not found"
	if _, _, err := t.acquire(1, 10000); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
	// Acquire 2 leases from the same node. They should return the same
	// table and expiration.
	l1, e1 := t.mustAcquire(1, descID)
	l2, e2 := t.mustAcquire(1, descID)
	if l1.GetID() != l2.GetID() {
		t.Fatalf("expected same lease, but found %v != %v", l1, l2)
	} else if e1 != e2 {
		t.Fatalf("expected same lease timestamps, but found %v != %v", e1, e2)
	}
	t.expectLeases(descID, "/1/1")
	// Node 2 never acquired a lease on descID, so we should expect an error.
	if err := t.release(2, l1); err == nil {
		t.Fatalf("expected error, but found none")
	}
	t.mustRelease(1, l1, nil)
	t.mustRelease(1, l2, nil)
	t.expectLeases(descID, "/1/1")

	// It is an error to acquire a lease for a specific version that doesn't
	// exist yet.
	expected = "version 2 for descriptor lease does not exist yet"
	if _, _, err := t.acquireMinVersion(1, descID, 2); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
	t.expectLeases(descID, "/1/1")

	// Publish a new version and explicitly acquire it.
	l2, _ = t.mustAcquire(1, descID)
	t.mustPublish(ctx, 1, descID)
	l3, _ := t.mustAcquireMinVersion(1, descID, 2)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the new version is released we don't
	// release the node lease.
	t.mustRelease(1, l3, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// We can still acquire a local reference on the old version since it hasn't
	// expired.
	l4, _ := t.mustAcquireMinVersion(1, descID, 1)
	t.mustRelease(1, l4, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the old version is released the node
	// lease is also released.
	t.mustRelease(1, l2, removalTracker)
	t.expectLeases(descID, "/2/1")

	// Acquire 2 node leases on version 2.
	l5, _ := t.mustAcquireMinVersion(1, descID, 2)
	l6, _ := t.mustAcquireMinVersion(2, descID, 2)
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
	l7, _ := t.mustAcquireMinVersion(1, descID, 3)
	l8, _ := t.mustAcquireMinVersion(2, descID, 3)
	t.expectLeases(descID, "/2/1 /2/2 /3/1 /3/2")

	t.mustRelease(1, l5, removalTracker)
	t.expectLeases(descID, "/2/2 /3/1 /3/2")
	t.mustRelease(2, l6, removalTracker)
	t.expectLeases(descID, "/3/1 /3/2")

	// Wait for version 4 to be published.
	wg.Wait()
	l9, _ := t.mustAcquireMinVersion(1, descID, 4)
	t.mustRelease(1, l7, removalTracker)
	t.mustRelease(2, l8, nil)
	t.expectLeases(descID, "/3/2 /4/1")
	t.mustRelease(1, l9, nil)
	t.expectLeases(descID, "/3/2 /4/1")
}

func TestLeaseManagerReacquire(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := tests.CreateTestServerParams()

	params.LeaseManagerConfig = base.NewLeaseManagerConfig()
	// Set the lease duration such that the next lease acquisition will
	// require the lease to be reacquired.
	params.LeaseManagerConfig.DescriptorLeaseDuration = 0

	removalTracker := lease.NewLeaseRemovalTracker()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	l1, e1 := t.mustAcquire(1, descID)
	t.expectLeases(descID, "/1/1")

	// Another lease acquisition from the same node will result in a new lease.
	rt := removalTracker.TrackRemoval(l1)
	l3, e3 := t.mustAcquire(1, descID)
	if l1.GetID() == l3.GetID() && e3.WallTime == e1.WallTime {
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
	t.mustRelease(1, l3, nil)
}

func TestLeaseManagerPublishVersionChanged(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := tests.CreateTestServerParams()
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
		_, err := n1.Publish(context.Background(), descID, func(catalog.MutableDescriptor) error {
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
		_, err := n2.Publish(context.Background(), descID, func(catalog.MutableDescriptor) error {
			return nil
		}, nil)
		if err != nil {
			panic(err)
		}
		close(n1update)
		wg.Done()
	}(n1update, n2start)

	wg.Wait()

	t.mustAcquire(1, descID)
	t.expectLeases(descID, "/3/1")
}

func TestLeaseManagerPublishIllegalVersionChange(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := tests.CreateTestServerParams()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.node(1).Publish(
		context.Background(), keys.LeaseTableID, func(desc catalog.MutableDescriptor) error {
			table := desc.(*sqlbase.MutableTableDescriptor)
			table.Version++
			return nil
		}, nil); !testutils.IsError(err, "updated version") {
		t.Fatalf("unexpected error: %+v", err)
	}
	if _, err := t.node(1).Publish(
		context.Background(), keys.LeaseTableID, func(desc catalog.MutableDescriptor) error {
			table := desc.(*sqlbase.MutableTableDescriptor)
			table.Version--
			return nil
		}, nil); !testutils.IsError(err, "updated version") {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func TestLeaseManagerDrain(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	params, _ := tests.CreateTestServerParams()
	leaseRemovalTracker := lease.NewLeaseRemovalTracker()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseReleasedEvent: leaseRemovalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	{
		l1, _ := t.mustAcquire(1, descID)
		l2, _ := t.mustAcquire(2, descID)
		t.mustRelease(1, l1, nil)
		t.expectLeases(descID, "/1/1 /1/2")

		// Removal tracker to track for node 1's lease removal once the node
		// starts draining.
		l1RemovalTracker := leaseRemovalTracker.TrackRemoval(l1)

		t.nodes[1].SetDraining(true, nil /* reporter */)
		t.nodes[2].SetDraining(true, nil /* reporter */)

		// Leases cannot be acquired when in draining mode.
		if _, _, err := t.acquire(1, descID); !testutils.IsError(err, "cannot acquire lease when draining") {
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
		t.nodes[1].SetDraining(false, nil /* reporter */)
		l1, _ := t.mustAcquire(1, descID)
		t.mustRelease(1, l1, nil)
		t.expectLeases(descID, "/1/1")
	}
}

// Test that we fail to lease a table that was marked for deletion.
func TestCantLeaseDeletedTable(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	var mu syncutil.Mutex
	clearSchemaChangers := false

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				mu.Lock()
				defer mu.Unlock()
				return clearSchemaChangers
			},
		},
		// Disable GC job.
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ int64) error { select {} }},
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
	tableDesc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "test", "t")
	// try to acquire at a bogus version to make sure we don't get back a lease we
	// already had.
	_, _, err = t.acquireMinVersion(1, tableDesc.ID, tableDesc.Version+1)
	if !testutils.IsError(err, "table is being dropped") {
		t.Fatalf("got a different error than expected: %v", err)
	}
}

func acquire(
	ctx context.Context, s *server.TestServer, descID sqlbase.ID,
) (catalog.Descriptor, hlc.Timestamp, error) {
	return s.LeaseManager().(*lease.Manager).Acquire(ctx, s.Clock().Now(), descID)
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

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			TestingDescriptorRefreshedEvent: func(descriptor *sqlbase.Descriptor) {
				mu.Lock()
				defer mu.Unlock()
				if waitTableID != descriptor.GetID() {
					return
				}
				if descriptor.Table(hlc.Timestamp{}).Dropped() {
					close(deleted)
					waitTableID = 0
				}
			},
		},
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				mu.Lock()
				defer mu.Unlock()
				return clearSchemaChangers
			},
		},
		// Disable GC job.
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ int64) error { select {} }},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	stmt := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	_, err := db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	ctx := context.Background()

	lease1, _, err := acquire(ctx, s.(*server.TestServer), tableDesc.ID)
	if err != nil {
		t.Fatal(err)
	}
	lease2, _, err := acquire(ctx, s.(*server.TestServer), tableDesc.ID)
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

	// Block until the Manager has processed the gossip update.
	<-deleted

	// We should still be able to acquire, because we have an active lease.
	lease3, _, err := acquire(ctx, s.(*server.TestServer), tableDesc.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Release everything.
	if err := s.LeaseManager().(*lease.Manager).Release(lease1); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseManager().(*lease.Manager).Release(lease2); err != nil {
		t.Fatal(err)
	}
	if err := s.LeaseManager().(*lease.Manager).Release(lease3); err != nil {
		t.Fatal(err)
	}
	// Now we shouldn't be able to acquire any more.
	_, _, err = acquire(ctx, s.(*server.TestServer), tableDesc.ID)
	if !testutils.IsError(err, "table is being dropped") {
		t.Fatalf("got a different error than expected: %v", err)
	}
}

// TestSubqueryLeases tests that all leases acquired by a subquery are
// properly tracked and released.
func TestSubqueryLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	fooRelease := make(chan struct{}, 10)
	fooAcquiredCount := int32(0)
	fooReleaseCount := int32(0)
	var tableID int64

	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() == "foo" {
						atomic.AddInt32(&fooAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(id sqlbase.ID, _ sqlbase.DescriptorVersion, _ error) {
					if int64(id) == atomic.LoadInt64(&tableID) {
						// Note: we don't use close(fooRelease) here because the
						// lease on "foo" may be re-acquired (and re-released)
						// multiple times, at least once for the first
						// CREATE/SELECT pair and one for the finalf DROP.
						atomic.AddInt32(&fooReleaseCount, 1)
						fooRelease <- struct{}{}
					}
				},
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.foo (v INT);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) > 0 {
		t.Fatalf("CREATE TABLE has acquired a lease: got %d, expected 0", atomic.LoadInt32(&fooAcquiredCount))
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "foo")
	atomic.StoreInt64(&tableID, int64(tableDesc.ID))

	if _, err := sqlDB.Exec(`
SELECT * FROM t.foo;
`); err != nil {
		t.Fatal(err)
	}

	prev := atomic.LoadInt32(&fooAcquiredCount)
	if prev == 0 {
		t.Fatal("plain SELECT did not get lease; got 0, expected > 0")
	}

	if _, err := sqlDB.Exec(`
SELECT EXISTS(SELECT * FROM t.foo);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) == prev {
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

// Test that an AS OF SYSTEM TIME query uses the table cache.
func TestAsOfSystemTimeUsesCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	fooAcquiredCount := int32(0)

	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() == "foo" {
						atomic.AddInt32(&fooAcquiredCount, 1)
					}
				},
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.foo (v INT);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) > 0 {
		t.Fatalf("CREATE TABLE has acquired a lease: got %d, expected 0", atomic.LoadInt32(&fooAcquiredCount))
	}

	var tsVal string
	if err := sqlDB.QueryRow("SELECT cluster_logical_timestamp()").Scan(&tsVal); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(
		fmt.Sprintf(`SELECT * FROM t.foo AS OF SYSTEM TIME %s;`, tsVal),
	); err != nil {
		t.Fatal(err)
	}

	count := atomic.LoadInt32(&fooAcquiredCount)
	if count == 0 {
		t.Fatal("SELECT did not get lease; got 0, expected > 0")
	}
}

// TestDescriptorRefreshOnRetry tests that all descriptors acquired by
// a query are properly released before the query is retried.
func TestDescriptorRefreshOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	fooAcquiredCount := int32(0)
	fooReleaseCount := int32(0)
	var tableID int64

	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				// Set this so we observe a release event from the cache
				// when the API releases the descriptor.
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() == "foo" {
						atomic.AddInt32(&fooAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(id sqlbase.ID, _ sqlbase.DescriptorVersion, _ error) {
					if int64(id) == atomic.LoadInt64(&tableID) {
						atomic.AddInt32(&fooReleaseCount, 1)
					}
				},
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.foo (v INT);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) > 0 {
		t.Fatalf("CREATE TABLE has acquired a descriptor")
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "foo")
	atomic.StoreInt64(&tableID, int64(tableDesc.ID))

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec("SAVEPOINT cockroach_restart")
	require.NoError(t, err)

	// This will acquire a descriptor. We'll check that it gets released before we retry.
	if _, err := tx.Exec(`
		SELECT * FROM t.foo;
		`); err != nil {
		t.Fatal(err)
	}

	// Descriptor has been acquired one more time than it has been released.
	aCount, rCount := atomic.LoadInt32(&fooAcquiredCount), atomic.LoadInt32(&fooReleaseCount)
	if aCount != rCount+1 {
		t.Fatalf("invalid descriptor acquisition counts = %d, %d", aCount, rCount)
	}

	if _, err := tx.Exec(
		"SELECT crdb_internal.force_retry('100s':::INTERVAL)"); !testutils.IsError(
		err, `forced by crdb_internal\.force_retry\(\)`) {
		t.Fatal(err)
	}

	_, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart")
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		if rCount = atomic.LoadInt32(&fooReleaseCount); rCount != aCount {
			return errors.Errorf("didnt release descriptor, %d != %d", rCount, aCount)
		}
		return nil
	})

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Test that a transaction created way in the past will use the correct
// table descriptor and will thus obey the modification time of the
// table descriptor.
func TestTxnObeysTableModificationTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	// This test intentionally relies on uncontended transactions not being pushed
	// in order to verify what it claims to verify. The default closed timestamp
	// interval in 20.1+ is 3s. When run under the race detector, the process can
	// stall for upwards of 3s leading to the write transaction getting pushed.
	//
	// In order to mitigate that push, we increase the target_duration when the
	// test is run under race.
	if util.RaceEnabled {
		_, err := sqlDB.Exec(
			"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '120s'")
		require.NoError(t, err)
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")

	// A read-write transaction that uses the old version of the descriptor.
	txReadWrite, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// A read-only transaction that uses the old version of the descriptor.
	txRead, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// A write-only transaction that uses the old version of the descriptor.
	txWrite, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Modify the table descriptor.
	if _, err := sqlDB.Exec(`ALTER TABLE t.kv ADD m CHAR DEFAULT 'z';`); err != nil {
		t.Fatal(err)
	}

	rows, err := txReadWrite.Query(`SELECT * FROM t.kv`)
	if err != nil {
		t.Fatal(err)
	}

	checkSelectResults := func(rows *gosql.Rows) {
		defer func() {
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		for rows.Next() {
			// The transaction is unable to see column m.
			var k, v, m string
			if err := rows.Scan(&k, &v, &m); !testutils.IsError(
				err, "expected 2 destination arguments in Scan, not 3",
			) {
				t.Fatalf("err = %v", err)
			}
			err = rows.Scan(&k, &v)
			if err != nil {
				t.Fatal(err)
			}
			if k != "a" || v != "b" {
				t.Fatalf("didn't find expected row: %s %s", k, v)
			}
		}
	}

	checkSelectResults(rows)

	rows, err = txRead.Query(`SELECT * FROM t.kv`)
	if err != nil {
		t.Fatal(err)
	}

	checkSelectResults(rows)

	// Read-only transaction commits just fine.
	if err := txRead.Commit(); err != nil {
		t.Fatal(err)
	}

	// This INSERT will cause the transaction to be pushed,
	// which will be detected when we attempt to Commit() below.
	if _, err := txReadWrite.Exec(`INSERT INTO t.kv VALUES ('c', 'd');`); err != nil {
		t.Fatal(err)
	}

	// The transaction read at one timestamp and wrote at another so it
	// has to be restarted because the spans read were modified by the backfill.
	if err := txReadWrite.Commit(); !testutils.IsError(err,
		"TransactionRetryError: retry txn \\(RETRY_SERIALIZABLE\\)") {
		t.Fatalf("err = %v", err)
	}

	// This INSERT will cause the transaction to be pushed transparently,
	// which will be detected when we attempt to Commit() below only because
	// a deadline has been set.
	if _, err := txWrite.Exec(`INSERT INTO t.kv VALUES ('c', 'd');`); err != nil {
		t.Fatal(err)
	}

	checkDeadlineErr := func(err error, t *testing.T) {
		var pqe (*pq.Error)
		if !errors.As(err, &pqe) || pgcode.MakeCode(string(pqe.Code)) != pgcode.SerializationFailure ||
			!testutils.IsError(err, "RETRY_COMMIT_DEADLINE_EXCEEDED") {
			t.Fatalf("expected deadline exceeded, got: %v", err)
		}
	}
	checkDeadlineErr(txWrite.Commit(), t)

	// Test the deadline exceeded error with a CREATE/DROP INDEX.
	txWrite, err = sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txUpdate, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Modify the table descriptor.
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.kv (v)`); err != nil {
		t.Fatal(err)
	}

	// This INSERT will cause the transaction to be pushed transparently,
	// which will be detected when we attempt to Commit() below only because
	// a deadline has been set.
	if _, err := txWrite.Exec(`INSERT INTO t.kv VALUES ('c', 'd');`); err != nil {
		t.Fatal(err)
	}

	checkDeadlineErr(txWrite.Commit(), t)

	if _, err := txUpdate.Exec(`UPDATE t.kv SET v = 'c' WHERE k = 'a';`); err != nil {
		t.Fatal(err)
	}

	checkDeadlineErr(txUpdate.Commit(), t)

	txWrite, err = sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txRead, err = sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Modify the table descriptor.
	if _, err := sqlDB.Exec(`DROP INDEX t.kv@foo`); err != nil {
		t.Fatal(err)
	}

	rows, err = txRead.Query(`SELECT k, v FROM t.kv@foo`)
	if err != nil {
		t.Fatal(err)
	}
	checkSelectResults(rows)

	// Uses old descriptor and inserts values into index span which
	// will be cleaned up.
	if _, err := txWrite.Exec(`INSERT INTO t.kv VALUES ('c', 'd');`); err != nil {
		t.Fatal(err)
	}

	if err := txRead.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := txWrite.Commit(); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, 4)

	// Allow async schema change waiting for GC to complete (when dropping an
	// index) and clear the index keys.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv"); len(tableDesc.GCMutations) != 0 {
			return errors.Errorf("%d gc mutations remaining", len(tableDesc.GCMutations))
		}
		return nil
	})

	tests.CheckKeyCount(t, kvDB, tableSpan, 2)

	// TODO(erik, vivek): Transactions using old descriptors should fail and
	// rollback when the index keys have been removed by ClearRange
	// and the consistency issue is resolved. See #31563.
}

// Test that a lease on a table descriptor is always acquired on the latest
// version of a descriptor.
func TestLeaseAtLatestVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	errChan := make(chan error, 1)
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() == "kv" {
						var err error
						if desc.GetVersion() != 2 {
							err = errors.Errorf("not seeing latest version")
						}
						errChan <- err
					}
				},
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
BEGIN;
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
CREATE TABLE t.timestamp (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
COMMIT;
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Insert an entry so that the transaction is guaranteed to be
	// assigned a timestamp.
	if _, err := tx.Exec(`
INSERT INTO t.timestamp VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	// Increment the table version after the txn has started.
	leaseMgr := s.LeaseManager().(*lease.Manager)
	if _, err := leaseMgr.Publish(
		context.Background(), tableDesc.ID, func(catalog.MutableDescriptor) error {
			// Do nothing: increments the version.
			return nil
		}, nil); err != nil {
		t.Error(err)
	}

	// This select will see version 1 of the table. It will first
	// acquire a lease on version 2 and note that the table descriptor is
	// invalid for the transaction, so it will read the previous version
	// and use it.
	rows, err := tx.Query(`SELECT * FROM t.kv`)
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

// BenchmarkLeaseAcquireByNameCached benchmarks the AcquireByName
// acquisition code path if a valid lease exists and is contained in
// nameCache. In particular this benchmark is done with
// parallelism, which is important to also benchmark locking.
func BenchmarkLeaseAcquireByNameCached(b *testing.B) {
	defer leaktest.AfterTest(b)()
	params, _ := tests.CreateTestServerParams()

	t := newLeaseTest(b, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "test")
	dbID := tableDesc.ParentID
	tableName := tableDesc.Name
	leaseManager := t.node(1)

	// Acquire the lease so it is put into the nameCache.
	_, _, err := leaseManager.AcquireByName(
		context.Background(),
		t.server.Clock().Now(),
		dbID,
		tableDesc.GetParentSchemaID(),
		tableName,
	)
	if err != nil {
		t.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := leaseManager.AcquireByName(
				context.Background(),
				t.server.Clock().Now(),
				dbID,
				tableDesc.GetParentSchemaID(),
				tableName,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

}

// This test makes sure leases get renewed automatically in the
// background if the lease is about to expire, without blocking. We
// first acquire a lease, then continue to re-acquire it until another
// lease is renewed.
func TestLeaseRenewedAutomatically(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	var testAcquiredCount int32
	var testAcquisitionBlockCount int32

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				// We want to track when leases get acquired and when they are renewed.
				// We also want to know when acquiring blocks to test lease renewal.
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetID() > keys.MaxReservedDescID {
						atomic.AddInt32(&testAcquiredCount, 1)
					}
				},
				LeaseAcquireResultBlockEvent: func(_ lease.AcquireBlockType) {
					atomic.AddInt32(&testAcquisitionBlockCount, 1)
				},
			},
		},
	}
	params.LeaseManagerConfig = base.NewLeaseManagerConfig()
	// The lease jitter is set to ensure newer leases have higher
	// expiration timestamps.
	params.LeaseManagerConfig.DescriptorLeaseJitterFraction = 0.0
	// The renewal timeout is set to be the duration, so background
	// renewal should begin immediately after accessing a lease.
	params.LeaseManagerConfig.DescriptorLeaseRenewalTimeout =
		params.LeaseManagerConfig.DescriptorLeaseDuration

	ctx := context.Background()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test1 (k CHAR PRIMARY KEY, v CHAR);
CREATE TABLE t.test2 ();
`); err != nil {
		t.Fatal(err)
	}

	test1Desc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "test1")
	test2Desc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "test2")
	dbID := test2Desc.ParentID

	// Acquire a lease on test1 by name.
	ts1, eo1, err := t.node(1).AcquireByName(
		ctx,
		t.server.Clock().Now(),
		dbID,
		test1Desc.GetParentSchemaID(),
		"test1",
	)
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, ts1); err != nil {
		t.Fatal(err)
	} else if count := atomic.LoadInt32(&testAcquiredCount); count != 1 {
		t.Fatalf("expected 1 lease to be acquired, but acquired %d times",
			count)
	}

	// Acquire a lease on test2 by ID.
	ts2, eo2, err := t.node(1).Acquire(ctx, t.server.Clock().Now(), test2Desc.ID)
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, ts2); err != nil {
		t.Fatal(err)
	} else if count := atomic.LoadInt32(&testAcquiredCount); count != 2 {
		t.Fatalf("expected 2 leases to be acquired, but acquired %d times",
			count)
	}

	// Reset testAcquisitionBlockCount as the first acqusition will always block.
	atomic.StoreInt32(&testAcquisitionBlockCount, 0)

	testutils.SucceedsSoon(t, func() error {
		// Acquire another lease by name on test1. At first this will be the
		// same lease, but eventually we will asynchronously renew a lease and
		// our acquire will get a newer lease.
		ts1, en1, err := t.node(1).AcquireByName(
			ctx,
			t.server.Clock().Now(),
			dbID,
			test1Desc.GetParentSchemaID(),
			"test1",
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := t.release(1, ts1); err != nil {
				t.Fatal(err)
			}
		}()

		// We check for the new expiry time because if our past acquire triggered
		// the background renewal, the next lease we get will be the result of the
		// background renewal.
		if en1.WallTime <= eo1.WallTime {
			return errors.Errorf("expected new lease expiration (%s) to be after old lease expiration (%s)",
				en1, eo1)
		} else if count := atomic.LoadInt32(&testAcquiredCount); count < 2 {
			return errors.Errorf("expected at least 2 leases to be acquired, but acquired %d times",
				count)
		} else if blockCount := atomic.LoadInt32(&testAcquisitionBlockCount); blockCount > 0 {
			t.Fatalf("expected repeated lease acquisition to not block, but blockCount is: %d", blockCount)
		}

		// Acquire another lease by ID on test2. At first this will be the same
		// lease, but eventually we will asynchronously renew a lease and our
		// acquire will get a newer lease.
		ts2, en2, err := t.node(1).Acquire(ctx, t.server.Clock().Now(), test2Desc.ID)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := t.release(1, ts2); err != nil {
				t.Fatal(err)
			}
		}()

		// We check for the new expiry time because if our past acquire triggered
		// the background renewal, the next lease we get will be the result of the
		// background renewal.
		if en2.WallTime <= eo2.WallTime {
			return errors.Errorf("expected new lease expiration (%s) to be after old lease expiration (%s)",
				en2, eo2)
		} else if count := atomic.LoadInt32(&testAcquiredCount); count < 3 {
			return errors.Errorf("expected at least 3 leases to be acquired, but acquired %d times",
				count)
		} else if blockCount := atomic.LoadInt32(&testAcquisitionBlockCount); blockCount > 0 {
			t.Fatalf("expected repeated lease acquisition to not block, but blockCount is: %d", blockCount)
		}

		return nil
	})
}

// Check that the table version is incremented with every schema change.
// The test also verifies that when a lease is in use, the first schema
// change can proceed, but the next one will wait until the lease is
// released. Furthermore, this also tests that the schema change transaction
// will rollback a transaction that violates the two version invariant
// thereby not blocking any table lease transaction trying to acquire a
// table lease.
func TestIncrementTableVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var violations int64
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		// Disable execution of schema changers after the schema change
		// transaction commits. This is to prevent executing the default
		// WaitForOneVersion() code that holds up a schema change
		// transaction until the new version has been published to the
		// entire cluster.
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				return true
			},
			TwoVersionLeaseViolation: func() {
				atomic.AddInt64(&violations, 1)
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if tableDesc.Version != 1 {
		t.Fatalf("invalid version %d", tableDesc.Version)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Grab a lease on the table.
	if _, err := tx.Exec("INSERT INTO t.kv VALUES ('a', 'b');"); err != nil {
		t.Fatal(err)
	}

	// Modify the table descriptor.
	if _, err := sqlDB.Exec(`ALTER TABLE t.kv RENAME to t.kv1`); err != nil {
		t.Fatal(err)
	}

	// The first schema change will succeed and increment the version.
	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if tableDesc.Version != 2 {
		t.Fatalf("invalid version %d", tableDesc.Version)
	}

	if l := atomic.LoadInt64(&violations); l > 0 {
		t.Fatalf("violations = %d", l)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.kv1 RENAME TO t.kv2`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Let the second schema change hit a retry because of a two version
	// lease violation.
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&violations) == 0 {
			return errors.Errorf("didnt retry schema change")
		}
		return nil
	})
	// The second schema change doesn't increment the table descriptor
	// version. Furthermore, it also doesn't block any reads on
	// the table descriptor. If the schema change transaction
	// doesn't rollback the transaction this descriptor read will
	// hang.
	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if tableDesc.Version != 2 {
		t.Fatalf("invalid version %d", tableDesc.Version)
	}

	// Transaction successfully used the old version.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if tableDesc.Version != 3 {
		t.Fatalf("invalid version %d", tableDesc.Version)
	}
}

// Tests that when a transaction has already returned results
// to the user and the transaction continues on to make a schema change,
// whenever the table lease two version invariant is violated and the
// transaction needs to be restarted, a retryable error is returned to the
// user.
func TestTwoVersionInvariantRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var violations int64
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		// Disable execution of schema changers after the schema change
		// transaction commits. This is to prevent executing the default
		// WaitForOneVersion() code that holds up a schema change
		// transaction until the new version has been published to the
		// entire cluster.
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				return true
			},
			TwoVersionLeaseViolation: func() {
				atomic.AddInt64(&violations, 1)
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if tableDesc.Version != 1 {
		t.Fatalf("invalid version %d", tableDesc.Version)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Grab a lease on the table.
	rows, err := tx.Query("SELECT * FROM t.kv")
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Modify the table descriptor increments the version.
	if _, err := sqlDB.Exec(`ALTER TABLE t.kv RENAME to t.kv1`); err != nil {
		t.Fatal(err)
	}

	txRetry, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Read some data using the transaction so that it cannot be
	// retried internally
	rows, err = txRetry.Query(`SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := txRetry.Exec(`ALTER TABLE t.kv1 RENAME TO t.kv2`); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// This can hang waiting for one version before tx.Commit() is
		// called below, so it is executed in another goroutine
		if err := txRetry.Commit(); !testutils.IsError(err,
			`TransactionRetryWithProtoRefreshError: cannot publish new versions for tables: \[\{kv1 53 1\}\], old versions still in use`,
		) {
			t.Errorf("err = %v", err)
		}
		wg.Done()
	}()

	// Make sure that txRetry does violate the two version lease invariant.
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&violations) == 0 {
			return errors.Errorf("didnt retry schema change")
		}
		return nil
	})
	// Commit the first transaction, unblocking txRetry.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestModificationTimeTxnOrdering(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	testingT.Skip("#22479")

	// Decide how long we should run this.
	maxTime := time.Duration(5) * time.Second
	if testutils.NightlyStress() {
		maxTime = time.Duration(2) * time.Minute
	}

	// Which table to exercise the test against.
	const descID = keys.LeaseTableID

	params, _ := tests.CreateTestServerParams()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test0 (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// When to end the test.
	end := timeutil.Now().Add(maxTime)
	var wg sync.WaitGroup
	wg.Add(2)

	log.Infof(ctx, "until %s", end)

	go func() {
		for count := 0; timeutil.Now().Before(end); count++ {
			log.Infof(ctx, "renaming test%d to test%d", count, count+1)
			if _, err := t.db.Exec(fmt.Sprintf(`ALTER TABLE t.test%d RENAME TO t.test%d`, count, count+1)); err != nil {
				t.Fatal(err)
			}
		}
		wg.Done()
	}()

	go func() {
		leaseMgr := t.node(1)
		for timeutil.Now().Before(end) {
			log.Infof(ctx, "publishing new descriptor")
			desc, err := leaseMgr.Publish(ctx, descID, func(catalog.MutableDescriptor) error { return nil }, nil)
			if err != nil {
				t.Fatalf("error while publishing: %v", err)
			}
			table := desc.(*sqlbase.ImmutableTableDescriptor)

			// Wait a little time to give a chance to other goroutines to
			// race past.
			time.Sleep(20 * time.Millisecond)

			// Now check that the version that was updated is indeed observable
			// in the database at the time it says it was modified.
			//
			// This checks that the modification timestamp is not lying about
			// the transaction commit time (and that the txn commit time wasn't
			// bumped past it).
			log.Infof(ctx, "checking version %d", table.Version)
			txn := kv.NewTxn(ctx, t.kvDB, roachpb.NodeID(0))
			// Make the txn look back at the known modification timestamp.
			txn.SetFixedTimestamp(ctx, table.ModificationTime)

			// Look up the descriptor.
			descKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, descID)
			dbDesc := &sqlbase.Descriptor{}
			ts, err := txn.GetProtoTs(ctx, descKey, dbDesc)
			if err != nil {
				t.Fatalf("error while reading proto: %v", err)
			}
			// Look at the descriptor that comes back from the database.
			dbTable := dbDesc.Table(ts)

			if dbTable.Version != table.Version || dbTable.ModificationTime != table.ModificationTime {
				t.Fatalf("db has version %d at ts %s, expected version %d at ts %s",
					dbTable.Version, dbTable.ModificationTime, table.Version, table.ModificationTime)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

// This test makes sure leases get renewed periodically.
// TODO(vivek): remove once epoch based leases is implemented.
func TestLeaseRenewedPeriodically(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	var mu syncutil.Mutex
	releasedIDs := make(map[sqlbase.ID]struct{})

	var testAcquiredCount int32
	var testAcquisitionBlockCount int32

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				// We want to track when leases get acquired and when they are renewed.
				// We also want to know when acquiring blocks to test lease renewal.
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetID() > keys.MaxReservedDescID {
						atomic.AddInt32(&testAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(id sqlbase.ID, _ sqlbase.DescriptorVersion, _ error) {
					if id < keys.MaxReservedDescID {
						return
					}
					mu.Lock()
					defer mu.Unlock()
					releasedIDs[id] = struct{}{}
				},
				LeaseAcquireResultBlockEvent: func(_ lease.AcquireBlockType) {
					atomic.AddInt32(&testAcquisitionBlockCount, 1)
				},
			},
			TestingDescriptorUpdateEvent: func(_ *sqlbase.Descriptor) error {
				return errors.Errorf("ignore gossip update")
			},
		},
	}
	params.LeaseManagerConfig = base.NewLeaseManagerConfig()
	// The lease jitter is set to ensure newer leases have higher
	// expiration timestamps.
	params.LeaseManagerConfig.DescriptorLeaseJitterFraction = 0.0
	// Lease duration to something small.
	params.LeaseManagerConfig.DescriptorLeaseDuration = 50 * time.Millisecond
	// Renewal timeout to 0 saying that the lease will get renewed only
	// after the lease expires when a request requests the descriptor.
	params.LeaseManagerConfig.DescriptorLeaseRenewalTimeout = 0

	ctx := context.Background()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test1 (k CHAR PRIMARY KEY, v CHAR);
CREATE TABLE t.test2 ();
`); err != nil {
		t.Fatal(err)
	}

	test1Desc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "test2")
	test2Desc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "test2")
	dbID := test2Desc.ParentID

	atomic.StoreInt32(&testAcquisitionBlockCount, 0)

	numReleasedLeases := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(releasedIDs)
	}
	if count := numReleasedLeases(); count != 0 {
		t.Fatalf("expected no leases to be releases, released %d", count)
	}

	// Acquire a lease on test1 by name.
	ts1, _, err := t.node(1).AcquireByName(
		ctx,
		t.server.Clock().Now(),
		dbID,
		test1Desc.GetParentSchemaID(),
		"test1",
	)
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, ts1); err != nil {
		t.Fatal(err)
	} else if count := atomic.LoadInt32(&testAcquisitionBlockCount); count != 1 {
		t.Fatalf("expected 1 lease to be acquired, but acquired %d times",
			count)
	}

	// Acquire a lease on test2 by ID.
	ts2, _, err := t.node(1).Acquire(ctx, t.server.Clock().Now(), test2Desc.ID)
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, ts2); err != nil {
		t.Fatal(err)
	} else if count := atomic.LoadInt32(&testAcquisitionBlockCount); count != 2 {
		t.Fatalf("expected 2 leases to be acquired, but acquired %d times",
			count)
	}

	// From now on henceforth do not acquire a lease, so any renewals can only
	// happen through the periodic lease renewal mechanism.

	// Reset testAcquisitionBlockCount as the first acqusitions will always block.
	atomic.StoreInt32(&testAcquisitionBlockCount, 0)

	// Check that lease acquisition happens independent of lease being requested.
	testutils.SucceedsSoon(t, func() error {
		if count := atomic.LoadInt32(&testAcquiredCount); count <= 4 {
			return errors.Errorf("expected more than 4 leases to be acquired, but acquired %d times", count)
		}

		if count := numReleasedLeases(); count != 2 {
			return errors.Errorf("expected 2 leases to be releases, released %d", count)
		}
		return nil
	})

	// No blocked acquisitions.
	if blockCount := atomic.LoadInt32(&testAcquisitionBlockCount); blockCount > 0 {
		t.Fatalf("expected lease acquisition to not block, but blockCount is: %d", blockCount)
	}
}

// TestReadBeforeDrop tests that a read over a table from a transaction
// initiated before the table is dropped succeeds.
func TestReadBeforeDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}
	// Test that once a table is dropped it cannot be used even when
	// a transaction is using a timestamp from the past.
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	rows, err := tx.Query(`SELECT * FROM t.kv`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var k, v string
		err := rows.Scan(&k, &v)
		if err != nil {
			t.Fatal(err)
		}
		if k != "a" || v != "b" {
			t.Fatalf("didn't find expected row: %s %s", k, v)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Tests that transactions with timestamps within the uncertainty interval
// of a TABLE CREATE are pushed to allow them to observe the created table.
func TestTableCreationPushesTxnsInRecentPast(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      params,
	})
	defer tc.Stopper().Stop(context.Background())
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
 CREATE DATABASE t;
 CREATE TABLE t.timestamp (k CHAR PRIMARY KEY, v CHAR);
 `); err != nil {
		t.Fatal(err)
	}

	// Create a transaction before the table is created.
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Create a transaction before the table is created. Use a different
	// node so that clock uncertainty is presumed and it gets pushed.
	tx1, err := tc.ServerConn(1).Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	// Was actually run in the past and so doesn't see the table.
	if _, err := tx.Exec(`
INSERT INTO t.kv VALUES ('a', 'b');
`); !testutils.IsError(err, "does not exist") {
		t.Fatal(err)
	}

	// Not sure whether run in the past and so sees clock uncertainty push.
	if _, err := tx1.Exec(`
INSERT INTO t.kv VALUES ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Tests that DeleteOrphanedLeases() deletes only orphaned leases.
func TestDeleteOrphanedLeases(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{},
	}

	ctx := context.Background()
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.before (k CHAR PRIMARY KEY, v CHAR);
CREATE TABLE t.after (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	beforeDesc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "before")
	afterDesc := sqlbase.TestingGetTableDescriptor(t.kvDB, keys.SystemSQLCodec, "t", "after")
	dbID := beforeDesc.ParentID

	// Acquire a lease on "before" by name.
	beforeTable, _, err := t.node(1).AcquireByName(
		ctx,
		t.server.Clock().Now(),
		dbID,
		beforeDesc.GetParentSchemaID(),
		"before",
	)
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, beforeTable); err != nil {
		t.Fatal(err)
	}

	// Assume server shuts down here and a new instance of the server starts up.
	// All leases created prior to this time are declared orphaned.
	now := timeutil.Now().UnixNano()

	// Acquire a lease on "after" by name after server startup.
	afterTable, _, err := t.node(1).AcquireByName(
		ctx,
		t.server.Clock().Now(),
		dbID,
		afterDesc.GetParentSchemaID(),
		"after",
	)
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, afterTable); err != nil {
		t.Fatal(err)
	}
	t.expectLeases(beforeDesc.ID, "/1/1")
	t.expectLeases(afterDesc.ID, "/1/1")

	// Call DeleteOrphanedLeases() with the server startup time.
	t.node(1).DeleteOrphanedLeases(now)
	// Orphaned lease is gone.
	t.expectLeases(beforeDesc.ID, "")
	t.expectLeases(afterDesc.ID, "/1/1")
}

// Test that acquiring a lease doesn't block on other transactions performing
// schema changes. Lease acquisitions run in high-priority transactions, thereby
// pushing any locks held by schema-changing transactions out of their ways.
func TestLeaseAcquisitionDoesntBlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec(`CREATE DATABASE t; CREATE TABLE t.test(k CHAR PRIMARY KEY, v CHAR);`)
	require.NoError(t, err)

	// Figure out the table ID.
	row := db.QueryRow("SELECT id FROM system.namespace WHERE name='test'")
	var descID sqlbase.ID
	require.NoError(t, row.Scan(&descID))

	// Spin up another goroutine performing a schema change. We'll suspend its
	// execution until the main goroutine is able to acquire its lease.
	schemaCh := make(chan error)
	schemaUnblock := make(chan struct{})
	go func() {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			schemaCh <- err
			return
		}
		_, err = tx.Exec("ALTER TABLE t.test ADD COLUMN v2 CHAR")
		schemaCh <- err
		if err != nil {
			return
		}

		<-schemaUnblock
		schemaCh <- tx.Commit()
	}()

	require.NoError(t, <-schemaCh)

	l, _, err := s.LeaseManager().(*lease.Manager).Acquire(ctx, s.Clock().Now(), descID)
	require.NoError(t, err)

	// Release the lease so that the schema change can proceed.
	require.NoError(t, s.LeaseManager().(*lease.Manager).Release(l))
	// Unblock the schema change.
	close(schemaUnblock)

	// Wait for the schema change to finish.
	require.NoError(t, <-schemaCh)
}

// Test that acquiring a lease doesn't block on other transactions performing
// schema changes. This is similar to the previous test, except it acquires a
// lease by table name instead of ID, and correspondingly the schema change
// touches the namespace table.
func TestLeaseAcquisitionByNameDoesntBlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := db.Exec(`CREATE DATABASE t`)
	require.NoError(t, err)

	// Spin up another goroutine performing a schema change - creating a table.
	// We'll suspend its execution until the main goroutine is able to acquire its
	// lease. The idea is that, before being suspended, this transaction has put
	// down locks on the system.namespace table. The point of the test is to check
	// that a lease acquisition pushes these locks out of its way.
	schemaCh := make(chan error)
	schemaUnblock := make(chan struct{})
	go func() {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			schemaCh <- err
			return
		}
		_, err = tx.Exec("CREATE TABLE t.test()")
		schemaCh <- err
		if err != nil {
			return
		}

		<-schemaUnblock
		schemaCh <- tx.Commit()
	}()

	require.NoError(t, <-schemaCh)
	_, err = db.Exec("SELECT * from t.test")
	require.Regexp(t, `pq: relation "t\.test" does not exist`, err)
	close(schemaUnblock)

	// Wait for the schema change to finish.
	require.NoError(t, <-schemaCh)
}

// TestIntentOnSystemConfigDoesNotPreventSchemaChange tests that failures to
// gossip the system config due to intents are rectified when later intents
// are aborted.
func TestIntentOnSystemConfigDoesNotPreventSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")

	connA, err := db.Conn(ctx)
	require.NoError(t, err)
	connB, err := db.Conn(ctx)
	require.NoError(t, err)

	txA, err := connA.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)
	txB, err := connB.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)

	// Lay down an intent on the system config span.
	_, err = txA.Exec("CREATE TABLE bar (i INT PRIMARY KEY)")
	require.NoError(t, err)

	_, err = txB.Exec("ALTER TABLE foo ADD COLUMN j INT NOT NULL DEFAULT 2")
	require.NoError(t, err)

	getFooVersion := func() (version int) {
		tx, err := db.Begin()
		require.NoError(t, err)
		// Prevent this transaction from blocking on intents.
		_, err = tx.Exec("SET TRANSACTION PRIORITY HIGH")
		require.NoError(t, err)
		require.NoError(t, tx.QueryRow(
			"SELECT version FROM crdb_internal.tables WHERE name = 'foo'").
			Scan(&version))
		require.NoError(t, tx.Commit())
		return version
	}

	// Fire off the commit. In order to return, the table descriptor will need
	// to make it through several versions. We wait until the version has been
	// incremented once before we rollback txA.
	origVersion := getFooVersion()
	errCh := make(chan error)
	go func() { errCh <- txB.Commit() }()
	testutils.SucceedsSoon(t, func() error {
		if got := getFooVersion(); got <= origVersion {
			return fmt.Errorf("got %d, expected greater", got)
		}
		return nil
	})

	// Roll back txA which had left an intent on the system config span which
	// prevented the leaseholders of origVersion of foo from being notified.
	// Ensure that those leaseholders are notified in a timely manner.
	require.NoError(t, txA.Rollback())

	const extremelyLongTime = 10 * time.Second
	select {
	case <-time.After(extremelyLongTime):
		t.Fatalf("schema change did not complete in %v", extremelyLongTime)
	case err := <-errCh:
		require.NoError(t, err)
	}
}

// TestFinalizeVersionEnablesRangefeedUpdates ensures that gossip is used when
// the version is initialized to something prior to VersionRangefeedLeases and
// then that rangefeeds are adopted once that version is finalized.
//
// TODO(ajwerner): Remove this test in 21.1 as it is no longer relevant.
func TestFinalizeVersionEnablesRangefeedUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The test first starts a cluster at a version below VersionRangefeedLeases
	// and ensure that schema changes don't block for too long. Meanwhile ensure
	// that no rangefeed has been created on the system config span. Then finalize
	// the version upgrade and ensure that a rangefeed is created and that
	// schema changes still work.

	ctx := context.Background()
	var rangefeedsCreated int64
	descriptorTablePrefix := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTablePrefix,
		EndKey: descriptorTablePrefix.PrefixEnd(),
	}
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLLeaseManager: &lease.ManagerTestingKnobs{
					VersionPollIntervalForRangefeeds: time.Millisecond,
				},
				Store: &kvserver.StoreTestingKnobs{
					// Add a filter to detect the creation of a rangefeed over the
					// descriptor table.
					TestingRangefeedFilter: func(
						args *roachpb.RangeFeedRequest, _ roachpb.Internal_RangeFeedServer,
					) *roachpb.Error {
						if args.Span.Overlaps(descriptorTableSpan) {
							atomic.AddInt64(&rangefeedsCreated, 1)
						}
						return nil
					},
				},
				Server: &server.TestingKnobs{
					// We're going to manually control when the upgrade takes place below
					// so disable the automatic upgrade.
					DisableAutomaticVersionUpgrade: 1,
					// Bootstrap the cluster at something below VersionRangefeedLeases so
					// that we can test the upgrade.
					BootstrapVersionOverride: clusterversion.VersionByKey(clusterversion.Version20_1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	// Lease table foo on node 2.
	db2 := tc.ServerConn(1)
	var junk int
	require.EqualValues(t, gosql.ErrNoRows, db2.QueryRow("SELECT * FROM foo").Scan(&junk))

	// Run a schema change which will require a notification to finish.
	tdb.Exec(t, "ALTER TABLE foo ADD COLUMN j INT NOT NULL DEFAULT 2")
	require.Equal(t, int64(0), atomic.LoadInt64(&rangefeedsCreated))

	// Upgrade to after VersionRangefeedLeases and ensure that a rangefeed is created.
	tdb.Exec(t, "SET CLUSTER SETTING version = crdb_internal.node_executable_version();")
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&rangefeedsCreated) == 0 {
			return errors.New("no rangefeeds created")
		}
		return nil
	})
	tdb.Exec(t, "ALTER TABLE foo ADD COLUMN k INT NOT NULL DEFAULT 2")
}

func ensureTestTakesLessThan(t *testing.T, allowed time.Duration) func() {
	start := timeutil.Now()
	return func() {
		if t.Failed() {
			return
		}
		if took := timeutil.Since(start); took > allowed {
			t.Fatalf("test took %v which is greater than %v", took, allowed)
		}
	}
}

// TestRangefeedUpdatesHandledProperlyInTheFaceOfRaces deals with the case where
// we have a request to lease a table descriptor, we read version 1 and prior to
// adding that to the state, we get an update indicating that that version is
// too old.
func TestRangefeedUpdatesHandledProperlyInTheFaceOfRaces(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ensureTestTakesLessThan(t, 30*time.Second)()

	ctx := context.Background()
	var interestingTable atomic.Value
	interestingTable.Store(sqlbase.ID(0))
	blockLeaseAcquisitionOfInterestingTable := make(chan chan struct{})
	unblockAll := make(chan struct{})
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &lease.ManagerTestingKnobs{
				AlwaysUseRangefeeds: true,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: args,
	})
	descUpdateChan := make(chan *sqlbase.Descriptor)
	args.Knobs.SQLLeaseManager = &lease.ManagerTestingKnobs{
		TestingDescriptorUpdateEvent: func(descriptor *sqlbase.Descriptor) error {
			// Use this testing knob to ensure that we see an update for the desc
			// in question. We don't care about events to refresh the first version
			// which can happen under rare stress scenarios.
			if descriptor.GetID() == interestingTable.Load().(sqlbase.ID) && descriptor.GetVersion() >= 2 {
				select {
				case descUpdateChan <- descriptor:
				case <-unblockAll:
				}
			}
			return nil
		},
		LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
			LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
				// Block the lease acquisition for the desc after the leasing
				// transaction has been issued. We'll wait to unblock it until after
				// the new version has been published and that even has been received.
				if desc.GetID() != interestingTable.Load().(sqlbase.ID) {
					return
				}
				blocked := make(chan struct{})
				select {
				case blockLeaseAcquisitionOfInterestingTable <- blocked:
					<-blocked
				case <-unblockAll:
				}
			},
		},
	}
	// Start a second server with our knobs.
	tc.AddServer(t, args)
	defer tc.Stopper().Stop(ctx)

	db1 := tc.ServerConn(0)
	tdb1 := sqlutils.MakeSQLRunner(db1)
	db2 := tc.ServerConn(1)

	// Create a couple of descriptors.
	tdb1.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")

	// Find the desc ID for the desc we'll be mucking with.
	var tableID sqlbase.ID
	tdb1.QueryRow(t, "SELECT table_id FROM crdb_internal.tables WHERE name = $1 AND database_name = current_database()",
		"foo").Scan(&tableID)
	interestingTable.Store(tableID)

	// Launch a goroutine to query foo. It will be blocked in lease acquisition.
	selectDone := make(chan error)
	go func() {
		var count int
		selectDone <- db2.QueryRow("SELECT count(*) FROM foo").Scan(&count)
	}()

	// Make sure it got blocked.
	toUnblockForLeaseAcquisition := <-blockLeaseAcquisitionOfInterestingTable

	// Launch a goroutine to perform a schema change which will lead to new
	// versions.
	alterErrCh := make(chan error)
	go func() {
		_, err := db1.Exec("ALTER TABLE foo ADD COLUMN j INT DEFAULT 1")
		alterErrCh <- err
	}()

	// Make sure we get an update. Note that this is after we have already
	// acquired a lease on the old version but have not yet recorded that fact.
	desc := <-descUpdateChan
	require.Equal(t, sqlbase.DescriptorVersion(2), desc.GetVersion())

	// Allow the original lease acquisition to proceed.
	close(toUnblockForLeaseAcquisition)

	// Ensure the query completes.
	<-selectDone

	// Allow everything to proceed as usual.
	close(unblockAll)
	// Ensure the schema change completes.
	<-alterErrCh

	// Ensure that the new schema is in use on n2.
	var i, j int
	require.Equal(t, gosql.ErrNoRows, db2.QueryRow("SELECT i, j FROM foo").Scan(&i, &j))
}
