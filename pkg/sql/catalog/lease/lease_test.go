// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Note that there's also lease_internal_test.go, in package lease.

package lease_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leaseTest struct {
	testing.TB
	cluster                  serverutils.TestClusterInterface
	server                   serverutils.ApplicationLayerInterface
	db                       *gosql.DB
	kvDB                     *kv.DB
	nodes                    map[uint32]*lease.Manager
	leaseManagerTestingKnobs lease.ManagerTestingKnobs
}

func init() {
	lease.MoveTablePrimaryIndexIDto2 = func(
		ctx context.Context, t *testing.T, s serverutils.ApplicationLayerInterface, id descpb.ID,
	) {
		require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			t, err := col.MutableByID(txn.KV()).Table(ctx, id)
			if err != nil {
				return err
			}
			t.PrimaryIndex.ID = 2
			t.NextIndexID++
			return col.WriteDesc(ctx, false /* kvTrace */, t, txn.KV())
		}))
	}

}

func newLeaseTest(tb testing.TB, params base.TestClusterArgs) *leaseTest {
	c := serverutils.StartCluster(tb, 3, params)
	s := c.Server(0).ApplicationLayer()
	lt := &leaseTest{
		TB:      tb,
		cluster: c,
		server:  s,
		db:      s.SQLConn(tb, serverutils.DBName("")),
		kvDB:    s.DB(),
		nodes:   map[uint32]*lease.Manager{},
	}

	if params.ServerArgs.Knobs.SQLLeaseManager != nil {
		lt.leaseManagerTestingKnobs =
			*params.ServerArgs.Knobs.SQLLeaseManager.(*lease.ManagerTestingKnobs)
	}
	return lt
}

func (t *leaseTest) cleanup() {
	t.cluster.Stopper().Stop(context.Background())
}

func (t *leaseTest) getLeases(descID descpb.ID) string {
	const sql = `
  SELECT version, "nodeID"
    FROM system.lease
   WHERE "descID" = $1 AND "nodeID" > $2
ORDER BY version, "nodeID";
`
	rows, err := t.db.Query(sql, descID, baseIDForLeaseTest)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	var prefix string
	for rows.Next() {
		var (
			version    int
			instanceID int
		)
		if err := rows.Scan(&version, &instanceID); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&buf, "%s/%d/%d", prefix, version, instanceID-baseIDForLeaseTest)
		prefix = " "
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func (t *leaseTest) expectLeases(descID descpb.ID, expected string) {
	testutils.SucceedsSoon(t, func() error {
		leases := t.getLeases(descID)
		if expected != leases {
			return errors.Errorf("expected %s, but found %s", expected, leases)
		}
		return nil
	})
}

func (t *leaseTest) acquire(nodeID uint32, descID descpb.ID) (lease.LeasedDescriptor, error) {
	return t.node(nodeID).Acquire(context.Background(), t.server.Clock().Now(), descID)
}

func (t *leaseTest) acquireMinVersion(
	nodeID uint32, descID descpb.ID, minVersion descpb.DescriptorVersion,
) (lease.LeasedDescriptor, error) {
	return t.node(nodeID).TestingAcquireAndAssertMinVersion(
		context.Background(), t.server.Clock().Now(), descID, minVersion)

}

func (t *leaseTest) mustAcquire(nodeID uint32, descID descpb.ID) lease.LeasedDescriptor {
	ld, err := t.acquire(nodeID, descID)
	if err != nil {
		t.Fatal(err)
	}
	return ld
}

func (t *leaseTest) mustAcquireMinVersion(
	nodeID uint32, descID descpb.ID, minVersion descpb.DescriptorVersion,
) lease.LeasedDescriptor {
	desc, err := t.acquireMinVersion(nodeID, descID, minVersion)
	if err != nil {
		t.Fatal(err)
	}
	return desc
}

func (t *leaseTest) release(nodeID uint32, desc lease.LeasedDescriptor) error {
	desc.Release(context.Background())
	return nil
}

// If leaseRemovalTracker is not nil, it will be used to block until the lease is
// released from the store. If the lease is not supposed to be released from the
// store (i.e. it's not expired and it's not for an old descriptor version),
// this shouldn't be set.
func (t *leaseTest) mustRelease(
	nodeID uint32, desc lease.LeasedDescriptor, leaseRemovalTracker *lease.LeaseRemovalTracker,
) {
	var tracker lease.RemovalTracker
	if leaseRemovalTracker != nil {
		tracker = leaseRemovalTracker.TrackRemoval(desc.Underlying())
	}
	desc.Release(context.Background())
	if leaseRemovalTracker != nil {
		if err := tracker.WaitForRemoval(); err != nil {
			t.Fatal(err)
		}
	}
}

func (t *leaseTest) publish(ctx context.Context, nodeID uint32, descID descpb.ID) error {
	_, err := t.node(nodeID).Publish(ctx, descID, func(catalog.MutableDescriptor) error {
		return nil
	}, nil)
	return err
}

func (t *leaseTest) mustPublish(ctx context.Context, nodeID uint32, descID descpb.ID) {
	if err := t.publish(ctx, nodeID, descID); err != nil {
		t.Fatal(err)
	}
}

const baseIDForLeaseTest = 1000

// node gets a Manager corresponding to a mock node. A new lease
// manager is initialized for each node. This allows for more complex
// inter-node lease testing.
func (t *leaseTest) node(nodeID uint32) *lease.Manager {
	nodeID += baseIDForLeaseTest
	mgr := t.nodes[nodeID]
	if mgr == nil {
		var c base.NodeIDContainer
		c.Set(context.Background(), roachpb.NodeID(nodeID))
		nc := base.NewSQLIDContainerForNode(&c)
		// Note: we create a fresh AmbientContext here, instead of using
		// t.server.AmbientCtx(), because we want the lease manager to
		// pretend to be a mock node with its own node ID.
		ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
		ambientCtx.AddLogTag("n", nc)
		// Hack the ExecutorConfig that we pass to the Manager to have a
		// different node id.
		cfgCpy := t.server.ExecutorConfig().(sql.ExecutorConfig)
		cfgCpy.NodeInfo.NodeID = nc
		mgr = lease.NewLeaseManager(
			ambientCtx,
			nc,
			cfgCpy.InternalDB,
			cfgCpy.Clock,
			cfgCpy.Settings,
			t.server.SettingsWatcher().(*settingswatcher.SettingsWatcher),
			cfgCpy.Codec,
			t.leaseManagerTestingKnobs,
			t.server.AppStopper(),
			cfgCpy.RangeFeedFactory,
		)
		ctx := logtags.AddTag(context.Background(), "leasemgr", nodeID)
		mgr.PeriodicallyRefreshSomeLeases(ctx)
		t.nodes[nodeID] = mgr
	}
	return mgr
}

func TestLeaseManager(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	defer log.Scope(testingT).Close(testingT)

	removalTracker := lease.NewLeaseRemovalTracker()
	var params base.TestClusterArgs
	params.ServerArgs.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	descID := t.makeTableForTest()
	ctx := context.Background()

	// We can't acquire a lease on a non-existent table.
	expected := "descriptor not found"
	if _, err := t.acquire(1, 10000); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
	// Acquire 2 leases from the same node. They should return the same
	// table and expiration.
	l1 := t.mustAcquire(1, descID)
	l2 := t.mustAcquire(1, descID)
	if l1.Underlying().GetID() != l2.Underlying().GetID() {
		t.Fatalf("expected same lease, but found %v != %v", l1, l2)
	} else if e1, e2 := l1.Expiration(), l2.Expiration(); e1 != e2 {
		t.Fatalf("expected same lease timestamps, but found %v != %v", e1, e2)
	}
	t.expectLeases(descID, "/1/1")
	t.mustRelease(1, l1, nil)
	t.mustRelease(1, l2, nil)
	t.expectLeases(descID, "/1/1")

	// It is an error to acquire a lease for a specific version that doesn't
	// exist yet.
	expected = "version 2 for descriptor foo does not exist yet"
	if _, err := t.acquireMinVersion(1, descID, 2); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}
	t.expectLeases(descID, "/1/1")

	// Publish a new version and explicitly acquire it.
	l2 = t.mustAcquire(1, descID)
	t.mustPublish(ctx, 1, descID)
	l3 := t.mustAcquireMinVersion(1, descID, 2)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the new version is released we don't
	// release the node lease.
	t.mustRelease(1, l3, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// We can still acquire a local reference on the old version since it hasn't
	// expired.
	l4 := t.mustAcquireMinVersion(1, descID, 1)
	t.mustRelease(1, l4, nil)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the old version is released the node
	// lease is also released.
	t.mustRelease(1, l2, removalTracker)
	t.expectLeases(descID, "/2/1")

	// Acquire 2 node leases on version 2.
	l5 := t.mustAcquireMinVersion(1, descID, 2)
	l6 := t.mustAcquireMinVersion(2, descID, 2)
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
	l7 := t.mustAcquireMinVersion(1, descID, 3)
	l8 := t.mustAcquireMinVersion(2, descID, 3)
	t.expectLeases(descID, "/2/1 /2/2 /3/1 /3/2")

	t.mustRelease(1, l5, removalTracker)
	t.expectLeases(descID, "/2/2 /3/1 /3/2")
	t.mustRelease(2, l6, removalTracker)
	t.expectLeases(descID, "/3/1 /3/2")

	// Wait for version 4 to be published.
	wg.Wait()
	l9 := t.mustAcquireMinVersion(1, descID, 4)
	t.mustRelease(1, l7, removalTracker)
	t.mustRelease(2, l8, nil)
	t.expectLeases(descID, "/3/2 /4/1")
	t.mustRelease(1, l9, nil)
	t.expectLeases(descID, "/3/2 /4/1")
}

func (t *leaseTest) makeTableForTest() descpb.ID {
	tdb := sqlutils.MakeSQLRunner(t.db)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	var descID descpb.ID
	tdb.QueryRow(t, "SELECT 'foo'::regclass::int").Scan(&descID)
	return descID
}

func TestLeaseManagerReacquire(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	defer log.Scope(testingT).Close(testingT)

	ctx := context.Background()
	var params base.TestClusterArgs
	params.ServerArgs.DefaultTestTenant = base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
		base.TestTenantProbabilistic, 112957,
	)
	params.ServerArgs.Settings = cluster.MakeTestingClusterSettings()
	// Set the lease duration such that the next lease acquisition will
	// require the lease to be reacquired.
	lease.LeaseDuration.Override(ctx, &params.ServerArgs.SV, 0)

	removalTracker := lease.NewLeaseRemovalTracker()
	params.ServerArgs.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseReleasedEvent: removalTracker.LeaseRemovedNotification,
			},
		},
	}

	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	descID := t.makeTableForTest()

	l1 := t.mustAcquire(1, descID)
	t.expectLeases(descID, "/1/1")
	e1 := l1.Expiration()

	// Another lease acquisition from the same node will result in a new lease.
	rt := removalTracker.TrackRemoval(l1.Underlying())
	l3 := t.mustAcquire(1, descID)
	e3 := l3.Expiration()
	if l1.Underlying().GetID() == l3.Underlying().GetID() && e3.WallTime == e1.WallTime {
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
	defer log.Scope(testingT).Close(testingT)

	t := newLeaseTest(testingT, base.TestClusterArgs{})
	defer t.cleanup()

	descID := t.makeTableForTest()

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
	defer log.Scope(testingT).Close(testingT)

	t := newLeaseTest(testingT, base.TestClusterArgs{})
	defer t.cleanup()

	if _, err := t.node(1).Publish(
		context.Background(), keys.LeaseTableID, func(desc catalog.MutableDescriptor) error {
			table := desc.(*tabledesc.Mutable)
			table.Version++
			return nil
		}, nil); !testutils.IsError(err, "updated version") {
		t.Fatalf("unexpected error: %+v", err)
	}
	if _, err := t.node(1).Publish(
		context.Background(), keys.LeaseTableID, func(desc catalog.MutableDescriptor) error {
			table := desc.(*tabledesc.Mutable)
			table.Version--
			return nil
		}, nil); !testutils.IsError(err, "updated version") {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func TestLeaseManagerDrain(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	defer log.Scope(testingT).Close(testingT)

	var params base.TestClusterArgs
	leaseRemovalTracker := lease.NewLeaseRemovalTracker()
	params.ServerArgs.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				LeaseReleasedEvent: leaseRemovalTracker.LeaseRemovedNotification,
			},
		},
	}
	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	ctx := context.Background()
	descID := t.makeTableForTest()

	{
		l1 := t.mustAcquire(1, descID)
		l2 := t.mustAcquire(2, descID)
		t.mustRelease(1, l1, nil)
		t.expectLeases(descID, "/1/1 /1/2")

		// Removal tracker to track for node 1's lease removal once the node
		// starts draining.
		l1RemovalTracker := leaseRemovalTracker.TrackRemoval(l1.Underlying())

		t.node(1).SetDraining(ctx, true, nil /* reporter */)
		t.node(2).SetDraining(ctx, true, nil /* reporter */)

		// Leases cannot be acquired when in draining mode.
		if _, err := t.acquire(1, descID); !testutils.IsError(err, "cannot acquire lease when draining") {
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
		t.node(1).SetDraining(ctx, false, nil /* reporter */)
		l1 := t.mustAcquire(1, descID)
		t.mustRelease(1, l1, nil)
		t.expectLeases(descID, "/1/1")
	}
}

// Test that we fail to lease a table that was marked for deletion.
func TestCantLeaseDeletedTable(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)()
	defer log.Scope(testingT).Close(testingT)

	var mu syncutil.Mutex
	clearSchemaChangers := false

	var params base.TestClusterArgs
	params.ServerArgs.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				mu.Lock()
				defer mu.Unlock()
				return clearSchemaChangers
			},
		},
		// Disable GC job.
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { select {} }},
	}

	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	_, err := t.db.Exec(`SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off';`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = t.db.Exec(`SET use_declarative_schema_changer = 'off';`)
	if err != nil {
		t.Fatal(err)
	}

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	_, err = t.db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "test", "t")

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
	// try to acquire at a bogus version to make sure we don't get back a lease we
	// already had.
	_, err = t.acquireMinVersion(1, tableDesc.GetID(), tableDesc.GetVersion()+123)
	if !testutils.IsError(err, "descriptor is being dropped") {
		t.Fatalf("got a different error than expected: %v", err)
	}
}

func acquire(
	ctx context.Context, s serverutils.ApplicationLayerInterface, descID descpb.ID,
) (lease.LeasedDescriptor, error) {
	return s.LeaseManager().(*lease.Manager).Acquire(ctx, s.Clock().Now(), descID)
}

// Test that once a table is marked as deleted, a lease's refcount dropping to 0
// means the lease is released immediately, as opposed to being released only
// when it expires.
func TestLeasesOnDeletedTableAreReleasedImmediately(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var mu syncutil.Mutex
	clearSchemaChangers := false

	var waitTableID descpb.ID
	deleted := make(chan bool)

	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			TestingDescriptorRefreshedEvent: func(descriptor *descpb.Descriptor) {
				mu.Lock()
				defer mu.Unlock()
				id, _, _, state, err := descpb.GetDescriptorMetadata(descriptor)
				if err != nil {
					t.Fatal(err)
				}
				if waitTableID != id {
					return
				}
				if state == descpb.DescriptorState_DROP {
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
		GCJob: &sql.GCJobTestingKnobs{RunBeforeResume: func(_ jobspb.JobID) error { select {} }},
	}
	srv, db, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	_, err := db.Exec(`SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off'`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`SET use_declarative_schema_changer = 'off';`)
	if err != nil {
		t.Fatal(err)
	}

	stmt := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	_, err = db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "test", "t")
	ctx := context.Background()

	lease1, err := acquire(ctx, s, tableDesc.GetID())
	if err != nil {
		t.Fatal(err)
	}
	lease2, err := acquire(ctx, s, tableDesc.GetID())
	if err != nil {
		t.Fatal(err)
	}

	// Block schema changers so that the table we're about to DROP is not actually
	// dropped; it will be left in a "deleted" state.
	// Also install a way to wait for the config update to be processed.
	mu.Lock()
	clearSchemaChangers = true
	waitTableID = tableDesc.GetID()
	mu.Unlock()

	// DROP the table
	_, err = db.Exec(`DROP TABLE test.t`)
	if err != nil {
		t.Fatal(err)
	}

	// Block until the Manager has processed the gossip update.
	<-deleted

	// We should still be able to acquire, because we have an active lease.
	lease3, err := acquire(ctx, s, tableDesc.GetID())
	if err != nil {
		t.Fatal(err)
	}

	// Release everything.
	lease1.Release(ctx)
	lease2.Release(ctx)
	lease3.Release(ctx)

	// Now we shouldn't be able to acquire any more.
	_, err = acquire(ctx, s, tableDesc.GetID())
	if !testutils.IsError(err, "descriptor is being dropped") {
		t.Fatalf("got a different error than expected: %v", err)
	}
}

// TestSubqueryLeases tests that all leases acquired by a subquery are
// properly tracked and released.
func TestSubqueryLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fooRelease := make(chan struct{}, 10)
	fooAcquiredCount := int32(0)
	fooReleaseCount := int32(0)
	var tableID int64
	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() == "foo" {
						atomic.AddInt32(&fooAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(id descpb.ID, _ descpb.DescriptorVersion, _ error) {
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.foo (v INT);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) > 0 {
		t.Fatalf("CREATE TABLE has acquired a lease: got %d, expected 0", atomic.LoadInt32(&fooAcquiredCount))
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "foo")
	atomic.StoreInt64(&tableID, int64(tableDesc.GetID()))

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
	defer log.Scope(t).Close(t)

	fooAcquiredCount := int32(0)
	var params base.TestServerArgs
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
	defer log.Scope(t).Close(t)

	fooAcquiredCount := int32(0)
	fooReleaseCount := int32(0)
	var tableID int64

	ctx := context.Background()
	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				// Set this so we observe a release event from the cache
				// when the API releases the descriptor.
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() == "foo" {
						log.Infof(ctx, "lease acquirer stack trace: %s", debug.Stack())
						atomic.AddInt32(&fooAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(id descpb.ID, _ descpb.DescriptorVersion, _ error) {
					log.Infof(ctx, "releasing lease for ID %d", int64(id))
					if int64(id) == atomic.LoadInt64(&tableID) {
						atomic.AddInt32(&fooReleaseCount, 1)
					}
				},
			},
		},
	}
	params.Settings = cluster.MakeTestingClusterSettings()
	// Disable the automatic stats collection, which could interfere with
	// the lease acquisition counts in this test.
	stats.AutomaticStatisticsClusterMode.Override(ctx, &params.Settings.SV, false)
	// Set a long lease duration so that the periodic task to refresh leases does
	// not run.
	lease.LeaseDuration.Override(ctx, &params.Settings.SV, 24*time.Hour)
	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.foo (v INT);
`); err != nil {
		t.Fatal(err)
	}

	if atomic.LoadInt32(&fooAcquiredCount) > 0 {
		t.Fatalf("CREATE TABLE has acquired a descriptor")
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "foo")
	atomic.StoreInt64(&tableID, int64(tableDesc.GetID()))
	log.Infof(ctx, "table ID for foo is %d", tableDesc.GetID())

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
		t.Logf("\nall stacks:\n\n%s\n", allstacks.Get())
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
	defer log.Scope(t).Close(t)

	var params base.TestServerArgs
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.DefaultTestTenant = base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(109385)
	srv, sqlDB, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`)
	require.NoError(t, err)

	// requireOneRow ensures `res` contains only one row with two
	// columns ("a", "b").
	requireOneRow := func(res *gosql.Rows) {
		for res.Next() {
			var k, v, m string
			require.Error(t, res.Scan(&k, &v, &m))
			require.NoError(t, res.Scan(&k, &v))
			require.Equal(t, "a", k)
			require.Equal(t, "b", v)
		}
		require.NoError(t, res.Close())
	}

	// A helper to assert err is a transaction restart error with an error string
	// that matches the supplied regex.
	requireRestartTransactionErrWithMsg := func(t *testing.T, err error, re string) {
		var pqe (*pq.Error)
		if !errors.As(err, &pqe) || pgcode.MakeCode(string(pqe.Code)) != pgcode.SerializationFailure ||
			!testutils.IsError(err, re) {
			t.Fatalf("expected a %v error, got: %v", re, err)
		}
	}

	// requireWriteTooOldErr ensures `err` is a WriteTooOldError.
	requireWriteTooOldErr := func(t *testing.T, err error) {
		requireRestartTransactionErrWithMsg(t, err, "WriteTooOldError")
	}

	// requireSessionExpiredErr ensures `err` is a liveness session expired error.
	requireSessionExpiredErr := func(t *testing.T, err error) {
		requireRestartTransactionErrWithMsg(t, err, "liveness session expired")
	}

	// A read-write transaction that uses the old version of the descriptor.
	txReadWrite, err := sqlDB.Begin()
	require.NoError(t, err)
	// A read-only transaction that uses the old version of the descriptor.
	txRead, err := sqlDB.Begin()
	require.NoError(t, err)
	// A write-only transaction that uses the old version of the descriptor.
	txWrite, err := sqlDB.Begin()
	require.NoError(t, err)

	// Modify the table descriptor.
	_, err = sqlDB.Exec(`ALTER TABLE t.kv ADD m CHAR DEFAULT 'z';`)
	require.NoError(t, err)
	// Wait a short bit so that the SCHEMA CHANGE GC job created by the above ADD COLUMN
	// gets to run, which will mark the old primary index of `t.kv` as tombstoned.
	time.Sleep(1 * time.Second)

	// Read-only transaction:
	// 1). reads uses a historical version of `t.kv` and sees only two columns `k`
	// and `v`;
	// 2). it commits just fine;
	rows, err := txRead.Query(`SELECT * FROM t.kv`)
	require.NoError(t, err)
	requireOneRow(rows)
	require.NoError(t, txRead.Commit())

	// Read-write transaction:
	// 1). reads uses a historical version of `t.kv` and sees only two columns `k`
	// and `v`;
	// 2). writes to that historical version of `t.kv` attempt to write to the old
	// primary index, which has previously been written with a higher timestamp
	// (when the SCHEMA CHANGE GC job lays a range tombstone on it). This is a
	// write-write conflict, and it will bump up transaction's write_ts, perform a
	// read refresh which would fail (bc reading from that new and higher
	// timestamp would return zero row, instead of one), and thus a
	// TransactionRetryWriteTooOld error will be returned.
	rows, err = txReadWrite.Query(`SELECT * FROM t.kv`)
	require.NoError(t, err)
	requireOneRow(rows)
	_, err = txReadWrite.Exec(`INSERT INTO t.kv VALUES ('c', 'd');`)
	requireWriteTooOldErr(t, err)
	require.NoError(t, txReadWrite.Rollback())

	// Write-only transaction:
	// 1). writes, similarly to the read-write transaction, will trigger a read
	// refresh and this time it will succeed (bc there is no reads).
	// 2). commits, however, will update the transaction's deadline, and the
	// leased descriptor such an "old" transaction is using is the very original
	// version (v1) of `t.kv` that expires at the modification time of v2 of
	// `t.kv`. But the bumped commit timestamp is larger than the leased
	// descriptor's expiration time, leading to a "liveness session expired"
	// error.
	_, err = txWrite.Exec(`INSERT INTO t.kv VALUES ('c', 'd');`)
	require.NoError(t, err)
	requireSessionExpiredErr(t, txWrite.Commit())
}

// Test that a lease on a table descriptor is always acquired on the latest
// version of a descriptor.
func TestLeaseAtLatestVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var params base.TestServerArgs
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "kv")
	var updated bool
	if err := crdb.ExecuteTx(context.Background(), sqlDB, nil, func(tx *gosql.Tx) error {
		// Insert an entry so that the transaction is guaranteed to be
		// assigned a timestamp.
		if _, err := tx.Exec(`
INSERT INTO t.timestamp VALUES ('a', 'b');
`); err != nil {
			return errors.WithStack(err)
		}

		// Increment the table version after the txn has started. Only do this once
		// even if there's a retry.
		if !updated {
			leaseMgr := s.LeaseManager().(*lease.Manager)
			if _, err := leaseMgr.Publish(
				context.Background(), tableDesc.GetID(), func(catalog.MutableDescriptor) error {
					// Do nothing: increments the version.
					return nil
				}, nil); err != nil {
				t.Fatal(err)
			}
			updated = true
		}

		// This select will see version 1 of the table. It will first
		// acquire a lease on version 2 and note that the table descriptor is
		// invalid for the transaction, so it will read the previous version
		// and use it.
		rows, err := tx.Query(`SELECT * FROM t.kv`)
		if err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(rows.Close())
	}); err != nil {
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
	defer log.Scope(b).Close(b)

	t := newLeaseTest(b, base.TestClusterArgs{})
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "test")
	dbID := tableDesc.GetParentID()
	tableName := tableDesc.GetName()
	leaseManager := t.node(1)

	// Acquire the lease so it is put into the nameCache.
	_, err := leaseManager.AcquireByName(
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
			_, err := leaseManager.AcquireByName(
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
	defer log.Scope(testingT).Close(testingT)

	ctx := context.Background()

	var testAcquiredCount int32
	var testAcquisitionBlockCount int32
	// Descriptor IDs for the two tables under test
	var test1ID atomic.Int32
	var test2ID atomic.Int32
	var params base.TestClusterArgs
	params.ServerArgs.DefaultTestTenant = base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
		base.TestTenantProbabilistic, 112957,
	)
	params.ServerArgs.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				// We want to track when leases get acquired and when they are renewed.
				// We also want to know when acquiring blocks to test lease renewal.
				LeaseAcquiredEvent: func(desc catalog.Descriptor, err error) {
					if err != nil {
						return
					}
					if _, isTable := desc.(catalog.TableDescriptor); isTable && !catalog.IsSystemDescriptor(desc) {
						atomic.AddInt32(&testAcquiredCount, 1)
					}
				},
				LeaseAcquireResultBlockEvent: func(typ lease.AcquireType, id descpb.ID) {
					if uint32(id) < bootstrap.TestingMinUserDescID() || typ == lease.AcquireBackground {
						return
					}
					if int32(id) != test1ID.Load() && int32(id) != test2ID.Load() {
						return
					}
					atomic.AddInt32(&testAcquisitionBlockCount, 1)
					// The test sets the IDs of the two tables only when we shouldn't  block. So if we
					// see a block event dump a stack to aid in debugging.
					log.Infof(ctx,
						"Lease acquisition of ID %d resulted in a block event. Stack trace to follow:\n%s",
						id, debug.Stack())
				},
			},
		},
	}
	params.ServerArgs.Settings = cluster.MakeTestingClusterSettings()
	// The lease jitter is set to ensure newer leases have higher
	// expiration timestamps.
	lease.LeaseJitterFraction.Override(ctx, &params.ServerArgs.SV, 0)
	// The renewal timeout is set to be the duration, so background
	// renewal should begin immediately after accessing a lease.
	lease.LeaseRenewalDuration.Override(ctx, &params.ServerArgs.SV,
		lease.LeaseDuration.Get(&params.ServerArgs.SV))

	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test1 (k CHAR PRIMARY KEY, v CHAR);
CREATE TABLE t.test2 ();
`); err != nil {
		t.Fatal(err)
	}
	// Disable the automatic stats collection, which could interfere with
	// the lease acquisition counts in this test.
	if _, err := t.db.Exec("SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false"); err != nil {
		t.Fatal(err)
	}

	test1Desc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "test1")
	test2Desc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "test2")
	dbID := test2Desc.GetParentID()

	// Acquire a lease on test1 by name.
	ts1, err := t.node(1).AcquireByName(
		ctx,
		t.server.Clock().Now(),
		dbID,
		test1Desc.GetParentSchemaID(),
		"test1",
	)
	eo1 := ts1.Expiration()
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, ts1); err != nil {
		t.Fatal(err)
	} else if count := atomic.LoadInt32(&testAcquiredCount); count != 1 {
		t.Fatalf("expected 1 lease to be acquired, but acquired %d times",
			count)
	}

	// Acquire a lease on test2 by ID.
	ts2, err := t.node(1).Acquire(ctx, t.server.Clock().Now(), test2Desc.GetID())
	if err != nil {
		t.Fatal(err)
	} else if err := t.release(1, ts2); err != nil {
		t.Fatal(err)
	} else if count := atomic.LoadInt32(&testAcquiredCount); count != 2 {
		t.Fatalf("expected 2 leases to be acquired, but acquired %d times",
			count)
	}
	eo2 := ts2.Expiration()

	// Save off the IDs of the two tables so that we increment testAcquisitionBlockCount
	// if we ever block waiting for those leases to expire.
	test1ID.Store(int32(test1Desc.GetID()))
	test2ID.Store(int32(test2Desc.GetID()))

	testutils.SucceedsSoon(t, func() error {
		// Acquire another lease by name on test1. At first this will be the
		// same lease, but eventually we will asynchronously renew a lease and
		// our acquire will get a newer lease.
		ts1, err := t.node(1).AcquireByName(
			ctx,
			t.server.Clock().Now(),
			dbID,
			test1Desc.GetParentSchemaID(),
			"test1",
		)
		if err != nil {
			t.Fatal(err)
		}
		en1 := ts1.Expiration()
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
			// acquireCount is 2 or 3: initial acquire on t1 (done), initial acquire
			// on t2 (done), asyn renew on t1 (maybe done)
			return errors.Errorf("expected at least 2 leases to be acquired, but acquired %d times",
				count)
		} else if blockCount := atomic.LoadInt32(&testAcquisitionBlockCount); blockCount > 0 {
			t.Fatalf("expected repeated lease acquisition to not block, but blockCount is: %d", blockCount)
		}

		// Acquire another lease by ID on test2. At first this will be the same
		// lease, but eventually we will asynchronously renew a lease and our
		// acquire will get a newer lease.
		ts2, err := t.node(1).Acquire(ctx, t.server.Clock().Now(), test2Desc.GetID())
		if err != nil {
			t.Fatal(err)
		}
		en2 := ts2.Expiration()
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
		} else if count := atomic.LoadInt32(&testAcquiredCount); count < 2 {
			// acquireCount is 2 or 3 or 4: initial acquire on t1 (done), initial
			// acquire on t2 (done), asyn renew on t1 (maybe done), asyn renew on t2
			// (maybe done)
			return errors.Errorf("expected at least 2 leases to be acquired, but acquired %d times",
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
	defer log.Scope(t).Close(t)

	var violations int64
	var params base.TestServerArgs
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	codec := srv.ApplicationLayer().Codec()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "kv")
	if tableDesc.GetVersion() != 1 {
		t.Fatalf("invalid version %d", tableDesc.GetVersion())
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
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "kv1")
	if tableDesc.GetVersion() != 2 {
		t.Fatalf("invalid version %d", tableDesc.GetVersion())
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
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "kv1")
	if tableDesc.GetVersion() != 2 {
		t.Fatalf("invalid version %d", tableDesc.GetVersion())
	}

	// Transaction successfully used the old version.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "t", "kv2")
	if tableDesc.GetVersion() != 3 {
		t.Fatalf("invalid version %d", tableDesc.GetVersion())
	}
}

// Tests that when a transaction has already returned results
// to the user and the transaction continues on to make a schema change,
// whenever the table lease two version invariant is violated and the
// transaction needs to be restarted, a retryable error is returned to the
// user. This specific version validations that release savepoint does the
// same with cockroach_restart, which commits on release.
func TestTwoVersionInvariantRetryErrorWitSavePoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var violations int64
	var params base.TestServerArgs
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "kv")
	if tableDesc.GetVersion() != 1 {
		t.Fatalf("invalid version %d", tableDesc.GetVersion())
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
	_, err = txRetry.Exec("SAVEPOINT cockroach_restart;")
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
		defer wg.Done()
		// This can hang waiting for one version before tx.Commit() is
		// called below, so it is executed in another goroutine.
		_, err := txRetry.Exec("RELEASE SAVEPOINT cockroach_restart;")
		if !testutils.IsError(err,
			fmt.Sprintf(`TransactionRetryWithProtoRefreshError: cannot publish new versions for descriptors: \[\{kv1 %d 1\}\], old versions still in use`, tableDesc.GetID()),
		) {
			t.Errorf("err = %v", err)
		}
		err = txRetry.Rollback()
		if err != nil {
			t.Errorf("err = %v", err)
		}
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

// Tests that when a transaction has already returned results
// to the user and the transaction continues on to make a schema change,
// whenever the table lease two version invariant is violated and the
// transaction needs to be restarted, a retryable error is returned to the
// user.
func TestTwoVersionInvariantRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var violations int64
	var params base.TestServerArgs
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "kv")
	if tableDesc.GetVersion() != 1 {
		t.Fatalf("invalid version %d", tableDesc.GetVersion())
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
			fmt.Sprintf(`TransactionRetryWithProtoRefreshError: cannot publish new versions for descriptors: \[\{kv1 %d 1\}\], old versions still in use`, tableDesc.GetID()),
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
	defer log.Scope(testingT).Close(testingT)

	// Decide how long we should run this.
	maxTime := time.Duration(20) * time.Second
	if skip.NightlyStress() {
		maxTime = time.Duration(2) * time.Minute
	}

	// Which table to exercise the test against.
	const descID = keys.LeaseTableID

	t := newLeaseTest(testingT, base.TestClusterArgs{})
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
			table := desc.(catalog.TableDescriptor)

			// Wait a little time to give a chance to other goroutines to
			// race past.
			time.Sleep(20 * time.Millisecond)

			// Now check that the version that was updated is indeed observable
			// in the database at the time it says it was modified.
			//
			// This checks that the modification timestamp is not lying about
			// the transaction commit time (and that the txn commit time wasn't
			// bumped past it).
			log.Infof(ctx, "checking version %d", table.GetVersion())
			txn := kv.NewTxn(ctx, t.kvDB, roachpb.NodeID(0))
			// Make the txn look back at the known modification timestamp.
			require.NoError(t, txn.SetFixedTimestamp(ctx, table.GetModificationTime()))

			// Look up the descriptor.
			descKey := catalogkeys.MakeDescMetadataKey(t.server.Codec(), descID)
			res, err := txn.Get(ctx, descKey)
			if err != nil {
				t.Fatalf("error while reading proto: %v", err)
			}
			b, err := descbuilder.FromSerializedValue(res.Value)
			if err != nil {
				t.Fatal(err)
			}
			// Look at the descriptor that comes back from the database.
			if b != nil && b.DescriptorType() == catalog.Table {
				dbTable := b.BuildImmutable()
				if v, modTime := dbTable.GetVersion(), dbTable.GetModificationTime(); v != table.GetVersion() || modTime != table.GetModificationTime() {
					t.Fatalf("db has version %d at ts %s, expected version %d at ts %s",
						v, modTime, table.GetVersion(), table.GetModificationTime())
				}
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
	defer log.Scope(testingT).Close(testingT)

	ctx := context.Background()

	var mu syncutil.Mutex
	releasedIDs := catalog.DescriptorIDSet{}
	var testAcquiredCount int32
	var testAcquisitionBlockCount int32
	var expected catalog.DescriptorIDSet

	var params base.TestClusterArgs
	params.ServerArgs.DefaultTestTenant = base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
		base.TestTenantProbabilistic, 112957,
	)
	params.ServerArgs.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				// We want to track when leases get acquired and when they are renewed.
				// We also want to know when acquiring blocks to test lease renewal.
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if !catalog.IsSystemDescriptor(desc) {
						atomic.AddInt32(&testAcquiredCount, 1)
					}
				},
				LeaseReleasedEvent: func(id descpb.ID, v descpb.DescriptorVersion, err error) {
					mu.Lock()
					defer mu.Unlock()
					if !expected.Contains(id) {
						return
					}
					releasedIDs.Add(id)
				},
				LeaseAcquireResultBlockEvent: func(typ lease.AcquireType, id descpb.ID) {
					mu.Lock()
					defer mu.Unlock()
					if !expected.Contains(id) || typ == lease.AcquireBackground {
						return
					}
					atomic.AddInt32(&testAcquisitionBlockCount, 1)
				},
			},
			TestingDescriptorUpdateEvent: func(_ *descpb.Descriptor) error {
				return errors.Errorf("ignore gossip update")
			},
		},
	}
	params.ServerArgs.Settings = cluster.MakeTestingClusterSettings()

	// The lease jitter is set to ensure newer leases have higher
	// expiration timestamps.
	lease.LeaseJitterFraction.Override(ctx, &params.ServerArgs.SV, 0)
	// Lease duration to something small.
	lease.LeaseDuration.Override(ctx, &params.ServerArgs.SV, 50*time.Millisecond)
	// Renewal timeout to 0 saying that the lease will get renewed only
	// after the lease expires when a request requests the descriptor.
	lease.LeaseRenewalDuration.Override(ctx, &params.ServerArgs.SV, 0)

	t := newLeaseTest(testingT, params)
	defer t.cleanup()

	if _, err := t.db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test1 (k CHAR PRIMARY KEY, v CHAR);
CREATE TABLE t.test2 ();
`); err != nil {
		t.Fatal(err)
	}

	test1Desc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "test1")
	test2Desc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "test2")
	dbID := test2Desc.GetParentID()
	func() {
		mu.Lock()
		defer mu.Unlock()
		expected = catalog.MakeDescriptorIDSet(test1Desc.GetID(), test2Desc.GetID())
		atomic.StoreInt32(&testAcquisitionBlockCount, 0)
	}()

	releasedLeases := func() catalog.DescriptorIDSet {
		mu.Lock()
		defer mu.Unlock()
		return catalog.MakeDescriptorIDSet(releasedIDs.Ordered()...)
	}
	if released := releasedLeases(); released.Len() != 0 {
		t.Fatalf("expected no leases to be released, released %v", released.Ordered())
	}

	// Acquire a lease on test1 by name.
	ts1, err := t.node(1).AcquireByName(
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
	ts2, err := t.node(1).Acquire(ctx, t.server.Clock().Now(), test2Desc.GetID())
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
		if count := atomic.LoadInt32(&testAcquiredCount); count < 4 {
			// Wait for the background renew goroutine to renew t1 and t2 at least
			// once, making AcquireCount at least 4: initial acquire on t1, initial
			// acquire on t2, renew on t1, renew on t2.
			return errors.Errorf("expected at least 4 leases to be acquired, but acquired %d times", count)
		}
		released := releasedLeases()
		if notYetReleased := expected.Difference(released); notYetReleased.Len() != 0 {
			return errors.Errorf("expected %v to be released, released %v", expected.Ordered(), released.Ordered())
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
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(109385),
	})
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
	defer log.Scope(t).Close(t)

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					MaxOffset: time.Second,
				},
			},
			DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(109385),
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(context.Background())
	sqlDB := tc.ApplicationLayer(0).SQLConn(t)

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
	tx1, err := tc.ApplicationLayer(1).SQLConn(t, serverutils.DBName("t")).Begin()
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
	defer log.Scope(testingT).Close(testingT)

	var params base.TestClusterArgs
	params.ServerArgs.Knobs = base.TestingKnobs{
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

	beforeDesc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "before")
	afterDesc := desctestutils.TestingGetPublicTableDescriptor(t.kvDB, t.server.Codec(), "t", "after")
	dbID := beforeDesc.GetParentID()

	// Acquire a lease on "before" by name.
	beforeTable, err := t.node(1).AcquireByName(
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
	afterTable, err := t.node(1).AcquireByName(
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
	t.expectLeases(beforeDesc.GetID(), "/1/1")
	t.expectLeases(afterDesc.GetID(), "/1/1")

	// Call DeleteOrphanedLeases() with the server startup time.
	t.node(1).DeleteOrphanedLeases(ctx, now)
	// Orphaned lease is gone.
	t.expectLeases(beforeDesc.GetID(), "")
	t.expectLeases(afterDesc.GetID(), "/1/1")
}

// Test that acquiring a lease doesn't block on other transactions performing
// schema changes. Lease acquisitions run in high-priority transactions, thereby
// pushing any locks held by schema-changing transactions out of their ways.
func TestLeaseAcquisitionDoesntBlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	_, err := db.Exec(`CREATE DATABASE t; CREATE TABLE t.test(k CHAR PRIMARY KEY, v CHAR);`)
	require.NoError(t, err)

	// Figure out the table ID.
	row := db.QueryRow("SELECT id FROM system.namespace WHERE name='test'")
	var descID descpb.ID
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

	l, err := s.LeaseManager().(*lease.Manager).Acquire(ctx, s.Clock().Now(), descID)
	require.NoError(t, err)

	// Release the lease so that the schema change can proceed.
	l.Release(ctx)
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

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
	defer log.Scope(t).Close(t)
	defer ensureTestTakesLessThan(t, 30*time.Second)()
	skip.UnderDuress(t, "test must take less than 30 seconds, so avoid slow configs")

	ctx := context.Background()
	var interestingTable atomic.Value
	interestingTable.Store(descpb.ID(0))
	blockLeaseAcquisitionOfInterestingTable := make(chan chan struct{})
	unblockAll := make(chan struct{})
	args := base.TestServerArgs{}
	args.DefaultTestTenant = base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
		base.TestTenantProbabilistic, 112957,
	)
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: args,
	})
	descUpdateChan := make(chan *descpb.Descriptor)
	args.Knobs.SQLLeaseManager = &lease.ManagerTestingKnobs{
		TestingDescriptorUpdateEvent: func(descriptor *descpb.Descriptor) error {
			// Use this testing knob to ensure that we see an update for the desc
			// in question. We don't care about events to refresh the first version
			// which can happen under rare stress scenarios.
			id, version, _, _, err := descpb.GetDescriptorMetadata(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if id == interestingTable.Load().(descpb.ID) && version >= 2 {
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
				if desc.GetID() != interestingTable.Load().(descpb.ID) {
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
	tc.AddAndStartServer(t, args)
	defer tc.Stopper().Stop(ctx)

	db1 := tc.ServerConn(0)
	tdb1 := sqlutils.MakeSQLRunner(db1)
	db2 := tc.ServerConn(1)

	// Create a couple of descriptors.
	tdb1.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")

	// Find the desc ID for the desc we'll be mucking with.
	var tableID descpb.ID
	tdb1.QueryRow(t, "SELECT table_id FROM crdb_internal.tables WHERE name = $1 AND database_name = current_database()",
		"foo").Scan(&tableID)
	interestingTable.Store(tableID)

	// Launch a goroutine to query foo. It will be blocked in lease acquisition.
	selectDone := make(chan error, 1)
	go func() {
		var count int
		selectDone <- db2.QueryRow("SELECT count(*) FROM foo").Scan(&count)
	}()

	// Make sure it got blocked.
	toUnblockForLeaseAcquisition := <-blockLeaseAcquisitionOfInterestingTable

	// Launch a goroutine to perform a schema change which will lead to new
	// versions.
	alterErrCh := make(chan error, 1)
	go func() {
		_, err := db1.Exec("ALTER TABLE foo RENAME COLUMN i TO j")
		alterErrCh <- err
	}()

	// Make sure we get an update. Note that this is after we have already
	// acquired a lease on the old version but have not yet recorded that fact.
	select {
	case err := <-alterErrCh:
		t.Fatalf("alter succeeded before expected: %v", err)
	case err := <-selectDone:
		t.Fatalf("select succeeded before expected: %v", err)
	case desc := <-descUpdateChan:
		_, version, _, _, err := descpb.GetDescriptorMetadata(desc)
		require.NoError(t, err)
		require.Equal(t, descpb.DescriptorVersion(2), version)
	}

	// Allow the original lease acquisition to proceed.
	close(toUnblockForLeaseAcquisition)

	// Ensure the query completes.
	<-selectDone

	// Allow everything to proceed as usual.
	close(unblockAll)
	// Ensure the schema change completes.
	<-alterErrCh

	// Ensure that the new schema is in use on n2.
	var j int
	require.Equal(t, gosql.ErrNoRows, db2.QueryRow("SELECT j FROM foo").Scan(&j))
}

// TestLeaseWithOfflineTables checks that leases on tables which had
// previously gone offline at some point are not gratuitously dropped.
// See #57834.
func TestLeaseWithOfflineTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var descID uint32
	testTableID := func() descpb.ID {
		return descpb.ID(atomic.LoadUint32(&descID))
	}

	var lmKnobs lease.ManagerTestingKnobs
	blockDescRefreshed := make(chan struct{}, 1)
	lmKnobs.TestingDescriptorRefreshedEvent = func(desc *descpb.Descriptor) {
		tbl, _, _, _, _ := descpb.GetDescriptors(desc)
		if tbl != nil && testTableID() == tbl.ID {
			blockDescRefreshed <- struct{}{}
		}
	}

	ctx := context.Background()
	var params base.TestServerArgs
	params.Knobs.SQLLeaseManager = &lmKnobs
	srv, db, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(db)

	// This statement prevents timer issues due to periodic lease refreshing.
	_, err := db.Exec(`
		SET CLUSTER SETTING sql.tablecache.lease.refresh_limit = 0;
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE DATABASE t;
		CREATE TABLE t.test(s STRING PRIMARY KEY);
	`)
	require.NoError(t, err)

	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "test")
	atomic.StoreUint32(&descID, uint32(desc.GetID()))

	// Sets table descriptor state and waits for that change to propagate to the
	// lease manager's refresh worker.
	setTableState := func(expected descpb.DescriptorState, next descpb.DescriptorState) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
		) error {
			desc, err := descsCol.MutableByID(txn.KV()).Table(ctx, testTableID())
			require.NoError(t, err)
			require.Equal(t, desc.State, expected)
			desc.State = next
			return descsCol.WriteDesc(ctx, false /* kvTrace */, desc, txn.KV())
		}))

		// Wait for the lease manager's refresh worker to have processed the
		// descriptor update.
		<-blockDescRefreshed
	}

	// Checks that the lease manager state for `t.test` matches expectations.
	checkLeaseState := func(shouldBePresent bool) {
		var found bool
		var wasTakenOffline bool
		fn := func(desc catalog.Descriptor, takenOffline bool, _ int, _ tree.DTimestamp) bool {
			if testTableID() != desc.GetID() {
				return true
			}
			wasTakenOffline = takenOffline
			found = true
			return false
		}
		s.LeaseManager().(*lease.Manager).VisitLeases(fn)
		if found && !wasTakenOffline {
			require.Truef(t, shouldBePresent, "lease should not have been present but was")
		} else if found {
			require.Falsef(t, shouldBePresent, "lease should have been present but was marked as taken offline")
		} else {
			require.Falsef(t, shouldBePresent, "lease should have been present but wasn't")
		}
	}

	// Check initial state.
	checkLeaseState(false /* shouldBePresent */)

	// Query the table, this should trigger a lease acquisition.
	runner.CheckQueryResults(t, "SELECT s FROM t.test", [][]string{})
	checkLeaseState(true /* shouldBePresent */)

	// Take the table offline and back online again.
	// This should not relinquish the lease anymore
	// and offline ones will now be held.
	setTableState(descpb.DescriptorState_PUBLIC, descpb.DescriptorState_OFFLINE)
	setTableState(descpb.DescriptorState_OFFLINE, descpb.DescriptorState_PUBLIC)
	checkLeaseState(true /* shouldBePresent */)

	// Take the table dropped and back online again.
	// This should relinquish the lease.
	setTableState(descpb.DescriptorState_PUBLIC, descpb.DescriptorState_DROP)
	setTableState(descpb.DescriptorState_DROP, descpb.DescriptorState_PUBLIC)
	checkLeaseState(false /* shouldBePresent */)

	// Query the table, thereby acquiring a lease once again.
	runner.CheckQueryResults(t, "SELECT s FROM t.test", [][]string{})
	checkLeaseState(true /* shouldBePresent */)

	// Do a no-op descriptor update, lease should still be present.
	setTableState(descpb.DescriptorState_PUBLIC, descpb.DescriptorState_PUBLIC)
	checkLeaseState(true /* shouldBePresent */)
}

// TestOutstandingLeasesMetric tests the gauge that keeps track of the number of
// outstanding SQL leases on a node.
//
// N.B.: If this flakes, it's probably because there are internal processes
// acquiring leases on things. If it starts to get flaky, it's probably easier
// to just delete it than deflake it.
func TestOutstandingLeasesMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	_, err := sqlDB.ExecContext(ctx, "CREATE TABLE a (a INT PRIMARY KEY)")
	assert.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, "CREATE TABLE b (a INT PRIMARY KEY)")
	assert.NoError(t, err)
	gauge := s.LeaseManager().(*lease.Manager).TestingOutstandingLeasesGauge()
	outstandingLeases := gauge.Value()

	_, err = sqlDB.ExecContext(ctx, "SELECT * FROM a")
	assert.NoError(t, err)

	afterQuery := gauge.Value()
	// Expect at least 2 leases: one for a, and one for the default database.
	// The reason that this isn't precise is that there are internal queries that
	// run in a server that might acquire leases. It's a pain to get these all
	// removed in our test scenario.
	actual := afterQuery - outstandingLeases
	if actual < 2 {
		t.Errorf("expected at least 2 outstanding leases, found %d", actual)
	}

	// Expect at least 3 leases: one for a, one for the default database, and one for b.
	_, err = sqlDB.ExecContext(ctx, "SELECT * FROM b")
	assert.NoError(t, err)

	afterQuery = gauge.Value()
	actual = afterQuery - outstandingLeases
	if actual < 3 {
		t.Errorf("expected at least 3 outstanding leases, found %d", actual)
	}
}

// TestHistoricalAcquireDroppedDescriptor ensures that a historical transaction
// can read an old descriptor.
func TestHistoricalAcquireDroppedDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const typeName = "foo"
	seenDrop := make(chan error)
	recvSeenDrop := seenDrop
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLLeaseManager: &lease.ManagerTestingKnobs{
					TestingDescriptorRefreshedEvent: func(descriptor *descpb.Descriptor) {
						_, _, name, state, err := descpb.GetDescriptorMetadata(descriptor)
						if err != nil {
							t.Fatal(err)
						}
						if name != typeName || seenDrop == nil {
							return
						}
						if state == descpb.DescriptorState_DROP {
							close(seenDrop)
							seenDrop = nil
						}
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE TYPE "+typeName+" AS ENUM ('a')")
	var now string
	tdb.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&now)
	tdb.CheckQueryResults(t, `WITH a AS (SELECT 'a'::`+typeName+`) SELECT * FROM a`, [][]string{{"a"}})
	tdb.CheckQueryResults(t, `WITH a AS (SELECT 'a'::`+typeName+`) SELECT * FROM a AS OF SYSTEM TIME `+now, [][]string{{"a"}})
	tdb.Exec(t, "DROP TYPE foo")
	// Make sure that the leases on the old version get dropped.
	<-recvSeenDrop
	// This should still work.
	tdb.CheckQueryResults(t, `WITH a AS (SELECT 'a'::`+typeName+`) SELECT * FROM a AS OF SYSTEM TIME `+now, [][]string{{"a"}})
}

// Tests acquiring read leases on previous versions of a table descriptor from
// store.
func TestHistoricalDescriptorAcquire(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Create a schema, create table, alter table a few times to get some history
	// of tables while keeping timestamp checkpoints for acquire query
	tdb.Exec(t, "CREATE SCHEMA sc")
	tdb.Exec(t, "CREATE TABLE sc.foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "INSERT INTO sc.foo VALUES (1)")

	var ts1Str string
	tdb.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts1Str)
	ts1, err := hlc.ParseHLC(ts1Str)
	require.NoError(t, err)

	tdb.Exec(t, "ALTER TABLE sc.foo ADD COLUMN id UUID NOT NULL DEFAULT gen_random_uuid()")
	tdb.Exec(t, "ALTER TABLE sc.foo RENAME COLUMN i TO former_id")
	tdb.Exec(t, "ALTER TABLE sc.foo RENAME COLUMN id TO current_id")

	// Store table descriptor ID
	var tableID atomic.Value
	storeID := func(val *atomic.Value, name string) {
		var id descpb.ID
		tdb.QueryRow(t, `SELECT id FROM system.namespace WHERE name = $1`, name).Scan(&id)
		require.NotEqual(t, descpb.ID(0), id)
		val.Store(id)
	}
	storeID(&tableID, "foo")

	// Acquire descriptor version valid at timestamp ts1. Waits for the most
	// recent version with the name column before doing so.
	_, err = s.LeaseManager().(*lease.Manager).WaitForOneVersion(ctx, tableID.Load().(descpb.ID), base.DefaultRetryOptions())
	require.NoError(t, err, "Failed to wait for one version of descriptor: %s", err)
	acquiredDescriptor, err := s.LeaseManager().(*lease.Manager).Acquire(ctx, ts1, tableID.Load().(descpb.ID))
	assert.NoError(t, err)

	// Ensure the modificationTime <= timestamp < expirationTime
	modificationTime := acquiredDescriptor.Underlying().GetModificationTime()
	assert.Truef(t, modificationTime.LessEq(ts1) &&
		ts1.Less(acquiredDescriptor.Expiration()), "modification: %s, ts1: %s, "+
		"expiration: %s", modificationTime.String(), ts1.String(),
		acquiredDescriptor.Expiration().String())
}

func TestDropDescriptorRacesWithAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We want to have a transaction to acquire a descriptor on one
	// node that starts and reads version 1. Then we'll write a new
	// version of the descriptor and then we'll let the acquisition
	// finish. Before the commit which added this test, that acquired
	// lease would not be dropped when it was not in use anymore because
	// it would think it was the latest.

	const tableName = "foo"
	leaseAcquiredEventCh := make(chan chan struct{}, 1)
	var seenUpdatesAtVersion2 int64
	leaseRefreshedForVersion2 := make(chan struct{})
	recvLeaseRefreshedForVersion2 := leaseRefreshedForVersion2
	testingKnobs := base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			TestingDescriptorUpdateEvent: func(descriptor *descpb.Descriptor) error {
				_, version, name, _, err := descpb.GetDescriptorMetadata(descriptor)
				if err != nil {
					t.Fatal(err)
				}
				if name != tableName {
					return nil
				}
				// Just so we don't get blocked on the refresh below.
				if version != 2 {
					return errors.New("swallowed")
				}
				if atomic.AddInt64(&seenUpdatesAtVersion2, 1) != 1 {
					return errors.New("swallowed")
				}
				return nil
			},
			TestingDescriptorRefreshedEvent: func(descriptor *descpb.Descriptor) {
				_, version, name, _, err := descpb.GetDescriptorMetadata(descriptor)
				if err != nil {
					t.Fatal(err)
				}
				if name != tableName || version != 2 {
					return
				}
				// Just so we don't get blocked on the refresh below.
				if leaseRefreshedForVersion2 != nil {
					close(leaseRefreshedForVersion2)
					leaseRefreshedForVersion2 = nil
				}
			},
			LeaseStoreTestingKnobs: lease.StorageTestingKnobs{
				RemoveOnceDereferenced: true,
				LeaseAcquiredEvent: func(desc catalog.Descriptor, _ error) {
					if desc.GetName() != tableName {
						return
					}
					unblock := make(chan struct{})
					select {
					case leaseAcquiredEventCh <- unblock:
					default:
						return
					}
					<-unblock
				},
			},
		},
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: testingKnobs,
		},
	})
	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)

	// Create our table. This will not acquire a lease.
	{
		_, err := db.Exec("CREATE TABLE foo ()")
		require.NoError(t, err)
	}

	// Attempt to acquire the lease; it will block.
	readFromFooErr := make(chan error, 1)
	go func() {
		_, err := db.Exec("SELECT rowid FROM foo")
		readFromFooErr <- err
	}()

	var unblockLeaseRefresh chan<- struct{}
	select {
	case unblockLeaseRefresh = <-leaseAcquiredEventCh:
	case <-readFromFooErr:
		t.Fatal("expected this to be blocked on lease acquisition")
	}

	// This will create a version 2 which is dropped and then will wait
	// to drain the name.
	dropErrChan := make(chan error, 1)
	go func() {
		_, err := db.Exec("SET use_declarative_schema_changer = off;DROP TABLE foo")
		dropErrChan <- err
	}()
	// Detect that the drop was noticed by the lease manager (note that this
	// precedes the older version being seen).
	<-recvLeaseRefreshedForVersion2

	// Now let the read proceed.
	close(unblockLeaseRefresh)
	require.NoError(t, <-readFromFooErr)
	require.NoError(t, <-dropErrChan)

	tc.Server(0).LeaseManager().(*lease.Manager).VisitLeases(func(
		desc catalog.Descriptor, takenOffline bool, refCount int, expiration tree.DTimestamp,
	) (wantMore bool) {
		t.Log(desc, takenOffline, refCount, expiration)
		return true
	})
}

// TestOfflineLeaseRefresh validates that no live lock can occur,
// after a table is brought offline. Specifically a table a will be
// brought offline, and then one transaction will attempt to bring it
// online while another transaction will attempt to do a read. The read
// transaction could previously push back the lease of transaction
// trying to online the table perpetually (as seen in issue #61798).
func TestOfflineLeaseRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	waitForTxn := make(chan chan struct{})
	waitForRqstFilter := make(chan chan struct{})
	errorChan := make(chan error)
	var txnID uuid.UUID
	var mu syncutil.RWMutex

	knobs := &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, req *kvpb.BatchRequest) *kvpb.Error {
			mu.RLock()
			checkRequest := req.Txn != nil && req.Txn.ID.Equal(txnID)
			mu.RUnlock()
			if _, ok := req.GetArg(kvpb.EndTxn); checkRequest && ok {
				notify := make(chan struct{})
				waitForRqstFilter <- notify
				<-notify
			}
			return nil
		},
	}
	params := base.TestServerArgs{Knobs: base.TestingKnobs{Store: knobs}}
	srv, conn, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Create t1 that will be offline, and t2,
	// that will serve inserts.
	_, err := conn.Exec(`
CREATE DATABASE d1;
CREATE TABLE d1.t1 (name int);
INSERT INTO d1.t1 values(5);
INSERT INTO d1.t1 values(5);
INSERT INTO d1.t1 values(5);
CREATE TABLE d1.t2 (name int);
`)
	require.NoError(t, err)

	// Force the table descriptor into a offline state
	cfg := s.ExecutorConfig().(sql.ExecutorConfig)
	var tableID descpb.ID
	require.NoError(t, sql.DescsTxn(ctx, &cfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		tn := tree.NewTableNameWithSchema("d1", "public", "t1")
		_, tableDesc, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), tn)
		if err != nil {
			return err
		}
		tableID = tableDesc.GetID()
		tableDesc.SetOffline("For unit test")
		err = descriptors.WriteDesc(ctx, false, tableDesc, txn.KV())
		if err != nil {
			return err
		}
		return nil
	}))

	go func() {
		err := sql.DescsTxn(ctx, &cfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			close(waitForRqstFilter)
			mu.Lock()
			waitForRqstFilter = make(chan chan struct{})
			txnID = txn.KV().ID()
			mu.Unlock()

			// Online the descriptor by making it public
			tableDesc, err := descriptors.MutableByID(txn.KV()).Table(ctx, tableID)
			if err != nil {
				return err
			}
			tableDesc.SetPublic()
			err = descriptors.WriteDesc(ctx, false, tableDesc, txn.KV())
			if err != nil {
				return err
			}
			// Allow the select on the table to proceed,
			// so that it waits on the channel at the appropriate
			// moment.
			notify := make(chan struct{})
			waitForTxn <- notify
			<-notify

			// Select from an unrelated table
			_, err = txn.ExecEx(ctx, "inline-exec", txn.KV(),
				sessiondata.RootUserSessionDataOverride,
				"insert into d1.t2 values (10);")
			return err

		})
		close(waitForTxn)
		close(waitForRqstFilter)
		errorChan <- err
	}()

	for notify := range waitForTxn {
		close(notify)
		mu.RLock()
		rqstFilterChannel := waitForRqstFilter
		mu.RUnlock()
		for notify2 := range rqstFilterChannel {
			// Push the query trying to online the table out by
			// leasing out the table again
			_, err = conn.Query("select  * from d1.t1")
			require.EqualError(t, err, "pq: relation \"t1\" is offline: For unit test",
				"Table offline error was not generated as expected")
			close(notify2)
		}
	}
	require.NoError(t, <-errorChan)
	close(errorChan)
}

// Validates that the transaction deadline can be extended
// past the original lease duration. Previously, we had a
// a limitation if the transaction took longer then the
// the lease, the transaction would fail because of the
// deadline.
func TestLeaseTxnDeadlineExtension(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	filterMu := syncutil.Mutex{}
	blockTxn := make(chan struct{})
	blockedOnce := false
	var txnID string

	var params base.TestServerArgs
	params.DefaultTestTenant = base.TestDoesNotWorkWithSharedProcessModeButWeDontKnowWhyYet(
		base.TestTenantProbabilistic, 112957,
	)
	params.Settings = cluster.MakeTestingClusterSettings()
	// Set the lease duration such that the next lease acquisition will
	// require the lease to be reacquired.
	lease.LeaseDuration.Override(ctx, &params.SV, 0)
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, req *kvpb.BatchRequest) *kvpb.Error {
			filterMu.Lock()
			// Wait for a commit with the txnID, and only allows
			// it to resume when the channel gets unblocked.
			if req.Txn != nil && req.Txn.ID.String() == txnID {
				filterMu.Unlock()
				// There will only be a single EndTxn request in
				// flight due to the transaction ID filter and
				// blocked once flag, so no mutex is needed here.
				if req.IsSingleEndTxnRequest() && !blockedOnce {
					<-blockTxn
					blockedOnce = true
				}
			} else {
				filterMu.Unlock()
			}
			return nil
		},
	}

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	// Setup tables for the test.
	_, err := conn.Exec(`
CREATE TABLE t1(val int);
	`)
	require.NoError(t, err)
	// Validates that transaction deadlines can move forward into
	// the future after lease expiry.
	t.Run("validate-lease-txn-deadline-ext", func(t *testing.T) {
		conn, err := tc.ServerConn(0).Conn(ctx)
		require.NoError(t, err)
		descModConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		waitChan := make(chan error)
		resumeChan := make(chan struct{})
		go func() {
			ctx = context.Background()
			// Start a transaction that will lease out a table,
			// and let the lease duration expire.
			_, err := conn.ExecContext(ctx, `
BEGIN;
SELECT * FROM t1;
	`)
			if err != nil {
				waitChan <- err
				return
			}
			// Fetch the transaction ID, so that we can delay the commit
			txnIDResult := conn.QueryRowContext(ctx, `SELECT id FROM crdb_internal.node_transactions WHERE session_id IN (SELECT * FROM [SHOW session_id]);`)
			if txnIDResult.Err() != nil {
				waitChan <- txnIDResult.Err()
				return
			}
			filterMu.Lock()
			err = txnIDResult.Scan(&txnID)
			blockedOnce = false
			filterMu.Unlock()
			if err != nil {
				waitChan <- err
				return
			}
			// Inform the main routine that it can cause an operation
			// to block us.
			waitChan <- nil
			<-resumeChan
			// Execute an insert once the other transaction
			// gets a lease. The lease renewal should adjust
			// our deadline.
			_, err = conn.ExecContext(ctx, `
INSERT INTO t1 VALUES (1);
COMMIT;`,
			)
			waitChan <- err
		}()

		// Wait for the TXN ID and hook to be setup.
		err = <-waitChan
		require.NoError(t, err)
		// Issue a select from a different connection that will
		// need a lease.
		descModConn.Exec(t, `
SELECT * FROM T1;`)
		resumeChan <- struct{}{}
		blockTxn <- struct{}{}
		err = <-waitChan
		require.NoError(t, err)
	})

	// Validates that the transaction deadline extension can be blocked,
	// if the lease can't be renewed, for example if the descriptor gets
	// modified.
	t.Run("validate-lease-txn-deadline-ext-blocked", func(t *testing.T) {
		conn, err := tc.ServerConn(0).Conn(ctx)
		require.NoError(t, err)
		descModConn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		waitChan := make(chan error)
		resumeChan := make(chan struct{})
		go func() {
			ctx = context.Background()
			// Start a transaction that will lease out a table,
			// and let the lease duration expire.
			_, err := conn.ExecContext(ctx, `
BEGIN;
SELECT * FROM t1;
	`)
			if err != nil {
				waitChan <- err
				return
			}
			// Fetch the transaction ID, so that we can delay the commit
			txnIDResult := conn.QueryRowContext(ctx, `SELECT id FROM crdb_internal.node_transactions WHERE session_id IN (SELECT * FROM [SHOW session_id]);`)
			if txnIDResult.Err() != nil {
				waitChan <- txnIDResult.Err()
				return
			}
			filterMu.Lock()
			err = txnIDResult.Scan(&txnID)
			blockedOnce = false
			filterMu.Unlock()
			if err != nil {
				waitChan <- err
				return
			}
			// Inform the main routine that it can cause an operation
			// to block us.
			waitChan <- nil
			<-resumeChan
			// Execute an insert on the same connection and attempt
			// to commit, this operation will fail.
			_, err = conn.ExecContext(ctx, `
INSERT INTO t1 VALUES (1);`,
			)
			if err != nil {
				waitChan <- err
				return
			}
			_, err = conn.ExecContext(ctx, `
COMMIT;`,
			)
			if err == nil {
				err = errors.New("Failing did not get expected error")
			} else if !testutils.IsError(err, "pq: restart transaction: TransactionRetryWithProtoRefreshError: TransactionRetryError: retry txn \\(RETRY_COMMIT_DEADLINE_EXCEEDED -.*") {
				err = errors.Wrap(err, "Failed unexpected error")
			} else {
				err = nil
			}
			waitChan <- err
		}()

		// Wait for the TXN ID and hook to be setup.
		err = <-waitChan
		require.NoError(t, err)
		// Issue an alter column on a different connection, which
		// will require a lease.
		descModConn.Exec(t, `
ALTER TABLE T1 ALTER COLUMN VAL SET DEFAULT 5;
SELECT * FROM T1`)
		resumeChan <- struct{}{}
		blockTxn <- struct{}{}
		err = <-waitChan
		require.NoError(t, err)
	})
}

// Validates that the transaction deadline will be
// updated for implicit transactions before the autocommit,
// if the deadline is found to be expired.
func TestLeaseBulkInsertWithImplicitTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	beforeExecute := syncutil.Mutex{}
	// Statement that will be paused.
	beforeExecuteStmt := ""
	beforeExecuteWait := make(chan chan struct{})
	// Statement that will allow any paused statement to resume.
	beforeExecuteResumeStmt := ""

	ctx := context.Background()

	var params base.TestClusterArgs
	params.ServerArgs.Settings = cluster.MakeTestingClusterSettings()
	// Set the lease duration such that the next lease acquisition will
	// require the lease to be reacquired.
	lease.LeaseDuration.Override(ctx, &params.ServerArgs.SV, 0)
	var leaseManager *lease.Manager
	leaseTableID := uint64(0)
	params.ServerArgs.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		// The before execute hook will be to set up to pause
		// the beforeExecuteStmt, which will then be resumed
		// when the beforeExecuteResumeStmt statement is observed.
		BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
			beforeExecute.Lock()
			if stmt == beforeExecuteStmt {
				tableID := descpb.ID(atomic.LoadUint64(&leaseTableID))
				beforeExecute.Unlock()
				waitChan := make(chan struct{})
				select {
				case beforeExecuteWait <- waitChan:
					<-waitChan
				case <-ctx.Done():
					return
				}
				// We will intentionally refresh the lease, since the lease duration
				// is intentionally set to zero inside this test. As a result, the
				// coordinator might not be aware of the SELECT pushing out the UPDATE in
				// time, since the transaction heart beat will be longer than whatever jitter
				// we extend the lease by. As a result in stress scenarios without this
				// change we may observed intermittent hangs.
				err := leaseManager.AcquireFreshestFromStore(ctx, tableID)
				if err != nil {
					panic(err)
				}
			} else {
				beforeExecute.Unlock()
			}
		},
		AfterExecute: func(ctx context.Context, stmt string, isInternal bool, err error) {
			beforeExecute.Lock()
			if stmt == beforeExecuteResumeStmt {
				beforeExecute.Unlock()
				resumeChan, ok := <-beforeExecuteWait
				if ok {
					close(resumeChan)
				}
			} else {
				beforeExecute.Unlock()
			}
		},
	}

	srv := serverutils.StartCluster(t, 3, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.Server(0).ApplicationLayer()
	conn := srv.ServerConn(0)
	// Setup tables for the test.
	_, err := conn.Exec(`
CREATE TABLE t1(val int);
ALTER TABLE t1 SPLIT AT VALUES (1);
	`)
	require.NoError(t, err)
	// Get the lease manager and table ID for acquiring a lease on.
	beforeExecute.Lock()
	leaseManager = s.LeaseManager().(*lease.Manager)
	beforeExecute.Unlock()
	tempTableID := uint64(0)
	err = conn.QueryRow("SELECT table_id FROM crdb_internal.tables WHERE name = $1 AND database_name = current_database()",
		"t1").Scan(&tempTableID)
	require.NoError(t, err)
	atomic.StoreUint64(&leaseTableID, tempTableID)

	// Executes a bulk UPDATE operation that will be repeatedly
	// pushed out by a SELECT operation on the same table. The
	// intention here is to confirm that autocommit will adjust
	// transaction readline for this.
	t.Run("validate-lease-txn-deadline-ext-update", func(t *testing.T) {
		updateCompleted := atomic.Value{}
		updateCompleted.Store(false)
		conn := s.SQLConn(t)
		updateConn := s.SQLConn(t)
		resultChan := make(chan error)
		_, err = conn.ExecContext(ctx, `
INSERT INTO t1 select a from generate_series(1, 100) g(a);
`,
		)
		require.NoError(t, err)
		go func() {
			const bulkUpdateQuery = "UPDATE t1 SET val = 2"
			beforeExecute.Lock()
			beforeExecuteStmt = bulkUpdateQuery
			beforeExecute.Unlock()
			// Execute a bulk UPDATE, which will get its
			// timestamp pushed by a read operation.
			_, err := updateConn.ExecContext(ctx, bulkUpdateQuery)
			updateCompleted.Store(true)
			close(beforeExecuteWait)
			resultChan <- err
		}()

		const (
			selectStmt = `SELECT * FROM t1`
			selectTxn  = `BEGIN PRIORITY HIGH; ` + selectStmt + `; COMMIT;`
		)
		beforeExecute.Lock()
		beforeExecuteResumeStmt = selectStmt
		beforeExecute.Unlock()
		// While the update hasn't completed executing, repeatedly
		// execute selects to push out the update operation. We will
		// do this for a limited amount of time, and let the commit
		// go through.
		spawnLimit := 0
		for updateCompleted.Load() == false &&
			spawnLimit < 4 {
			_, err = conn.ExecContext(ctx, selectTxn)
			require.NoError(t, err)
			spawnLimit++
		}
		// Disable the execution hooks, and allow the statement to continue
		// like normal after being pushed a limited number of times.
		beforeExecute.Lock()
		beforeExecuteStmt, beforeExecuteResumeStmt = "", ""
		beforeExecute.Unlock()
		resumeChan, channelReadOk := <-beforeExecuteWait
		if channelReadOk {
			close(resumeChan)
		}
		require.NoError(t, <-resultChan)
	})
}

// TestAmbiguousResultIsRetried ensures that when acquiring a lease gets an
// ambiguous result, that the code checks to see if the row was written, and
// removes it before proceeding to create a new lease.
//
// This is important to avoid leaking leases when nodes are being shut down.
func TestAmbiguousResultIsRetried(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type filter = kvserverbase.ReplicaResponseFilter
	var f atomic.Value
	noop := filter(func(context.Context, *kvpb.BatchRequest, *kvpb.BatchResponse) *kvpb.Error {
		return nil
	})
	f.Store(noop)
	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(ctx context.Context, request *kvpb.BatchRequest, response *kvpb.BatchResponse) *kvpb.Error {
					return f.Load().(filter)(ctx, request, response)
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec := s.Codec()

	sqlutils.MakeSQLRunner(sqlDB).Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, "CREATE TABLE foo ()")

	tableID := sqlutils.QueryTableID(t, sqlDB, "defaultdb", "public", "foo")

	tablePrefix := codec.TablePrefix(keys.LeaseTableID)
	var txnID atomic.Value
	txnID.Store(uuid.UUID{})

	testCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsAfterEndTxn := make(chan chan *kvpb.Error)
	f.Store(filter(func(ctx context.Context, request *kvpb.BatchRequest, response *kvpb.BatchResponse) *kvpb.Error {
		switch r := request.Requests[0].GetInner().(type) {
		case *kvpb.ConditionalPutRequest:
			if !bytes.HasPrefix(r.Key, tablePrefix) {
				return nil
			}
			in, _, _, err := codec.DecodeIndexPrefix(r.Key)
			if err != nil {
				return kvpb.NewError(errors.WithAssertionFailure(err))
			}
			var a tree.DatumAlloc
			_, in, err = keyside.Decode(&a, types.Bytes, in, encoding.Ascending)
			if !assert.NoError(t, err) {
				return kvpb.NewError(err)
			}
			id, _, err := keyside.Decode(
				&a, types.Int, in, encoding.Ascending,
			)
			assert.NoError(t, err)
			if tree.MustBeDInt(id) == tree.DInt(tableID) {
				txnID.Store(request.Txn.ID)
			}
		case *kvpb.EndTxnRequest:
			if request.Txn.ID != txnID.Load().(uuid.UUID) {
				return nil
			}
			errCh := make(chan *kvpb.Error)
			select {
			case errorsAfterEndTxn <- errCh:
			case <-testCtx.Done():
				return nil
			}
			return <-errCh
		}
		return nil
	}))

	// Make sure that the lease gets acquired and then, upon an ambiguous
	// failure, the retry happens, and there is just one lease.
	selectErr := make(chan error)
	go func() {
		_, err := sqlDB.Exec("SELECT * FROM foo")
		selectErr <- err
	}()
	unblock := <-errorsAfterEndTxn
	unblock <- kvpb.NewError(kvpb.NewAmbiguousResultError(errors.New("boom")))
	// Make sure we see a retry, then let it succeed.
	close(<-errorsAfterEndTxn)
	// Allow anything further to proceed.
	cancel()
	// Ensure that the query completed successfully.
	require.NoError(t, <-selectErr)
}

// TestDescriptorRemovedFromCacheWhenLeaseRenewalForThisDescriptorFails makes sure that, during a lease
// periodical refresh, if the descriptor, whose lease we intend to refresh, does not exist anymore, we delete
// this descriptor from "cache" (i.e. manager.mu.descriptor).
func TestDescriptorRemovedFromCacheWhenLeaseRenewalForThisDescriptorFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// typeDescID will be set to id of the created type `typ` later.
	mu := syncutil.Mutex{}
	typeDescID := descpb.InvalidID
	typeDescName := ""
	var tdb *sqlutils.SQLRunner
	dropCompleted := make(chan bool)

	// The overall testing strategy is
	// 1. Add a testing knob immediately before the acquire a node lease inside refreshSomeLeases;
	// 2. Create a new type `typ` and acquire a lease of it;
	// 3. When the lease manager attempts to refresh the lease on `typ`, the testing knob is trigger which removes
	//    `typ` from storage;
	// 4. This allows refreshSomeLeases fail with a DescriptorNotFound error and trigger the logic that removes this
	//    descriptor entry from the lease manager's cache (namely, manager.mu.descriptor).
	// 5. Finally, we assert that the entry for `typ` is no longer in the cache.
	var params base.TestServerArgs
	params.Knobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			TestingBeforeAcquireLeaseDuringRefresh: func(id descpb.ID) error {
				mu.Lock()
				defer mu.Unlock()
				if typeDescID != descpb.InvalidID && id == typeDescID {
					// Drop this type to trigger the logic that remove unfound descriptor from lease manager cache.
					tdb.Exec(t, fmt.Sprintf("DROP TYPE %v", typeDescName))
					dropCompleted <- true
				}
				return nil
			},
		},
	}
	params.Settings = cluster.MakeTestingClusterSettings()

	// Set lease duration to something small so that the periodical lease refresh is kicked off often where the testing
	// knob will be invoked, and eventually the logic to remove unfound descriptor from cache will be triggered.
	lease.LeaseDuration.Override(ctx, &params.SV, time.Second)

	srv, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb = sqlutils.MakeSQLRunner(sqlDB)

	sql := `
					CREATE DATABASE test;
					USE test;
					CREATE TYPE typ as enum ('a', 'b');
					`
	tdb.Exec(t, sql)

	// Ensure `typ` is present in the lease manger by acquiring a lease on it.
	typeDesc := desctestutils.TestingGetPublicTypeDescriptor(kvDB, s.Codec(), "test", "typ")
	lm := s.LeaseManager().(*lease.Manager)
	_, err := lm.Acquire(ctx, s.Clock().Now(), typeDesc.GetID())
	require.NoError(t, err)

	// Set typeDescID such that the next periodical lease refresh will trigger the testing knob that drops `typ`.
	mu.Lock()
	typeDescID = typeDesc.GetID()
	typeDescName = typeDesc.GetName()
	mu.Unlock()

	// Wait until the testing knob drops `typ`
	<-dropCompleted

	// Assert that soon (when the next periodical lease refresh happens) the testing knob will drop `typ`,
	// and consequently trigger the logic to remove the descriptor from lease manager due to a failure
	// to acquire a lease on this descriptor.
	testutils.SucceedsSoon(t, func() error {
		if lm.TestingDescriptorStateIsNil(typeDesc.GetID()) {
			return nil
		}

		return errors.Errorf("descriptor %v(#%v) is still there. Expected: descriptor removed from cache.",
			typeDesc.GetName(), typeDesc.GetID())
	})
}
