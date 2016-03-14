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

package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

type leaseTest struct {
	*testing.T
	server *testServer
	db     *sql.DB
	nodes  map[uint32]*csql.LeaseManager
}

func newLeaseTest(t *testing.T) *leaseTest {
	s, db, _ := setup(t)
	return &leaseTest{
		T:      t,
		server: s,
		db:     db,
		nodes:  map[uint32]*csql.LeaseManager{},
	}
}

func (t *leaseTest) cleanup() {
	cleanup(t.server, t.db)
}

func (t *leaseTest) getLeases(descID csql.ID) string {
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

func (t *leaseTest) expectLeases(descID csql.ID, expected string) {
	leases := t.getLeases(descID)
	if expected != leases {
		t.Fatalf("expected %s, but found %s", expected, leases)
	}
}

func (t *leaseTest) acquire(nodeID uint32, descID csql.ID, version csql.DescriptorVersion) (*csql.LeaseState, error) {
	var lease *csql.LeaseState
	pErr := t.server.DB().Txn(func(txn *client.Txn) *roachpb.Error {
		var pErr *roachpb.Error
		lease, pErr = t.node(nodeID).Acquire(txn, descID, version)
		return pErr
	})
	return lease, pErr.GoError()
}

func (t *leaseTest) mustAcquire(nodeID uint32, descID csql.ID, version csql.DescriptorVersion) *csql.LeaseState {
	lease, err := t.acquire(nodeID, descID, version)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func (t *leaseTest) release(nodeID uint32, lease *csql.LeaseState) error {
	return t.node(nodeID).Release(lease)
}

func (t *leaseTest) mustRelease(nodeID uint32, lease *csql.LeaseState) {
	if err := t.release(nodeID, lease); err != nil {
		t.Fatal(err)
	}
}

func (t *leaseTest) publish(nodeID uint32, descID csql.ID) *roachpb.Error {
	return t.node(nodeID).Publish(descID,
		func(*csql.TableDescriptor) error {
			return nil
		})
}

func (t *leaseTest) mustPublish(nodeID uint32, descID csql.ID) {
	if err := t.publish(nodeID, descID); err != nil {
		t.Fatal(err)
	}
}

func (t *leaseTest) node(nodeID uint32) *csql.LeaseManager {
	mgr := t.nodes[nodeID]
	if mgr == nil {
		mgr = csql.NewLeaseManager(nodeID, *t.server.DB(), t.server.Clock())
		t.nodes[nodeID] = mgr
	}
	return mgr
}

func TestLeaseManager(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)
	t := newLeaseTest(testingT)
	defer t.cleanup()

	const descID = keys.LeaseTableID

	// We can't acquire a lease on a non-existent table.
	expected := "table ID 10000 not found"
	if _, err := t.acquire(1, 10000, 0); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	l1 := t.mustAcquire(1, descID, 0)
	t.expectLeases(descID, "/1/1")
	// Node 2 never acquired a lease on descID, so we should expect an error.
	if err := t.release(2, l1); err == nil {
		t.Fatalf("expected error, but found none")
	}
	t.mustRelease(1, l1)
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
	t.mustRelease(1, l3)
	t.expectLeases(descID, "/1/1 /2/1")

	// We can still acquire a local reference on the old version since it hasn't
	// expired.
	l4 := t.mustAcquire(1, descID, 1)
	t.mustRelease(1, l4)
	t.expectLeases(descID, "/1/1 /2/1")

	// When the last local reference on the old version is released the node
	// lease is also released.
	t.mustRelease(1, l2)
	t.expectLeases(descID, "/2/1")

	// It is an error to acquire a lease for an old version once a new version
	// exists and there are no local references for the old version.
	expected = "lease.go.*: table .* unable to acquire lease on old version: 1 < 2"
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

	t.mustRelease(1, l5)
	t.expectLeases(descID, "/2/2 /3/1 /3/2")
	t.mustRelease(2, l6)
	t.expectLeases(descID, "/3/1 /3/2")

	// Wait for version 4 to be published.
	wg.Wait()
	l9 := t.mustAcquire(1, descID, 4)
	t.mustRelease(1, l7)
	t.mustRelease(2, l8)
	t.expectLeases(descID, "/3/2 /4/1")
	t.mustRelease(1, l9)
	t.expectLeases(descID, "/3/2 /4/1")
}

func TestLeaseManagerReacquire(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)
	t := newLeaseTest(testingT)
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

	t.mustRelease(1, l1)
	t.mustRelease(1, l2)
	t.mustRelease(1, l3)
}

func TestLeaseManagerPublishVersionChanged(testingT *testing.T) {
	defer leaktest.AfterTest(testingT)
	t := newLeaseTest(testingT)
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
		err := n1.Publish(descID, func(*csql.TableDescriptor) error {
			if n2start != nil {
				// Signal node 2 to start.
				close(n2start)
				n2start = nil
			}
			// Wait for node 2 signal indicating that node 2 finished publication of
			// a new version.
			<-n1update
			return nil
		})
		if err != nil {
			panic(err)
		}
		wg.Done()
	}(n1update, n2start)

	go func(n1update, n2start chan struct{}) {
		// Wait for node 1 signal indicating that node 1 is in its update()
		// function.
		<-n2start
		err := n2.Publish(descID, func(*csql.TableDescriptor) error {
			return nil
		})
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
