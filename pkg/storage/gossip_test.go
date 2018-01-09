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

package storage_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

func TestGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.TODO())

	errors := make(chan error)
	descs := make(chan *roachpb.RangeDescriptor)
	unregister := tc.Servers[0].Gossip().RegisterCallback(gossip.KeyFirstRangeDescriptor,
		func(_ string, content roachpb.Value) {
			var desc roachpb.RangeDescriptor
			if err := content.GetProto(&desc); err != nil {
				errors <- err
			} else {
				descs <- &desc
			}
		},
	)
	// Unregister the callback before attempting to stop the stopper to prevent
	// deadlock. This is still flaky in theory since a callback can fire between
	// the last read from the channels and this unregister, but testing has
	// shown this solution to be sufficiently robust for now.
	defer unregister()

	// Wait for the specified descriptor to be gossiped for the first range. We
	// loop because the timing of replica addition and lease transfer can cause
	// extra gossiping of the first range.
	waitForGossip := func(desc roachpb.RangeDescriptor) {
		for {
			select {
			case err := <-errors:
				t.Fatal(err)
			case gossiped := <-descs:
				if reflect.DeepEqual(&desc, gossiped) {
					return
				}
				log.Infof(context.TODO(), "expected\n%+v\nbut found\n%+v", desc, gossiped)
			}
		}
	}

	// Expect an initial callback of the first range descriptor.
	select {
	case err := <-errors:
		t.Fatal(err)
	case <-descs:
	}

	// Add two replicas. The first range descriptor should be gossiped after each
	// addition.
	var desc roachpb.RangeDescriptor
	firstRangeKey := keys.MinKey
	for i := 1; i <= 2; i++ {
		var err error
		if desc, err = tc.AddReplicas(firstRangeKey, tc.Target(i)); err != nil {
			t.Fatal(err)
		}
		waitForGossip(desc)
	}

	// Transfer the lease to a new node. This should cause the first range to be
	// gossiped again.
	if err := tc.TransferRangeLease(desc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// Remove a non-lease holder replica.
	desc, err := tc.RemoveReplicas(firstRangeKey, tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// TODO(peter): Re-enable or remove when we've resolved the discussion
	// about removing the lease-holder replica. See #7872.

	// // Remove the lease holder replica.
	// leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	// desc, err = tc.RemoveReplicas(firstRangeKey, leaseHolder)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// select {
	// case err := <-errors:
	// 	t.Fatal(err)
	// case gossiped := <-descs:
	// 	if !reflect.DeepEqual(desc, gossiped) {
	// 		t.Fatalf("expected\n%+v\nbut found\n%+v", desc, gossiped)
	// 	}
	// }
}

// TestGossipHandlesReplacedNode tests that we can shut down a node and
// replace it with a new node at the same address (simulating a node getting
// restarted after losing its data) without the cluster breaking.
func TestGossipHandlesReplacedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Shorten the raft tick interval and election timeout to make range leases
	// much shorter than normal. This keeps us from having to wait so long for
	// the replaced node's leases to time out, but has still shown itself to be
	// long enough to avoid flakes.
	serverArgs := base.TestServerArgs{
		Addr:     util.IsolatedTestAddr.String(),
		Insecure: true, // because our certs are only valid for 127.0.0.1
		RetryOptions: retry.Options{
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		},
	}
	serverArgs.RaftTickInterval = 50 * time.Millisecond
	serverArgs.RaftElectionTimeoutTicks = 10

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			// Use manual replication so that we can ensure the range is properly
			// replicated to all three nodes before stopping one of them.
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      serverArgs,
		})
	defer tc.Stopper().Stop(context.TODO())

	// Ensure that the first range is fully replicated before moving on.
	firstRangeKey := keys.MinKey
	if _, err := tc.AddReplicas(firstRangeKey, tc.Target(1), tc.Target(2)); err != nil {
		t.Fatal(err)
	}

	// Take down a node other than the first node and replace it with a new one.
	// Replacing the first node would be better from an adversarial testing
	// perspective because it typically has the most leases on it, but that also
	// causes the test to take significantly longer as a result.
	oldNodeIdx := 0
	newServerArgs := serverArgs
	newServerArgs.Addr = tc.Servers[oldNodeIdx].ServingAddr()
	newServerArgs.PartOfCluster = true
	newServerArgs.JoinAddr = tc.Servers[1].ServingAddr()
	log.Infof(ctx, "stopping server %d", oldNodeIdx)
	tc.StopServer(oldNodeIdx)
	tc.AddServer(t, newServerArgs)

	tc.WaitForStores(t, tc.Server(1).Gossip())

	// Ensure that all servers still running are responsive. If the two remaining
	// original nodes don't refresh their connection to the address of the first
	// node, they can get stuck here.
	for i, server := range tc.Servers {
		if i == oldNodeIdx {
			continue
		}
		kvClient := server.DB()
		if err := kvClient.Put(ctx, fmt.Sprintf("%d", i), i); err != nil {
			t.Errorf("failed Put to node %d: %s", i, err)
		}
	}
}
