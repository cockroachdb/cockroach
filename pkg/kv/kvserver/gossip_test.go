// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/stretchr/testify/require"
)

func TestGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())

	errors := make(chan error, 1)
	descs := make(chan *roachpb.RangeDescriptor)
	unregister := tc.Servers[0].Gossip().RegisterCallback(gossip.KeyFirstRangeDescriptor,
		func(_ string, content roachpb.Value) {
			var desc roachpb.RangeDescriptor
			if err := content.GetProto(&desc); err != nil {
				select {
				case errors <- err:
				default:
				}
			} else {
				select {
				case descs <- &desc:
				case <-time.After(45 * time.Second):
					t.Logf("had to drop descriptor %+v", desc)
				}
			}
		},
		// Redundant callbacks are required by this test.
		gossip.Redundant,
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
				log.Infof(context.Background(), "expected\n%+v\nbut found\n%+v", desc, gossiped)
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
		if desc, err = tc.AddVoters(firstRangeKey, tc.Target(i)); err != nil {
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
	desc, err := tc.RemoveVoters(firstRangeKey, tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// TODO(peter): Re-enable or remove when we've resolved the discussion
	// about removing the lease-holder replica. See #7872.

	// // Remove the lease holder replica.
	// leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	// desc, err = tc.RemoveVoters(firstRangeKey, leaseHolder)
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
	defer log.Scope(t).Close(t)

	// As of Nov 2018 it takes 3.6s.
	skip.UnderShort(t)
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
			ServerArgs: serverArgs,
		})
	defer tc.Stopper().Stop(context.Background())

	// Take down the first node and replace it with a new one.
	oldNodeIdx := 0
	newServerArgs := serverArgs
	newServerArgs.Addr = tc.Servers[oldNodeIdx].ServingRPCAddr()
	newServerArgs.SQLAddr = tc.Servers[oldNodeIdx].ServingSQLAddr()
	newServerArgs.PartOfCluster = true
	newServerArgs.JoinAddr = tc.Servers[1].ServingRPCAddr()
	log.Infof(ctx, "stopping server %d", oldNodeIdx)
	tc.StopServer(oldNodeIdx)
	tc.AddAndStartServer(t, newServerArgs)

	tc.WaitForNStores(t, tc.NumServers(), tc.Server(1).GossipI().(*gossip.Gossip))

	// Ensure that all servers still running are responsive. If the two remaining
	// original nodes don't refresh their connection to the address of the first
	// node, they can get stuck here.
	for i, server := range tc.Servers {
		if i == oldNodeIdx {
			continue
		}
		kvClient := server.DB()
		if err := kvClient.Put(ctx, fmt.Sprintf("%d", i), i); err != nil {
			t.Errorf("failed Put to node %d: %+v", i, err)
		}
	}
}

// TestGossipAfterAbortOfSystemConfigTransactionAfterFailureDueToIntents tests
// that failures to gossip the system config due to intents are rectified when
// later intents are aborted.
func TestGossipAfterAbortOfSystemConfigTransactionAfterFailureDueToIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	db := tc.Server(0).DB()

	txA := db.NewTxn(ctx, "a")
	txB := db.NewTxn(ctx, "b")

	require.NoError(t, txA.SetSystemConfigTrigger(true /* forSystemTenant */))
	db1000 := dbdesc.NewInitial(1000, "1000", security.AdminRoleName())
	require.NoError(t, txA.Put(ctx,
		keys.SystemSQLCodec.DescMetadataKey(1000),
		db1000.DescriptorProto()))

	require.NoError(t, txB.SetSystemConfigTrigger(true /* forSystemTenant */))
	db2000 := dbdesc.NewInitial(2000, "2000", security.AdminRoleName())
	require.NoError(t, txB.Put(ctx,
		keys.SystemSQLCodec.DescMetadataKey(2000),
		db2000.DescriptorProto()))

	const someTime = 10 * time.Millisecond
	clearNotifictions := func(ch <-chan struct{}) {
		for {
			select {
			case <-ch:
			case <-time.After(someTime):
				return
			}
		}
	}
	systemConfChangeCh := tc.Server(0).GossipI().(*gossip.Gossip).RegisterSystemConfigChannel()
	clearNotifictions(systemConfChangeCh)
	require.NoError(t, txB.Commit(ctx))
	select {
	case <-systemConfChangeCh:
		// This case is rare but happens sometimes. We gossip the node liveness
		// in a bunch of cases so we just let the test finish here. The important
		// thing is that sometimes we get to the next phase.
		t.Log("got unexpected update. This can happen for a variety of " +
			"reasons like lease transfers. The test is exiting without testing anything")
		return
	case <-time.After(someTime):
		// Did not expect an update so this is the happy case
	}
	// Roll back the transaction which had laid down the intent which blocked the
	// earlier gossip update, make sure we get a gossip notification now.
	const aLongTime = 20 * someTime
	require.NoError(t, txA.Rollback(ctx))
	select {
	case <-systemConfChangeCh:
		// Got an update.
	case <-time.After(aLongTime):
		t.Fatal("expected update")
	}
}
