// Copyright 2016 The Cockroach Authors.
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
// Author: Andrei Matei (andreimatei1@gmail.com)

package testcluster

import (
	gosql "database/sql"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/pkg/errors"
)

// TestCluster represents a set of TestServers. The hope is that it can be used
// analoguous to TestServer, but with control over range replication.
type TestCluster struct {
	Servers []*server.TestServer
	Conns   []*gosql.DB
}

// ClusterArgs contains the parameters one can set when creating a test
// cluster. It contains a base.TestServerArgs instance which will be copied over
// to every server.
//
// The zero value means "full replication".
type ClusterArgs struct {
	// ServerArgs will be copied to each constituent TestServer.
	ServerArgs base.TestServerArgs
	// ReplicationMode controls how replication is to be done in the cluster.
	ReplicationMode ReplicationMode
	// Stopper can be used to stop the cluster. If not set, a stopper will be
	// constructed and it can be gotten through TestCluster.Stopper().
	Stopper *stop.Stopper
}

// ReplicationMode represents the replication settings for a TestCluster.
type ReplicationMode int

const (
	// ReplicationFull means that each range is to be replicated everywhere. This
	// will be done by overriding the default zone config. The replication will be
	// performed as in production, by the replication queue.
	// TestCluster.WaitForFullReplication() can be used to wait for replication to
	// be stable at any point in a test.
	// TODO(andrei): ReplicationFull should not be an option, or at least not the
	// default option. Instead, the production default replication should be the
	// default for TestCluster too. But I'm not sure how to implement
	// `WaitForFullReplication` for that.
	ReplicationFull ReplicationMode = iota
	// ReplicationManual means that the split and replication queues of all
	// servers are stopped, and the test must manually control splitting and
	// replication through the TestServer.
	ReplicationManual
)

// StartTestCluster starts up a TestCluster made up of `nodes` in-memory testing
// servers.
// The cluster should be stopped using cluster.Stopper().Stop().
func StartTestCluster(t testing.TB, nodes int, args ClusterArgs) *TestCluster {
	if nodes < 1 {
		t.Fatal("invalid cluster size: ", nodes)
	}
	if args.ServerArgs.JoinAddr != "" {
		t.Fatal("can't specify a join addr when starting a cluster")
	}
	if args.ServerArgs.Stopper != nil {
		t.Fatal("can't set individual server stoppers when starting a cluster")
	}
	storeKnobs := args.ServerArgs.Knobs.Store
	if storeKnobs != nil &&
		(storeKnobs.(*storage.StoreTestingKnobs).DisableSplitQueue ||
			storeKnobs.(*storage.StoreTestingKnobs).DisableReplicateQueue) {
		t.Fatal("can't disable an individual server's queues when starting a cluster; " +
			"the cluster controls replication")
	}

	if args.Stopper == nil {
		args.Stopper = stop.NewStopper()
		args.ServerArgs.Stopper = args.Stopper
	}

	switch args.ReplicationMode {
	case ReplicationFull:
		// Force all ranges to be replicated everywhere.
		cfg := config.DefaultZoneConfig()
		cfg.ReplicaAttrs = make([]roachpb.Attributes, nodes)
		fn := config.TestingSetDefaultZoneConfig(cfg)
		args.Stopper.AddCloser(stop.CloserFn(fn))
	case ReplicationManual:
		if args.ServerArgs.Knobs.Store == nil {
			args.ServerArgs.Knobs.Store = &storage.StoreTestingKnobs{}
		}
		storeKnobs := args.ServerArgs.Knobs.Store.(*storage.StoreTestingKnobs)
		storeKnobs.DisableSplitQueue = true
		storeKnobs.DisableReplicateQueue = true
	default:
		t.Fatal("unexpected replication mode")
	}

	tc := &TestCluster{}
	args.ServerArgs.PartOfCluster = true
	first, conn, _ := serverutils.StartServer(t, args.ServerArgs)
	tc.Servers = append(tc.Servers, first.(*server.TestServer))
	tc.Conns = append(tc.Conns, conn)
	args.ServerArgs.JoinAddr = first.ServingAddr()
	for i := 1; i < nodes; i++ {
		s, conn, _ := serverutils.StartServer(t, args.ServerArgs)
		tc.Servers = append(tc.Servers, s.(*server.TestServer))
		tc.Conns = append(tc.Conns, conn)
	}

	tc.waitForStores(t)
	return tc
}

// waitForStores waits for all of the store descriptors to be gossiped. Servers
// other than the first "bootstrap" their stores asynchronously, but we'd like
// to wait for all of the stores to be initialized before returning the
// TestCluster.
func (tc *TestCluster) waitForStores(t testing.TB) {
	// Register a gossip callback for the store descriptors.
	g := tc.Servers[0].Gossip()
	var storesMu sync.Mutex
	stores := map[roachpb.StoreID]struct{}{}
	storesDone := make(chan error)
	unregister := g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix),
		func(_ string, content roachpb.Value) {
			var desc roachpb.StoreDescriptor
			if err := content.GetProto(&desc); err != nil {
				storesDone <- err
				return
			}
			storesMu.Lock()
			stores[desc.StoreID] = struct{}{}
			if len(stores) == len(tc.Servers) {
				close(storesDone)
			}
			storesMu.Unlock()
		})
	defer unregister()

	// Wait for the store descriptors to be gossiped.
	if err := <-storesDone; err != nil {
		t.Fatal(err)
	}
}

// Stopper returns a Stopper to be used to stop the TestCluster.
func (tc *TestCluster) Stopper() *stop.Stopper {
	return tc.Servers[0].Stopper()
}

// lookupRange returns the descriptor of the range containing key.
func (tc *TestCluster) lookupRange(key roachpb.RKey) (roachpb.RangeDescriptor, error) {
	rangeLookupReq := roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(key),
		},
		MaxRanges:       1,
		ConsiderIntents: false,
	}
	resp, pErr := client.SendWrapped(tc.Servers[0].GetDistSender(), nil, &rangeLookupReq)
	if pErr != nil {
		return roachpb.RangeDescriptor{}, errors.Errorf(
			"%q: lookup range unexpected error: %s", key, pErr)
	}
	return resp.(*roachpb.RangeLookupResponse).Ranges[0], nil
}

// SplitRange splits the range containing splitKey.
// The right range created by the split starts at the split key and extends to the
// original range's end key.
// Returns the new descriptors of the left and right ranges.
//
// splitKey must correspond to a SQL table key (it must end with a family ID /
// col ID).
func (tc *TestCluster) SplitRange(
	splitKey roachpb.Key,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor, error) {
	splitRKey, err := keys.Addr(splitKey)
	if err != nil {
		return nil, nil, err
	}
	origRangeDesc, err := tc.lookupRange(splitRKey)
	if err != nil {
		return nil, nil, err
	}
	if origRangeDesc.StartKey.Equal(splitRKey) {
		return nil, nil, errors.Errorf(
			"cannot split range %+v at start key %q", origRangeDesc, splitKey)
	}
	splitReq := roachpb.AdminSplitRequest{
		Span: roachpb.Span{
			Key: splitKey,
		},
		SplitKey: splitKey,
	}
	_, pErr := client.SendWrapped(tc.Servers[0].GetDistSender(), nil, &splitReq)
	if pErr != nil {
		return nil, nil, errors.Errorf(
			"%q: split unexpected error: %s", splitReq.SplitKey, pErr)
	}

	leftRangeDesc := new(roachpb.RangeDescriptor)
	rightRangeDesc := new(roachpb.RangeDescriptor)
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(origRangeDesc.StartKey), leftRangeDesc); err != nil {
		return nil, nil, err
	}
	// The split point might not be exactly the one we requested (it can be
	// adjusted slightly so we don't split in the middle of SQL rows). Update it
	// to the real point.
	splitRKey = leftRangeDesc.EndKey
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(splitRKey), rightRangeDesc); err != nil {
		return nil, nil, err
	}
	return leftRangeDesc, rightRangeDesc, nil
}

// ReplicationTarget identifies a node/store pair.
type ReplicationTarget struct {
	NodeID  roachpb.NodeID
	StoreID roachpb.StoreID
}

// Target returns a ReplicationTarget for the specified server.
func (tc *TestCluster) Target(serverIdx int) ReplicationTarget {
	s := tc.Servers[serverIdx]
	return ReplicationTarget{
		NodeID:  s.GetNode().Descriptor.NodeID,
		StoreID: s.GetFirstStoreID(),
	}
}

// AddReplicas adds replicas for a range on a set of stores.
// It's illegal to have multiple replicas of the same range on stores of a single
// node.
// The method blocks until a snapshot of the range has been copied to all the
// new replicas and the new replicas become part of the Raft group.
func (tc *TestCluster) AddReplicas(
	rangeDesc *roachpb.RangeDescriptor, dests ...ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	startKey := rangeDesc.StartKey
	// TODO(andrei): the following code has been adapted from
	// multiTestContext.replicateRange(). Find a way to share.
	for _, dest := range dests {
		// Perform a consistent read to get the updated range descriptor (as opposed
		// to just going to one of the stores), to make sure we have the effects of
		// the previous ChangeReplicas call. By the time ChangeReplicas returns the
		// raft leader is guaranteed to have the updated version, but followers are
		// not.
		if err := tc.Servers[0].DB().GetProto(
			keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
			return nil, err
		}

		// Ask a random replica of the range to up-replicate.
		store, err := tc.findMemberStore(rangeDesc.Replicas[0].StoreID)
		if err != nil {
			return nil, err
		}
		replica, err := store.GetReplica(rangeDesc.RangeID)
		if err != nil {
			return nil, err
		}
		err = replica.ChangeReplicas(context.Background(),
			roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  dest.NodeID,
				StoreID: dest.StoreID,
			}, rangeDesc)
		if err != nil {
			return nil, err
		}
	}

	// Wait for the replication to complete on all destination nodes.
	err := util.RetryForDuration(time.Second*5, func() error {
		for _, dest := range dests {
			// Use LookupReplica(keys) instead of GetRange(rangeID) to ensure that the
			// snapshot has been transferred and the descriptor initialized.
			store, err := tc.findMemberStore(dest.StoreID)
			if err != nil {
				log.Errorf("unexpected error: %s", err)
				return err
			}
			if store.LookupReplica(startKey, nil) == nil {
				return errors.Errorf("range not found on store %d", dest)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
		return nil, err
	}
	return rangeDesc, nil
}

// RemoveReplica removes a replica from a range.
func (tc *TestCluster) RemoveReplica(
	rangeDesc *roachpb.RangeDescriptor, target ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	startKey := rangeDesc.StartKey

	// Perform a consistent read to get the updated range descriptor (as opposed
	// to just going to one of the stores), to make sure we have the effects of
	// the previous ChangeReplicas call. By the time ChangeReplicas returns the
	// raft leader is guaranteed to have the updated version, but followers are
	// not.
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
		return nil, err
	}

	// Ask a random replica of the range to up-replicate.
	store, err := tc.findMemberStore(rangeDesc.Replicas[0].StoreID)
	if err != nil {
		return nil, err
	}
	replica, err := store.GetReplica(rangeDesc.RangeID)
	if err != nil {
		return nil, err
	}
	if err := replica.ChangeReplicas(context.Background(),
		roachpb.REMOVE_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  target.NodeID,
			StoreID: target.StoreID,
		}, rangeDesc); err != nil {
		return nil, err
	}

	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
		return nil, err
	}
	return rangeDesc, nil
}

// TransferRangeLease transfers the lease for a range from whoever has it to
// a particular store. That store must already have a replica of the range. If
// that replica already has the (active) lease, this method is a no-op.
func (tc *TestCluster) TransferRangeLease(
	rangeDesc *roachpb.RangeDescriptor, dest ReplicationTarget,
) error {
	destReplicaDesc, ok := rangeDesc.GetReplicaDescriptor(dest.StoreID)
	if !ok {
		log.Fatalf("Couldn't find store %d in range %+v", dest.StoreID, rangeDesc)
	}

	leaseHolderDesc, err := tc.FindRangeLeaseHolder(rangeDesc,
		&ReplicationTarget{
			NodeID:  destReplicaDesc.NodeID,
			StoreID: destReplicaDesc.StoreID,
		})
	if err != nil {
		return err
	}
	if leaseHolderDesc.StoreID == dest.StoreID {
		// The intended replica already has the lease. Nothing to do.
		return nil
	}
	oldStore, err := tc.findMemberStore(leaseHolderDesc.StoreID)
	if err != nil {
		return err
	}
	oldReplica, err := oldStore.GetReplica(rangeDesc.RangeID)
	if err != nil {
		return err
	}
	// Ask the lease holder to transfer the lease.
	if err := oldReplica.AdminTransferLease(destReplicaDesc); err != nil {
		return err
	}
	return nil
}

// FindRangeLeaseHolder returns the current lease holder for the given range. If
// there is no lease at the time of the call, a replica is gets one as a
// side-effect of calling this; if hint is not nil, that replica will be the
// one.
//
// One of the Stores in the cluster is used as a Sender to send a dummy read
// command, which will either result in success (if a replica on that Node has
// the lease), in a NotLeaseHolderError pointing to the current lease holder (if
// there is an active lease), or in the replica on that store acquiring the
// lease (if there isn't an active lease).
// If an active lease existed for the range, it's extended as a side-effect.
func (tc *TestCluster) FindRangeLeaseHolder(
	rangeDesc *roachpb.RangeDescriptor,
	hint *ReplicationTarget,
) (ReplicationTarget, error) {
	var hintReplicaDesc roachpb.ReplicaDescriptor
	if hint != nil {
		var ok bool
		if hintReplicaDesc, ok = rangeDesc.GetReplicaDescriptor(hint.StoreID); !ok {
			return ReplicationTarget{}, errors.Errorf(
				"bad hint; store doesn't have a replica of the range")
		}
	} else {
		hint = &ReplicationTarget{
			NodeID:  rangeDesc.Replicas[0].NodeID,
			StoreID: rangeDesc.Replicas[0].StoreID}
		hintReplicaDesc = rangeDesc.Replicas[0]
	}
	// TODO(andrei): Using a dummy GetRequest for the purpose of figuring out the
	// lease holder is a hack. Instead, we should have a dedicate admin command.
	getReq := roachpb.GetRequest{
		Span: roachpb.Span{
			Key: rangeDesc.StartKey.AsRawKey(),
		},
	}

	store, err := tc.findMemberStore(hint.StoreID)
	if err != nil {
		return ReplicationTarget{}, err
	}
	_, pErr := client.SendWrappedWith(
		store, nil,
		roachpb.Header{RangeID: rangeDesc.RangeID, Replica: hintReplicaDesc},
		&getReq)
	if pErr != nil {
		if nle, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok {
			if nle.Replica == nil {
				return ReplicationTarget{}, errors.Errorf(
					"unexpected NotLeaseHolderError with leader unknown")
			}
			return ReplicationTarget{
				NodeID: nle.LeaseHolder.NodeID, StoreID: nle.LeaseHolder.StoreID}, nil
		}
		return ReplicationTarget{}, pErr.GoError()
	}
	// The replica we sent the request to either was already or just became
	// the lease holder.
	return *hint, nil
}

// findMemberStore returns the store containing a given replica.
func (tc *TestCluster) findMemberStore(
	storeID roachpb.StoreID,
) (*storage.Store, error) {
	for _, server := range tc.Servers {
		if server.Stores().HasStore(storeID) {
			store, err := server.Stores().GetStore(storeID)
			if err != nil {
				return nil, err
			}
			return store, nil
		}
	}
	return nil, errors.Errorf("store not found")
}

// WaitForFullReplication waits until all of the nodes in the cluster have the
// same number of replicas.
func (tc *TestCluster) WaitForFullReplication() error {
	// TODO (WillHaack): Optimize sleep time.
	for notReplicated := true; notReplicated; time.Sleep(100 * time.Millisecond) {
		notReplicated = false
		var numReplicas int
		err := tc.Servers[0].Stores().VisitStores(func(s *storage.Store) error {
			numReplicas = s.ReplicaCount()
			return nil
		})
		if err != nil {
			return err
		}
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(s *storage.Store) error {
				if numReplicas != s.ReplicaCount() {
					notReplicated = true
				}
				return nil
			})
			if err != nil {
				return err
			}
			if notReplicated {
				break
			}
		}
	}
	return nil
}
