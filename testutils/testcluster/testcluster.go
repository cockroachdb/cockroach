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
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"
)

// TestCluster represents a set of TestServers. The hope is that it can be used
// analoguous to TestServer, but with control over range replication.
type TestCluster struct {
	Servers []*server.TestServer
	Conns   []*gosql.DB
	stopper *stop.Stopper
	mu      struct {
		syncutil.Mutex
		serverStoppers []*stop.Stopper
	}
}

var _ serverutils.TestClusterInterface = &TestCluster{}

// NumServers is part of TestClusterInterface.
func (tc *TestCluster) NumServers() int {
	return len(tc.Servers)
}

// Server is part of TestClusterInterface.
func (tc *TestCluster) Server(idx int) serverutils.TestServerInterface {
	return tc.Servers[idx]
}

// ServerConn is part of TestClusterInterface.
func (tc *TestCluster) ServerConn(idx int) *gosql.DB {
	return tc.Conns[idx]
}

// Stopper returns the stopper for this testcluster.
func (tc *TestCluster) Stopper() *stop.Stopper {
	return tc.stopper
}

// stopServers stops the stoppers for each individual server in the cluster.
// This method ensures that servers that were previously stopped explicitly are
// not double-stopped.
func (tc *TestCluster) stopServers() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for i := range tc.mu.serverStoppers {
		if tc.mu.serverStoppers[i] != nil {
			tc.mu.serverStoppers[i].Stop()
			tc.mu.serverStoppers[i] = nil
		}
	}
}

// StopServer stops an individual server in the cluster.
func (tc *TestCluster) StopServer(idx int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.serverStoppers[idx] != nil {
		tc.mu.serverStoppers[idx].Stop()
		tc.mu.serverStoppers[idx] = nil
	}
}

// StartTestCluster starts up a TestCluster made up of `nodes` in-memory testing
// servers.
// The cluster should be stopped using cluster.Stop().
func StartTestCluster(t testing.TB, nodes int, args base.TestClusterArgs) *TestCluster {
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

	switch args.ReplicationMode {
	case base.ReplicationAuto:
	case base.ReplicationManual:
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
	tc.stopper = stop.NewStopper()

	args.ServerArgs.PartOfCluster = true
	for i := 0; i < nodes; i++ {
		serverArgs := args.ServerArgs
		serverArgs.Stopper = stop.NewStopper()
		if i > 0 {
			serverArgs.JoinAddr = tc.Servers[0].ServingAddr()
		}
		s, conn, _ := serverutils.StartServer(t, serverArgs)
		tc.Servers = append(tc.Servers, s.(*server.TestServer))
		tc.Conns = append(tc.Conns, conn)
		tc.mu.Lock()
		tc.mu.serverStoppers = append(tc.mu.serverStoppers, serverArgs.Stopper)
		tc.mu.Unlock()
	}

	// Create a closer that will stop the individual server stoppers when the
	// cluster stopper is stopped.
	tc.stopper.AddCloser(stop.CloserFn(tc.stopServers))

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
	var storesMu syncutil.Mutex
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

// LookupRange returns the descriptor of the range containing key.
func (tc *TestCluster) LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error) {
	rangeLookupReq := roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(keys.MustAddr(key)),
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
	origRangeDesc, err := tc.LookupRange(splitKey)
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
		return nil, nil, errors.Wrap(err, "could not look up left-hand side descriptor")
	}
	// The split point might not be exactly the one we requested (it can be
	// adjusted slightly so we don't split in the middle of SQL rows). Update it
	// to the real point.
	splitRKey = leftRangeDesc.EndKey
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(splitRKey), rightRangeDesc); err != nil {
		return nil, nil, errors.Wrap(err, "could not look up right-hand side descriptor")
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

func (tc *TestCluster) changeReplicas(
	action roachpb.ReplicaChangeType,
	startKey roachpb.RKey,
	targets ...ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	rangeDesc := &roachpb.RangeDescriptor{}

	// TODO(andrei): the following code has been adapted from
	// multiTestContext.replicateRange(). Find a way to share.
	for _, target := range targets {
		// Perform a consistent read to get the updated range descriptor (as opposed
		// to just going to one of the stores), to make sure we have the effects of
		// the previous ChangeReplicas call. By the time ChangeReplicas returns the
		// raft leader is guaranteed to have the updated version, but followers are
		// not.
		if err := tc.Servers[0].DB().GetProto(
			keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
			return nil, err
		}

		// Ask an arbitrary replica of the range to perform the change. Note that
		// the target for addition/removal is specified, this is about the choice
		// of which replica receives the ChangeReplicas operation.
		store, err := tc.findMemberStore(rangeDesc.Replicas[0].StoreID)
		if err != nil {
			return nil, err
		}
		replica, err := store.GetReplica(rangeDesc.RangeID)
		if err != nil {
			return nil, err
		}
		err = replica.ChangeReplicas(context.Background(),
			action,
			roachpb.ReplicaDescriptor{
				NodeID:  target.NodeID,
				StoreID: target.StoreID,
			}, rangeDesc)
		if err != nil {
			return nil, err
		}
	}
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
		return nil, err
	}
	return rangeDesc, nil
}

// AddReplicas adds replicas for a range on a set of stores.
// It's illegal to have multiple replicas of the same range on stores of a single
// node.
// The method blocks until a snapshot of the range has been copied to all the
// new replicas and the new replicas become part of the Raft group.
func (tc *TestCluster) AddReplicas(
	startKey roachpb.Key, targets ...ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	rKey := keys.MustAddr(startKey)
	rangeDesc, err := tc.changeReplicas(
		roachpb.ADD_REPLICA, rKey, targets...,
	)
	if err != nil {
		return nil, err
	}

	// Wait for the replication to complete on all destination nodes.
	if err := util.RetryForDuration(time.Second*5, func() error {
		for _, target := range targets {
			// Use LookupReplica(keys) instead of GetRange(rangeID) to ensure that the
			// snapshot has been transferred and the descriptor initialized.
			store, err := tc.findMemberStore(target.StoreID)
			if err != nil {
				log.Errorf(context.TODO(), "unexpected error: %s", err)
				return err
			}
			if store.LookupReplica(rKey, nil) == nil {
				return errors.Errorf("range not found on store %d", target)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return rangeDesc, nil
}

// RemoveReplicas removes one or more replicas from a range.
func (tc *TestCluster) RemoveReplicas(
	startKey roachpb.Key, targets ...ReplicationTarget,
) (*roachpb.RangeDescriptor, error) {
	return tc.changeReplicas(roachpb.REMOVE_REPLICA, keys.MustAddr(startKey), targets...)
}

// TransferRangeLease transfers the lease for a range from whoever has it to
// a particular store. That store must already have a replica of the range. If
// that replica already has the (active) lease, this method is a no-op.
func (tc *TestCluster) TransferRangeLease(
	rangeDesc *roachpb.RangeDescriptor, dest ReplicationTarget,
) error {
	err := tc.Servers[0].DB().AdminTransferLease(rangeDesc.StartKey.AsRawKey(), dest.StoreID)
	if err != nil {
		return errors.Wrapf(err, "%q: transfer lease unexpected error", rangeDesc.StartKey)
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
			if nle.LeaseHolder == nil {
				return ReplicationTarget{}, errors.Errorf(
					"unexpected NotLeaseHolderError with lease holder unknown")
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

// WaitForFullReplication waits until all stores in the cluster
// have no ranges with replication pending.
func (tc *TestCluster) WaitForFullReplication() error {
	opts := retry.Options{
		InitialBackoff: time.Millisecond * 10,
		MaxBackoff:     time.Millisecond * 100,
		Multiplier:     2,
	}

	notReplicated := true
	for r := retry.Start(opts); r.Next() && notReplicated; {
		notReplicated = false
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(s *storage.Store) error {
				if err := s.ComputeMetrics(); err != nil {
					return err
				}
				if s.Registry().GetGauge("ranges.replication-pending").Value() > 0 {
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

type testClusterFactoryImpl struct{}

// TestClusterFactory can be passed to serverutils.InitTestClusterFactory
var TestClusterFactory serverutils.TestClusterFactory = testClusterFactoryImpl{}

// New is part of TestClusterFactory interface.
func (testClusterFactoryImpl) StartTestCluster(
	t testing.TB, numNodes int, args base.TestClusterArgs,
) serverutils.TestClusterInterface {
	return StartTestCluster(t, numNodes, args)
}
