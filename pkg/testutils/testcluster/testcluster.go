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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	// Quiesce the servers in parallel to avoid deadlocks. If we stop servers
	// serially when we lose quorum (2 out of 3 servers have stopped) the last
	// server may never finish due to waiting for a Raft command that can't
	// commit due to the lack of quorum.
	var wg sync.WaitGroup
	wg.Add(len(tc.mu.serverStoppers))
	for _, s := range tc.mu.serverStoppers {
		go func(s *stop.Stopper) {
			defer wg.Done()
			if s != nil {
				s.Quiesce(context.TODO())
			}
		}(s)
	}
	wg.Wait()

	for i := range tc.mu.serverStoppers {
		if tc.mu.serverStoppers[i] != nil {
			tc.mu.serverStoppers[i].Stop(context.TODO())
			tc.mu.serverStoppers[i] = nil
		}
	}
}

// StopServer stops an individual server in the cluster.
func (tc *TestCluster) StopServer(idx int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.serverStoppers[idx] != nil {
		tc.mu.serverStoppers[idx].Stop(context.TODO())
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

	for i := 0; i < nodes; i++ {
		var serverArgs base.TestServerArgs
		if perNodeServerArgs, ok := args.ServerArgsPerNode[i]; ok {
			serverArgs = perNodeServerArgs
		} else {
			serverArgs = args.ServerArgs
		}
		serverArgs.PartOfCluster = true
		if i > 0 {
			serverArgs.JoinAddr = tc.Servers[0].ServingAddr()
		}
		tc.AddServer(t, serverArgs)
	}

	// Create a closer that will stop the individual server stoppers when the
	// cluster stopper is stopped.
	tc.stopper.AddCloser(stop.CloserFn(tc.stopServers))

	tc.WaitForStores(t, tc.Servers[0].Gossip())

	if args.ReplicationMode == base.ReplicationAuto {
		if err := tc.WaitForFullReplication(); err != nil {
			t.Fatal(err)
		}
	}
	return tc
}

// AddServer creates a server with the specified arguments and appends it to
// the TestCluster.
func (tc *TestCluster) AddServer(t testing.TB, serverArgs base.TestServerArgs) {
	serverArgs.Stopper = stop.NewStopper()
	s, conn, _ := serverutils.StartServer(t, serverArgs)
	tc.Servers = append(tc.Servers, s.(*server.TestServer))
	tc.Conns = append(tc.Conns, conn)
	tc.mu.Lock()
	tc.mu.serverStoppers = append(tc.mu.serverStoppers, serverArgs.Stopper)
	tc.mu.Unlock()
}

// WaitForStores waits for all of the store descriptors to be gossiped. Servers
// other than the first "bootstrap" their stores asynchronously, but we'd like
// to wait for all of the stores to be initialized before returning the
// TestCluster.
func (tc *TestCluster) WaitForStores(t testing.TB, g *gossip.Gossip) {
	// Register a gossip callback for the store descriptors.
	var storesMu syncutil.Mutex
	stores := map[roachpb.StoreID]struct{}{}
	storesDone := make(chan error)
	storesDoneOnce := storesDone
	unregister := g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix),
		func(_ string, content roachpb.Value) {
			storesMu.Lock()
			defer storesMu.Unlock()
			if storesDoneOnce == nil {
				return
			}

			var desc roachpb.StoreDescriptor
			if err := content.GetProto(&desc); err != nil {
				storesDoneOnce <- err
				return
			}

			stores[desc.StoreID] = struct{}{}
			if len(stores) == len(tc.Servers) {
				close(storesDoneOnce)
				storesDoneOnce = nil
			}
		})
	defer unregister()

	// Wait for the store descriptors to be gossiped.
	for err := range storesDone {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// LookupRange is part of TestClusterInterface.
func (tc *TestCluster) LookupRange(key roachpb.Key) (roachpb.RangeDescriptor, error) {
	return tc.Servers[0].LookupRange(key)
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
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor, error) {
	return tc.Servers[0].SplitRange(splitKey)
}

// Target returns a ReplicationTarget for the specified server.
func (tc *TestCluster) Target(serverIdx int) roachpb.ReplicationTarget {
	s := tc.Servers[serverIdx]
	return roachpb.ReplicationTarget{
		NodeID:  s.GetNode().Descriptor.NodeID,
		StoreID: s.GetFirstStoreID(),
	}
}

func (tc *TestCluster) changeReplicas(
	changeType roachpb.ReplicaChangeType, startKey roachpb.RKey, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	ctx := context.TODO()
	if err := tc.Servers[0].DB().AdminChangeReplicas(
		ctx, startKey.AsRawKey(), changeType, targets,
	); err != nil {
		return roachpb.RangeDescriptor{}, errors.Wrap(err, "AdminChangeReplicas error")
	}
	var rangeDesc roachpb.RangeDescriptor
	if err := tc.Servers[0].DB().GetProto(
		ctx, keys.RangeDescriptorKey(startKey), &rangeDesc,
	); err != nil {
		return roachpb.RangeDescriptor{}, errors.Wrap(err, "range descriptor lookup error")
	}
	return rangeDesc, nil
}

// AddReplicas is part of TestClusterInterface.
func (tc *TestCluster) AddReplicas(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	rKey := keys.MustAddr(startKey)
	rangeDesc, err := tc.changeReplicas(
		roachpb.ADD_REPLICA, rKey, targets...,
	)
	if err != nil {
		return roachpb.RangeDescriptor{}, err
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
		return roachpb.RangeDescriptor{}, err
	}
	return rangeDesc, nil
}

// RemoveReplicas is part of the TestServerInterface.
func (tc *TestCluster) RemoveReplicas(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	return tc.changeReplicas(roachpb.REMOVE_REPLICA, keys.MustAddr(startKey), targets...)
}

// TransferRangeLease is part of the TestServerInterface.
func (tc *TestCluster) TransferRangeLease(
	rangeDesc roachpb.RangeDescriptor, dest roachpb.ReplicationTarget,
) error {
	err := tc.Servers[0].DB().AdminTransferLease(context.TODO(),
		rangeDesc.StartKey.AsRawKey(), dest.StoreID)
	if err != nil {
		return errors.Wrapf(err, "%q: transfer lease unexpected error", rangeDesc.StartKey)
	}
	return nil
}

// FindRangeLease is similar to FindRangeLeaseHolder but returns a Lease proto
// without verifying if the lease is still active. Instead, it returns a time-
// stamp taken off the queried node's clock.
func (tc *TestCluster) FindRangeLease(
	rangeDesc roachpb.RangeDescriptor, hint *roachpb.ReplicationTarget,
) (_ *roachpb.Lease, now hlc.Timestamp, _ error) {
	if hint != nil {
		var ok bool
		if _, ok = rangeDesc.GetReplicaDescriptor(hint.StoreID); !ok {
			return nil, hlc.Timestamp{}, errors.Errorf(
				"bad hint: %+v; store doesn't have a replica of the range", hint)
		}
	} else {
		hint = &roachpb.ReplicationTarget{
			NodeID:  rangeDesc.Replicas[0].NodeID,
			StoreID: rangeDesc.Replicas[0].StoreID}
	}

	// Find the server indicated by the hint and send a LeaseInfoRequest through
	// it.
	var hintServer *server.TestServer
	for _, s := range tc.Servers {
		if s.GetNode().Descriptor.NodeID == hint.NodeID {
			hintServer = s
			break
		}
	}
	if hintServer == nil {
		return nil, hlc.Timestamp{}, errors.Errorf("bad hint: %+v; no such node", hint)
	}
	leaseReq := roachpb.LeaseInfoRequest{
		Span: roachpb.Span{
			Key: rangeDesc.StartKey.AsRawKey(),
		},
	}
	leaseResp, pErr := client.SendWrappedWith(
		context.TODO(),
		hintServer.DB().GetSender(),
		roachpb.Header{
			// INCONSISTENT read, since we want to make sure that the node used to
			// send this is the one that processes the command, for the hint to
			// matter.
			ReadConsistency: roachpb.INCONSISTENT,
		},
		&leaseReq)
	if pErr != nil {
		return nil, hlc.Timestamp{}, pErr.GoError()
	}
	return leaseResp.(*roachpb.LeaseInfoResponse).Lease, hintServer.Clock().Now(), nil
}

// FindRangeLeaseHolder is part of TestClusterInterface.
func (tc *TestCluster) FindRangeLeaseHolder(
	rangeDesc roachpb.RangeDescriptor, hint *roachpb.ReplicationTarget,
) (roachpb.ReplicationTarget, error) {
	lease, now, err := tc.FindRangeLease(rangeDesc, hint)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
	}
	if lease == nil {
		return roachpb.ReplicationTarget{}, errors.New("no active lease")
	}
	// Find lease replica in order to examine the lease state.
	store, err := tc.findMemberStore(lease.Replica.StoreID)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
	}
	replica, err := store.GetReplica(rangeDesc.RangeID)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
	}
	if !replica.IsLeaseValid(lease, now) {
		return roachpb.ReplicationTarget{}, errors.New("no valid lease")
	}
	replicaDesc := lease.Replica
	return roachpb.ReplicationTarget{NodeID: replicaDesc.NodeID, StoreID: replicaDesc.StoreID}, nil
}

// WaitForSplitAndReplication waits for a range which starts with
// startKey and then verifies that each replica in the range
// descriptor has been created.
func (tc *TestCluster) WaitForSplitAndReplication(startKey roachpb.Key) error {
	return util.RetryForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
		desc, err := tc.LookupRange(startKey)
		if err != nil {
			return errors.Wrapf(err, "unable to lookup range for %s", startKey)
		}
		// Verify the split first.
		if !desc.StartKey.Equal(startKey) {
			return errors.Errorf("expected range start key %s; got %s",
				startKey, desc.StartKey)
		}
		// Once we've verified the split, make sure that replicas exist.
		for _, rDesc := range desc.Replicas {
			store, err := tc.findMemberStore(rDesc.StoreID)
			if err != nil {
				return err
			}
			repl, err := store.GetReplica(desc.RangeID)
			if err != nil {
				return err
			}
			actualReplicaDesc, err := repl.GetReplicaDescriptor()
			if err != nil {
				return err
			}
			if actualReplicaDesc != rDesc {
				return errors.Errorf("expected replica %s; got %s", rDesc, actualReplicaDesc)
			}
		}
		return nil
	})
}

// findMemberStore returns the store containing a given replica.
func (tc *TestCluster) findMemberStore(storeID roachpb.StoreID) (*storage.Store, error) {
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
	if int32(len(tc.Servers)) < config.DefaultZoneConfig().NumReplicas {
		return nil
	}

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
				if err := s.ComputeMetrics(context.TODO(), 0); err != nil {
					return err
				}
				if s.Metrics().UnderReplicatedRangeCount.Value() > 0 {
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

// StartTestCluster is part of TestClusterFactory interface.
func (testClusterFactoryImpl) StartTestCluster(
	t testing.TB, numNodes int, args base.TestClusterArgs,
) serverutils.TestClusterInterface {
	return StartTestCluster(t, numNodes, args)
}
