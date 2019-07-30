// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcluster

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// TestCluster represents a set of TestServers. The hope is that it can be used
// analoguous to TestServer, but with control over range replication.
type TestCluster struct {
	Servers         []*server.TestServer
	Conns           []*gosql.DB
	stopper         *stop.Stopper
	replicationMode base.TestClusterReplicationMode
	scratchRangeID  roachpb.RangeID
	mu              struct {
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

	if err := checkServerArgsForCluster(
		args.ServerArgs, args.ReplicationMode, disallowJoinAddr,
	); err != nil {
		t.Fatal(err)
	}
	for _, sargs := range args.ServerArgsPerNode {
		if err := checkServerArgsForCluster(
			sargs, args.ReplicationMode, disallowJoinAddr,
		); err != nil {
			t.Fatal(err)
		}
	}

	tc := &TestCluster{
		stopper:         stop.NewStopper(),
		replicationMode: args.ReplicationMode,
	}
	tc.stopper = stop.NewStopper()

	for i := 0; i < nodes; i++ {
		var serverArgs base.TestServerArgs
		if perNodeServerArgs, ok := args.ServerArgsPerNode[i]; ok {
			serverArgs = perNodeServerArgs
		} else {
			serverArgs = args.ServerArgs
		}

		// If there are multiple nodes, place them in different localities by
		// default.
		if nodes > 0 {
			tiers := []roachpb.Tier{
				{Key: "region", Value: "test"},
				{Key: "dc", Value: fmt.Sprintf("dc%d", i+1)},
			}
			serverArgs.Locality = roachpb.Locality{Tiers: tiers}
		}

		if i > 0 {
			serverArgs.JoinAddr = tc.Servers[0].ServingAddr()
		}
		if err := tc.doAddServer(t, serverArgs); err != nil {
			t.Fatal(err)
		}
		// We want to wait for stores for each server in order to have predictable
		// store IDs. Otherwise, stores can be asynchronously bootstrapped in an
		// unexpected order (#22342).
		tc.WaitForStores(t, tc.Servers[0].Gossip())
	}

	// Create a closer that will stop the individual server stoppers when the
	// cluster stopper is stopped.
	tc.stopper.AddCloser(stop.CloserFn(tc.stopServers))

	if tc.replicationMode == base.ReplicationAuto {
		if err := tc.WaitForFullReplication(); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until a NodeStatus is persisted for every node (see #25488, #25649, #31574).
	tc.WaitForNodeStatuses(t)
	return tc
}

type checkType bool

const (
	disallowJoinAddr checkType = false
	allowJoinAddr    checkType = true
)

// checkServerArgsForCluster sanity-checks TestServerArgs to work for a cluster
// with a given replicationMode.
func checkServerArgsForCluster(
	args base.TestServerArgs, replicationMode base.TestClusterReplicationMode, checkType checkType,
) error {
	if checkType == disallowJoinAddr && args.JoinAddr != "" {
		return errors.Errorf("can't specify a join addr when starting a cluster: %s",
			args.JoinAddr)
	}
	if args.Stopper != nil {
		return errors.Errorf("can't set individual server stoppers when starting a cluster")
	}
	if args.Knobs.Store != nil {
		storeKnobs := args.Knobs.Store.(*storage.StoreTestingKnobs)
		if storeKnobs.DisableSplitQueue || storeKnobs.DisableReplicateQueue {
			return errors.Errorf("can't disable an individual server's queues when starting a cluster; " +
				"the cluster controls replication")
		}
	}

	if replicationMode != base.ReplicationAuto && replicationMode != base.ReplicationManual {
		return errors.Errorf("unexpected replication mode: %s", replicationMode)
	}

	return nil
}

// AddServer creates a server with the specified arguments and appends it to
// the TestCluster.
//
// The new Server's copy of serverArgs might be changed according to the
// cluster's ReplicationMode.
func (tc *TestCluster) AddServer(t testing.TB, serverArgs base.TestServerArgs) {
	if serverArgs.JoinAddr == "" && len(tc.Servers) > 0 {
		serverArgs.JoinAddr = tc.Servers[0].ServingAddr()
	}
	if err := tc.doAddServer(t, serverArgs); err != nil {
		t.Fatal(err)
	}
}

func (tc *TestCluster) doAddServer(t testing.TB, serverArgs base.TestServerArgs) error {
	serverArgs.PartOfCluster = true
	// Check args even though they might have been checked in StartTestCluster;
	// this method might be called for servers being added after the cluster was
	// started, in which case the check has not been performed.
	if err := checkServerArgsForCluster(
		serverArgs,
		tc.replicationMode,
		// Allow JoinAddr here; servers being added after the TestCluster has been
		// started should have a JoinAddr filled in at this point.
		allowJoinAddr,
	); err != nil {
		return err
	}
	serverArgs.Stopper = stop.NewStopper()
	if tc.replicationMode == base.ReplicationManual {
		var stkCopy storage.StoreTestingKnobs
		if stk := serverArgs.Knobs.Store; stk != nil {
			stkCopy = *stk.(*storage.StoreTestingKnobs)
		}
		stkCopy.DisableSplitQueue = true
		stkCopy.DisableMergeQueue = true
		stkCopy.DisableReplicateQueue = true
		serverArgs.Knobs.Store = &stkCopy
	}

	s, conn, _ := serverutils.StartServer(t, serverArgs)

	if tc.replicationMode == base.ReplicationManual && len(tc.Servers) == 0 {
		// We've already disabled the merge queue via testing knobs above, but ALTER
		// TABLE ... SPLIT AT will throw an error unless we also disable merges via
		// the cluster setting.
		//
		// TODO(benesch): this won't be necessary once we have sticky bits for
		// splits.
		if _, err := conn.Exec(`SET CLUSTER SETTING kv.range_merge.queue_enabled = false`); err != nil {
			t.Fatal(err)
		}
	}

	// Disable LBS if the server being added has a scan interval arg this low.
	if serverArgs.ScanInterval > 0 && serverArgs.ScanInterval <= 100*time.Millisecond {
		if _, err := conn.Exec(`SET CLUSTER SETTING kv.range_split.by_load_enabled = false`); err != nil {
			t.Fatal(err)
		}
	}

	tc.Servers = append(tc.Servers, s.(*server.TestServer))
	tc.Conns = append(tc.Conns, conn)
	tc.mu.Lock()
	tc.mu.serverStoppers = append(tc.mu.serverStoppers, serverArgs.Stopper)
	tc.mu.Unlock()
	return nil
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

// LookupRangeOrFatal is part of TestClusterInterface.
func (tc *TestCluster) LookupRangeOrFatal(t testing.TB, key roachpb.Key) roachpb.RangeDescriptor {
	t.Helper()
	desc, err := tc.LookupRange(key)
	if err != nil {
		t.Fatalf(`looking up range for %s: %+v`, key, err)
	}
	return desc
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

// SplitRangeOrFatal is the same as SplitRange but will Fatal the test on error.
func (tc *TestCluster) SplitRangeOrFatal(
	t testing.TB, splitKey roachpb.Key,
) (roachpb.RangeDescriptor, roachpb.RangeDescriptor) {
	lhsDesc, rhsDesc, err := tc.Servers[0].SplitRange(splitKey)
	if err != nil {
		t.Fatalf(`splitting at %s: %+v`, splitKey, err)
	}
	return lhsDesc, rhsDesc
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
	var beforeDesc roachpb.RangeDescriptor
	if err := tc.Servers[0].DB().GetProto(
		ctx, keys.RangeDescriptorKey(startKey), &beforeDesc,
	); err != nil {
		return roachpb.RangeDescriptor{}, errors.Wrap(err, "range descriptor lookup error")
	}
	desc, err := tc.Servers[0].DB().AdminChangeReplicas(
		ctx, startKey.AsRawKey(), changeType, targets, beforeDesc,
	)
	if err != nil {
		return roachpb.RangeDescriptor{}, errors.Wrap(err, "AdminChangeReplicas error")
	}
	return *desc, nil
}

// AddReplicas is part of TestClusterInterface.
func (tc *TestCluster) AddReplicas(
	startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) (roachpb.RangeDescriptor, error) {
	rKey := keys.MustAddr(startKey)
	errRetry := errors.Errorf("target not found")
	for {
		rangeDesc, err := tc.changeReplicas(
			roachpb.ADD_REPLICA, rKey, targets...,
		)
		if err != nil {
			return roachpb.RangeDescriptor{}, err
		}

		// Wait for the replication to complete on all destination nodes.
		if err := retry.ForDuration(time.Second*25, func() error {
			for _, target := range targets {
				// Use LookupReplica(keys) instead of GetRange(rangeID) to ensure that the
				// snapshot has been transferred and the descriptor initialized.
				store, err := tc.findMemberStore(target.StoreID)
				if err != nil {
					log.Errorf(context.TODO(), "unexpected error: %s", err)
					return err
				}
				repl := store.LookupReplica(rKey)
				if repl == nil {
					return errors.Wrapf(errRetry, "for target %s", target)
				}
				desc := repl.Desc()
				if _, ok := desc.GetReplicaDescriptor(target.StoreID); !ok {
					return errors.Errorf("target store %d not yet in range descriptor %v", target.StoreID, desc)
				}
			}
			return nil
		}); errors.Cause(err) == errRetry {
			log.Warningf(context.Background(), "target was likely downreplicated again; retrying after %s", err)
			continue
		} else if err != nil {
			return roachpb.RangeDescriptor{}, err
		}
		return rangeDesc, nil
	}
}

// AddReplicasOrFatal is part of TestClusterInterface.
func (tc *TestCluster) AddReplicasOrFatal(
	t testing.TB, startKey roachpb.Key, targets ...roachpb.ReplicationTarget,
) roachpb.RangeDescriptor {
	t.Helper()
	desc, err := tc.AddReplicas(startKey, targets...)
	if err != nil {
		t.Fatalf(`could not add %v replicas to range containing %s: %+v`,
			targets, startKey, err)
	}
	return desc
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
) (_ roachpb.Lease, now hlc.Timestamp, _ error) {
	if hint != nil {
		var ok bool
		if _, ok = rangeDesc.GetReplicaDescriptor(hint.StoreID); !ok {
			return roachpb.Lease{}, hlc.Timestamp{}, errors.Errorf(
				"bad hint: %+v; store doesn't have a replica of the range", hint)
		}
	} else {
		hint = &roachpb.ReplicationTarget{
			NodeID:  rangeDesc.Replicas().All()[0].NodeID,
			StoreID: rangeDesc.Replicas().All()[0].StoreID}
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
		return roachpb.Lease{}, hlc.Timestamp{}, errors.Errorf("bad hint: %+v; no such node", hint)
	}

	return hintServer.GetRangeLease(context.TODO(), rangeDesc.StartKey.AsRawKey())
}

// FindRangeLeaseHolder is part of TestClusterInterface.
func (tc *TestCluster) FindRangeLeaseHolder(
	rangeDesc roachpb.RangeDescriptor, hint *roachpb.ReplicationTarget,
) (roachpb.ReplicationTarget, error) {
	lease, now, err := tc.FindRangeLease(rangeDesc, hint)
	if err != nil {
		return roachpb.ReplicationTarget{}, err
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

// ScratchRange returns the start key of a span of keyspace suitable for use as
// kv scratch space (it doesn't overlap system spans or SQL tables). The range
// is lazily split off on the first call to ScratchRange.
func (tc *TestCluster) ScratchRange(t testing.TB) roachpb.Key {
	scratchKey := keys.MakeTablePrefix(math.MaxUint32)
	if tc.scratchRangeID > 0 {
		return scratchKey
	}
	_, right, err := tc.SplitRange(scratchKey)
	if err != nil {
		t.Fatal(err)
	}
	tc.scratchRangeID = right.RangeID
	return scratchKey
}

// WaitForSplitAndInitialization waits for a range which starts with startKey
// and then verifies that each replica in the range descriptor has been created.
//
// NB: This doesn't actually wait for full upreplication to whatever the zone
// config specifies.
func (tc *TestCluster) WaitForSplitAndInitialization(startKey roachpb.Key) error {
	return retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
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
		for _, rDesc := range desc.Replicas().All() {
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
//
// TODO(andrei): This method takes inexplicably long.
// I think it shouldn't need any retries. See #38565.
func (tc *TestCluster) WaitForFullReplication() error {
	start := timeutil.Now()
	defer func() {
		end := timeutil.Now()
		log.Infof(context.TODO(), "WaitForFullReplication took: %s", end.Sub(start))
	}()

	if len(tc.Servers) < 3 {
		// If we have less than three nodes, we will never have full replication.
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
				if n := s.ClusterNodeCount(); n != len(tc.Servers) {
					log.Infof(context.TODO(), "%s only sees %d/%d available nodes", s, n, len(tc.Servers))
					notReplicated = true
					return nil
				}
				// Force upreplication. Otherwise, if we rely on the scanner to do it,
				// it'll take a while.
				if err := s.ForceReplicationScanAndProcess(); err != nil {
					return err
				}
				if err := s.ComputeMetrics(context.TODO(), 0); err != nil {
					// This can sometimes fail since ComputeMetrics calls
					// updateReplicationGauges which needs the system config gossiped.
					log.Info(context.TODO(), err)
					notReplicated = true
					return nil
				}
				if n := s.Metrics().UnderReplicatedRangeCount.Value(); n > 0 {
					log.Infof(context.TODO(), "%s has %d underreplicated ranges", s, n)
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

// WaitForNodeStatuses waits until a NodeStatus is persisted for every node and
// store in the cluster.
func (tc *TestCluster) WaitForNodeStatuses(t testing.TB) {
	testutils.SucceedsSoon(t, func() error {
		url := tc.Server(0).ServingAddr()
		nodeID := tc.Server(0).NodeID()
		conn, err := tc.Server(0).RPCContext().GRPCDialNode(url, nodeID).Connect(context.Background())
		if err != nil {
			return err
		}
		client := serverpb.NewStatusClient(conn)
		response, err := client.Nodes(context.Background(), &serverpb.NodesRequest{})
		if err != nil {
			return err
		}

		if len(response.Nodes) < tc.NumServers() {
			return fmt.Errorf("expected %d nodes registered, got %+v", tc.NumServers(), response)
		}

		// Check that all the nodes in the testcluster have a status. We tolerate
		// other nodes having statuses (in some tests the cluster is configured with
		// a pre-existing store).
		nodeIDs := make(map[roachpb.NodeID]bool)
		for _, node := range response.Nodes {
			if len(node.StoreStatuses) == 0 {
				return fmt.Errorf("missing StoreStatuses in NodeStatus: %+v", node)
			}
			nodeIDs[node.Desc.NodeID] = true
		}
		for _, s := range tc.Servers {
			if id := s.GetNode().Descriptor.NodeID; !nodeIDs[id] {
				return fmt.Errorf("missing n%d in NodeStatus: %+v", id, response)
			}
		}
		return nil
	})
}

// WaitForNodeLiveness waits until a liveness record is persisted for every
// node in the cluster.
func (tc *TestCluster) WaitForNodeLiveness(t testing.TB) {
	testutils.SucceedsSoon(t, func() error {
		db := tc.Servers[0].DB()
		for _, s := range tc.Servers {
			key := keys.NodeLivenessKey(s.NodeID())
			var liveness storagepb.Liveness
			if err := db.GetProto(context.Background(), key, &liveness); err != nil {
				return err
			}
			if (liveness == storagepb.Liveness{}) {
				return fmt.Errorf("no liveness record")
			}
			fmt.Printf("n%d: found liveness\n", s.NodeID())
		}
		return nil
	})
}

// ReplicationMode implements TestClusterInterface.
func (tc *TestCluster) ReplicationMode() base.TestClusterReplicationMode {
	return tc.replicationMode
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
