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
	"github.com/cockroachdb/cockroach/config"
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

	var servers []*server.TestServer
	var conns []*gosql.DB
	args.ServerArgs.PartOfCluster = true
	first, conn, _ := serverutils.StartServer(t, args.ServerArgs)
	servers = append(servers, first.(*server.TestServer))
	conns = append(conns, conn)
	args.ServerArgs.JoinAddr = first.ServingAddr()
	for i := 1; i < nodes; i++ {
		s, conn, _ := serverutils.StartServer(t, args.ServerArgs)
		servers = append(servers, s.(*server.TestServer))
		conns = append(conns, conn)
	}

	return &TestCluster{Servers: servers, Conns: conns}
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
