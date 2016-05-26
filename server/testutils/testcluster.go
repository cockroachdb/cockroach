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
// Author: David Taylor (david@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package testutils

import (
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/server/testingshim"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
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

// lookupRange returns the descriptor of the range containing key.
func (tc *TestCluster) lookupRange(key roachpb.RKey) *roachpb.RangeDescriptor {
	// Iterate through the servers. One of the is bound to have a replica of the
	// range.
	for _, server := range tc.Servers {
		// Does this server have a replica?
		rangeID, replicaDesc, err := server.Stores().LookupReplica(key, nil)
		if _, ok := err.(*roachpb.RangeKeyMismatchError); ok {
			continue
		}
		if err != nil {
			log.Fatalf("Unexpected error finding replica for key %q: %s", key, err)
		}
		// Get the store with the replica.
		store, err := server.Stores().GetStore(replicaDesc.StoreID)
		if err != nil {
			panic(err)
		}
		// Get the replica.
		replica, err := store.GetReplica(rangeID)
		if err != nil {
			panic(err)
		}
		// Get the range descriptor.
		return replica.Desc()
	}
	panic("unreached")
}

func adminSplitArgs(splitKey roachpb.Key) roachpb.AdminSplitRequest {
	return roachpb.AdminSplitRequest{
		Span: roachpb.Span{
			Key: splitKey,
		},
		SplitKey: splitKey,
	}
}

func addr(k roachpb.Key) roachpb.RKey {
	rk, err := keys.Addr(k)
	if err != nil {
		panic(err)
	}
	return rk
}

// SplitRange splits the range containing splitKey.
// The right range created by the split starts at the split key and extends to the
// original range's end key.
// Returns the new ids of the left and right ranges.
func (tc *TestCluster) SplitRange(
	splitKey roachpb.Key,
) (*roachpb.RangeDescriptor, *roachpb.RangeDescriptor) {
	origRangeDesc := tc.lookupRange(addr(splitKey))
	if origRangeDesc.StartKey.Equal(addr(splitKey)) {
		log.Fatalf("cannot split range %+v at start key %q", origRangeDesc, splitKey)
	}
	splitReq := adminSplitArgs(splitKey)
	_, pErr := client.SendWrapped(tc.Servers[0].GetDistSender(), nil, &splitReq)
	if pErr != nil {
		log.Fatalf("%q: split unexpected error: %s", splitReq.SplitKey, pErr)
	}
	leftRangeDesc := tc.lookupRange(origRangeDesc.StartKey)
	rightRangeDesc := tc.lookupRange(addr(splitKey))
	return leftRangeDesc, rightRangeDesc
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
	t *testing.T, rangeDesc *roachpb.RangeDescriptor, dests ...ReplicationTarget,
) *roachpb.RangeDescriptor {
	startKey := rangeDesc.StartKey
	// TODO(andrei): the following code has been adapted from
	// multiTestContext.replicateRange(). Find a way to share.
	for _, dest := range dests {
		// Perform a consistent read to get the updated range descriptor (as oposed
		// to just going to one of the stores), to make sure we have the effects of
		// the previous ChangeReplicas call. By the time ChangeReplicas returns the
		// raft leader is guaranteed to have the updated version, but followers are
		// not.
		if err := tc.Servers[0].DB().GetProto(
			keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
			panic(err)
		}

		// Ask a random replica of the range to up-replicate.
		replica, err := tc.findMemberStore(
			rangeDesc.Replicas[0].StoreID).GetReplica(rangeDesc.RangeID)
		if err != nil {
			panic(err)
		}
		err = replica.ChangeReplicas(roachpb.ADD_REPLICA,
			roachpb.ReplicaDescriptor{
				NodeID:  dest.NodeID,
				StoreID: dest.StoreID,
			}, rangeDesc)
		if err != nil {
			panic(err)
		}
	}

	// Wait for the replication to complete on all destination nodes.
	util.SucceedsSoon(t, func() error {
		for _, dest := range dests {
			// Use LookupReplica(keys) instead of GetRange(rangeID) to ensure that the
			// snapshot has been transferred and the descriptor initialized.
			if tc.findMemberStore(dest.StoreID).LookupReplica(startKey, nil) == nil {
				return errors.Errorf("range not found on store %d", dest)
			}
		}
		return nil
	})
	if err := tc.Servers[0].DB().GetProto(
		keys.RangeDescriptorKey(startKey), rangeDesc); err != nil {
		panic(err)
	}
	return rangeDesc
}

// findMemberStore returns the store containing a given replica.
func (tc *TestCluster) findMemberStore(storeID roachpb.StoreID) *storage.Store {
	for _, server := range tc.Servers {
		if server.Stores().HasStore(storeID) {
			store, err := server.Stores().GetStore(storeID)
			if err != nil {
				panic(err)
			}
			return store
		}
	}
	panic("unreached")
}

// StartTestCluster starts up a TestCluster made up of `nodes` in-memory testing
// servers, and creates database `name`.
// The cluster has replication disabled; all replication must be controlled
// manually.
// Also returns a cleanup func that stops and cleans up all nodes and
// connections.
func StartTestCluster(
	t testing.TB, nodes int, name string, params testingshim.TestServerParams,
) (*TestCluster, func()) {
	if nodes < 1 {
		t.Fatal("invalid cluster size: ", nodes)
	}
	if params.JoinAddr != "" {
		t.Fatal("can't specify a join addr when starting a cluster")
	}
	var servers []*server.TestServer

	ctx := server.MakeTestContext()
	ctx.TestingKnobs = params.Knobs
	serversStopper := stop.NewStopper()
	first := server.StartTestServerWithContext(t, &ctx, serversStopper)
	servers = append(servers, &first)
	for i := 1; i < nodes; i++ {
		// Make a new Context; can't reuse one because the server started with it
		// changes members.
		ctx := server.MakeTestContext()
		ctx.TestingKnobs = params.Knobs
		ctx.JoinUsing = first.ServingAddr()
		server := server.StartTestServerWithContext(t, &ctx, serversStopper)
		servers = append(servers, &server)
	}

	var conns []*gosql.DB
	var closes []func() error
	var cleanups []func()

	for i, s := range servers {
		pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), security.RootUser,
			fmt.Sprintf("node%d", i))
		pgURL.Path = name
		db, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		closes = append(closes, db.Close)
		cleanups = append(cleanups, cleanupFn)
		conns = append(conns, db)
	}

	if _, err := conns[0].Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatal(err)
	}

	f := func() {
		for _, fn := range closes {
			_ = fn()
		}
		serversStopper.Stop()
		for _, fn := range cleanups {
			fn()
		}
	}

	return &TestCluster{Servers: servers, Conns: conns}, f
}
