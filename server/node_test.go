// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"bytes"
	"fmt"
	"math"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"golang.org/x/net/context"
)

// createTestNode creates an rpc server using the specified address,
// gossip instance, KV database and a node using the specified slice
// of engines. The server, clock and node are returned. If gossipBS is
// not nil, the gossip bootstrap address is set to gossipBS.
func createTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*rpc.Server, *hlc.Clock, *Node, *util.Stopper) {
	tlsConfig, err := security.LoadTLSConfigFromDir(security.EmbeddedPrefix + "test_certs")
	if err != nil {
		t.Fatal(err)
	}
	ctx := storage.StoreContext{}
	ctx.Context = context.Background()

	stopper := util.NewStopper()
	ctx.Clock = hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(ctx.Clock, tlsConfig, stopper)
	ctx.ScanInterval = 10 * time.Hour
	rpcServer := rpc.NewServer(addr, rpcContext)
	if err := rpcServer.Start(); err != nil {
		t.Fatal(err)
	}
	g := gossip.New(rpcContext, testContext.GossipInterval, testContext.GossipBootstrapResolvers)
	if gossipBS != nil {
		// Handle possibility of a :0 port specification.
		if gossipBS == addr {
			gossipBS = rpcServer.Addr()
		}
		g.SetResolvers([]gossip.Resolver{gossip.NewResolverFromAddress(gossipBS)})
		g.Start(rpcServer, stopper)
	}
	ctx.Gossip = g
	ctx.DB = client.NewKV(nil,
		kv.NewDistSender(&kv.DistSenderContext{Clock: ctx.Clock}, g))
	// TODO(bdarnell): arrange to have the transport closed.
	ctx.Transport = multiraft.NewLocalRPCTransport()
	node := NewNode(ctx)
	return rpcServer, ctx.Clock, node, stopper
}

// createAndStartTestNode creates a new test node and starts it. The server and node are returned.
func createAndStartTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*rpc.Server, *Node, *util.Stopper) {
	rpcServer, _, node, stopper := createTestNode(addr, engines, gossipBS, t)
	if err := node.start(rpcServer, engines, proto.Attributes{}, stopper); err != nil {
		t.Fatal(err)
	}
	return rpcServer, node, stopper
}

func formatKeys(keys []proto.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		buf.WriteString(fmt.Sprintf("%d: %s\n", i, key))
	}
	return buf.String()
}

// TestBootstrapCluster verifies the results of bootstrapping a
// cluster. Uses an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	stopper := util.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	localDB, err := BootstrapCluster("cluster-1", e, stopper)
	if err != nil {
		t.Fatal(err)
	}
	defer stopper.Stop()

	// Scan the complete contents of the local database.
	sr := &proto.ScanResponse{}
	if err := localDB.Run(client.Call{
		Args: &proto.ScanRequest{
			RequestHeader: proto.RequestHeader{
				Key:    engine.KeyLocalPrefix.PrefixEnd(), // skip local keys
				EndKey: engine.KeyMax,
				User:   storage.UserRoot,
			},
			MaxResults: math.MaxInt64,
		},
		Reply: sr}); err != nil {
		t.Fatal(err)
	}
	var keys []proto.Key
	for _, kv := range sr.Rows {
		keys = append(keys, kv.Key)
	}
	var expectedKeys = []proto.Key{
		engine.MakeKey(proto.Key("\x00\x00meta1"), engine.KeyMax),
		engine.MakeKey(proto.Key("\x00\x00meta2"), engine.KeyMax),
		proto.Key("\x00acct"),
		proto.Key("\x00node-idgen"),
		proto.Key("\x00perm"),
		proto.Key("\x00range-tree-root"),
		proto.Key("\x00store-idgen"),
		proto.Key("\x00zone"),
	}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("expected keys mismatch:\n%s\n  -- vs. -- \n\n%s",
			formatKeys(keys), formatKeys(expectedKeys))
	}

	// TODO(spencer): check values.
}

// TestBootstrapNewStore starts a cluster with two unbootstrapped
// stores and verifies both stores are added and started.
func TestBootstrapNewStore(t *testing.T) {
	stopper := util.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	_, err := BootstrapCluster("cluster-1", e, stopper)
	if err != nil {
		t.Fatal(err)
	}
	stopper.Stop()

	// Start a new node with two new stores which will require bootstrapping.
	engines := []engine.Engine{
		e,
		engine.NewInMem(proto.Attributes{}, 1<<20),
		engine.NewInMem(proto.Attributes{}, 1<<20),
	}
	_, node, stopper := createAndStartTestNode(util.CreateTestAddr("tcp"), engines, nil, t)
	defer stopper.Stop()

	// Non-initialized stores (in this case the new in-memory-based
	// store) will be bootstrapped by the node upon start. This happens
	// in a goroutine, so we'll have to wait a bit (maximum 1s) until
	// we can find the new node.
	if err := util.IsTrueWithin(func() bool { return node.lSender.GetStoreCount() == 3 }, 1*time.Second); err != nil {
		t.Error(err)
	}

	// Check whether all stores are started properly.
	if err := node.lSender.VisitStores(func(s *storage.Store) error {
		if s.IsStarted() == false {
			return util.Errorf("fail to start store: %s", s)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// TestNodeJoin verifies a new node is able to join a bootstrapped
// cluster consisting of one node.
func TestNodeJoin(t *testing.T) {
	stopper := util.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	_, err := BootstrapCluster("cluster-1", e, stopper)
	if err != nil {
		t.Fatal(err)
	}
	stopper.Stop()

	// Set an aggressive gossip interval to make sure information is exchanged tout de suite.
	testContext.GossipInterval = gossip.TestInterval
	// Start the bootstrap node.
	engines1 := []engine.Engine{e}
	addr1 := util.CreateTestAddr("tcp")
	server1, node1, stopper1 := createAndStartTestNode(addr1, engines1, addr1, t)
	defer stopper1.Stop()

	// Create a new node.
	engines2 := []engine.Engine{engine.NewInMem(proto.Attributes{}, 1<<20)}
	server2, node2, stopper2 := createAndStartTestNode(util.CreateTestAddr("tcp"), engines2, server1.Addr(), t)
	defer stopper2.Stop()

	// Verify new node is able to bootstrap its store.
	if err := util.IsTrueWithin(func() bool { return node2.lSender.GetStoreCount() == 1 }, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDKey(node1.Descriptor.NodeID)
	node2Key := gossip.MakeNodeIDKey(node2.Descriptor.NodeID)
	if err := util.IsTrueWithin(func() bool {
		if val, err := node1.ctx.Gossip.GetInfo(node2Key); err != nil {
			return false
		} else if addr2 := val.(*gossip.NodeDescriptor).Address.String(); addr2 != server2.Addr().String() {
			t.Errorf("addr2 gossip %s doesn't match addr2 address %s", addr2, server2.Addr().String())
		}
		if val, err := node2.ctx.Gossip.GetInfo(node1Key); err != nil {
			return false
		} else if addr1 := val.(*gossip.NodeDescriptor).Address.String(); addr1 != server1.Addr().String() {
			t.Errorf("addr1 gossip %s doesn't match addr1 address %s", addr1, server1.Addr().String())
		}
		return true
	}, 50*time.Millisecond); err != nil {
		t.Error(err)
	}
}

// TestCorruptedClusterID verifies that a node fails to start when a
// store's cluster ID is empty.
func TestCorruptedClusterID(t *testing.T) {
	stopper := util.NewStopper()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	_, err := BootstrapCluster("cluster-1", e, stopper)
	if err != nil {
		t.Fatal(err)
	}
	stopper.Stop()

	// Set the cluster ID to an empty string.
	sIdent := proto.StoreIdent{
		ClusterID: "",
		NodeID:    1,
		StoreID:   1,
	}
	if err = engine.MVCCPutProto(e, nil, engine.StoreIdentKey(), proto.ZeroTimestamp, nil, &sIdent); err != nil {
		t.Fatal(err)
	}

	engines := []engine.Engine{e}
	server, _, node, stopper := createTestNode(util.CreateTestAddr("tcp"), engines, nil, t)
	if err := node.start(server, engines, proto.Attributes{}, stopper); err == nil {
		t.Errorf("unexpected success")
	}
	stopper.Stop()
}
