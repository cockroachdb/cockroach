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
// implied.  See the License for the specific language governing
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

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

// createTestNode creates an rpc server using the specified address,
// gossip instance, KV database and a node using the specified slice
// of engines. The server and node are returned. If startGossip is
// true, the gossip instance is started.
func createTestNode(addr net.Addr, engines []storage.Engine, gossipBS net.Addr, t *testing.T) (
	*rpc.Server, *Node) {
	rpcServer := rpc.NewServer(addr)
	if err := rpcServer.Start(); err != nil {
		t.Fatal(err)
	}
	g := gossip.New()
	if gossipBS != nil {
		// Handle possibility of a :0 port specification.
		if gossipBS == addr {
			gossipBS = rpcServer.Addr()
		}
		g.SetBootstrap([]net.Addr{gossipBS})
		g.Start(rpcServer)
	}
	db := kv.NewDB(g)
	node := NewNode(db, g)
	if err := node.start(rpcServer, engines); err != nil {
		t.Fatal(err)
	}
	return rpcServer, node
}

func formatKeys(keys []storage.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		buf.WriteString(fmt.Sprintf("%d: %s\n", i, key))
	}
	return buf.String()
}

// TestBootstrapCluster verifies the results of bootstrapping a
// cluster. Uses an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	engine := storage.NewInMem(1 << 20)
	localDB, err := BootstrapCluster("cluster-1", engine)
	if err != nil {
		t.Fatal(err)
	}
	// Scan the complete contents of the local database.
	sr := <-localDB.Scan(&storage.ScanRequest{
		StartKey:   storage.KeyMin,
		EndKey:     storage.KeyMax,
		MaxResults: math.MaxInt64,
	})
	if sr.Error != nil {
		t.Fatal(sr.Error)
	}
	var keys []storage.Key
	for _, kv := range sr.Rows {
		keys = append(keys, kv.Key)
	}
	var expectedKeys = []storage.Key{
		storage.Key("\x00\x00\x00range-1"),
		storage.Key("\x00\x00\x00range-id-generator"),
		storage.Key("\x00\x00\x00store-ident"),
		storage.Key("\x00\x00meta1\xff"),
		storage.Key("\x00\x00meta2\xff"),
		storage.Key("\x00acct"),
		storage.Key("\x00node-id-generator"),
		storage.Key("\x00perm"),
		storage.Key("\x00store-id-generator-1"),
		storage.Key("\x00zone"),
	}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("expected keys mismatch:\n%s\n  -- vs. -- \n\n%s",
			formatKeys(keys), formatKeys(expectedKeys))
	}

	// TODO(spencer): check values.
}

// TestBootstrapNewStore starts a cluster with two unbootstrapped
// stores and verifies both stores are added.
func TestBootstrapNewStore(t *testing.T) {
	engine := storage.NewInMem(1 << 20)
	if _, err := BootstrapCluster("cluster-1", engine); err != nil {
		t.Fatal(err)
	}
	// Provide a list of engines for initializing a node.
	engines := []storage.Engine{
		engine,
		storage.NewInMem(1 << 20),
		storage.NewInMem(1 << 20),
	}
	server, node := createTestNode(util.CreateTestAddr("tcp"), engines, nil, t)
	defer server.Close()

	// Non-initialized stores (in this case the new in-memory-based
	// store) will be bootstrapped by the node upon start. This happens
	// in a goroutine, so we'll have to wait a bit (maximum 10ms) until
	// we can find the new node.
	if err := util.IsTrueWithin(func() bool { return node.getStoreCount() == 3 }, 5*time.Second); err != nil {
		t.Error(err)
	}
}

// TestNodeJoin verifies a new node is able to join a bootstrapped
// cluster consisting of one node.
func TestNodeJoin(t *testing.T) {
	engine := storage.NewInMem(1 << 20)
	if _, err := BootstrapCluster("cluster-1", engine); err != nil {
		t.Fatal(err)
	}
	// Set an aggressive gossip interval to make sure information is exchanged tout de suite.
	*gossip.GossipInterval = 500 * time.Millisecond
	// Start the bootstrap node.
	engines1 := []storage.Engine{engine}
	addr1 := util.CreateTestAddr("tcp")
	server1, node1 := createTestNode(addr1, engines1, addr1, t)
	defer server1.Close()

	// Create a new node.
	engines2 := []storage.Engine{storage.NewInMem(1 << 20)}
	server2, node2 := createTestNode(util.CreateTestAddr("tcp"), engines2, server1.Addr(), t)
	defer server2.Close()

	// Verify new node is able to bootstrap its store.
	if err := util.IsTrueWithin(func() bool { return node2.getStoreCount() == 1 }, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDGossipKey(node1.Attributes.NodeID)
	node2Key := gossip.MakeNodeIDGossipKey(node2.Attributes.NodeID)
	if err := util.IsTrueWithin(func() bool {
		if val, err := node1.gossip.GetInfo(node2Key); err != nil {
			return false
		} else if val.(net.Addr).String() != server2.Addr().String() {
			t.Error("addr2 gossip %s doesn't match addr2 address %s", val.(net.Addr).String(), server2.Addr().String())
		}
		if val, err := node2.gossip.GetInfo(node1Key); err != nil {
			return false
		} else if val.(net.Addr).String() != server1.Addr().String() {
			t.Error("addr1 gossip %s doesn't match addr1 address %s", val.(net.Addr).String(), server1.Addr().String())
		}
		return true
	}, 1*time.Second); err != nil {
		t.Error(err)
	}
}
