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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// createTestNode creates an rpc server using the specified address,
// gossip instance, KV database and a node using the specified slice
// of engines. The server and node are returned. If gossipBS is not
// nil, the gossip bootstrap address is set to gossipBS.
func createTestNode(addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T) (
	*rpc.Server, *Node) {
	tlsConfig, err := rpc.LoadTestTLSConfig("..")
	if err != nil {
		t.Fatal(err)
	}

	rpcServer := rpc.NewServer(addr, tlsConfig)
	if err := rpcServer.Start(); err != nil {
		t.Fatal(err)
	}
	g := gossip.New(tlsConfig)
	if gossipBS != nil {
		// Handle possibility of a :0 port specification.
		if gossipBS == addr {
			gossipBS = rpcServer.Addr()
		}
		g.SetBootstrap([]net.Addr{gossipBS})
		g.Start(rpcServer)
	}
	clock := hlc.NewClock(hlc.UnixNano)
	db := kv.NewDB(kv.NewDistKV(g), clock)
	node := NewNode(db, g)
	if err := node.start(rpcServer, clock, engines, nil); err != nil {
		t.Fatal(err)
	}
	return rpcServer, node
}

func formatKeys(keys []engine.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		buf.WriteString(fmt.Sprintf("%d: %s\n", i, key))
	}
	return buf.String()
}

// TestBootstrapCluster verifies the results of bootstrapping a
// cluster. Uses an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	e := engine.NewInMem(engine.Attributes{}, 1<<20)
	localDB, err := BootstrapCluster("cluster-1", e)
	if err != nil {
		t.Fatal(err)
	}
	defer localDB.Close()

	// Scan the complete contents of the local database.
	sr := <-localDB.Scan(&proto.ScanRequest{
		RequestHeader: proto.RequestHeader{
			Key:    engine.KeyMin,
			EndKey: engine.KeyMax,
			User:   storage.UserRoot,
		},
		MaxResults: math.MaxInt64,
	})
	if sr.Error != nil {
		t.Fatal(sr.Error)
	}
	var keys []engine.Key
	for _, kv := range sr.Rows {
		keys = append(keys, kv.Key)
	}
	var expectedKeys = []engine.Key{
		engine.Key("\x00\x00\x00range-1"),
		engine.Key("\x00\x00\x00range-id-generator"),
		engine.Key("\x00\x00\x00store-ident"),
		engine.Key("\x00\x00meta1\xff"),
		engine.Key("\x00\x00meta2\xff"),
		engine.Key("\x00acct"),
		engine.Key("\x00node-id-generator"),
		engine.Key("\x00perm"),
		engine.Key("\x00store-id-generator-1"),
		engine.Key("\x00zone"),
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
	e := engine.NewInMem(engine.Attributes{}, 1<<20)
	localDB, err := BootstrapCluster("cluster-1", e)
	if err != nil {
		t.Fatal(err)
	}
	localDB.Close()

	// Start a new node with two new stores which will require bootstrapping.
	engines := []engine.Engine{
		e,
		engine.NewInMem(engine.Attributes{}, 1<<20),
		engine.NewInMem(engine.Attributes{}, 1<<20),
	}
	server, node := createTestNode(util.CreateTestAddr("tcp"), engines, nil, t)
	defer server.Close()

	// Non-initialized stores (in this case the new in-memory-based
	// store) will be bootstrapped by the node upon start. This happens
	// in a goroutine, so we'll have to wait a bit (maximum 10ms) until
	// we can find the new node.
	if err := util.IsTrueWithin(func() bool { return node.localKV.GetStoreCount() == 3 }, 50*time.Millisecond); err != nil {
		t.Error(err)
	}
}

// TestNodeJoin verifies a new node is able to join a bootstrapped
// cluster consisting of one node.
func TestNodeJoin(t *testing.T) {
	e := engine.NewInMem(engine.Attributes{}, 1<<20)
	db, err := BootstrapCluster("cluster-1", e)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Set an aggressive gossip interval to make sure information is exchanged tout de suite.
	*gossip.GossipInterval = 10 * time.Millisecond
	// Start the bootstrap node.
	engines1 := []engine.Engine{e}
	addr1 := util.CreateTestAddr("tcp")
	server1, node1 := createTestNode(addr1, engines1, addr1, t)
	defer server1.Close()

	// Create a new node.
	engines2 := []engine.Engine{engine.NewInMem(engine.Attributes{}, 1<<20)}
	server2, node2 := createTestNode(util.CreateTestAddr("tcp"), engines2, server1.Addr(), t)
	defer server2.Close()

	// Verify new node is able to bootstrap its store.
	if err := util.IsTrueWithin(func() bool { return node2.localKV.GetStoreCount() == 1 }, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDGossipKey(node1.Descriptor.NodeID)
	node2Key := gossip.MakeNodeIDGossipKey(node2.Descriptor.NodeID)
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
	}, 50*time.Millisecond); err != nil {
		t.Error(err)
	}
}
