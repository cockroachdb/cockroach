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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestGossipInfoStore verifies operation of gossip instance infostore.
func TestGossipInfoStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	rpcContext := newInsecureRPCContext(stopper)
	g := NewTest(1, rpcContext, rpc.NewServer(rpcContext), stopper, metric.NewRegistry())
	slice := []byte("b")
	if err := g.AddInfo("s", slice, time.Hour); err != nil {
		t.Fatal(err)
	}
	if val, err := g.GetInfo("s"); !bytes.Equal(val, slice) || err != nil {
		t.Errorf("error fetching string: %v", err)
	}
	if _, err := g.GetInfo("s2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"s2\"")
	}
}

// TestGossipOverwriteNode verifies that if a new node is added with the same
// address as an old node, that old node is removed from the cluster.
func TestGossipOverwriteNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	rpcContext := newInsecureRPCContext(stopper)
	g := NewTest(1, rpcContext, rpc.NewServer(rpcContext), stopper, metric.NewRegistry())
	node1 := &roachpb.NodeDescriptor{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1.1.1.1:1")}
	node2 := &roachpb.NodeDescriptor{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2.2.2.2:2")}
	if err := g.SetNodeDescriptor(node1); err != nil {
		t.Fatal(err)
	}
	if err := g.SetNodeDescriptor(node2); err != nil {
		t.Fatal(err)
	}
	if val, err := g.GetNodeDescriptor(node1.NodeID); err != nil {
		t.Error(err)
	} else if val.NodeID != node1.NodeID {
		t.Errorf("expected node %d, got %+v", node1.NodeID, val)
	}
	if val, err := g.GetNodeDescriptor(node2.NodeID); err != nil {
		t.Error(err)
	} else if val.NodeID != node2.NodeID {
		t.Errorf("expected node %d, got %+v", node2.NodeID, val)
	}

	// Give node3 the same address as node1, which should cause node1 to be
	// removed from the cluster.
	node3 := &roachpb.NodeDescriptor{NodeID: 3, Address: node1.Address}
	if err := g.SetNodeDescriptor(node3); err != nil {
		t.Fatal(err)
	}
	if val, err := g.GetNodeDescriptor(node3.NodeID); err != nil {
		t.Error(err)
	} else if val.NodeID != node3.NodeID {
		t.Errorf("expected node %d, got %+v", node3.NodeID, val)
	}

	// Quiesce the stopper now to ensure that the update has propagated before
	// checking whether node 1 has been removed from the infoStore.
	stopper.Quiesce(context.TODO())
	expectedErr := "unable to look up descriptor for node"
	if val, err := g.GetNodeDescriptor(node1.NodeID); !testutils.IsError(err, expectedErr) {
		t.Errorf("expected error %q fetching node %d; got error %v and node %+v",
			expectedErr, node1.NodeID, err, val)
	}
}

func TestGossipGetNextBootstrapAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	resolverSpecs := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"localhost:9004",
	}

	resolvers := []resolver.Resolver{}
	for _, rs := range resolverSpecs {
		resolver, err := resolver.NewResolver(rs)
		if err == nil {
			resolvers = append(resolvers, resolver)
		}
	}
	if len(resolvers) != 3 {
		t.Errorf("expected 3 resolvers; got %d", len(resolvers))
	}
	server := rpc.NewServer(newInsecureRPCContext(stopper))
	g := NewTest(0, nil, server, stop.NewStopper(), metric.NewRegistry())
	g.setResolvers(resolvers)

	// Using specified resolvers, fetch bootstrap addresses 3 times
	// and verify the results match expected addresses.
	expAddresses := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"localhost:9004",
	}
	for i := 0; i < len(expAddresses); i++ {
		g.mu.Lock()
		if addr := g.getNextBootstrapAddressLocked(); addr == nil {
			t.Errorf("%d: unexpected nil addr when expecting %s", i, expAddresses[i])
		} else if addrStr := addr.String(); addrStr != expAddresses[i] {
			t.Errorf("%d: expected addr %s; got %s", i, expAddresses[i], addrStr)
		}
		g.mu.Unlock()
	}
}

func TestGossipRaceLogStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	local := startGossip(1, stopper, t, metric.NewRegistry())

	local.mu.Lock()
	peer := startGossip(2, stopper, t, metric.NewRegistry())
	local.startClientLocked(&peer.mu.is.NodeAddr)
	local.mu.Unlock()

	// Race gossiping against LogStatus.
	gun := make(chan struct{})
	for i := uint8(0); i < 10; i++ {
		go func() {
			<-gun
			local.LogStatus()
			gun <- struct{}{}
		}()
		gun <- struct{}{}
		if err := local.AddInfo(
			strconv.FormatUint(uint64(i), 10),
			[]byte{i},
			time.Hour,
		); err != nil {
			t.Fatal(err)
		}
		<-gun
	}
	close(gun)
}

// TestGossipOutgoingLimitEnforced verifies that a gossip node won't open more
// outgoing connections than it should. If the gossip implementation is racy
// with respect to opening outgoing connections, this may not fail every time
// it's run, but should fail very quickly if run under stress.
func TestGossipOutgoingLimitEnforced(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	// This test has an implicit dependency on the maxPeers logic deciding that
	// maxPeers is 3 for a 5-node cluster, so let's go ahead and make that
	// explicit.
	maxPeers := maxPeers(5)
	if maxPeers > 3 {
		t.Fatalf("maxPeers(5)=%d, which is higher than this test's assumption", maxPeers)
	}

	local := startGossip(1, stopper, t, metric.NewRegistry())
	local.mu.Lock()
	localAddr := local.mu.is.NodeAddr
	local.mu.Unlock()
	var peers []*Gossip
	for i := 0; i < 4; i++ {
		// After creating a new node, join it to the first node to ensure that the
		// network is connected (and thus all nodes know each other's addresses)
		// before we start the actual test.
		newPeer := startGossip(roachpb.NodeID(i+2), stopper, t, metric.NewRegistry())
		newPeer.mu.Lock()
		newPeer.startClientLocked(&localAddr)
		newPeer.mu.Unlock()
		peers = append(peers, newPeer)
	}

	// Wait until the network is at least mostly connected.
	testutils.SucceedsSoon(t, func() error {
		local.mu.Lock()
		defer local.mu.Unlock()
		if local.mu.incoming.len() == maxPeers {
			return nil
		}
		return fmt.Errorf("local.mu.incoming.len() = %d, want %d", local.mu.incoming.len(), maxPeers)
	})

	// Verify that we can't open more than maxPeers connections. We have to muck
	// with the infostore's data so that the other nodes will appear far enough
	// away to be worth opening a connection to.
	local.mu.Lock()
	err := local.mu.is.visitInfos(func(key string, i *Info) error {
		copy := *i
		copy.Hops = maxHops + 1
		copy.Value.Timestamp.WallTime++
		return local.mu.is.addInfo(key, &copy)
	})
	local.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for range peers {
		local.tightenNetwork(context.TODO())
	}

	if outgoing := local.outgoing.gauge.Value(); outgoing > int64(maxPeers) {
		t.Errorf("outgoing nodeSet has %d connections; the max should be %d", outgoing, maxPeers)
	}
	local.clientsMu.Lock()
	if numClients := len(local.clientsMu.clients); numClients > maxPeers {
		t.Errorf("local gossip has %d clients; the max should be %d", numClients, maxPeers)
	}
	local.clientsMu.Unlock()
}

// TestGossipNoForwardSelf verifies that when a Gossip instance is full, it
// redirects clients elsewhere (in particular not to itself).
//
// NB: Stress testing this test really stresses the OS networking stack
// more than anything else. For example, on Linux it may quickly deplete
// the ephemeral port range (due to the TIME_WAIT state).
// On a box which only runs tests, this can be circumvented by running
//
//	sudo bash -c "echo 1 > /proc/sys/net/ipv4/tcp_tw_recycle"
//
// See https://vincent.bernat.im/en/blog/2014-tcp-time-wait-state-linux.html
// for details.
//
// On OSX, things similarly fall apart. See #7524 and #5218 for some discussion
// of this.
func TestGossipNoForwardSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	local := startGossip(1, stopper, t, metric.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start one loopback client plus enough additional clients to fill the
	// incoming clients.
	peers := []*Gossip{local}
	local.server.mu.Lock()
	maxSize := local.server.mu.incoming.maxSize
	local.server.mu.Unlock()
	for i := 0; i < maxSize; i++ {
		peers = append(peers, startGossip(roachpb.NodeID(i+2), stopper, t, metric.NewRegistry()))
	}

	for _, peer := range peers {
		c := newClient(log.AmbientContext{}, local.GetNodeAddr(), makeMetrics())

		testutils.SucceedsSoon(t, func() error {
			conn, err := peer.rpcContext.GRPCDial(c.addr.String(), grpc.WithBlock())
			if err != nil {
				return err
			}

			stream, err := NewGossipClient(conn).Gossip(ctx)
			if err != nil {
				return err
			}

			if err := c.requestGossip(peer, stream); err != nil {
				return err
			}

			// Wait until the server responds, so we know we're connected.
			_, err = stream.Recv()
			return err
		})
	}

	numClients := len(peers) * 2
	disconnectedCh := make(chan *client)

	// Start a few overflow peers and assert that they don't get forwarded to us
	// again.
	for i := 0; i < numClients; i++ {
		local.server.mu.Lock()
		maxSize := local.server.mu.incoming.maxSize
		local.server.mu.Unlock()
		peer := startGossip(roachpb.NodeID(i+maxSize+2), stopper, t, metric.NewRegistry())

		for {
			localAddr := local.GetNodeAddr()
			c := newClient(log.AmbientContext{}, localAddr, makeMetrics())
			peer.mu.Lock()
			c.startLocked(peer, disconnectedCh, peer.rpcContext, stopper, peer.rpcContext.NewBreaker())
			peer.mu.Unlock()

			disconnectedClient := <-disconnectedCh
			if disconnectedClient != c {
				t.Fatalf("expected %p to be disconnected, got %p", c, disconnectedClient)
			} else if c.forwardAddr == nil {
				// Under high load, clients sometimes fail to connect for reasons
				// unrelated to the test, so we need to permit some.
				t.Logf("node #%d: got nil forwarding address", peer.NodeID.Get())
				continue
			} else if *c.forwardAddr == *localAddr {
				t.Errorf("node #%d: got local's forwarding address", peer.NodeID.Get())
			}
			break
		}
	}
}

// TestGossipCullNetwork verifies that a client will be culled from
// the network periodically (at cullInterval duration intervals).
func TestGossipCullNetwork(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	local := startGossip(1, stopper, t, metric.NewRegistry())
	local.SetCullInterval(5 * time.Millisecond)

	local.mu.Lock()
	for i := 0; i < minPeers; i++ {
		peer := startGossip(roachpb.NodeID(i+2), stopper, t, metric.NewRegistry())
		local.startClientLocked(peer.GetNodeAddr())
	}
	local.mu.Unlock()

	const slowGossipDuration = time.Minute

	if err := util.RetryForDuration(slowGossipDuration, func() error {
		if peers := len(local.Outgoing()); peers != minPeers {
			return errors.Errorf("%d of %d peers connected", peers, minPeers)
		}
		return nil
	}); err != nil {
		t.Fatalf("condition failed to evaluate within %s: %s", slowGossipDuration, err)
	}

	local.manage()

	if err := util.RetryForDuration(slowGossipDuration, func() error {
		// Verify that a client is closed within the cull interval.
		if peers := len(local.Outgoing()); peers != minPeers-1 {
			return errors.Errorf("%d of %d peers connected", peers, minPeers-1)
		}
		return nil
	}); err != nil {
		t.Fatalf("condition failed to evaluate within %s: %s", slowGossipDuration, err)
	}
}

func TestGossipOrphanedStallDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	local := startGossip(1, stopper, t, metric.NewRegistry())
	local.SetStallInterval(5 * time.Millisecond)

	// Make sure we have the sentinel to ensure that its absence is not the
	// cause of stall detection.
	if err := local.AddInfo(KeySentinel, nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	peerStopper := stop.NewStopper()
	peer := startGossip(2, peerStopper, t, metric.NewRegistry())

	peerNodeID := peer.NodeID.Get()
	peerAddr := peer.GetNodeAddr()
	peerAddrStr := peerAddr.String()

	local.mu.Lock()
	local.startClientLocked(peerAddr)
	local.mu.Unlock()

	testutils.SucceedsSoon(t, func() error {
		for _, peerID := range local.Outgoing() {
			if peerID == peerNodeID {
				return nil
			}
		}
		return errors.Errorf("node %d not yet connected", peerNodeID)
	})

	testutils.SucceedsSoon(t, func() error {
		for _, resolver := range local.GetResolvers() {
			if resolver.Addr() == peerAddrStr {
				return nil
			}
		}
		return errors.Errorf("node %d descriptor not yet available", peerNodeID)
	})

	local.bootstrap()
	local.manage()

	peerStopper.Stop(context.TODO())

	testutils.SucceedsSoon(t, func() error {
		for _, peerID := range local.Outgoing() {
			if peerID == peerNodeID {
				return errors.Errorf("node %d still connected", peerNodeID)
			}
		}
		return nil
	})

	peerStopper = stop.NewStopper()
	defer peerStopper.Stop(context.TODO())
	startGossipAtAddr(peerNodeID, peerAddr, peerStopper, t, metric.NewRegistry())

	testutils.SucceedsSoon(t, func() error {
		for _, peerID := range local.Outgoing() {
			if peerID == peerNodeID {
				return nil
			}
		}
		return errors.Errorf("node %d not yet connected", peerNodeID)
	})
}

// TestGossipCantJoinTwoClusters verifies that a node can't
// participate in two separate clusters if two nodes from different
// clusters are specified as bootstrap hosts. Previously, this would
// be allowed, because a node verifies the cluster ID only at startup.
// If after joining the first cluster via that cluster's init node,
// the init node shuts down, the joining node will reconnect via its
// second bootstrap host and begin to participate [illegally] in
// another cluster.
func TestGossipJoinTwoClusters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const interval = 10 * time.Millisecond
	var stoppers []*stop.Stopper
	var g []*Gossip
	var clusterIDs []uuid.UUID
	var addrs []net.Addr

	// Create three gossip nodes, init the first two with no bootstrap
	// hosts, but unique cluster IDs. The third host has the first two
	// hosts as bootstrap hosts, but has the same cluster ID as the
	// first of its bootstrap hosts.
	for i := 0; i < 3; i++ {
		stopper := stop.NewStopper()
		stoppers = append(stoppers, stopper)
		defer func() {
			select {
			case <-stopper.ShouldQuiesce():
			default:
				stopper.Stop(context.TODO())
			}
		}()
		rpcCtx := newInsecureRPCContext(stopper)
		server := rpc.NewServer(rpcCtx)
		ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, ln.Addr())

		var resolvers []resolver.Resolver
		// Only third node has resolvers.
		switch i {
		case 0, 1:
			clusterIDs = append(clusterIDs, uuid.MakeV4())
		case 2:
			clusterIDs = append(clusterIDs, clusterIDs[0])
			for j := 0; j < 2; j++ {
				resolver, err := resolver.NewResolver(addrs[j].String())
				if err != nil {
					t.Fatal(err)
				}
				resolvers = append(resolvers, resolver)
			}
		}

		// node ID must be non-zero
		gnode := NewTest(
			roachpb.NodeID(i+1), rpcCtx, server, stopper, metric.NewRegistry(),
		)
		g = append(g, gnode)
		gnode.SetStallInterval(interval)
		gnode.SetBootstrapInterval(interval)
		gnode.SetClusterID(clusterIDs[i])
		gnode.Start(ln.Addr(), resolvers)
	}

	// Wait for connections.
	testutils.SucceedsSoon(t, func() error {
		// The first gossip node should have one gossip client address
		// in nodeMap if the 2nd gossip node connected. The second gossip
		// node should have none.
		g[0].mu.Lock()
		defer g[0].mu.Unlock()
		if a, e := len(g[0].mu.nodeMap), 1; a != e {
			return errors.Errorf("expected %s to contain %d nodes, got %d", g[0].mu.nodeMap, e, a)
		}
		g[1].mu.Lock()
		defer g[1].mu.Unlock()
		if a, e := len(g[1].mu.nodeMap), 0; a != e {
			return errors.Errorf("expected %s to contain %d nodes, got %d", g[1].mu.nodeMap, e, a)
		}
		return nil
	})

	// Kill node 0 to force node 2 to bootstrap with node 1.
	stoppers[0].Stop(context.TODO())
	// Wait for twice the bootstrap interval, and verify that
	// node 2 still has not connected to node 1.
	time.Sleep(2 * interval)

	g[1].mu.Lock()
	if a, e := len(g[1].mu.nodeMap), 0; a != e {
		t.Errorf("expected %s to contain %d nodes, got %d", g[1].mu.nodeMap, e, a)
	}
	g[1].mu.Unlock()
}
