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
	"math"
	"net"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/netutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

// startGossip creates and starts a gossip instance.
func startGossip(nodeID roachpb.NodeID, stopper *stop.Stopper, t *testing.T, registry *metric.Registry) *Gossip {
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)

	server := rpc.NewServer(rpcContext)
	g := New(rpcContext, server, nil, stopper, registry)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr()
	g.SetNodeID(nodeID)
	if err := g.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
	}); err != nil {
		t.Fatal(err)
	}
	g.start(addr)
	time.Sleep(time.Millisecond)
	return g
}

type fakeGossipServer struct {
	nodeAddr   util.UnresolvedAddr
	nodeIDChan chan roachpb.NodeID
}

func newFakeGossipServer(grpcServer *grpc.Server, stopper *stop.Stopper) *fakeGossipServer {
	s := &fakeGossipServer{
		nodeIDChan: make(chan roachpb.NodeID, 1),
	}
	RegisterGossipServer(grpcServer, s)
	return s
}

func (s *fakeGossipServer) Gossip(stream Gossip_GossipServer) error {
	for {
		args, err := stream.Recv()
		if err != nil {
			return err
		}

		select {
		case s.nodeIDChan <- args.NodeID:
		default:
		}

		if err := stream.Send(&Response{
			// Just don't conflict with other nodes.
			NodeID: math.MaxInt32,
		}); err != nil {
			return err
		}
	}
}

// startFakeServerGossips creates local gossip instances and remote
// faked gossip instance. The remote gossip instance launches its
// faked gossip service just for check the client message.
func startFakeServerGossips(t *testing.T) (*Gossip, *fakeGossipServer, *stop.Stopper) {
	stopper := stop.NewStopper()
	lRPCContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)

	lserver := rpc.NewServer(lRPCContext)
	local := New(lRPCContext, lserver, nil, stopper, metric.NewRegistry())
	lln, err := netutil.ListenAndServeGRPC(stopper, lserver, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	local.start(lln.Addr())

	rRPCContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)

	rserver := rpc.NewServer(rRPCContext)
	rln, err := netutil.ListenAndServeGRPC(stopper, rserver, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	remote := newFakeGossipServer(rserver, stopper)
	addr := rln.Addr()
	remote.nodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())
	return local, remote, stopper
}

func gossipSucceedsSoon(t *testing.T, stopper *stop.Stopper, disconnected chan *client, gossip map[*client]*Gossip, f func() error) {
	// Use an insecure context since we don't need a valid cert.
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)

	for c := range gossip {
		disconnected <- c
	}

	util.SucceedsSoon(t, func() error {
		select {
		case client := <-disconnected:
			// If the client wasn't able to connect, restart it.
			client.start(gossip[client], disconnected, rpcContext, stopper, gossip[client].GetNodeID())
		default:
		}

		return f()
	})
}

// TestClientGossip verifies a client can gossip a delta to the server.
func TestClientGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	local := startGossip(1, stopper, t, metric.NewRegistry())
	remote := startGossip(2, stopper, t, metric.NewRegistry())
	disconnected := make(chan *client, 1)
	c := newClient(remote.GetNodeAddr(), makeMetrics())

	defer func() {
		stopper.Stop()
		if c != <-disconnected {
			t.Errorf("expected client disconnect after remote close")
		}
	}()

	if err := local.AddInfo("local-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := remote.AddInfo("remote-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	gossipSucceedsSoon(t, stopper, disconnected, map[*client]*Gossip{
		c: local,
	}, func() error {
		if _, err := remote.GetInfo("local-key"); err != nil {
			return err
		}
		if _, err := local.GetInfo("remote-key"); err != nil {
			return err
		}
		return nil
	})
}

// TestClientGossipMetrics verifies a that gossip stats are generated.
func TestClientGossipMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t, metric.NewRegistry())
	remote := startGossip(2, stopper, t, metric.NewRegistry())

	gossipSucceedsSoon(
		t, stopper, make(chan *client, 2),
		map[*client]*Gossip{
			newClient(local.GetNodeAddr(), makeMetrics()):  remote,
			newClient(remote.GetNodeAddr(), makeMetrics()): local,
		},
		func() error {
			if err := local.AddInfo("local-key", nil, time.Hour); err != nil {
				t.Fatal(err)
			}
			if err := remote.AddInfo("remote-key", nil, time.Hour); err != nil {
				t.Fatal(err)
			}

			// Infos/Bytes Sent/Received should not be zero.
			for i, s := range []*server{local.server, remote.server} {
				for _, rate := range []metric.Rates{
					s.nodeMetrics.InfosSent,
					s.nodeMetrics.InfosReceived,
					s.nodeMetrics.BytesSent,
					s.nodeMetrics.BytesReceived,
				} {
					counter := rate.Counter
					if count := counter.Count(); count <= 0 {
						return errors.Errorf("%d: expected metrics counter %q > 0; = %d", i, counter.GetName(), count)
					}
				}
			}

			// Since there are two gossip nodes, there should be at least one incoming
			// and outgoing connection.
			for i, s := range []*server{local.server, remote.server} {
				s.mu.Lock()
				gauge := s.mu.incoming.gauge
				s.mu.Unlock()
				if gauge == nil {
					return errors.Errorf("%d: missing gauge \"incoming\"", i)
				}
				if count := gauge.Value(); count <= 0 {
					return errors.Errorf("%d: expected metrics gauge %q > 0; = %d", i, gauge.GetName(), count)
				}
			}
			return nil
		})
}

// TestClientNodeID verifies a client's gossip request with correct NodeID.
func TestClientNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	local, remote, stopper := startFakeServerGossips(t)
	nodeID := roachpb.NodeID(1)
	local.SetNodeID(nodeID)

	// Use an insecure context. We're talking to tcp socket which are not in the certs.
	rpcContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)
	c := newClient(&remote.nodeAddr, makeMetrics())
	disconnected := make(chan *client, 1)
	disconnected <- c

	defer func() {
		stopper.Stop()
		if c != <-disconnected {
			t.Errorf("expected client disconnect after remote close")
		}
	}()

	// A gossip client may fail to start if the grpc connection times out which
	// can happen under load (such as in CircleCI or using `make stress`). So we
	// loop creating clients until success or the test times out.
	for {
		// Wait for c.gossip to start.
		select {
		case receivedNodeID := <-remote.nodeIDChan:
			if receivedNodeID != nodeID {
				t.Fatalf("client should send NodeID with %v, got %v", nodeID, receivedNodeID)
			}
			return
		case <-disconnected:
			// The client hasn't been started or failed to start, loop and try again.
			c.start(local, disconnected, rpcContext, stopper, local.GetNodeID())
		}
	}
}

func verifyServerMaps(g *Gossip, expCount int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.mu.nodeMap) == expCount
}

// TestClientDisconnectLoopback verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectLoopback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t, metric.NewRegistry())
	// startClient requires locks are held, so acquire here.
	local.mu.Lock()
	lAddr := local.mu.is.NodeAddr
	local.startClient(&lAddr, local.mu.is.NodeID)
	local.mu.Unlock()
	local.manage()
	util.SucceedsSoon(t, func() error {
		ok := local.findClient(func(c *client) bool { return c.addr.String() == lAddr.String() }) != nil
		if !ok && verifyServerMaps(local, 0) {
			return nil
		}
		return errors.New("local client still connected to itself")
	})
}

// TestClientDisconnectRedundant verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectRedundant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t, metric.NewRegistry())
	remote := startGossip(2, stopper, t, metric.NewRegistry())
	// startClient requires locks are held, so acquire here.
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	lAddr := local.mu.is.NodeAddr
	local.startClient(&rAddr, remote.mu.is.NodeID)
	remote.startClient(&lAddr, local.mu.is.NodeID)
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage()
	remote.manage()
	util.SucceedsSoon(t, func() error {
		// Check which of the clients is connected to the other.
		ok1 := local.findClient(func(c *client) bool { return c.addr.String() == rAddr.String() }) != nil
		ok2 := remote.findClient(func(c *client) bool { return c.addr.String() == lAddr.String() }) != nil
		// We expect node 1 to disconnect; if both are still connected,
		// it's possible that node 1 gossiped before node 2 connected, in
		// which case we have to gossip from node 1 to trigger the
		// disconnect redundant client code.
		if ok1 && ok2 {
			if err := local.AddInfo("local-key", nil, time.Second); err != nil {
				t.Fatal(err)
			}
		} else if !ok1 && ok2 && verifyServerMaps(local, 1) && verifyServerMaps(remote, 0) {
			return nil
		}
		return errors.New("local client to remote not yet closed as redundant")
	})
}

// TestClientDisallowMultipleConns verifies that the server disallows
// multiple connections from the same client node ID.
func TestClientDisallowMultipleConns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t, metric.NewRegistry())
	remote := startGossip(2, stopper, t, metric.NewRegistry())
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	// Start two clients from local to remote. RPC client cache is
	// disabled via the context, so we'll start two different outgoing
	// connections.
	local.startClient(&rAddr, remote.mu.is.NodeID)
	local.startClient(&rAddr, remote.mu.is.NodeID)
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage()
	remote.manage()
	util.SucceedsSoon(t, func() error {
		// Verify that the remote server has only a single incoming
		// connection and the local server has only a single outgoing
		// connection.
		local.mu.Lock()
		remote.mu.Lock()
		outgoing := local.outgoing.len()
		incoming := remote.mu.incoming.len()
		local.mu.Unlock()
		remote.mu.Unlock()
		if outgoing == 1 && incoming == 1 && verifyServerMaps(local, 0) && verifyServerMaps(remote, 1) {
			return nil
		}
		return errors.Errorf("incorrect number of incoming (%d) or outgoing (%d) connections", incoming, outgoing)
	})
}

// TestClientRegisterInitNodeID verifies two client's gossip request with NodeID 0.
func TestClientRegisterWithInitNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Create three gossip nodes, and connect to the first with NodeID 0.
	var g []*Gossip
	var gossipAddr string
	for i := 0; i < 3; i++ {
		RPCContext := rpc.NewContext(&base.Context{Insecure: true}, nil, stopper)

		server := rpc.NewServer(RPCContext)
		ln, err := netutil.ListenAndServeGRPC(stopper, server, util.TestAddr)
		if err != nil {
			t.Fatal(err)
		}

		// Connect to the first gossip node.
		if gossipAddr == "" {
			gossipAddr = ln.Addr().String()
		}

		var resolvers []resolver.Resolver
		resolver, err := resolver.NewResolver(RPCContext.Context, gossipAddr)
		if err != nil {
			t.Fatal(err)
		}
		resolvers = append(resolvers, resolver)
		gnode := New(RPCContext, server, resolvers, stopper, metric.NewRegistry())
		// node ID must be non-zero
		gnode.SetNodeID(roachpb.NodeID(i + 1))
		g = append(g, gnode)
		gnode.Start(ln.Addr())
	}

	util.SucceedsSoon(t, func() error {
		// The first gossip node should have two gossip client address
		// in nodeMap if these three gossip nodes registered success.
		g[0].mu.Lock()
		defer g[0].mu.Unlock()
		if a, e := len(g[0].mu.nodeMap), 2; a != e {
			return errors.Errorf("expected %s to contain %d nodes, got %d", g[0].mu.nodeMap, e, a)
		}
		return nil
	})
}

type testResolver struct {
	addr         string
	numTries     int
	numFails     int
	numSuccesses int
}

func (tr *testResolver) Type() string { return "tcp" }

func (tr *testResolver) Addr() string { return tr.addr }

func (tr *testResolver) GetAddress() (net.Addr, error) {
	defer func() { tr.numTries++ }()
	if tr.numTries < tr.numFails {
		return nil, errors.New("bad address")
	}
	return util.NewUnresolvedAddr("tcp", tr.addr), nil
}

// TestClientRetryBootstrap verifies that an initial failure to connect
// to a bootstrap host doesn't stall the bootstrapping process in the
// absence of any additional activity. This can happen during acceptance
// tests if the DNS can't lookup hostnames when gossip is started.
func TestClientRetryBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t, metric.NewRegistry())
	remote := startGossip(2, stopper, t, metric.NewRegistry())

	if err := local.AddInfo("local-key", []byte("hello"), 0*time.Second); err != nil {
		t.Fatal(err)
	}

	local.SetBootstrapInterval(10 * time.Millisecond)
	local.SetResolvers([]resolver.Resolver{
		&testResolver{addr: remote.GetNodeAddr().String(), numFails: 3, numSuccesses: 1},
	})
	local.bootstrap()
	local.manage()

	util.SucceedsSoon(t, func() error {
		_, err := remote.GetInfo("local-key")
		return err
	})
}

// TestClientForwardUnresolved verifies that a client does not resolve a forward
// address prematurely.
func TestClientForwardUnresolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	const nodeID = 1
	local := startGossip(nodeID, stopper, t, metric.NewRegistry())
	addr := local.GetNodeAddr()

	client := newClient(addr, makeMetrics()) // never started

	newAddr := util.UnresolvedAddr{
		NetworkField: "tcp",
		AddressField: "localhost:2345",
	}
	reply := &Response{
		NodeID:          nodeID,
		Addr:            *addr,
		AlternateNodeID: nodeID + 1,
		AlternateAddr:   &newAddr,
	}
	if err := client.handleResponse(local, reply); !testutils.IsError(err, "received forward") {
		t.Fatal(err)
	}
	if !proto.Equal(client.forwardAddr, &newAddr) {
		t.Fatalf("unexpected forward address %v, expected %v", client.forwardAddr, &newAddr)
	}
}
