// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"context"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// startGossip creates and starts a gossip instance.
func startGossip(
	clusterID uuid.UUID,
	nodeID roachpb.NodeID,
	stopper *stop.Stopper,
	t *testing.T,
	registry *metric.Registry,
) *Gossip {
	return startGossipAtAddr(clusterID, nodeID, util.IsolatedTestAddr, stopper, t, registry)
}

func startGossipAtAddr(
	clusterID uuid.UUID,
	nodeID roachpb.NodeID,
	addr net.Addr,
	stopper *stop.Stopper,
	t *testing.T,
	registry *metric.Registry,
) *Gossip {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	rpcContext.NodeID.Set(context.Background(), nodeID)

	server := rpc.NewServer(rpcContext)
	g := NewTest(nodeID, rpcContext, server, stopper, registry, zonepb.DefaultZoneConfigRef())
	ln, err := netutil.ListenAndServeGRPC(stopper, server, addr)
	if err != nil {
		t.Fatal(err)
	}
	addr = ln.Addr()
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
func startFakeServerGossips(
	t *testing.T, clusterID uuid.UUID, localNodeID roachpb.NodeID, stopper *stop.Stopper,
) (*Gossip, *fakeGossipServer) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	lRPCContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)

	lserver := rpc.NewServer(lRPCContext)
	local := NewTest(localNodeID, lRPCContext, lserver, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef())
	lln, err := netutil.ListenAndServeGRPC(stopper, lserver, util.IsolatedTestAddr)
	if err != nil {
		t.Fatal(err)
	}
	local.start(lln.Addr())

	rRPCContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	rserver := rpc.NewServer(rRPCContext)
	remote := newFakeGossipServer(rserver, stopper)
	rln, err := netutil.ListenAndServeGRPC(stopper, rserver, util.IsolatedTestAddr)
	if err != nil {
		t.Fatal(err)
	}
	addr := rln.Addr()
	remote.nodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())

	return local, remote
}

func gossipSucceedsSoon(
	t *testing.T,
	stopper *stop.Stopper,
	clusterID uuid.UUID,
	disconnected chan *client,
	gossip map[*client]*Gossip,
	f func() error,
) {
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	// Use an insecure context since we don't need a valid cert.
	rpcContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)

	for c := range gossip {
		disconnected <- c
	}

	testutils.SucceedsSoon(t, func() error {
		select {
		case client := <-disconnected:
			// If the client wasn't able to connect, restart it.
			g := gossip[client]
			g.mu.Lock()
			client.startLocked(g, disconnected, rpcContext, stopper, rpcContext.NewBreaker(""))
			g.mu.Unlock()
		default:
		}

		return f()
	})
}

// TestClientGossip verifies a client can gossip a delta to the server.
func TestClientGossip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	local := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())
	disconnected := make(chan *client, 1)
	c := newClient(log.AmbientContext{Tracer: tracing.NewTracer()}, remote.GetNodeAddr(), makeMetrics())

	defer func() {
		stopper.Stop(context.Background())
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

	gossipSucceedsSoon(t, stopper, clusterID, disconnected, map[*client]*Gossip{
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

// TestClientGossipMetrics verifies that gossip stats are generated.
func TestClientGossipMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	local := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())

	if err := local.AddInfo("local-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := remote.AddInfo("remote-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	gossipSucceedsSoon(
		t, stopper, clusterID, make(chan *client, 2),
		map[*client]*Gossip{
			newClient(log.AmbientContext{Tracer: tracing.NewTracer()}, local.GetNodeAddr(), remote.nodeMetrics): remote,
		},
		func() error {
			// Infos/Bytes Sent/Received should not be zero.
			for i, s := range []*server{local.server, remote.server} {
				for _, counter := range []*metric.Counter{
					s.nodeMetrics.InfosSent,
					s.nodeMetrics.InfosReceived,
					s.nodeMetrics.BytesSent,
					s.nodeMetrics.BytesReceived,
				} {
					if count := counter.Count(); count <= 0 {
						return errors.Errorf("%d: expected metrics counter %q > 0; = %d", i, counter.GetName(), count)
					}
				}
			}

			// Since there are two gossip nodes, there should be exactly one incoming
			// or outgoing connection due to gossip's connection de-duplication.
			for i, g := range []*Gossip{local, remote} {
				g.mu.Lock()
				defer g.mu.Unlock()

				count := int64(0)
				for _, gauge := range []*metric.Gauge{g.mu.incoming.gauge, g.outgoing.gauge} {
					if gauge == nil {
						return errors.Errorf("%d: missing gauge", i)
					}
					count += gauge.Value()
				}
				const expected = 1
				if count != expected {
					return errors.Errorf("%d: expected metrics incoming + outgoing connection count == %d; = %d", i, expected, count)
				}
			}
			return nil
		})
}

// TestClientNodeID verifies a client's gossip request with correct NodeID.
func TestClientNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	disconnected := make(chan *client, 1)

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	localNodeID := roachpb.NodeID(1)
	local, remote := startFakeServerGossips(t, clusterID, localNodeID, stopper)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	// Use an insecure context. We're talking to tcp socket which are not in the certs.
	rpcContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)

	c := newClient(log.AmbientContext{Tracer: tracing.NewTracer()}, &remote.nodeAddr, makeMetrics())
	disconnected <- c

	defer func() {
		stopper.Stop(context.Background())
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
			if receivedNodeID != localNodeID {
				t.Fatalf("client should send NodeID with %v, got %v", localNodeID, receivedNodeID)
			}
			return
		case <-disconnected:
			// The client hasn't been started or failed to start, loop and try again.
			local.mu.Lock()
			c.startLocked(local, disconnected, rpcContext, stopper, rpcContext.NewBreaker(""))
			local.mu.Unlock()
		}
	}
}

func verifyServerMaps(g *Gossip, expCount int) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.mu.nodeMap) == expCount
}

// TestClientDisconnectLoopback verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectLoopback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	local := startGossip(uuid.Nil, 1, stopper, t, metric.NewRegistry())
	local.mu.Lock()
	lAddr := local.mu.is.NodeAddr
	local.startClientLocked(&lAddr)
	local.mu.Unlock()
	local.manage()
	testutils.SucceedsSoon(t, func() error {
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
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	local := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	lAddr := local.mu.is.NodeAddr
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage()
	remote.manage()

	// Gossip a key on local and wait for it to show up on remote. This
	// guarantees we have an active local to remote client connection.
	if err := local.AddInfo("local-key", nil, 0); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		c := local.findClient(func(c *client) bool { return c.addr.String() == rAddr.String() })
		if c == nil {
			// Restart the client connection in the loop. It might have failed due to
			// a heartbeat time.
			local.mu.Lock()
			local.startClientLocked(&rAddr)
			local.mu.Unlock()
			return fmt.Errorf("unable to find local to remote client")
		}
		_, err := remote.GetInfo("local-key")
		return err
	})

	// Start a remote to local client. This client will get removed as being
	// redundant as there is already a connection between the two nodes.
	remote.mu.Lock()
	remote.startClientLocked(&lAddr)
	remote.mu.Unlock()

	testutils.SucceedsSoon(t, func() error {
		// Check which of the clients is connected to the other.
		ok1 := local.findClient(func(c *client) bool { return c.addr.String() == rAddr.String() }) != nil
		ok2 := remote.findClient(func(c *client) bool { return c.addr.String() == lAddr.String() }) != nil
		if ok1 && !ok2 && verifyServerMaps(local, 0) && verifyServerMaps(remote, 1) {
			return nil
		}
		return fmt.Errorf("remote to local client not yet closed as redundant: local=%t remote=%t",
			ok1, ok2)
	})
}

// TestClientDisallowMultipleConns verifies that the server disallows
// multiple connections from the same client node ID.
func TestClientDisallowMultipleConns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	local := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())

	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	// Start two clients from local to remote. RPC client cache is
	// disabled via the context, so we'll start two different outgoing
	// connections.
	local.startClientLocked(&rAddr)
	local.startClientLocked(&rAddr)
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage()
	remote.manage()
	testutils.SucceedsSoon(t, func() error {
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
	defer stopper.Stop(context.Background())
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	// Create three gossip nodes, and connect to the first with NodeID 0.
	var g []*Gossip
	var gossipAddr string
	for i := 0; i < 3; i++ {
		nodeID := roachpb.NodeID(i + 1)

		rpcContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
		server := rpc.NewServer(rpcContext)
		// node ID must be non-zero
		gnode := NewTest(
			nodeID, rpcContext, server, stopper, metric.NewRegistry(), zonepb.DefaultZoneConfigRef(),
		)
		g = append(g, gnode)

		ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
		if err != nil {
			t.Fatal(err)
		}

		// Connect to the first gossip node.
		if gossipAddr == "" {
			gossipAddr = ln.Addr().String()
		}

		var resolvers []resolver.Resolver
		resolver, err := resolver.NewResolver(gossipAddr)
		if err != nil {
			t.Fatal(err)
		}
		resolvers = append(resolvers, resolver)
		gnode.Start(ln.Addr(), resolvers)
	}

	testutils.SucceedsSoon(t, func() error {
		// The first gossip node should have two gossip client address
		// in nodeMap if these three gossip nodes registered success.
		g[0].mu.Lock()
		defer g[0].mu.Unlock()
		if a, e := len(g[0].mu.nodeMap), 2; a != e {
			return errors.Errorf("expected %v to contain %d nodes, got %d", g[0].mu.nodeMap, e, a)
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
	defer stopper.Stop(context.Background())

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()
	local := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())

	if err := local.AddInfo("local-key", []byte("hello"), 0*time.Second); err != nil {
		t.Fatal(err)
	}

	local.SetBootstrapInterval(10 * time.Millisecond)
	resolvers := []resolver.Resolver{
		&testResolver{addr: remote.GetNodeAddr().String(), numFails: 3, numSuccesses: 1},
	}
	local.setResolvers(resolvers)
	local.bootstrap()
	local.manage()

	testutils.SucceedsSoon(t, func() error {
		_, err := remote.GetInfo("local-key")
		return err
	})
}

// TestClientForwardUnresolved verifies that a client does not resolve a forward
// address prematurely.
func TestClientForwardUnresolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	const nodeID = 1
	local := startGossip(uuid.Nil, nodeID, stopper, t, metric.NewRegistry())
	addr := local.GetNodeAddr()

	client := newClient(log.AmbientContext{Tracer: tracing.NewTracer()}, addr, makeMetrics()) // never started

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
	local.mu.Lock()
	local.outgoing.addPlaceholder() // so that the resolvePlaceholder in handleResponse doesn't fail
	local.mu.Unlock()
	if err := client.handleResponse(
		context.Background(), local, reply,
	); !testutils.IsError(err, "received forward") {
		t.Fatal(err)
	}
	if !client.forwardAddr.Equal(&newAddr) {
		t.Fatalf("unexpected forward address %v, expected %v", client.forwardAddr, &newAddr)
	}
}
