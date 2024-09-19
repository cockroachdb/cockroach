// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gossip

import (
	"context"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// startGossip creates and starts a gossip instance.
func startGossip(
	clusterID uuid.UUID,
	nodeID roachpb.NodeID,
	stopper *stop.Stopper,
	t *testing.T,
	registry *metric.Registry,
) (*Gossip, *rpc.Context) {
	return startGossipAtAddr(clusterID, nodeID, util.IsolatedTestAddr, stopper, t, registry)
}

func startGossipAtAddr(
	clusterID uuid.UUID,
	nodeID roachpb.NodeID,
	addr net.Addr,
	stopper *stop.Stopper,
	t *testing.T,
	registry *metric.Registry,
) (*Gossip, *rpc.Context) {
	ctx := context.Background()
	clock := hlc.NewClockForTesting(nil)
	rpcContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)
	rpcContext.NodeID.Set(ctx, nodeID)

	server, err := rpc.NewServer(ctx, rpcContext)
	require.NoError(t, err)
	g := NewTest(nodeID, stopper, registry)
	RegisterGossipServer(server, g)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, addr)
	require.NoError(t, err)
	addr = ln.Addr()
	require.NoError(t, g.SetNodeDescriptor(&roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
	}))
	g.start(addr)
	time.Sleep(time.Millisecond)
	return g, rpcContext
}

type fakeGossipServer struct {
	nodeAddr     util.UnresolvedAddr
	receivedArgs chan Request
}

func newFakeGossipServer(grpcServer *grpc.Server, stopper *stop.Stopper) *fakeGossipServer {
	s := &fakeGossipServer{
		receivedArgs: make(chan Request, 1),
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
		case s.receivedArgs <- *args:
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
// faked gossip service just for check the client message. Also, it returns the
// rpc context for both local and remote servers.
func startFakeServerGossips(
	t *testing.T, clusterID uuid.UUID, localNodeID roachpb.NodeID, stopper *stop.Stopper,
) (*Gossip, *fakeGossipServer, *rpc.Context, *rpc.Context) {
	ctx := context.Background()
	clock := hlc.NewClockForTesting(nil)
	lRPCContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)

	lserver, err := rpc.NewServer(ctx, lRPCContext)
	require.NoError(t, err)
	local := NewTest(localNodeID, stopper, metric.NewRegistry())
	RegisterGossipServer(lserver, local)
	lln, err := netutil.ListenAndServeGRPC(stopper, lserver, util.IsolatedTestAddr)
	require.NoError(t, err)
	local.start(lln.Addr())

	rRPCContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)
	rserver, err := rpc.NewServer(ctx, rRPCContext)
	require.NoError(t, err)
	remote := newFakeGossipServer(rserver, stopper)
	rln, err := netutil.ListenAndServeGRPC(stopper, rserver, util.IsolatedTestAddr)
	require.NoError(t, err)
	addr := rln.Addr()
	remote.nodeAddr = util.MakeUnresolvedAddr(addr.Network(), addr.String())

	return local, remote, lRPCContext, rRPCContext
}

func gossipSucceedsSoon(
	t *testing.T,
	stopper *stop.Stopper,
	clusterID uuid.UUID,
	disconnected chan *client,
	gossip map[*client]*Gossip,
	f func() error,
) {
	ctx := context.Background()
	clock := hlc.NewClockForTesting(nil)
	// Use an insecure context since we don't need a valid cert.
	rpcContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)

	for c := range gossip {
		disconnected <- c
	}

	testutils.SucceedsSoon(t, func() error {
		select {
		case client := <-disconnected:
			// If the client wasn't able to connect, restart it.
			g := gossip[client]
			g.mu.Lock()
			client.startLocked(g, disconnected, rpcContext, stopper)
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

	ctx := context.Background()

	local, _ := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote, _ := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())
	disconnected := make(chan *client, 1)
	c := newClient(log.MakeTestingAmbientCtxWithNewTracer(), remote.GetNodeAddr(), roachpb.Locality{}, makeMetrics())

	defer func() {
		stopper.Stop(ctx)
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

	local, _ := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote, _ := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())

	if err := local.AddInfo("local-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := remote.AddInfo("remote-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	gossipSucceedsSoon(
		t, stopper, clusterID, make(chan *client, 2),
		map[*client]*Gossip{
			newClient(log.MakeTestingAmbientCtxWithNewTracer(), local.GetNodeAddr(), roachpb.Locality{}, remote.nodeMetrics): remote,
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

	ctx := context.Background()
	stopper := stop.NewStopper()
	disconnected := make(chan *client, 1)

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	localNodeID := roachpb.NodeID(1)
	local, remote, _, _ := startFakeServerGossips(t, clusterID, localNodeID, stopper)

	clock := hlc.NewClockForTesting(nil)
	// Use an insecure context. We're talking to tcp socket which are not in the certs.
	rpcContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)

	c := newClient(log.MakeTestingAmbientCtxWithNewTracer(), &remote.nodeAddr, roachpb.Locality{}, makeMetrics())
	disconnected <- c

	defer func() {
		stopper.Stop(ctx)
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
		case args := <-remote.receivedArgs:
			if args.NodeID != localNodeID {
				t.Fatalf("client should send NodeID with %v, got %v", localNodeID, args.NodeID)
			}
			return
		case <-disconnected:
			// The client hasn't been started or failed to start, loop and try again.
			local.mu.Lock()
			c.startLocked(local, disconnected, rpcContext, stopper)
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
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	local, localCtx := startGossip(uuid.Nil, 1, stopper, t, metric.NewRegistry())
	local.mu.Lock()
	lAddr := local.mu.is.NodeAddr
	local.startClientLocked(lAddr, roachpb.Locality{}, localCtx)
	local.mu.Unlock()
	local.manage(localCtx)
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

	local, localCtx := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote, remoteCtx := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())
	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	lAddr := local.mu.is.NodeAddr
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage(localCtx)
	remote.manage(remoteCtx)

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
			local.startClientLocked(rAddr, roachpb.Locality{}, localCtx)
			local.mu.Unlock()
			return fmt.Errorf("unable to find local to remote client")
		}
		_, err := remote.GetInfo("local-key")
		return err
	})

	// Start a remote to local client. This client will get removed as being
	// redundant as there is already a connection between the two nodes.
	remote.mu.Lock()
	remote.startClientLocked(lAddr, roachpb.Locality{}, remoteCtx)
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

	local, localCtx := startGossip(clusterID, 1, stopper, t, metric.NewRegistry())
	remote, remoteCtx := startGossip(clusterID, 2, stopper, t, metric.NewRegistry())

	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	// Start two clients from local to remote. RPC client cache is
	// disabled via the context, so we'll start two different outgoing
	// connections.
	local.startClientLocked(rAddr, roachpb.Locality{}, localCtx)
	local.startClientLocked(rAddr, roachpb.Locality{}, localCtx)
	local.mu.Unlock()
	remote.mu.Unlock()
	local.manage(localCtx)
	remote.manage(remoteCtx)
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
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClockForTesting(nil)

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	// Create three gossip nodes, and connect to the first with NodeID 0.
	var g []*Gossip
	var gossipAddr string
	for i := 0; i < 3; i++ {
		nodeID := roachpb.NodeID(i + 1)

		rpcContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)
		server, err := rpc.NewServer(ctx, rpcContext)
		require.NoError(t, err)
		// node ID must be non-zero
		gnode := NewTest(nodeID, stopper, metric.NewRegistry())
		RegisterGossipServer(server, gnode)
		g = append(g, gnode)

		ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
		require.NoError(t, err)

		// Connect to the first gossip node.
		if gossipAddr == "" {
			gossipAddr = ln.Addr().String()
		}

		addresses := []util.UnresolvedAddr{util.MakeUnresolvedAddr("tcp", gossipAddr)}
		gnode.Start(ln.Addr(), addresses, rpcContext)
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

// TestClientForwardUnresolved verifies that a client does not resolve a forward
// address prematurely.
func TestClientForwardUnresolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	const nodeID = 1
	local, _ := startGossip(uuid.Nil, nodeID, stopper, t, metric.NewRegistry())
	addr := local.GetNodeAddr()

	client := newClient(log.MakeTestingAmbientCtxWithNewTracer(), addr, roachpb.Locality{}, makeMetrics()) // never started

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

// TestClientHighStampsDiff verifies that a client sends a diff of the high
// water stamps rather than sending the whole map.
func TestClientSendsHighStampsDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	disconnected := make(chan *client, 1)

	// Shared cluster ID by all gossipers (this ensures that the gossipers
	// don't talk to servers from unrelated tests by accident).
	clusterID := uuid.MakeV4()

	localNodeID := roachpb.NodeID(1)
	local, remote, _, rCtx := startFakeServerGossips(t, clusterID, localNodeID, stopper)

	clock := hlc.NewClockForTesting(nil)
	// Use an insecure context. We're talking to tcp socket which are not in the
	// certs.
	rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)

	// Create a client and let it connect to the remote address.
	c := newClient(log.MakeTestingAmbientCtxWithNewTracer(), &remote.nodeAddr, roachpb.Locality{}, makeMetrics())
	disconnected <- c

	ctxNew, cancel := context.WithCancel(c.AnnotateCtx(context.Background()))
	defer func() {
		cancel()
	}()

	conn, err := rCtx.GRPCUnvalidatedDial(c.addr.String(), roachpb.Locality{}).Connect(ctxNew)
	require.NoError(t, err)

	stream, err := NewGossipClient(conn).Gossip(ctx)
	require.NoError(t, err)

	// Add an info to generate some deltas and allow the request to be sent.
	err = local.AddInfo("local-key", nil, time.Hour)
	require.NoError(t, err)

	// The first thing the client does is to request the gossips from the server.
	// It attaches ALL the high water timestamps that it has.
	err = c.requestGossip(local, stream)
	require.NoError(t, err)

	args := <-remote.receivedArgs
	local.mu.Lock()
	currentHighStamps := local.mu.is.getHighWaterStamps()
	local.mu.Unlock()
	require.Equal(t, args.HighWaterStamps, currentHighStamps)

	// Expect that the requests will only contain high water stamps if they are
	// different from what was previously sent. Since we didn't change any info,
	// the client should send an empty map of high water stamps.
	err = c.sendGossip(local, stream, true /* firstReq */)
	require.NoError(t, err)

	args = <-remote.receivedArgs
	require.Empty(t, args.HighWaterStamps)

	// Adding an info causes an update in the high water stamps.
	err = local.AddInfo("local-key", nil, time.Hour)
	require.NoError(t, err)

	err = c.sendGossip(local, stream, false /* firstReq */)
	require.NoError(t, err)

	// Now that the timestamp is newer than what was previously sent, expect that
	// the high water stamps will contain the new timestamp.
	args = <-remote.receivedArgs
	local.mu.Lock()
	currentHighStamps = local.mu.is.getHighWaterStamps()
	local.mu.Unlock()
	require.Equal(t, args.HighWaterStamps, currentHighStamps)

	defer func() {
		stopper.Stop(ctx)
		if c != <-disconnected {
			t.Errorf("expected client disconnect after remote close")
		}
	}()
}
