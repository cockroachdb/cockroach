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
)

func gossipSucceedsSoon(
	t *testing.T,
	stopper *stop.Stopper,
	disconnected chan *client,
	gossip map[*client]*Gossip,
	f func() error,
) {
	for c := range gossip {
		disconnected <- c
	}

	testutils.SucceedsSoon(t, func() error {
		select {
		case client := <-disconnected:
			// If the client wasn't able to connect, restart it.
			g := gossip[client]
			g.mu.Lock()
			client.startLocked(g, disconnected, stopper)
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

	ctx := context.Background()

	tn := NewTestNetwork()
	local := tn.StartGossip(1, stopper, metric.NewRegistry())
	remote := tn.StartGossip(2, stopper, metric.NewRegistry())
	disconnected := make(chan *client, 1)
	c := newClient(log.MakeTestingAmbientCtxWithNewTracer(), remote.GetNodeAddr(), makeMetrics())

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

// TestClientGossipMetrics verifies that gossip stats are generated.
func TestClientGossipMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	tn := NewTestNetwork()
	local := tn.StartGossip(1, stopper, metric.NewRegistry())
	remote := tn.StartGossip(2, stopper, metric.NewRegistry())

	if err := local.AddInfo("local-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := remote.AddInfo("remote-key", nil, time.Hour); err != nil {
		t.Fatal(err)
	}

	gossipSucceedsSoon(
		t, stopper, make(chan *client, 2),
		map[*client]*Gossip{
			newClient(log.MakeTestingAmbientCtxWithNewTracer(), local.GetNodeAddr(), remote.nodeMetrics): remote,
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

	tn := NewTestNetwork()
	local := tn.StartGossip(1, stopper, metric.NewRegistry())
	remote := tn.StartGossip(2, stopper, metric.NewRegistry())

	local.mu.Lock()
	lAddr := local.mu.is.NodeAddr
	local.mu.Unlock()
	remote.startClientLocked(lAddr)

	remote.mu.Lock()
	remoteClient := remote.clientsMu.clients[0]
	remote.mu.Unlock()

	defer func() {
		stopper.Stop(ctx)

		if remoteClient != <-remote.disconnected {
			t.Errorf("expected client disconnect after remote close")
		}
	}()

	testutils.SucceedsSoon(t, func() error {
		incoming := local.Incoming()
		if size := len(incoming); size != 1 {
			return errors.Newf("expected 1 client, received %d", size)
		}
		receivedNodeID := incoming[0]
		if receivedNodeID != 2 {
			return errors.Newf("client should send NodeID with 2, got %v", receivedNodeID)
		}
		return nil
	})
}

func getConnectedCount(g *Gossip) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.mu.nodeMap)
}

// TestClientDisconnectLoopback verifies that the gossip server
// will drop an outgoing client connection that is already an
// inbound client connection of another node.
func TestClientDisconnectLoopback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tn := NewTestNetwork()
	local := tn.StartGossip(1, stopper, metric.NewRegistry())
	local.mu.Lock()
	lAddr := local.mu.is.NodeAddr
	local.startClientLocked(lAddr)
	local.mu.Unlock()
	local.manage()
	testutils.SucceedsSoon(t, func() error {
		ok := local.findClient(func(c *client) bool { return c.addr.String() == lAddr.String() }) != nil
		if !ok && getConnectedCount(local) == 0 {
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

	tn := NewTestNetwork()
	local := tn.StartGossip(1, stopper, metric.NewRegistry())
	remote := tn.StartGossip(2, stopper, metric.NewRegistry())
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
			local.startClientLocked(rAddr)
			local.mu.Unlock()
			return fmt.Errorf("unable to find local to remote client")
		}
		_, err := remote.GetInfo("local-key")
		return err
	})

	// Start a remote to local client. This client will get removed as being
	// redundant as there is already a connection between the two nodes.
	remote.mu.Lock()
	remote.startClientLocked(lAddr)
	remote.mu.Unlock()

	testutils.SucceedsSoon(t, func() error {
		// Check which of the clients is connected to the other.
		ok1 := local.findClient(func(c *client) bool { return c.addr.String() == rAddr.String() }) != nil
		ok2 := remote.findClient(func(c *client) bool { return c.addr.String() == lAddr.String() }) != nil
		if !ok1 || ok2 {
			return fmt.Errorf("remote to local client not yet closed as redundant: local=%t remote=%t",
				ok1, ok2)
		}
		if getConnectedCount(local) != 0 && getConnectedCount(remote) != 1 {
			return fmt.Errorf("wrong number connected: local=%d remote=%d",
				getConnectedCount(local), getConnectedCount(remote))
		}
		return nil
	})
}

// TestClientDisallowMultipleConns verifies that the server disallows
// multiple connections from the same client node ID.
func TestClientDisallowMultipleConns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	tn := NewTestNetwork()
	local := tn.StartGossip(1, stopper, metric.NewRegistry())
	remote := tn.StartGossip(2, stopper, metric.NewRegistry())

	local.mu.Lock()
	remote.mu.Lock()
	rAddr := remote.mu.is.NodeAddr
	// Start two clients from local to remote.
	local.startClientLocked(rAddr)
	local.startClientLocked(rAddr)
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
		localOut := local.outgoing.len()
		remoteIn := remote.mu.incoming.len()
		local.mu.Unlock()
		remote.mu.Unlock()
		localCount := getConnectedCount(local)
		remoteCount := getConnectedCount(remote)
		if localOut == 1 && remoteIn == 1 && localCount == 0 && remoteCount == 1 {
			return nil
		}
		return errors.Errorf("incorrect number of incoming (%d, %d) or outgoing (%d, %d) connections", remoteIn, localCount, localOut, remoteCount)
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
	tn := NewTestNetwork()
	var g []*Gossip
	var gossipAddr string
	for i := 0; i < 3; i++ {
		nodeID := roachpb.NodeID(i + 1)

		rpcContext := rpc.NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)
		server, err := rpc.NewServer(ctx, rpcContext)
		require.NoError(t, err)
		// node ID must be non-zero
		gnode := tn.StartGossip(nodeID, stopper, metric.NewRegistry())
		RegisterGossipServer(server, gnode)
		g = append(g, gnode)

		ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
		require.NoError(t, err)

		// Connect to the first gossip node.
		if gossipAddr == "" {
			gossipAddr = ln.Addr().String()
		}

		addresses := []util.UnresolvedAddr{util.MakeUnresolvedAddr("tcp", gossipAddr)}
		gnode.Start(ln.Addr(), addresses)
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
	tn := NewTestNetwork()
	local := tn.StartGossip(nodeID, stopper, metric.NewRegistry())
	addr := local.GetNodeAddr()

	client := newClient(log.MakeTestingAmbientCtxWithNewTracer(), addr, makeMetrics()) // never started

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
