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
	"errors"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

// TestGossipInfoStore verifies operation of gossip instance infostore.
func TestGossipInfoStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rpcContext := rpc.NewContext(nil, nil, stopper)
	g := New(rpcContext, nil, stopper)
	// Have to call g.SetNodeID before call g.AddInfo
	g.SetNodeID(roachpb.NodeID(1))
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

func TestGossipGetNextBootstrapAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer resolver.SetLookupTimeout(time.Minute)()

	resolverSpecs := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"localhost:9004",
	}

	resolvers := []resolver.Resolver{}
	for _, rs := range resolverSpecs {
		resolver, err := resolver.NewResolver(&base.Context{Insecure: true}, rs)
		if err == nil {
			resolvers = append(resolvers, resolver)
		}
	}
	if len(resolvers) != 3 {
		t.Errorf("expected 3 resolvers; got %d", len(resolvers))
	}
	g := New(nil, resolvers, nil)

	// Using specified resolvers, fetch bootstrap addresses 3 times
	// and verify the results match expected addresses.
	expAddresses := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"localhost:9004",
	}
	for i := 0; i < len(expAddresses); i++ {
		if addr := g.getNextBootstrapAddress(); addr == nil {
			t.Errorf("%d: unexpected nil addr when expecting %s", i, expAddresses[i])
		} else if addrStr := addr.String(); addrStr != expAddresses[i] {
			t.Errorf("%d: expected addr %s; got %s", i, expAddresses[i], addrStr)
		}
	}
}

func TestGossipNoForwardSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start one loopback client plus enough additional clients to fill the
	// incoming clients.
	peers := []*Gossip{local}
	for i := 0; i < local.server.incoming.maxSize; i++ {
		peers = append(peers, startGossip(roachpb.NodeID(i+2), stopper, t))
	}

	for _, peer := range peers {
		c := newClient(&local.is.NodeAddr)

		util.SucceedsSoon(t, func() error {
			conn, err := peer.rpcContext.GRPCDial(c.addr.String(), grpc.WithBlock())
			if err != nil {
				return err
			}

			stream, err := NewGossipClient(conn).Gossip(ctx)
			if err != nil {
				return err
			}

			if err := c.requestGossip(peer, peer.is.NodeAddr, stream); err != nil {
				return err
			}

			// Wait until the server responds, so we know we're connected.
			_, err = stream.Recv()
			return err
		})
	}

	const numClients = 50
	disconnectedCh := make(chan *client)
	numFailedConns := 0

	// Start a few overflow peers and assert that they don't get forwarded to us
	// again.
	for i := 0; i < numClients; i++ {
		peer := startGossip(roachpb.NodeID(i+local.server.incoming.maxSize+2), stopper, t)

		c := newClient(&local.is.NodeAddr)
		c.start(peer, disconnectedCh, peer.rpcContext, stopper)

		disconnectedClient := <-disconnectedCh
		if disconnectedClient != c {
			t.Fatalf("expected %p to be disconnected, got %p", c, disconnectedClient)
		} else if c.forwardAddr == nil {
			// Under high load, clients sometimes fail to connect for reasons
			// unrelated to the test, so we need to permit some.
			numFailedConns++
			t.Logf("node #%d: got nil forwarding address", peer.is.NodeID)
		} else if *c.forwardAddr == local.is.NodeAddr {
			t.Errorf("node #%d: got local's forwarding address", peer.is.NodeID)
		}
	}

	if numFailedConns > numClients/10 {
		t.Errorf("%d clients disconnected for unexpected reasons", numFailedConns)
	}
}

// TestGossipCullNetwork verifies that a client will be culled from
// the network periodically (at cullInterval duration intervals).
func TestGossipCullNetwork(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t)
	local.SetCullInterval(5 * time.Millisecond)

	local.mu.Lock()
	for i := 0; i < minPeers; i++ {
		peer := startGossip(roachpb.NodeID(i+2), stopper, t)
		local.startClient(&peer.is.NodeAddr, stopper)
	}
	local.mu.Unlock()
	local.manage()

	util.SucceedsSoon(t, func() error {
		// Verify that a client is closed within the cull interval.
		if len(local.Outgoing()) == minPeers-1 {
			return nil
		}
		return errors.New("no network culling occurred")
	})
}
