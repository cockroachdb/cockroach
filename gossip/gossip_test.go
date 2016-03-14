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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

	// Set up an http server for testing the http load balancer.
	i := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i++
		fmt.Fprintf(w, `{"address": {"network": "tcp", "address": "10.10.0.%d:12345"}}`, i)
	})
	s := httptest.NewServer(handler)
	defer s.Close()

	resolverSpecs := []string{
		"127.0.0.1:9000",
		"tcp=127.0.0.1:9001",
		"unix=/tmp/unix-socket12345",
		fmt.Sprintf("http-lb=%s", s.Listener.Addr()),
		"foo=127.0.0.1:9003", // error should not resolve.
		"http-lb=",           // error should not resolve.
		"localhost:9004",
	}

	resolvers := []resolver.Resolver{}
	for _, rs := range resolverSpecs {
		resolver, err := resolver.NewResolver(&base.Context{Insecure: true}, rs)
		if err == nil {
			resolvers = append(resolvers, resolver)
		}
	}
	if len(resolvers) != 5 {
		t.Errorf("expected 5 resolvers; got %d", len(resolvers))
	}
	g := New(nil, resolvers, nil)

	// Using specified resolvers, fetch bootstrap addresses 10 times
	// and verify the results match expected addresses.
	expAddresses := []string{
		"127.0.0.1:9000",
		"127.0.0.1:9001",
		"/tmp/unix-socket12345",
		"10.10.0.1:12345",
		"localhost:9004",
		"10.10.0.2:12345",
		"10.10.0.3:12345",
		"10.10.0.4:12345",
		"10.10.0.5:12345",
		"10.10.0.6:12345",
	}
	for i := 0; i < len(expAddresses); i++ {
		if addr := g.getNextBootstrapAddress(); addr == nil {
			t.Errorf("%d: unexpected nil addr when expecting %s", i, expAddresses[i])
		} else if addrStr := addr.String(); addrStr != expAddresses[i] {
			t.Errorf("%d: expected addr %s; got %s", i, expAddresses[i], addrStr)
		}
	}
}

// TestGossipCullNetwork verifies that a client will be culled from
// the network periodically (at cullInterval duration intervals).
func TestGossipCullNetwork(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create the local gossip and minPeers peers.
	stopper := stop.NewStopper()
	defer stopper.Stop()
	local := startGossip(1, stopper, t)
	local.SetCullInterval(5 * time.Millisecond)
	peers := []*Gossip{}
	for i := 0; i < minPeers; i++ {
		peers = append(peers, startGossip(roachpb.NodeID(i+2), stopper, t))
	}

	// Start clients to all peers and start the local gossip's manage routine.
	local.mu.Lock()
	for _, p := range peers {
		pAddr := p.is.NodeAddr
		local.startClient(&pAddr, stopper)
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
