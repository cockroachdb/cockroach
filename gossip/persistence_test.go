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

package gossip_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/gossip/simulation"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type testPersistence struct {
	read, write bool
	info        gossip.BootstrapInfo
}

// Read implements the gossip.Persistence interface.
func (tp *testPersistence) Read(info *gossip.BootstrapInfo) error {
	tp.read = true
	*info = tp.info
	return nil
}

// Write implements the gossip.Persistence interface.
func (tp *testPersistence) Write(info *gossip.BootstrapInfo) error {
	tp.write = true
	tp.info = *info
	return nil
}

// TestGossipPersistence
func TestGossipPersistence(t *testing.T) {
	defer leaktest.AfterTest(t)

	const numNodes = 3
	network := simulation.NewNetwork(3)
	defer network.Stop()

	// Set persistence for each of the nodes.
	persists := []*testPersistence{}
	for _, n := range network.Nodes {
		tp := &testPersistence{}
		persists = append(persists, tp)
		if err := n.Gossip.SetPersistence(tp); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for the gossip network to connect.
	network.RunUntilFullyConnected()

	for i, p := range persists {
		if !p.read {
			t.Errorf("%d: expected read from persistence", i)
		}
		if !p.write {
			t.Errorf("%d: expected write from persistence", i)
		}
		// Verify all gossip addresses are written to each persistent store.
		if len(p.info.Addresses) != 3 {
			t.Errorf("%d: expected 3 addresses, have: %s", i, p.info.Addresses)
		}
	}

	// Create an unaffiliated gossip node with only itself as a resolver,
	// leaving it no way to reach the gossip network.
	node, err := network.CreateNode()
	if err != nil {
		t.Fatal(err)
	}
	node.Gossip.SetBootstrapInterval(1 * time.Millisecond)

	r, err := resolver.NewResolverFromAddress(node.Addr)
	if err != nil {
		t.Fatal(err)
	}
	node.Gossip.SetResolvers([]resolver.Resolver{r})
	if err := network.StartNode(node); err != nil {
		t.Fatal(err)
	}

	// Wait for a bit to ensure no connection.
	select {
	case <-time.After(10 * time.Millisecond):
		// expected outcome...
	case <-node.Gossip.Connected:
		t.Fatal("unexpectedly connected to gossip")
	}

	// Give the new node persistence with info established from a node
	// in the established network.
	tp := &testPersistence{
		info: persists[0].info,
	}
	if err := node.Gossip.SetPersistence(tp); err != nil {
		t.Fatal(err)
	}

	network.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		if cycle > 100 {
			t.Fatal("failed to connect to gossip")
		}
		select {
		case <-node.Gossip.Connected:
			return false
		default:
			return true
		}
	})
}
