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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// verifyConvergence verifies that info from each node is visible from
// every node in the network within numCycles cycles of the gossip protocol.
func verifyConvergence(numNodes, maxCycles int, t *testing.T) {
	network := NewSimulationNetwork(numNodes, "unix", DefaultTestGossipInterval)

	if connectedCycle := network.RunUntilFullyConnected(); connectedCycle > maxCycles {
		t.Errorf("expected a fully-connected network within %d cycles; took %d",
			maxCycles, connectedCycle)
	}
	network.Stop()
}

// TestConvergence verifies a 10 node gossip network
// converges within 10 cycles.
// TODO(spencer): During race detector tests, it can take >= 8 cycles.
// Figure out a more deterministic setup.
func TestConvergence(t *testing.T) {
	verifyConvergence(10, 10, t)
}

// TestGossipInfoStore verifies operation of gossip instance infostore.
func TestGossipInfoStore(t *testing.T) {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := New(rpcContext)
	g.AddInfo("i", int64(1), time.Hour)
	if val, err := g.GetInfo("i"); val.(int64) != int64(1) || err != nil {
		t.Errorf("error fetching int64: %v", err)
	}
	if _, err := g.GetInfo("i2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"i2\"")
	}
	g.AddInfo("f", float64(3.14), time.Hour)
	if val, err := g.GetInfo("f"); val.(float64) != float64(3.14) || err != nil {
		t.Errorf("error fetching float64: %v", err)
	}
	if _, err := g.GetInfo("f2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"f2\"")
	}
	g.AddInfo("s", "b", time.Hour)
	if val, err := g.GetInfo("s"); val.(string) != "b" || err != nil {
		t.Errorf("error fetching string: %v", err)
	}
	if _, err := g.GetInfo("s2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"s2\"")
	}
}

// TestGossipGroupsInfoStore verifies gossiping of groups via the
// gossip instance infostore.
func TestGossipGroupsInfoStore(t *testing.T) {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := New(rpcContext)

	// For int64.
	g.RegisterGroup("i", 3, MinGroup)
	for i := 0; i < 3; i++ {
		g.AddInfo(fmt.Sprintf("i.%d", i), int64(i), time.Hour)
	}
	values, err := g.GetGroupInfos("i")
	if err != nil {
		t.Errorf("error fetching int64 group: %v", err)
	}
	if len(values) != 3 {
		t.Errorf("incorrect number of values in group: %v", values)
	}
	for i := 0; i < 3; i++ {
		if values[i].(int64) != int64(i) {
			t.Errorf("index %d has incorrect value: %d, expected %d", i, values[i].(int64), i)
		}
	}
	if _, err := g.GetGroupInfos("i2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"i2\"")
	}

	// For float64.
	g.RegisterGroup("f", 3, MinGroup)
	for i := 0; i < 3; i++ {
		g.AddInfo(fmt.Sprintf("f.%d", i), float64(i), time.Hour)
	}
	values, err = g.GetGroupInfos("f")
	if err != nil {
		t.Errorf("error fetching float64 group: %v", err)
	}
	if len(values) != 3 {
		t.Errorf("incorrect number of values in group: %v", values)
	}
	for i := 0; i < 3; i++ {
		if values[i].(float64) != float64(i) {
			t.Errorf("index %d has incorrect value: %f, expected %d", i, values[i].(float64), i)
		}
	}

	// For string.
	g.RegisterGroup("s", 3, MinGroup)
	for i := 0; i < 3; i++ {
		g.AddInfo(fmt.Sprintf("s.%d", i), fmt.Sprintf("%d", i), time.Hour)
	}
	values, err = g.GetGroupInfos("s")
	if err != nil {
		t.Errorf("error fetching string group: %v", err)
	}
	if len(values) != 3 {
		t.Errorf("incorrect number of values in group: %v", values)
	}
	for i := 0; i < 3; i++ {
		if values[i].(string) != fmt.Sprintf("%d", i) {
			t.Errorf("index %d has incorrect value: %d, expected %s", i, values[i], fmt.Sprintf("%d", i))
		}
	}
}
