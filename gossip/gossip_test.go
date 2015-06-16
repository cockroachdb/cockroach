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

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/hlc"
)

var testBaseContext = testutils.NewTestBaseContext()
var serverTestBaseContext = testutils.NewServerTestBaseContext()

// TestGossipInfoStore verifies operation of gossip instance infostore.
func TestGossipInfoStore(t *testing.T) {
	rpcContext := rpc.NewContext(testBaseContext, hlc.NewClock(hlc.UnixNano), nil)
	g := New(rpcContext, TestInterval, TestBootstrap)
	if err := g.AddInfo("i", int64(1), time.Hour); err != nil {
		t.Fatal(err)
	}
	if val, err := g.GetInfo("i"); val.(int64) != int64(1) || err != nil {
		t.Errorf("error fetching int64: %v", err)
	}
	if _, err := g.GetInfo("i2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"i2\"")
	}
	if err := g.AddInfo("f", float64(3.14), time.Hour); err != nil {
		t.Fatal(err)
	}
	if val, err := g.GetInfo("f"); val.(float64) != float64(3.14) || err != nil {
		t.Errorf("error fetching float64: %v", err)
	}
	if _, err := g.GetInfo("f2"); err == nil {
		t.Errorf("expected error fetching nonexistent key \"f2\"")
	}
	if err := g.AddInfo("s", "b", time.Hour); err != nil {
		t.Fatal(err)
	}
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
	rpcContext := rpc.NewContext(testBaseContext, hlc.NewClock(hlc.UnixNano), nil)
	g := New(rpcContext, TestInterval, TestBootstrap)

	// For int64.
	if err := g.RegisterGroup("i", 3, MinGroup); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := g.AddInfo(fmt.Sprintf("i.%d", i), int64(i), time.Hour); err != nil {
			t.Fatal(err)
		}
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
	if err := g.RegisterGroup("f", 3, MinGroup); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := g.AddInfo(fmt.Sprintf("f.%d", i), float64(i), time.Hour); err != nil {
			t.Fatal(err)
		}
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
	if err := g.RegisterGroup("s", 3, MinGroup); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := g.AddInfo(fmt.Sprintf("s.%d", i), fmt.Sprintf("%d", i), time.Hour); err != nil {
			t.Fatal(err)
		}
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

func TestGossipGetNextBootstrapAddress(t *testing.T) {
	resolverSpecs := []string{
		"127.0.0.1:9000",
		"tcp=127.0.0.1:9001",
		"unix=/tmp/unix-socket12345",
		"lb=127.0.0.1:9002",
		"foo=127.0.0.1:9003", // error should not resolve.
		"lb=",                // error should not resolve.
		"localhost:9004",
		"lb=127.0.0.1:9005",
	}

	resolvers := []resolver.Resolver{}
	for _, rs := range resolverSpecs {
		resolver, err := resolver.NewResolver(&base.Context{}, rs)
		if err == nil {
			resolvers = append(resolvers, resolver)
		}
	}
	if len(resolvers) != 6 {
		t.Errorf("expected 6 resolvers; got %d", len(resolvers))
	}
	g := New(nil, TestInterval, resolvers)

	// Using specified resolvers, fetch bootstrap addresses 10 times
	// and verify the results match expected addresses.
	expAddresses := []string{
		"127.0.0.1:9001",
		"/tmp/unix-socket12345",
		"127.0.0.1:9002",
		"localhost:9004",
		"127.0.0.1:9005",
		"127.0.0.1:9000",
		"127.0.0.1:9002",
		"127.0.0.1:9005",
		"127.0.0.1:9002",
		"127.0.0.1:9005",
	}
	for i := 0; i < 10; i++ {
		addr := g.getNextBootstrapAddress()
		if addr.String() != expAddresses[i] {
			t.Errorf("%d: expected addr %s; got %s", i, expAddresses[i], addr.String())
		}
	}
}
