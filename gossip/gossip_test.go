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
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestGossipInfoStore verifies operation of gossip instance infostore.
func TestGossipInfoStore(t *testing.T) {
	defer leaktest.AfterTest(t)
	rpcContext := rpc.NewContext(&base.Context{}, hlc.NewClock(hlc.UnixNano), nil)
	g := New(rpcContext, TestBootstrap)
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
	defer leaktest.AfterTest(t)
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
	g := New(nil, resolvers)

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
