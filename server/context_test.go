// Copyright 2015 The Cockroach Authors.
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
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package server

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestParseNodeAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := NewContext()
	ctx.Attrs = "attr1=val1::attr2=val2"
	ctx.Stores = "mem=1"
	ctx.GossipBootstrap = SelfGossipAddr
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := ctx.InitStores(stopper); err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	if err := ctx.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}
	expected := []string{"attr1=val1", "attr2=val2"}
	if !reflect.DeepEqual(ctx.NodeAttributes.GetAttrs(), expected) {
		t.Fatalf("Unexpected attributes: %v", ctx.NodeAttributes.GetAttrs())
	}
}

// TestParseGossipBootstrapAddrs verifies that GossipBootstrap is
// parsed correctly.
func TestParseGossipBootstrapAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := NewContext()
	ctx.GossipBootstrap = "localhost:12345,,localhost:23456"
	ctx.Stores = "mem=1"
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := ctx.InitStores(stopper); err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	if err := ctx.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}
	r1, err := resolver.NewResolver(&ctx.Context, "tcp=localhost:12345")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := resolver.NewResolver(&ctx.Context, "tcp=localhost:23456")
	if err != nil {
		t.Fatal(err)
	}
	expected := []resolver.Resolver{r1, r2}
	if !reflect.DeepEqual(ctx.GossipBootstrapResolvers, expected) {
		t.Fatalf("Unexpected bootstrap addresses: %v, expected: %v", ctx.GossipBootstrapResolvers, expected)
	}
}
