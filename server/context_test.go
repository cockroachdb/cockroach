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

	"github.com/cockroachdb/cockroach/gossip"
)

func TestParseNodeAttributes(t *testing.T) {
	ctx := NewContext()
	ctx.Attrs = "attr1=val1::attr2=val2"
	ctx.Stores = "mem=1"
	ctx.GossipBootstrap = "self://"
	if err := ctx.Init(); err != nil {
		t.Fatalf("Failed to initialize the context: %v", err)
	}
	expected := []string{"attr1=val1", "attr2=val2"}
	if !reflect.DeepEqual(ctx.NodeAttributes.GetAttrs(), expected) {
		t.Fatalf("Unexpected attributes: %v", ctx.NodeAttributes.GetAttrs())
	}
}

// TestParseGossipBootstrapAddrs verifies that GossipBootstrap is
// parsed correctly.
func TestParseGossipBootstrapAddrs(t *testing.T) {
	ctx := NewContext()
	ctx.GossipBootstrap = "localhost:12345,,localhost:23456"
	ctx.Stores = "mem=1"
	if err := ctx.Init(); err != nil {
		t.Fatalf("Failed to initialize the context: %v", err)
	}
	expected := []*gossip.Resolver{
		{"tcp", "localhost:12345", false},
		{"tcp", "localhost:23456", false},
	}
	if !reflect.DeepEqual(ctx.GossipBootstrapResolvers, expected) {
		t.Fatalf("Unexpected bootstrap addresses: %v, expected: %v", ctx.GossipBootstrapResolvers, expected)
	}
}
