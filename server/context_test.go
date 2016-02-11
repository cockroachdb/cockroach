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
// permissions and limitations under the License.
//
// Author: Kenji Kaneda (kenji.kaneda@gmail.com)

package server

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/gossip/resolver"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestParseStores(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		stores   string
		expFail  bool
		expCount int
		expAttrs []roachpb.Attributes
	}{
		{"", true, 0, nil},
		{"/d1", false, 1, []roachpb.Attributes{
			roachpb.Attributes{},
		}},
		{"/d1,/d2", false, 2, []roachpb.Attributes{
			roachpb.Attributes{},
			roachpb.Attributes{},
		}},
		{"mem=1", false, 1, []roachpb.Attributes{
			roachpb.Attributes{[]string{"mem"}},
		}},
		{"ssd=/d1,hdd=/d2", false, 2, []roachpb.Attributes{
			roachpb.Attributes{[]string{"ssd"}},
			roachpb.Attributes{[]string{"hdd"}},
		}},
		{"ssd:f=/d1,hdd:a:b:c=/d2", false, 2, []roachpb.Attributes{
			roachpb.Attributes{[]string{"ssd", "f"}},
			roachpb.Attributes{[]string{"hdd", "a", "b", "c"}},
		}},
	}

	for i, tc := range testCases {
		ctx := NewContext()
		ctx.Stores = tc.stores
		stopper := stop.NewStopper()
		err := ctx.InitStores(stopper)
		if (err != nil) != tc.expFail {
			t.Errorf("%d: expected failure? %t; got err=%s", i, tc.expFail, err)
		}
		stopper.Stop()
		if l := len(ctx.Engines); l != tc.expCount {
			t.Errorf("%d: expected %d engines; got %d", i, tc.expCount, l)
			continue
		}
		if l := len(tc.expAttrs); l != tc.expCount {
			t.Fatalf("%d: test case expAttrs has incorrect length %d", i, l)
		}
		for j, e := range ctx.Engines {
			if !reflect.DeepEqual(e.Attrs(), tc.expAttrs[j]) {
				t.Errorf("%d: engine %d expected attributes %s; got %s", i, j, tc.expAttrs[j], e.Attrs())
			}
		}
	}
}

func TestParseInitNodeAttributes(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := NewContext()
	ctx.Attrs = "attr1=val1::attr2=val2"
	ctx.Stores = "mem=1"
	stopper := stop.NewStopper()
	defer stopper.Stop()
	if err := ctx.InitStores(stopper); err != nil {
		t.Fatalf("Failed to initialize stores: %s", err)
	}
	if err := ctx.InitNode(); err != nil {
		t.Fatalf("Failed to initialize node: %s", err)
	}

	if a, e := ctx.NodeAttributes.Attrs, []string{"attr1=val1", "attr2=val2"}; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected attributes: %v, found: %v", e, a)
	}
}

// TestParseJoinUsingAddrs verifies that JoinUsing is parsed
// correctly.
func TestParseJoinUsingAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := NewContext()
	ctx.JoinUsing = "localhost:12345,,localhost:23456"
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
