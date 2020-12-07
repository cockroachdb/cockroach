// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNodesString(t *testing.T) {
	defer leaktest.AfterTest(t)

	ns := func(ids ...int) nodes {
		var nodes []node
		for _, id := range ids {
			nodes = append(nodes, node{id: roachpb.NodeID(id)})
		}
		return nodes
	}

	var tests = []struct {
		ns  nodes
		exp string
	}{
		{ns(), "n{}"},
		{ns(1), "n{1}"},
		{ns(1, 2, 3), "n{1,2,3}"},
		{ns(3, 4, 7), "n{3,4,7}"},
	}

	for _, test := range tests {
		if got := test.ns.String(); got != test.exp {
			t.Fatalf("expected %s, got %s", test.exp, got)
		}
	}
}

func TestNodesIdentical(t *testing.T) {
	defer leaktest.AfterTest(t)

	list := func(nodes ...string) nodes { // takes in strings of the form "id@epoch"
		var ns []node
		for _, n := range nodes {
			parts := strings.Split(n, "@")
			id, _ := strconv.Atoi(parts[0])
			epoch, _ := strconv.Atoi(parts[1])
			ns = append(ns, node{id: roachpb.NodeID(id), epoch: int64(epoch)})
		}
		return ns
	}

	var tests = []struct {
		a, b    nodes
		expOk   bool
		expDiff string
	}{
		{list(), list(), true, ""},
		{list("1@2"), list("1@2"), true, ""},
		{list("1@2"), list("1@3"), false, "n1 was restarted"},
		{list("3@2"), list("5@2"), false, "found different set of nodes"},
		{list("2@1", "1@2"), list("1@2", "2@1"), true, ""},
		{list("1@2"), list("1@2", "2@1"), false, "node added to the cluster"},
		{list("1@1", "2@1"), list("1@1"), false, "node removed from the cluster"},
	}

	for _, test := range tests {
		ok, diff := test.a.identical(test.b)
		if ok != test.expOk {
			t.Fatalf("expected identical = %t, got %t", test.expOk, ok)
		}
		if diff != test.expDiff {
			t.Fatalf("expected diff %q, got %q", test.expDiff, diff)
		}
	}
}
