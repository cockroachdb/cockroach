// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationcluster

import (
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNodesString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ns := func(ids ...int) Nodes {
		var nodes []Node
		for _, id := range ids {
			nodes = append(nodes, Node{ID: roachpb.NodeID(id)})
		}
		return nodes
	}

	var tests = []struct {
		ns  Nodes
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
	defer leaktest.AfterTest(t)()

	list := func(nodes ...string) Nodes { // takes in strings of the form "ID@Epoch"
		var ns []Node
		for _, n := range nodes {
			parts := strings.Split(n, "@")
			id, err := strconv.Atoi(parts[0])
			if err != nil {
				t.Fatal(err)
			}
			epoch, err := strconv.Atoi(parts[1])
			if err != nil {
				t.Fatal(err)
			}
			ns = append(ns, Node{ID: roachpb.NodeID(id), Epoch: int64(epoch)})
		}
		return ns
	}

	var tests = []struct {
		a, b    Nodes
		expOk   bool
		expDiff string
	}{
		{list(), list(), true, ""},
		{list("1@2"), list("1@2"), true, ""},
		{list("2@1", "1@2"), list("1@2", "2@1"), true, ""},
		{list("1@2"), list("1@3"), false, "n1's Epoch changed"},
		{list("1@2"), list("1@2", "2@1"), false, "n2 joined the cluster"},
		{list("1@1", "2@1"), list("1@1"), false, "n2 was decommissioned"},
		{list("3@2", "4@6"), list("4@8", "5@2"), false, "n3 was decommissioned, n4's Epoch changed, n5 joined the cluster"},
	}

	for _, test := range tests {
		ok, diffs := test.a.Identical(test.b)
		if ok != test.expOk {
			t.Fatalf("expected Identical = %t, got %t", test.expOk, ok)
		}

		strDiffs := make([]string, len(diffs))
		for i, diff := range diffs {
			strDiffs[i] = string(diff)
		}
		sort.Strings(strDiffs)

		if strings.Join(strDiffs, ", ") != test.expDiff {
			t.Fatalf("expected diff %q, got %q", test.expDiff, strDiffs)
		}
	}
}
