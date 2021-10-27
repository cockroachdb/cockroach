// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func runCLINodeStatus(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, c.Range(1, 3))

	db := c.Conn(ctx, 1)
	defer db.Close()

	WaitFor3XReplication(t, db)

	lastWords := func(s string) []string {
		var result []string
		s = cli.ElideInsecureDeprecationNotice(s)
		lines := strings.Split(s, "\n")
		for _, line := range lines {
			words := strings.Fields(line)
			if n := len(words); n > 0 {
				result = append(result, words[n-2]+" "+words[n-1])
			}
		}
		return result
	}

	nodeStatus := func() (raw string, _ []string) {
		out, err := c.RunWithBuffer(ctx, t.L(), c.Node(1),
			"./cockroach node status --insecure -p {pgport:1}")
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		raw = string(out)
		return raw, lastWords(string(out))
	}

	{
		expected := []string{
			"is_available is_live",
			"true true",
			"true true",
			"true true",
		}
		raw, actual := nodeStatus()
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s:\nfrom:\n%s", expected, actual, raw)
		}
	}

	waitUntil := func(expected []string) {
		var raw string
		var actual []string
		// Node liveness takes ~9s to time out. Give the test double that time.
		for i := 0; i < 20; i++ {
			raw, actual = nodeStatus()
			if reflect.DeepEqual(expected, actual) {
				break
			}
			t.L().Printf("not done: %s vs %s\n", expected, actual)
			time.Sleep(time.Second)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s from:\n%s", expected, actual, raw)
		}
	}

	// Kill node 2 and wait for it to be marked as !is_available and !is_live.
	c.Stop(ctx, c.Node(2))
	waitUntil([]string{
		"is_available is_live",
		"true true",
		"false false",
		"true true",
	})

	// Kill node 3 and wait for all of the nodes to be marked as
	// !is_available. Node 1 is not available because the liveness check can no
	// longer write to the liveness range due to lack of quorum. This test is
	// verifying that "node status" still returns info in this situation since
	// it only accesses gossip info.
	c.Stop(ctx, c.Node(3))
	waitUntil([]string{
		"is_available is_live",
		"false true",
		"false false",
		"false false",
	})

	// Stop the cluster and restart only 2 of the nodes. Verify that three nodes
	// show up in the node status output.
	c.Stop(ctx, c.Range(1, 3))
	c.Start(ctx, c.Range(1, 2))

	// Wait for the cluster to come back up.
	WaitFor3XReplication(t, db)

	waitUntil([]string{
		"is_available is_live",
		"true true",
		"true true",
		"false false",
	})

	// Start node again to satisfy roachtest.
	c.Start(ctx, c.Node(3))
}
