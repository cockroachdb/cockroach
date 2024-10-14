// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func runCLINodeStatus(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, 3))

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	require.NoError(t, err)

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

	nodeStatus := func() (_ string, _ []string, err error) {
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), fmt.Sprintf("./cockroach node status --certs-dir=%s -p {pgport:1}", install.CockroachNodeCertsDir))
		if err != nil {
			return "", nil, err
		}
		return result.Stdout, lastWords(result.Stdout), nil
	}

	{
		expected := []string{
			"is_available is_live",
			"true true",
			"true true",
			"true true",
		}
		raw, actual, err := nodeStatus()
		if err != nil {
			t.Fatalf("node status failed: %v\n%s", err, raw)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("expected %s, but found %s:\nfrom:\n%s", expected, actual, raw)
		}
	}

	waitUntil := func(expected []string) {
		var (
			raw    string
			actual []string
			err    error
		)
		// Node liveness takes ~9s to time out. Give the test double that time.
		for i := 0; i < 20; i++ {
			if raw, actual, err = nodeStatus(); err != nil {
				t.L().Printf("node status failed: %v\n%s", err, raw)
			} else if reflect.DeepEqual(expected, actual) {
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
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(2))
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
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(3))
	waitUntil([]string{
		"is_available is_live",
		"false false",
		"false false",
		"false false",
	})

	// Stop the cluster and restart only 2 of the nodes. Verify that three nodes
	// show up in the node status output.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Range(1, 2))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, 2))

	waitUntil([]string{
		"is_available is_live",
		"true true",
		"true true",
		"false false",
	})

	// Start node again to satisfy roachtest.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(3))
}
