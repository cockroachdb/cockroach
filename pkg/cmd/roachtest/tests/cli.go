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
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, 5))

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	err := roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	require.NoError(t, err)

	// Create user ranges at default 3x replication. With 5 nodes and 2 killed,
	// some of these ranges will lose quorum — the test verifies that
	// "node status" still works in that scenario.
	_, err = db.ExecContext(ctx, `CREATE TABLE defaultdb.t (k INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER TABLE defaultdb.t SPLIT AT SELECT generate_series(1, 100)`)
	require.NoError(t, err)
	err = roachtestutil.WaitFor3XReplication(ctx, t.L(), db)
	require.NoError(t, err)

	// System database ranges and meta ranges default to 5x replication
	// (DefaultSystemZoneConfig). Wait for them to reach 5 replicas, since
	// we're going to kill 2 nodes and need these ranges to retain quorum.
	// Without this, SQL connections fail and `node status` (which uses SQL)
	// won't work. We check database_name = 'system' (for system tables) and
	// start_key LIKE '/Meta%' (for meta addressing ranges). Other pre-table
	// system ranges (liveness, etc.) also replicate to 5 but upreplicate
	// quickly. We must NOT include timeseries ranges (start_key LIKE
	// '/System/tsd%') — those inherit num_replicas=3 from the default zone
	// config and would never reach 5.
	t.L().Printf("waiting for system/meta ranges to reach 5 replicas")
	for i := 0; ; i++ {
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT count(*) FROM [SHOW CLUSTER RANGES WITH TABLES]
			WHERE (database_name = 'system' OR start_key LIKE '/Meta%')
			AND array_length(replicas, 1) < 5`,
		).Scan(&n); err != nil {
			t.Fatalf("querying system range replication: %v", err)
		}
		if n == 0 {
			t.L().Printf("all system/meta ranges at 5 replicas")
			break
		}
		if i%10 == 0 {
			t.L().Printf("waiting for %d system/meta ranges to reach 5 replicas", n)
		}
		if i > 300 {
			t.Fatalf("timed out waiting for system/meta range replication (still %d ranges < 5 replicas)", n)
		}
		time.Sleep(time.Second)
	}

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

	// Verify all 5 nodes are available and live.
	{
		expected := []string{
			"is_available is_live",
			"true true",
			"true true",
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

	// Kill node 4 and wait for it to be marked as !is_available and !is_live.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(4))
	waitUntil([]string{
		"is_available is_live",
		"true true",
		"true true",
		"true true",
		"false false",
		"true true",
	})

	// Kill node 5. System ranges are 5x replicated and retain quorum with
	// 3 of 5 nodes alive. Some user ranges (3x replicated) may lose quorum.
	// This verifies that "node status" still works in this scenario.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(5))
	waitUntil([]string{
		"is_available is_live",
		"true true",
		"true true",
		"true true",
		"false false",
		"false false",
	})

	// Stop the remaining nodes and restart them. Verify that all five nodes
	// show up in the node status output.
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Range(1, 3))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(1, 3))

	waitUntil([]string{
		"is_available is_live",
		"true true",
		"true true",
		"true true",
		"false false",
		"false false",
	})

	// Start remaining nodes to satisfy roachtest.
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Range(4, 5))
}
