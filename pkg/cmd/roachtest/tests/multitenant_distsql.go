// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/stretchr/testify/require"
)

func registerMultiTenantDistSQL(r registry.Registry) {
	const numInstances = 20
	for _, bundle := range []string{"on", "off"} {
		for _, timeout := range []int{0, 1} {
			b := bundle
			to := timeout
			r.Add(registry.TestSpec{
				Skip:             "https://github.com/cockroachdb/cockroach/issues/128366",
				SkipDetails:      "test is broken",
				Name:             fmt.Sprintf("multitenant/distsql/instances=%d/bundle=%s/timeout=%d", numInstances, b, to),
				Owner:            registry.OwnerSQLQueries,
				Cluster:          r.MakeClusterSpec(4),
				CompatibleClouds: registry.CloudsWithServiceRegistration,
				Suites:           registry.Suites(registry.Nightly),
				Leases:           registry.MetamorphicLeases,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantDistSQL(ctx, t, c, numInstances, b == "on", to)
				},
			})
		}
	}
}

func runMultiTenantDistSQL(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	numInstances int,
	bundle bool,
	timeoutMillis int,
) {
	// This test sets a smaller default range size than the default due to
	// performance and resource limitations. We set the minimum range max bytes to
	// 1 byte to bypass the guardrails.
	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_MIN_RANGE_MAX_BYTES=1")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(1))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(2))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(3))
	storageNodes := c.Range(1, 3)

	tenantName := "test-tenant"
	var nodes intsets.Fast
	for i := 0; i < numInstances; i++ {
		node := (i % c.Spec().NodeCount) + 1
		sqlInstance := i / c.Spec().NodeCount
		instStartOps := option.StartVirtualClusterOpts(
			tenantName, c.Node(node),
			option.StorageCluster(storageNodes),
			option.VirtualClusterInstance(sqlInstance),
		)
		t.L().Printf("Starting instance %d on node %d", i, node)
		c.StartServiceForVirtualCluster(ctx, t.L(), instStartOps, settings)
		nodes.Add(i + 1)
	}

	storConn := c.Conn(ctx, t.L(), 1)
	// Open things up, so we can configure range sizes below.
	_, err := storConn.Exec(`ALTER TENANT $1 SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true`, tenantName)
	require.NoError(t, err)

	m := c.NewMonitor(ctx, c.Nodes(1, 2, 3))

	inst1Conn, err := c.ConnE(ctx, t.L(), 1, option.VirtualClusterName(tenantName))
	require.NoError(t, err)
	_, err = inst1Conn.Exec("CREATE TABLE t(n INT, i INT,s STRING, PRIMARY KEY(n,i))")
	require.NoError(t, err)

	// DistSQL needs at least a range per node to distribute query everywhere
	// and test takes too long and too many resources with default range sizes
	// so make them much smaller.
	_, err = inst1Conn.Exec(`ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 1000,range_max_bytes = 100000`)
	require.NoError(t, err)

	insertCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < numInstances; i++ {
		li := i
		m.Go(func(ctx context.Context) error {
			node := (li % c.Spec().NodeCount) + 1
			sqlInstance := li / c.Spec().NodeCount
			dbi, err := c.ConnE(ctx, t.L(), node, option.VirtualClusterName(tenantName), option.SQLInstance(sqlInstance))
			require.NoError(t, err)
			iter := 0
			for {
				_, err = dbi.ExecContext(insertCtx, "INSERT INTO t SELECT $1,generate_series(1,100)+$2*100,repeat('asdfasdf',1024)", li, iter)
				select {
				case <-insertCtx.Done():
					t.L().Printf("worker %d done:%v", li, insertCtx.Err())
					return nil
				default:
					// proceed to report error
				}
				require.NoError(t, err, "instance idx = %d, iter = %d", li, iter)
				iter++
			}
		})
	}

	// Loop until all instances show up in the query.
	attempts := 180
	for {
		time.Sleep(time.Second)
		res, err := inst1Conn.Query("EXPLAIN (VEC) SELECT DISTINCT i FROM t")
		attempts--
		if err != nil {
			require.Greater(t, attempts, 0, "All nodes didn't show up in time")
			continue
		}

		var nodesInPlan intsets.Fast
		var resStr string
		for res.Next() {
			str := ""
			err = res.Scan(&str)
			require.NoError(t, err)
			resStr += str + "\n"
			fields := strings.Fields(str)
			l := len(fields)
			if l > 2 && fields[l-2] == "Node" {
				n, err := strconv.Atoi(fields[l-1])
				if err == nil {
					nodesInPlan.Add(n)
				}
			}
		}
		if nodes.Equals(nodesInPlan) {
			t.L().Printf("Nodes all present")
			cancel()
			break
		}
		t.L().Printf("Only %d nodes present: %v, expected %v", nodesInPlan.Len(), nodesInPlan, nodes)
		require.Greater(t, attempts, 0, "All nodes didn't show up in time. EXPLAIN (VEC):\n%s", resStr)
	}
	m.Wait()

	// Don't move on until statistics are collected. Originally just
	// debugging feature but leaving it in because its nice to know
	// stats are working in this situation.
	rowCount := 0
	attempts = 180
	for rowCount == 0 {
		time.Sleep(time.Second)
		if err := inst1Conn.QueryRow("SELECT row_count FROM [SHOW STATISTICS FOR TABLE t]").Scan(&rowCount); err != nil {
			attempts--
			require.Greater(t, attempts, 0, "Statistics for table didn't show up in time")
		}
	}
	t.L().Printf("Stats calculated")

	if timeoutMillis != 0 {
		_, err := inst1Conn.Exec(fmt.Sprintf("SET statement_timeout='%d';", timeoutMillis))
		require.NoError(t, err)
	}

	// run query
	var debug string
	if bundle {
		debug = "(DEBUG)"
	}
	res, err := inst1Conn.Query(fmt.Sprintf("EXPLAIN ANALYZE %s SELECT DISTINCT i FROM t", debug))

	if timeoutMillis != 0 {
		require.EqualError(t, err, "pq: query execution canceled due to statement timeout")
	} else {
		require.NoError(t, err)
		for res.Next() {
			str := ""
			err = res.Scan(&str)
			require.NoError(t, err)
			t.L().Printf(str)
		}

		if bundle {
			// Open bundle and verify its contents
			sqlConnCtx := clisqlclient.Context{}
			pgURL, err := c.ExternalPGUrl(ctx, t.L(), c.Node(1), roachprod.PGURLOptions{VirtualClusterName: tenantName})
			require.NoError(t, err)
			conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, pgURL[0])
			bundles, err := clisqlclient.StmtDiagListBundles(ctx, conn)
			require.NoError(t, err)

			err = clisqlclient.StmtDiagDownloadBundle(ctx, conn, bundles[len(bundles)-1].ID, "bundle.zip")
			require.NoError(t, err)

			read, err := zip.OpenReader("bundle.zip")
			require.NoError(t, err)
			defer func() { _ = read.Close() }()
			required := []string{"plan.txt", "statement.sql", "env.sql", "schema.sql", "stats-defaultdb.public.t.sql"}
			zipNames := make(map[string]struct{})
			for _, f := range read.File {
				zipNames[f.Name] = struct{}{}
			}
			for _, i := range required {
				_, found := zipNames[i]
				require.True(t, found, "%s not found in bundle", i)
			}
		}
	}
}
