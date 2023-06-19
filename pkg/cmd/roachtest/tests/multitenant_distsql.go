// Copyright 2022 The Cockroach Authors.
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
	"archive/zip"
	"context"
	gosql "database/sql"
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
				Name:    fmt.Sprintf("multitenant/distsql/instances=%d/bundle=%s/timeout=%d", numInstances, b, to),
				Owner:   registry.OwnerSQLQueries,
				Cluster: r.MakeClusterSpec(4),
				Leases:  registry.MetamorphicLeases,
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
	c.Put(ctx, t.Cockroach(), "./cockroach")
	// This test sets a smaller default range size than the default due to
	// performance and resource limitations. We set the minimum range max bytes to
	// 1 byte to bypass the guardrails.
	settings := install.MakeClusterSettings(install.SecureOption(true))
	settings.Env = append(settings.Env, "COCKROACH_MIN_RANGE_MAX_BYTES=1")
	tenantEnvOpt := createTenantEnvVar(settings.Env[len(settings.Env)-1])
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(1))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(2))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Node(3))

	const (
		tenantID           = 11
		tenantBaseHTTPPort = 8081
		tenantBaseSQLPort  = 26259
		// localPortOffset is used to avoid port conflicts with nodes on a local
		// cluster.
		localPortOffset = 1000
	)

	tenantHTTPPort := func(offset int) int {
		if c.IsLocal() || numInstances > c.Spec().NodeCount {
			return tenantBaseHTTPPort + localPortOffset + offset
		}
		return tenantBaseHTTPPort
	}
	tenantSQLPort := func(offset int) int {
		if c.IsLocal() || numInstances > c.Spec().NodeCount {
			return tenantBaseSQLPort + localPortOffset + offset
		}
		return tenantBaseSQLPort
	}

	storConn := c.Conn(ctx, t.L(), 1)
	_, err := storConn.Exec(`SELECT crdb_internal.create_tenant($1::INT)`, tenantID)
	require.NoError(t, err)

	instances := make([]*tenantNode, 0, numInstances)
	instance1 := createTenantNode(ctx, t, c, c.Node(1), tenantID, 2 /* node */, tenantHTTPPort(0), tenantSQLPort(0),
		createTenantCertNodes(c.All()), tenantEnvOpt)
	instances = append(instances, instance1)
	defer instance1.stop(ctx, t, c)
	instance1.start(ctx, t, c, "./cockroach")

	// Open things up so we can configure range sizes below.
	_, err = storConn.Exec(`ALTER TENANT [$1] SET CLUSTER SETTING sql.zone_configs.allow_for_secondary_tenant.enabled = true`, tenantID)
	require.NoError(t, err)

	// Create numInstances sql pods and spread them evenly across the machines.
	var nodes intsets.Fast
	nodes.Add(1)
	for i := 1; i < numInstances; i++ {
		node := ((i + 1) % c.Spec().NodeCount) + 1
		inst, err := newTenantInstance(ctx, instance1, t, c, node, tenantHTTPPort(i), tenantSQLPort(i))
		instances = append(instances, inst)
		require.NoError(t, err)
		defer inst.stop(ctx, t, c)
		inst.start(ctx, t, c, "./cockroach")
		nodes.Add(i + 1)
	}

	m := c.NewMonitor(ctx, c.Nodes(1, 2, 3))

	inst1Conn, err := gosql.Open("postgres", instance1.pgURL)
	require.NoError(t, err)
	_, err = inst1Conn.Exec("CREATE TABLE t(n INT, i INT,s STRING, PRIMARY KEY(n,i))")
	require.NoError(t, err)

	// DistSQL needs at least a range per node to distribute query everywhere
	// and test takes too long and too much resources with default range sizes
	// so make them much smaller.
	_, err = inst1Conn.Exec(`ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 1000,range_max_bytes = 100000`)
	require.NoError(t, err)

	insertCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, inst := range instances {
		url := inst.pgURL
		li := i
		m.Go(func(ctx context.Context) error {
			dbi, err := gosql.Open("postgres", url)
			require.NoError(t, err)
			iter := 0
			for {
				_, err = dbi.ExecContext(insertCtx, "INSERT INTO t SELECT $1,generate_series(1,100)+$2*100,repeat('asdfasdf',1024)", li, iter)
				select {
				case <-insertCtx.Done():
					t.L().Printf("worker %d done:%v", li, insertCtx.Err())
					return nil
				default:
					// procede to report error
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
		if err != nil {
			attempts--
			require.Greater(t, attempts, 0, "All nodes didn't show up in time")
			continue
		}

		var nodesInPlan intsets.Fast
		for res.Next() {
			str := ""
			err = res.Scan(&str)
			require.NoError(t, err)
			fields := strings.Fields(str)
			l := len(fields)
			if l > 2 && fields[l-2] == "Node" {
				n, err := strconv.Atoi(fields[l-1])
				if err == nil {
					nodesInPlan.Add(n)
				}
			}
		}
		if nodes == nodesInPlan {
			t.L().Printf("Nodes all present")
			cancel()
			break
		} else {
			t.L().Printf("Only %d nodes present: %v", nodesInPlan.Len(), nodesInPlan)
		}

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
			conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, instance1.pgURL)
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
