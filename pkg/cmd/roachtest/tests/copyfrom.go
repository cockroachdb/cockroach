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
	"context"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const tpchLineitemFmt = `https://storage.googleapis.com/cockroach-fixtures/tpch-csv/sf-%d/lineitem.tbl.1`

// There's an extra dummy field because the file above ends lines with delimiter and standard CSV behavior is to
// interpret that as a column.
var lineitemSchema string = `
CREATE TABLE  lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      DECIMAL(15,2) NOT NULL,
	l_extendedprice DECIMAL(15,2) NOT NULL,
	l_discount      DECIMAL(15,2) NOT NULL,
	l_tax           DECIMAL(15,2) NOT NULL,
	l_returnflag    CHAR(1) NOT NULL,
	l_linestatus    CHAR(1) NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  CHAR(25) NOT NULL,
	l_shipmode      CHAR(10) NOT NULL,
	l_comment       VARCHAR(44) NOT NULL,
	l_dummy         CHAR(1),
	PRIMARY KEY     (l_orderkey, l_linenumber));
	`

var lineitemSchemaIndexes string = `
CREATE INDEX  l_ok ON lineitem    (l_orderkey);
CREATE INDEX  l_pk ON lineitem    (l_partkey);
CREATE INDEX  l_sk ON lineitem    (l_suppkey);
CREATE INDEX  l_sd ON lineitem    (l_shipdate);
CREATE INDEX  l_cd ON lineitem    (l_commitdate);
CREATE INDEX  l_rd ON lineitem    (l_receiptdate);
CREATE INDEX  l_pk_sk ON lineitem (l_partkey, l_suppkey);
CREATE INDEX  l_sk_pk ON lineitem (l_suppkey, l_partkey);
`

func initTest(ctx context.Context, t test.Test, c cluster.Cluster, sf int) {
	if runtime.GOOS == "linux" {
		if err := repeatRunE(
			ctx, t, c, c.All(), "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}
		if err := repeatRunE(
			ctx,
			t,
			c,
			c.All(),
			"install dependencies",
			`sudo apt-get install -qq postgresql`,
		); err != nil {
			t.Fatal(err)
		}
	}
	csv := fmt.Sprintf(tpchLineitemFmt, sf)
	c.Run(ctx, c.Node(1), "rm -f /tmp/lineitem-table.csv")
	c.Run(ctx, c.Node(1), fmt.Sprintf("curl '%s' -o /tmp/lineitem-table.csv", csv))
}

func runTest(ctx context.Context, t test.Test, c cluster.Cluster, pg string) {
	start := timeutil.Now()
	det, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(1), fmt.Sprintf(`cat /tmp/lineitem-table.csv | %s -c "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';"`, pg))
	if err != nil {
		t.L().Printf("stdout:\n%v\n", det.Stdout)
		t.L().Printf("stderr:\n%v\n", det.Stderr)
		// N.B. we don't error here allowing for offline work, ie we assume file
		// is downloaded already, if not we'll error below.
	}
	dur := timeutil.Since(start)
	t.L().Printf("%v\n", det.Stdout)
	rows := 0
	copy := ""
	_, err = fmt.Sscan(det.Stdout, &copy, &rows)
	require.NoError(t, err)
	rate := int(float64(rows) / dur.Seconds())

	det, err = c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(1), "wc -c /tmp/lineitem-table.csv")
	require.NoError(t, err)
	var bytes float64
	_, err = fmt.Sscan(det.Stdout, &bytes)
	require.NoError(t, err)
	dataRate := bytes / 1024 / 1024 / dur.Seconds()
	t.L().Printf("results: %d rows/s, %f mb/s", rate, dataRate)
	// Write the copy rate into the stats.json file to be used by roachperf.
	c.Run(ctx, c.Node(1), "mkdir", "-p", t.PerfArtifactsDir())
	cmd := fmt.Sprintf(
		`echo '{ "copy_row_rate": %d, "copy_data_rate": %f}' > %s/stats.json`,
		rate, dataRate, t.PerfArtifactsDir(),
	)
	c.Run(ctx, c.Node(1), cmd)
}

func runCopyFromPG(ctx context.Context, t test.Test, c cluster.Cluster, sf int) {
	initTest(ctx, t, c, sf)
	c.Run(ctx, c.Node(1), "sudo -i -u postgres psql -c 'DROP TABLE IF EXISTS lineitem'")
	c.Run(ctx, c.Node(1), fmt.Sprintf("sudo -i -u postgres psql -c '%s'", lineitemSchema))
	runTest(ctx, t, c, "sudo -i -u postgres psql")
}

func runCopyFromCRDB(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	sf int,
	fastPath, atomic bool,
	vectorize string,
	pebbleTweaks bool,
) {
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	clusterSettings := install.MakeClusterSettings()
	clusterSettings.Env = append(clusterSettings.Env, "GODEBUG=gctrace=1,schedtrace=1000")

	startOpts := option.DefaultStartOptsNoBackups()
	// See:	https://github.com/cockroachdb/pebble/issues/2173
	if pebbleTweaks {
		clusterSettings.Env = append(clusterSettings.Env, "COCKROACH_ROCKSDB_CONCURRENCY=10")
		startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
			fmt.Sprintf(`--store={store-dir},pebble=[Options] l0_compaction_concurrency=3 mem_table_size=%d lbase_max_bytes=%d`, 512<<20, 1024<<20))
	}
	c.Start(ctx, t.L(), startOpts, clusterSettings, c.All())
	initTest(ctx, t, c, sf)
	db, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)

	for _, stmt := range []string{
		"CREATE USER test",
		"GRANT admin TO test",
		fmt.Sprintf("ALTER ROLE ALL SET copy_from_atomic_enabled = %t", atomic),
		fmt.Sprintf("ALTER ROLE ALL SET vectorize = '%s'", vectorize),
		fmt.Sprintf("ALTER ROLE ALL SET copy_fast_path_enabled = %t", fastPath),
	} {
		_, err = db.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}
	urls, err := c.InternalPGUrl(ctx, t.L(), c.Node(1), "")
	require.NoError(t, err)
	m := c.NewMonitor(ctx, c.All())
	m.Go(func(ctx context.Context) error {
		// psql w/ url first are doesn't support --db arg so have to stick it in url.
		urlStr := strings.Replace(urls[0], "?", "/defaultdb?", 1)
		u, err := url.Parse(urlStr)
		require.NoError(t, err)
		u.User = url.User("test")
		t.L().Printf("url: %s", u.String())
		c.Run(ctx, c.Node(1), fmt.Sprintf("psql %s -c 'SELECT 1'", u))
		c.Run(ctx, c.Node(1), fmt.Sprintf("psql %s -c '%s'", u, lineitemSchema))
		c.Run(ctx, c.Node(1), fmt.Sprintf("psql %s -c '%s'", u, lineitemSchemaIndexes))
		runTest(ctx, t, c, fmt.Sprintf("psql '%s'", u))
		return nil
	})
	m.Wait()
}

func registerCopyFrom(r registry.Registry) {
	testcases := []struct {
		sf        int
		nodes     int
		vectorize string
	}{
		{1, 1, "off"},
		{1, 1, "on"},
		{1, 3, "off"},
		{1, 3, "on"},
	}

	for _, tc := range testcases {
		tc := tc
		for _, atomic := range []string{"atomic", "nonatomic"} {
			for _, pebbleTweaks := range []string{"", "ex"} {
				r.Add(registry.TestSpec{
					Name:    fmt.Sprintf("copyfrom/crdb%s-%s/sf=%d/nodes=%d/vectorize=%s", pebbleTweaks, atomic, tc.sf, tc.nodes, tc.vectorize),
					Owner:   registry.OwnerKV,
					Cluster: r.MakeClusterSpec(tc.nodes),
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
						runCopyFromCRDB(ctx, t, c, tc.sf, true /*fastpath*/, atomic == "atomic" /*atomic*/, tc.vectorize, pebbleTweaks == "ex")
					},
				})
			}
		}
	}
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("copyfrom/pg/sf=%d/nodes=%d", 1, 1),
		Owner:   registry.OwnerKV,
		Cluster: r.MakeClusterSpec(1),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCopyFromPG(ctx, t, c, 1)
		},
	})
}
