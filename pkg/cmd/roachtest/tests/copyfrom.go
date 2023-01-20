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
		t.Fatal(err)
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
	c.Run(ctx, c.Node(1), "mkdir", t.PerfArtifactsDir())
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

func runCopyFromCRDB(ctx context.Context, t test.Test, c cluster.Cluster, sf int, atomic bool) {
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
	initTest(ctx, t, c, sf)
	db, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	stmt := fmt.Sprintf("ALTER ROLE ALL SET copy_from_atomic_enabled = %t", atomic)
	_, err = db.ExecContext(ctx, stmt)
	require.NoError(t, err)
	urls, err := c.InternalPGUrl(ctx, t.L(), c.Node(1), "")
	require.NoError(t, err)
	m := c.NewMonitor(ctx, c.All())
	m.Go(func(ctx context.Context) error {
		// psql w/ url first are doesn't support --db arg so have to do this.
		url := strings.Replace(urls[0], "?", "/defaultdb?", 1)
		c.Run(ctx, c.Node(1), fmt.Sprintf("psql %s -c 'SELECT 1'", url))
		c.Run(ctx, c.Node(1), fmt.Sprintf("psql %s -c '%s'", url, lineitemSchema))
		runTest(ctx, t, c, fmt.Sprintf("psql '%s'", url))
		return nil
	})
	m.Wait()
}

func registerCopyFrom(r registry.Registry) {
	testcases := []struct {
		sf    int
		nodes int
	}{
		{sf: 1, nodes: 1},
	}

	for _, tc := range testcases {
		tc := tc
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("copyfrom/crdb-atomic/sf=%d/nodes=%d", tc.sf, tc.nodes),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(tc.nodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopyFromCRDB(ctx, t, c, tc.sf, true /*atomic*/)
			},
		})
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("copyfrom/crdb-nonatomic/sf=%d/nodes=%d", tc.sf, tc.nodes),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(tc.nodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopyFromCRDB(ctx, t, c, tc.sf, false /*atomic*/)
			},
		})
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("copyfrom/pg/sf=%d/nodes=%d", tc.sf, tc.nodes),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(tc.nodes),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopyFromPG(ctx, t, c, tc.sf)
			},
		})
	}
}
