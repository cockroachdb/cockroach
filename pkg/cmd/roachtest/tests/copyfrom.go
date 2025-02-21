// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const tpchLineitemFmt = `https://storage.googleapis.com/cockroach-fixtures-us-east1/tpch-csv/sf-%d/lineitem.tbl.1`

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
	if !c.IsLocal() {
		if err := c.Install(ctx, t.L(), c.All(), "postgresql"); err != nil {
			t.Fatal(err)
		}
	} else {
		t.L().Printf("when running locally, ensure that psql is installed")
	}
	csv := fmt.Sprintf(tpchLineitemFmt, sf)
	c.Run(ctx, option.WithNodes(c.Node(1)), "rm -f /tmp/lineitem-table.csv")
	c.Run(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("curl '%s' -o /tmp/lineitem-table.csv", csv))
}

func runTest(ctx context.Context, t test.Test, c cluster.Cluster, pg string, atomic bool) {
	var err error
	var start time.Time
	var det install.RunResultDetails

	rOpts := base.DefaultRetryOptions()
	rOpts.MaxRetries = 5
	rOpts.MaxBackoff = 10 * time.Second
	r := retry.StartWithCtx(ctx, rOpts)
	succeeded := false
	for r.Next() {
		start = timeutil.Now()
		det, err = c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), fmt.Sprintf(`cat /tmp/lineitem-table.csv | %s -c "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';"`, pg))
		if err == nil {
			succeeded = true
			break
		}
		if pgerror.GetPGCode(err) != pgcode.SerializationFailure {
			t.L().Printf("err: %v\n", err)
			t.L().Printf("stdout:\n%v\n", det.Stdout)
			t.L().Printf("stderr:\n%v\n", det.Stderr)
			if atomic {
				// With atomic COPY we're more likely to encounter an error, and
				// in the ideal world that error should have 40001 error code
				// set ("serialization failure"), but we might be stripping that
				// information in the roachtest infra. We don't think there is
				// anything wrong with the COPY, so we allow retries for atomic
				// COPY unconditionally.
				t.L().Printf("retrying atomic COPY due to an error: \n%s\n", err)
			} else {
				t.Fatal(err)
			}
		} else {
			t.L().Printf("retrying due to retryable error: \n%s\n", err)
		}
	}
	if !succeeded {
		t.Fatalf("exceeded the limit of retries for serializable errors")
	}
	dur := timeutil.Since(start)
	t.L().Printf("%v\n", det.Stdout)
	rows := 0
	copy := ""
	_, err = fmt.Sscan(det.Stdout, &copy, &rows)
	require.NoError(t, err)
	rate := int(float64(rows) / dur.Seconds())

	det, err = c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)), "wc -c /tmp/lineitem-table.csv")
	require.NoError(t, err)
	var bytes float64
	_, err = fmt.Sscan(det.Stdout, &bytes)
	require.NoError(t, err)
	dataRate := bytes / 1024 / 1024 / dur.Seconds()
	t.L().Printf("results: %d rows/s, %f mb/s", rate, dataRate)
	// Write the copy rate into the stats.json file to be used by roachperf.
	c.Run(ctx, option.WithNodes(c.Node(1)), "mkdir", t.PerfArtifactsDir())
	cmd := fmt.Sprintf(
		`echo '{ "copy_row_rate": %d, "copy_data_rate": %f}' > %s/stats.json`,
		rate, dataRate, t.PerfArtifactsDir(),
	)
	c.Run(ctx, option.WithNodes(c.Node(1)), cmd)
}

func runCopyFromPG(ctx context.Context, t test.Test, c cluster.Cluster, sf int) {
	initTest(ctx, t, c, sf)
	c.Run(ctx, option.WithNodes(c.Node(1)), "sudo -i -u postgres psql -c 'DROP TABLE IF EXISTS lineitem'")
	c.Run(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("sudo -i -u postgres psql -c '%s'", lineitemSchema))
	runTest(ctx, t, c, "sudo -i -u postgres psql", false /* atomic */)
}

func runCopyFromCRDB(ctx context.Context, t test.Test, c cluster.Cluster, sf int, atomic bool) {
	startOpts := option.DefaultStartOpts()
	// Enable the verbose logging on relevant files to have better understanding
	// in case the test fails.
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--vmodule=copy_from=2,insert=2")
	// roachtest frequently runs on overloaded instances and can timeout as a result
	clusterSettings := install.MakeClusterSettings(install.ClusterSettingsOption{"kv.closed_timestamp.target_duration": "60s"})
	c.Start(ctx, t.L(), startOpts, clusterSettings, c.All())
	initTest(ctx, t, c, sf)
	db, err := c.ConnE(ctx, t.L(), 1)
	require.NoError(t, err)
	stmts := []string{
		"CREATE USER importer WITH PASSWORD '123'",
		fmt.Sprintf("ALTER ROLE importer SET copy_from_atomic_enabled = %t", atomic),
	}
	for _, stmt := range stmts {
		_, err = db.ExecContext(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
	}
	urls, err := c.InternalPGUrl(ctx, t.L(), c.Node(1), roachprod.PGURLOptions{Auth: install.AuthUserPassword})
	require.NoError(t, err)
	m := c.NewMonitor(ctx, c.All())
	m.Go(func(ctx context.Context) error {
		// psql w/ url first doesn't support --db arg so have to do this.
		urlstr := strings.Replace(urls[0], "?", "/defaultdb?", 1)
		u, err := url.Parse(urlstr)
		require.NoError(t, err)
		u.User = url.UserPassword("importer", "123")
		urlstr = u.String()
		c.Run(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("psql '%s' -c 'SELECT 1'", urlstr))
		c.Run(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("psql '%s' -c '%s'", urlstr, lineitemSchema))
		runTest(ctx, t, c, fmt.Sprintf("psql '%s'", urlstr), atomic)
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
			Name:             fmt.Sprintf("copyfrom/crdb-atomic/sf=%d/nodes=%d", tc.sf, tc.nodes),
			Owner:            registry.OwnerSQLQueries,
			Benchmark:        true,
			Cluster:          r.MakeClusterSpec(tc.nodes),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopyFromCRDB(ctx, t, c, tc.sf, true /* atomic */)
			},
		})
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("copyfrom/crdb-nonatomic/sf=%d/nodes=%d", tc.sf, tc.nodes),
			Owner:            registry.OwnerSQLQueries,
			Benchmark:        true,
			Cluster:          r.MakeClusterSpec(tc.nodes),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopyFromCRDB(ctx, t, c, tc.sf, false /* atomic */)
			},
		})
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("copyfrom/pg/sf=%d/nodes=%d", tc.sf, tc.nodes),
			Owner:            registry.OwnerSQLQueries,
			Benchmark:        true,
			Cluster:          r.MakeClusterSpec(tc.nodes),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runCopyFromPG(ctx, t, c, tc.sf)
			},
		})
	}
}
