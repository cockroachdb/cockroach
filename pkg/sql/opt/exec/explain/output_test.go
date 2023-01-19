// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestOutputBuilder(t *testing.T) {
	example := func(flags explain.Flags) *explain.OutputBuilder {
		ob := explain.NewOutputBuilder(flags)
		ob.AddField("distributed", "true")
		ob.EnterMetaNode("meta")
		{
			ob.EnterNode(
				"render",
				colinfo.ResultColumns{{Name: "a", Typ: types.Int}, {Name: "b", Typ: types.String}},
				colinfo.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Descending},
				},
			)
			ob.AddField("render 0", "foo")
			ob.AddField("render 1", "bar")
			{
				ob.EnterNode("join", colinfo.ResultColumns{{Name: "x", Typ: types.Int}}, nil)
				ob.AddField("type", "outer")
				{
					{
						ob.EnterNode("scan", colinfo.ResultColumns{{Name: "x", Typ: types.Int}}, nil)
						ob.AddField("table", "foo")
						ob.LeaveNode()
					}
					{
						ob.EnterNode("scan", nil, nil) // Columns should show up as "()".
						ob.AddField("table", "bar")
						ob.LeaveNode()
					}
				}
				ob.LeaveNode()
			}
			ob.LeaveNode()
		}
		ob.LeaveNode()
		return ob
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "output"), func(t *testing.T, d *datadriven.TestData) string {
		var flags explain.Flags
		for _, arg := range d.CmdArgs {
			switch arg.Key {
			case "verbose":
				flags.Verbose = true
			case "types":
				flags.Verbose = true
				flags.ShowTypes = true
			default:
				panic(fmt.Sprintf("unknown argument %s", arg.Key))
			}
		}
		ob := example(flags)
		switch d.Cmd {
		case "string":
			return ob.BuildString()

		case "tree":
			treeYaml, err := yaml.Marshal(ob.BuildProtoTree())
			if err != nil {
				panic(err)
			}
			return string(treeYaml)

		default:
			panic(fmt.Sprintf("unknown command %s", d.Cmd))
		}
	})
}

func TestEmptyOutputBuilder(t *testing.T) {
	ob := explain.NewOutputBuilder(explain.Flags{Verbose: true})
	if str := ob.BuildString(); str != "" {
		t.Errorf("expected empty string, got '%s'", str)
	}
	if rows := ob.BuildStringRows(); len(rows) != 0 {
		t.Errorf("expected no rows, got %v", rows)
	}
}

func TestMaxDiskSpillUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	distSQLKnobs := &execinfra.TestingKnobs{}
	distSQLKnobs.ForceDiskSpill = true
	testClusterArgs.ServerArgs.Knobs.DistSQL = distSQLKnobs
	testClusterArgs.ServerArgs.Insecure = true
	serverutils.InitTestServerFactory(server.TestServerFactory)
	tc := testcluster.StartTestCluster(t, 1, testClusterArgs)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]

	_, err := conn.ExecContext(ctx, `
CREATE TABLE t (a PRIMARY KEY, b) AS SELECT i, i FROM generate_series(1, 10) AS g(i)
`)
	assert.NoError(t, err)
	maxDiskUsageRE := regexp.MustCompile(`max sql temp disk usage: (\d+)`)

	queryMatchRE := func(query string, re *regexp.Regexp) bool {
		rows, err := conn.QueryContext(ctx, query)
		assert.NoError(t, err)
		for rows.Next() {
			var res string
			assert.NoError(t, rows.Scan(&res))
			var sb strings.Builder
			sb.WriteString(res)
			sb.WriteByte('\n')
			if matches := re.FindStringSubmatch(res); len(matches) > 0 {
				return true
			}
		}
		return false
	}

	// We are expecting disk spilling to show up because we enabled ForceDiskSpill
	// knob above.
	assert.True(t, queryMatchRE(`EXPLAIN ANALYZE (VERBOSE, DISTSQL) select * from t join t AS x on t.b=x.a`, maxDiskUsageRE), "didn't find max sql temp disk usage: in explain")
	assert.False(t, queryMatchRE(`EXPLAIN ANALYZE (VERBOSE, DISTSQL) select * from t `, maxDiskUsageRE), "found unexpected max sql temp disk usage: in explain")

}

func TestCPUTimeEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "multinode cluster setup times out under stress")
	skip.UnderRace(t, "multinode cluster setup times out under race")

	if !grunning.Supported() {
		return
	}

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	distSQLKnobs := &execinfra.TestingKnobs{}
	distSQLKnobs.ForceDiskSpill = true
	testClusterArgs.ServerArgs.Knobs.DistSQL = distSQLKnobs
	testClusterArgs.ServerArgs.Insecure = true
	const numNodes = 3

	serverutils.InitTestServerFactory(server.TestServerFactory)
	tc := testcluster.StartTestCluster(t, numNodes, testClusterArgs)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(tc.Conns[0])

	runQuery := func(query string, hideCPU bool) {
		rows := db.QueryStr(t, "EXPLAIN ANALYZE "+query)
		var err error
		var foundCPU bool
		var cpuTime time.Duration
		for _, row := range rows {
			if len(row) != 1 {
				t.Fatalf("expected one column")
			}
			if strings.Contains(row[0], "sql cpu time") {
				foundCPU = true
				cpuStr := strings.Split(row[0], " ")
				require.Equal(t, len(cpuStr), 4)
				cpuTime, err = time.ParseDuration(cpuStr[3])
				require.NoError(t, err)
				break
			}
		}
		if hideCPU {
			require.Falsef(t, foundCPU, "expected not to output CPU time for query: %s", query)
		} else {
			require.NotZerof(t, cpuTime, "expected nonzero CPU time for query: %s", query)
		}
	}

	// Mutation queries shouldn't output CPU time.
	runQuery("CREATE TABLE t (x INT PRIMARY KEY, y INT);", true /* hideCPU */)
	runQuery("INSERT INTO t (SELECT t, t%127 FROM generate_series(1, 10000) g(t));", true /* hideCPU */)

	// Split the table across the nodes in order to make the following test cases
	// more interesting.
	for _, stmt := range []string{
		`ALTER TABLE t SPLIT AT VALUES (2500)`,
		`ALTER TABLE t SPLIT AT VALUES (5000)`,
		`ALTER TABLE t SPLIT AT VALUES (7500)`,
		`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1], 2500)`,
		`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[2], 5000)`,
		`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[3], 7500)`,
	} {
		testutils.SucceedsSoon(t, func() error {
			_, err := db.DB.ExecContext(ctx, stmt)
			return err
		})
	}

	runQuery("SELECT * FROM t;", false /* hideCPU */)
	runQuery("SELECT count(*) FROM t;", false /* hideCPU */)
	runQuery("SELECT * FROM (SELECT * FROM t WHERE x > 2000 AND x < 3000) s1 JOIN t ON s1.x = t.x", false /* hideCPU */)
	runQuery("SELECT * FROM (VALUES (1), (2), (3)) v(a) INNER LOOKUP JOIN t ON a = x", false /* hideCPU */)
	runQuery("SELECT count(*) FROM generate_series(1, 100000)", false /* hideCPU */)
}
