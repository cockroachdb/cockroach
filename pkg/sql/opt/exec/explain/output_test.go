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
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
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

	datadriven.RunTest(t, "testdata/output", func(t *testing.T, d *datadriven.TestData) string {
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

		case "datums":
			rows := ob.BuildExplainRows()

			var buf bytes.Buffer
			tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
			for _, r := range rows {
				for j := range r {
					if j > 0 {
						fmt.Fprint(tw, "\t")
					}
					fmt.Fprint(tw, tree.AsStringWithFlags(r[j], tree.FmtExport))
				}
				fmt.Fprint(tw, "\n")
			}
			_ = tw.Flush()

			return util.RemoveTrailingSpaces(buf.String())
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
