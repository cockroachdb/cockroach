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
	"fmt"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	yaml "gopkg.in/yaml.v2"
)

func ExampleOutputBuilder() {
	example := func(name string, flags explain.Flags) {
		ob := explain.NewOutputBuilder(flags)
		ob.AddField("distributed", "true")
		ob.EnterMetaNode("meta")
		{
			ob.EnterNode(
				"render",
				sqlbase.ResultColumns{{Name: "a", Typ: types.Int}, {Name: "b", Typ: types.String}},
				sqlbase.ColumnOrdering{
					{ColIdx: 0, Direction: encoding.Ascending},
					{ColIdx: 1, Direction: encoding.Descending},
				},
			)
			ob.AddField("render 0", "foo")
			ob.AddField("render 1", "bar")
			{
				ob.EnterNode("join", sqlbase.ResultColumns{{Name: "x", Typ: types.Int}}, nil)
				ob.AddField("type", "outer")
				{
					{
						ob.EnterNode("scan", sqlbase.ResultColumns{{Name: "x", Typ: types.Int}}, nil)
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

		fmt.Printf("-- %s (datums) --\n", name)
		fmt.Print(util.RemoveTrailingSpaces(buf.String()))

		fmt.Printf("\n-- %s (string) --\n", name)
		fmt.Print(ob.BuildString())

		treeYaml, err := yaml.Marshal(ob.BuildProtoTree())
		if err != nil {
			panic(err)
		}
		fmt.Printf("\n-- %s (tree) --\n%s\n", name, treeYaml)
	}

	example("basic", explain.Flags{})
	example("verbose", explain.Flags{Verbose: true})
	example("verbose+types", explain.Flags{Verbose: true, ShowTypes: true})

	// Output:
	// -- basic (datums) --
	//                      distributed  true
	// meta
	//  └── render
	//       │              render 0     foo
	//       │              render 1     bar
	//       └── join
	//            │         type         outer
	//            ├── scan
	//            │         table        foo
	//            └── scan
	//                      table        bar
	//
	// -- basic (string) --
	//                      distributed  true
	// meta
	//  └── render
	//       │              render 0     foo
	//       │              render 1     bar
	//       └── join
	//            │         type         outer
	//            ├── scan
	//            │         table        foo
	//            └── scan
	//                      table        bar
	//
	// -- basic (tree) --
	// name: meta
	// attrs: []
	// children:
	// - name: render
	//   attrs:
	//   - key: render 0
	//     value: foo
	//   - key: render 1
	//     value: bar
	//   children:
	//   - name: join
	//     attrs:
	//     - key: type
	//       value: outer
	//     children:
	//     - name: scan
	//       attrs:
	//       - key: table
	//         value: foo
	//       children: []
	//     - name: scan
	//       attrs:
	//       - key: table
	//         value: bar
	//       children: []
	//
	// -- verbose (datums) --
	//                      0          distributed  true
	// meta                 0  meta
	//  └── render          1  render                      (a, b)  +a,-b
	//       │              1          render 0     foo
	//       │              1          render 1     bar
	//       └── join       2  join                        (x)
	//            │         2          type         outer
	//            ├── scan  3  scan                        (x)
	//            │         3          table        foo
	//            └── scan  3  scan                        ()
	//                      3          table        bar
	//
	// -- verbose (string) --
	//                      distributed  true
	// meta
	//  └── render                              (a, b)  +a,-b
	//       │              render 0     foo
	//       │              render 1     bar
	//       └── join                           (x)
	//            │         type         outer
	//            ├── scan                      (x)
	//            │         table        foo
	//            └── scan                      ()
	//                      table        bar
	//
	// -- verbose (tree) --
	// name: meta
	// attrs: []
	// children:
	// - name: render
	//   attrs:
	//   - key: render 0
	//     value: foo
	//   - key: render 1
	//     value: bar
	//   children:
	//   - name: join
	//     attrs:
	//     - key: type
	//       value: outer
	//     children:
	//     - name: scan
	//       attrs:
	//       - key: table
	//         value: foo
	//       children: []
	//     - name: scan
	//       attrs:
	//       - key: table
	//         value: bar
	//       children: []
	//
	// -- verbose+types (datums) --
	//                      0          distributed  true
	// meta                 0  meta
	//  └── render          1  render                      (a int, b string)  +a,-b
	//       │              1          render 0     foo
	//       │              1          render 1     bar
	//       └── join       2  join                        (x int)
	//            │         2          type         outer
	//            ├── scan  3  scan                        (x int)
	//            │         3          table        foo
	//            └── scan  3  scan                        ()
	//                      3          table        bar
	//
	// -- verbose+types (string) --
	//                      distributed  true
	// meta
	//  └── render                              (a int, b string)  +a,-b
	//       │              render 0     foo
	//       │              render 1     bar
	//       └── join                           (x int)
	//            │         type         outer
	//            ├── scan                      (x int)
	//            │         table        foo
	//            └── scan                      ()
	//                      table        bar
	//
	// -- verbose+types (tree) --
	// name: meta
	// attrs: []
	// children:
	// - name: render
	//   attrs:
	//   - key: render 0
	//     value: foo
	//   - key: render 1
	//     value: bar
	//   children:
	//   - name: join
	//     attrs:
	//     - key: type
	//       value: outer
	//     children:
	//     - name: scan
	//       attrs:
	//       - key: table
	//         value: foo
	//       children: []
	//     - name: scan
	//       attrs:
	//       - key: table
	//         value: bar
	//       children: []
}
