// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package execbuilder

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

var (
	testDataGlob = flag.String("d", "testdata/[^.]*", "test data glob")
)

func TestBuild(t *testing.T) {
	defer leaktest.AfterTest(t)()

	paths, err := filepath.Glob(*testDataGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", *testDataGlob)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx := context.Background()
			semaCtx := tree.MakeSemaContext(false /* privileged */)
			evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				switch d.Cmd {
				case "exec-raw":
					_, err := sqlDB.Exec(d.Input)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					return ""

				case "build", "exec", "exec-explain":
					// Parse the SQL.
					stmt, err := parser.ParseOne(d.Input)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					eng := s.Executor().(exec.TestEngineFactory).NewTestEngine()
					defer eng.Close()

					// Build and optimize the opt expression tree.
					o := xform.NewOptimizer(eng.Catalog(), xform.OptimizeAll)
					builder := optbuilder.New(ctx, &semaCtx, &evalCtx, o.Factory(), stmt)
					root, props, err := builder.Build()
					if err != nil {
						d.Fatalf(t, "BuildOpt: %v", err)
					}
					ev := o.Optimize(root, props)

					if d.Cmd == "build" {
						return ev.String()
					}

					// Build the execution node tree.
					node, err := New(eng.Factory(), ev).Build()
					if err != nil {
						d.Fatalf(t, "BuildExec: %v", err)
					}

					// Execute the node tree.
					var results []tree.Datums
					if d.Cmd == "exec-explain" {
						results, err = eng.Explain(node)
					} else {
						results, err = eng.Execute(node)
					}
					if err != nil {
						d.Fatalf(t, "Exec: %v", err)
					}

					// Format the results.
					var buf bytes.Buffer
					tw := tabwriter.NewWriter(
						&buf,
						2,   /* minwidth */
						1,   /* tabwidth */
						2,   /* padding */
						' ', /* padchar */
						0,   /* flags */
					)
					for _, r := range results {
						for j, val := range r {
							if j > 0 {
								fmt.Fprintf(tw, "\t")
							}
							if d, ok := val.(*tree.DString); ok && utf8.ValidString(string(*d)) {
								str := string(*d)
								if str == "" {
									str = "·"
								}
								// Avoid the quotes on strings.
								fmt.Fprintf(tw, "%s", str)
							} else {
								fmt.Fprintf(tw, "%s", val)
							}
						}
						fmt.Fprintf(tw, "\n")
					}
					_ = tw.Flush()
					return buf.String()

				case "catalog":
					// Create the engine in order to get access to its catalog.
					eng := s.Executor().(exec.TestEngineFactory).NewTestEngine()
					defer eng.Close()

					parts := strings.Split(d.Input, ".")
					name := tree.NewTableName(tree.Name(parts[0]), tree.Name(parts[1]))
					tbl, err := eng.Catalog().FindTable(context.Background(), name)
					if err != nil {
						d.Fatalf(t, "Catalog: %v", err)
					}

					tp := treeprinter.New()
					optbase.FormatCatalogTable(tbl, tp)
					return tp.String()

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}
