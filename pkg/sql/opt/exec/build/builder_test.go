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

package build

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"text/tabwriter"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	optbuild "github.com/cockroachdb/cockroach/pkg/sql/opt/build"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestLogicalProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	catalog := testCatalog{kvDB: kvDB}

	datadriven.RunTest(t, "testdata/build", func(d *datadriven.TestData) string {
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

			// Build and optimize the opt expression tree.
			o := xform.NewOptimizer(catalog, 0 /* maxSteps */)
			root, props, err := optbuild.NewBuilder(ctx, o.Factory(), stmt).Build()
			if err != nil {
				d.Fatalf(t, "BuildOpt: %v", err)
			}
			ev := o.Optimize(root, props)

			if d.Cmd == "build" {
				return ev.String()
			}

			// Build the execution node tree.
			eng := s.Executor().(*sql.Executor).NewExecEngine()
			defer eng.Close()
			node, err := NewBuilder(eng.Factory(), ev).Build()
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

		default:
			d.Fatalf(t, "unsupported command: %s", d.Cmd)
			return ""
		}
	})
}

// testCatalog implements the sqlbase.Catalog interface.
type testCatalog struct {
	kvDB *client.DB
}

// FindTable implements the sqlbase.Catalog interface.
func (c testCatalog) FindTable(ctx context.Context, name *tree.TableName) (optbase.Table, error) {
	return sqlbase.GetTableDescriptor(c.kvDB, string(name.SchemaName), string(name.TableName)), nil
}
