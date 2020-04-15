// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxconstraint_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// The test files support only one command:
//
//   - inverted-index-constraints [arg | arg=val | arg=(val1,val2, ...)]...
//
//   Takes a scalar expression and computes inverted index constraints.
//   Arguments:
//
//     - vars=<type>
//
//       Sets the types for the inverted index column in the expression.
//
//     - inverted-col=@<index>
//
//       The one column of the inverted index refers to an index var.
//
func TestInvertedIndexConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/inverted", func(t *testing.T, path string) {
		semaCtx := tree.MakeSemaContext()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var varTypes []*types.T
			var indexCol string
			var err error

			// Parse the args.
			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				switch key {
				case "vars":
					varTypes, err = exprgen.ParseTypes(vals)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
				case "index-col":
					indexCol = vals[0]
				default:
					d.Fatalf(t, "unknown argument: %s", key)
				}
			}

			// Build a string of the columns and types to be used in the CREATE TABLE statement.
			var columns strings.Builder
			for i, typ := range varTypes {
				columns.WriteString(fmt.Sprintf("\"@%d\" %s,", i+1, typ))
			}

			// Create a test table with a column of varType and an inverted
			// index.
			catalog := testcat.New()
			_, err = catalog.ExecuteDDL(fmt.Sprintf("CREATE TABLE t (%s INVERTED INDEX (\"%s\"))", columns.String(), indexCol))
			if err != nil {
				t.Fatal(err)
			}

			var f norm.Factory
			f.Init(&evalCtx, catalog)
			md := f.Metadata()

			// Fetch the tableID.
			tn := tree.NewUnqualifiedTableName("t")
			tabID := md.AddTable(catalog.Table(tn), tn)
			md.Table(tabID)

			// Fetch the index.
			// Index 1 is the inverted index (0 is the primary index).
			index := md.Table(tabID).Index(1)

			iVarHelper := tree.MakeTypesOnlyIndexedVarHelper(varTypes)

			switch d.Cmd {
			case "inverted-index-constraints":
				var filters memo.FiltersExpr
				if filters, err = buildFilters(d.Input, &semaCtx, &evalCtx, &f); err != nil {
					d.Fatalf(t, "%v", err)
				}

				// Initialize the iterator.
				var iter idxconstraint.InvertedIndexConstraintIter
				iter.Init(index, tabID, filters, &f, &evalCtx)

				var buf bytes.Buffer

				// Fetch the first constraint.
				// TODO(mgartner): Print and verify all possible constraints in test output.
				ok := iter.Next()
				if !ok {
					return "No contraints"
				}

				// Append text representing the first constraint.
				result := iter.Constraint()
				for i := 0; i < result.Spans.Count(); i++ {
					fmt.Fprintf(&buf, "%s\n", result.Spans.Get(i))
				}

				// Append text representing the remaining filters.
				remainingFilter := iter.RemainingFilters()
				if !remainingFilter.IsTrue() {
					execBld := execbuilder.New(nil, f.Memo(), catalog, &remainingFilter, &evalCtx)
					expr, err := execBld.BuildScalar(&iVarHelper)
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					fmt.Fprintf(&buf, "Remaining filter: %s\n", expr)
				}

				return buf.String()

			default:
				d.Fatalf(t, "unsupported command: %s", d.Cmd)
				return ""
			}
		})
	})
}
