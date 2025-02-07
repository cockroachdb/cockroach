// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestEval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)

	dir := filepath.Join("../testdata", "eval")
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/sem/eval/testdata/eval")
		if err != nil {
			t.Fatal(err)
		}
		dir = runfile
	}
	walk := func(t *testing.T, getExpr func(*testing.T, *datadriven.TestData) string) {
		datadriven.Walk(t, dir, func(t *testing.T, path string) {
			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
				if d.Cmd != "eval" {
					t.Fatalf("unsupported command %s", d.Cmd)
				}
				return getExpr(t, d) + "\n"
			})
		})
	}

	// The opt and no-opt tests don't do an end-to-end SQL test. Do that
	// here by executing a SELECT. In order to make the output be the same
	// we have to also figure out what the expected output type is so we
	// can correctly format the datum.
	t.Run("sql", func(t *testing.T) {
		defer log.Scope(t).Close(t)

		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		walk(t, func(t *testing.T, d *datadriven.TestData) string {
			var res gosql.NullString
			if err := sqlDB.QueryRow(fmt.Sprintf("SELECT (%s)::STRING", d.Input)).Scan(&res); err != nil {
				return strings.TrimPrefix(err.Error(), "pq: ")
			}
			if !res.Valid {
				return "NULL"
			}

			// We have a non-null result. We can't just return
			// res.String here because these strings don't
			// match the datum.String() representations. For
			// example, a bitarray has a res.String of something
			// like `1001001` but the datum representation is
			// `B'1001001'`. Thus we have to parse res.String (a
			// SQL result) back into a datum and return that.

			expr, err := parser.ParseExpr(d.Input)
			if err != nil {
				t.Fatal(err)
			}
			// expr.TypeCheck to avoid constant folding.
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typedExpr, err := expr.TypeCheck(ctx, &semaCtx, types.AnyElement)
			if err != nil {
				// An error here should have been found above by QueryRow.
				t.Fatal(err)
			}

			switch typedExpr.ResolvedType().Family() {
			case types.TupleFamily:
				// ParseAndRequireString doesn't handle tuples, so we have to convert them ourselves.
				var datums tree.Datums
				// Fetch the original expression's tuple values.
				tuple := typedExpr.(*tree.Tuple)
				for i, s := range strings.Split(res.String[1:len(res.String)-1], ",") {
					if s == "" {
						continue
					}
					// Figure out the type of the tuple value.
					expr, err := tuple.Exprs[i].TypeCheck(ctx, &semaCtx, types.AnyElement)
					if err != nil {
						t.Fatal(err)
					}
					// Now parse the new string as the expected type.
					datum, _, err := tree.ParseAndRequireString(expr.ResolvedType(), s, evalCtx)
					if err != nil {
						t.Errorf("%s: %s", err, s)
						return err.Error()
					}
					datums = append(datums, datum)
				}
				return tree.NewDTuple(typedExpr.ResolvedType(), datums...).String()
			}
			datum, _, err := tree.ParseAndRequireString(typedExpr.ResolvedType(), res.String, evalCtx)
			if err != nil {
				t.Errorf("%s: %s", err, res.String)
				return err.Error()
			}
			return datum.String()
		})
	})

	t.Run("vectorized", func(t *testing.T) {
		rng, _ := randutil.NewTestRand()
		var monitorRegistry colexecargs.MonitorRegistry
		defer monitorRegistry.Close(ctx)
		var closerRegistry colexecargs.CloserRegistry
		defer closerRegistry.Close(ctx)
		walk(t, func(t *testing.T, d *datadriven.TestData) string {
			st := cluster.MakeTestingClusterSettings()
			flowCtx := &execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: evalCtx,
				Mon:     evalCtx.TestingMon,
			}
			expr, err := parser.ParseExpr(d.Input)
			require.NoError(t, err)
			if _, ok := expr.(*tree.RangeCond); ok {
				// RangeCond gets normalized to comparison expressions and its Eval
				// method returns an error, so skip it for execution.
				return strings.TrimSpace(d.Expected)
			}
			semaCtx := tree.MakeSemaContext(nil /* resolver */)
			typedExpr, err := expr.TypeCheck(ctx, &semaCtx, types.AnyElement)
			if err != nil {
				// Skip this test as it's testing an expected error which would be
				// caught before execution.
				return strings.TrimSpace(d.Expected)
			}

			batchesReturned := 0
			args := &colexecargs.NewColOperatorArgs{
				Spec: &execinfrapb.ProcessorSpec{
					Input: []execinfrapb.InputSyncSpec{{}},
					Core: execinfrapb.ProcessorCoreUnion{
						Noop: &execinfrapb.NoopCoreSpec{},
					},
					Post: execinfrapb.PostProcessSpec{
						RenderExprs: []execinfrapb.Expression{{LocalExpr: typedExpr}},
					},
					ResultTypes: []*types.T{typedExpr.ResolvedType()},
				},
				Inputs: []colexecargs.OpWithMetaInfo{{
					Root: &colexecop.CallbackOperator{
						NextCb: func() coldata.Batch {
							if batchesReturned > 0 {
								return coldata.ZeroBatch
							}
							// It doesn't matter what types we create the input
							// batch with.
							batch := coldata.NewMemBatch([]*types.T{}, coldata.StandardColumnFactory)
							batch.SetLength(1)
							batchesReturned++
							return batch
						}},
				}},
				// Unsupported post-processing specs are wrapped and run through
				// the row execution engine.
				ProcessorConstructor: rowexec.NewProcessor,
				MonitorRegistry:      &monitorRegistry,
				CloserRegistry:       &closerRegistry,
			}
			// If the expression is of the boolean type, in 50% cases we'll
			// additionally run it as a filter (i.e. as a "selection" operator
			// as opposed to a "projection").
			var doFilter bool
			if typedExpr.ResolvedType().Family() == types.BoolFamily && rng.Float64() < 0.5 {
				doFilter = true
				args.Spec.Core.Noop = nil
				args.Spec.Core.Filterer = &execinfrapb.FiltererSpec{
					Filter: execinfrapb.Expression{LocalExpr: typedExpr},
				}
			}
			result, err := colbuilder.NewColOperator(ctx, flowCtx, args)
			require.NoError(t, err)

			mat := colexec.NewMaterializer(
				nil, /* streamingMemAcc */
				flowCtx,
				0, /* processorID */
				result.OpWithMetaInfo,
				[]*types.T{typedExpr.ResolvedType()},
			)

			mat.Start(ctx)
			row, meta := mat.Next()
			if meta != nil {
				if meta.Err != nil {
					return fmt.Sprint(meta.Err)
				}
				t.Fatalf("unexpected metadata: %+v", meta)
			}
			if doFilter {
				switch strings.ToLower(strings.TrimSpace(d.Expected)) {
				case "true":
					if row == nil {
						t.Fatalf("the row should not be filtered out: %s", d.Input)
					}
				case "false", "null":
					if row != nil {
						t.Fatalf("the row should be filtered out: %s", d.Input)
					}
					// The row is filtered out, so we just return the expected
					// output here.
					return strings.TrimSpace(d.Expected)
				default:
					t.Fatalf("unexpected bool value: %s", d.Expected)
				}
			} else if row == nil {
				t.Fatal("unexpected end of input")
			}
			return row[0].Datum.String()
		})
	})
}
