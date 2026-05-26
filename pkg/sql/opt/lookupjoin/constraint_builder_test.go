// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lookupjoin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/lookupjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	tu "github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// The test files support only one command:
//
//   - lookup-constraints [arg | arg=val | arg=(val1,val2, ...)]...
//
//   Builds lookup join constrains for the given left and right table
//   definitions, and an index for the right table.Arguments:
//
//     - left=(<column> <type> [not null] [as <expr> [stored|virtual], ...)
//
//       Information about the left columns.

// - right=(<column> <type> [not null] [as <expr> [stored|virtual], ...)
//
//	Information about the left columns.
//
// - index=(<column> [asc|desc], ...)
//
//	Information for the index on the right table.
func TestLookupConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, tu.TestDataPath(t), func(t *testing.T, path string) {
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext(nil /* resolver */)
		evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
		evalCtx.SessionData().VariableInequalityLookupJoinEnabled = true

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			testCatalog := testcat.New()
			var f norm.Factory
			f.Init(ctx, &evalCtx, testCatalog)
			md := f.Metadata()

			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				cols := strings.Join(vals, ", ")
				switch key {
				case "left", "right":
					fmt.Printf("CREATE TABLE %s (%s)\n", key, cols)
					_, err := testCatalog.ExecuteDDL(fmt.Sprintf("CREATE TABLE %s_table (%s)", key, cols))
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

				case "index":
					_, err := testCatalog.ExecuteDDL(fmt.Sprintf("CREATE INDEX ON right_table (%s)", cols))
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

				default:
					d.Fatalf(t, "unknown argument: %s", key)
				}
			}

			addTable := func(name string) (_ opt.TableID, cols opt.ColSet, _ error) {
				tn := tree.NewUnqualifiedTableName(tree.Name(name))
				table := testCatalog.Table(tn)
				tableID := md.AddTable(table, tn)
				for i, n := 0, md.Table(tableID).ColumnCount(); i < n; i++ {
					colID := tableID.ColumnID(i)
					cols.Add(colID)
					col := md.Table(tableID).Column(i)
					if col.IsComputed() {
						expr, err := parser.ParseExpr(col.ComputedExprStr())
						if err != nil {
							return 0, opt.ColSet{}, err
						}
						b := optbuilder.NewScalar(context.Background(), &semaCtx, &evalCtx, &f)
						compExpr, err := b.Build(expr)
						if err != nil {
							return 0, opt.ColSet{}, err
						}
						var sharedProps props.Shared
						memo.BuildSharedProps(compExpr, &sharedProps, &evalCtx)
						md.TableMeta(tableID).AddComputedCol(colID, compExpr, sharedProps.OuterCols)
					}
				}
				return tableID, cols, nil
			}

			_, leftCols, err := addTable("left_table")
			if err != nil {
				t.Fatal(err)
			}
			rightTable, rightCols, err := addTable("right_table")
			if err != nil {
				t.Fatal(err)
			}
			index := md.Table(rightTable).Index(1)
			allCols := leftCols.Union(rightCols)

			switch d.Cmd {
			case "lookup-constraints":
				// Allow specifying optional filters using the "optional:" delimiter.
				var filters, optionalFilters memo.FiltersExpr
				var err error
				if idx := strings.Index(d.Input, "optional:"); idx >= 0 {
					optional := d.Input[idx+len("optional:"):]
					optionalFilters, err = makeFilters(optional, allCols, &semaCtx, &evalCtx, &f)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					d.Input = d.Input[:idx]
				}
				if filters, err = makeFilters(d.Input, allCols, &semaCtx, &evalCtx, &f); err != nil {
					d.Fatalf(t, "%v", err)
				}

				var cb lookupjoin.ConstraintBuilder
				cb.Init(ctx, &f, md, f.EvalContext(), rightTable, leftCols, rightCols)

				lookupConstraint, _ := cb.Build(index, filters, optionalFilters,
					memo.FiltersExpr{} /* derivedFkOnFilters */)
				var b strings.Builder
				if lookupConstraint.IsUnconstrained() {
					b.WriteString("lookup join not possible")
				}
				if len(lookupConstraint.KeyCols) > 0 {
					b.WriteString("key cols:\n")
					for i := range lookupConstraint.KeyCols {
						b.WriteString("  ")
						b.WriteString(string(index.Column(i).ColName()))
						b.WriteString(" = ")
						b.WriteString(md.ColumnMeta(lookupConstraint.KeyCols[i]).Alias)
						b.WriteString("\n")
					}
				}
				if len(lookupConstraint.InputProjections) > 0 {
					b.WriteString("input projections:\n")
					for i := range lookupConstraint.InputProjections {
						col := lookupConstraint.InputProjections[i].Col
						colMeta := md.ColumnMeta(col)
						b.WriteString("  ")
						b.WriteString(colMeta.Alias)
						b.WriteString(" = ")
						b.WriteString(formatScalar(lookupConstraint.InputProjections[i].Element, &f, &semaCtx, &evalCtx))
						b.WriteString(" [type=")
						b.WriteString(colMeta.Type.SQLString())
						b.WriteString("]\n")
					}
				}
				if len(lookupConstraint.LookupExpr) > 0 {
					b.WriteString("lookup expression:\n  ")
					b.WriteString(formatScalar(&lookupConstraint.LookupExpr, &f, &semaCtx, &evalCtx))
					b.WriteString("\n")
				}
				if len(lookupConstraint.RemainingFilters) > 0 {
					b.WriteString("remaining filters:\n  ")
					b.WriteString(formatScalar(&lookupConstraint.RemainingFilters, &f, &semaCtx, &evalCtx))
					b.WriteString("\n")
				}
				return b.String()

			default:
				d.Fatalf(t, "unsupported command: %s", d.Cmd)
				return ""
			}
		})
	})
}

func TestIsCanonicalFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fb := makeFilterBuilder(t)

	tests := []struct {
		name   string
		filter string
		want   bool
	}{
		// Test that True, False, Null values are hit as const.
		{name: "eq-int",
			filter: "i = 10",
			want:   true,
		},
		{name: "neq-int",
			filter: "i != 10",
			want:   false,
		},
		{name: "eq-null",
			filter: "i = NULL",
			want:   true,
		},
		{name: "eq-true",
			filter: "b = TRUE",
			want:   true,
		},
		{name: "in-tuple",
			filter: "i IN (1,2)",
			want:   true,
		},
		{name: "and-eq-lt",
			filter: "i = 9 AND i < 10",
			want:   false,
		},
		{name: "or-eq-lt",
			filter: "i = 10 OR i < 10",
			want:   false,
		},
		{name: "and-in-lt",
			filter: "i IN (10, 20, 30) AND i > 10",
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := fb.buildFilter(tt.filter)
			if got := lookupjoin.TestingIsCanonicalLookupJoinFilter(filter); got != tt.want {
				t.Errorf("isCanonicalFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// makeFilters returns a FiltersExpr generated from the input string that is
// normalized within the context of a Select. By normalizing within a Select,
// rules that only match on Selects are applied, such as SimplifySelectFilters.
// This ensures that these test filters mimic the filters that will be created
// during a real query.
// TODO(mgartner): This function is a duplicate of one in the partialidx_test
// package. Extract this to a utility that can be used in both packages.
func makeFilters(
	input string, cols opt.ColSet, semaCtx *tree.SemaContext, evalCtx *eval.Context, f *norm.Factory,
) (memo.FiltersExpr, error) {
	filters, err := makeFiltersExpr(input, semaCtx, evalCtx, f)
	if err != nil {
		return nil, err
	}

	// Create an output set of columns for the fake relation with all the
	// columns in the test case.
	colStatsMap := props.ColStatsMap{}
	cols.ForEach(func(col opt.ColumnID) {
		colStat, _ := colStatsMap.Add(opt.MakeColSet(col))
		colStat.DistinctCount = 100
		colStat.NullCount = 10
	})

	// Create a non-zero cardinality to prevent the fake Select from
	// simplifying into a ValuesExpr.
	card := props.Cardinality{Min: 0, Max: 1}

	// Create stats for the fake relation.
	stats := props.Statistics{
		Available:   true,
		RowCount:    1000,
		ColStats:    colStatsMap,
		Selectivity: props.OneSelectivity,
	}

	// Create a fake Select and input so that normalization rules are run.
	p := &props.Relational{OutputCols: cols, Cardinality: card}
	*p.Statistics() = stats
	fakeRel := f.ConstructFakeRel(&memo.FakeRelPrivate{Props: p})
	sel := f.ConstructSelect(fakeRel, filters)

	// If the normalized relational expression is a Select, return the filters.
	if s, ok := sel.(*memo.SelectExpr); ok {
		return s.Filters, nil
	}

	// Otherwise, the filters may be either true or false. Check the cardinality
	// to determine which one.
	if sel.Relational().Cardinality.IsZero() {
		return memo.FiltersExpr{f.ConstructFiltersItem(memo.FalseSingleton)}, nil
	}

	return memo.TrueFilter, nil
}

// makeFiltersExpr returns a FiltersExpr generated from the input string.
func makeFiltersExpr(
	input string, semaCtx *tree.SemaContext, evalCtx *eval.Context, f *norm.Factory,
) (memo.FiltersExpr, error) {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		return nil, err
	}

	b := optbuilder.NewScalar(context.Background(), semaCtx, evalCtx, f)
	root, err := b.Build(expr)
	if err != nil {
		return nil, err
	}

	return memo.FiltersExpr{f.ConstructFiltersItem(root)}, nil
}

func formatScalar(
	e opt.Expr, f *norm.Factory, semaCtx *tree.SemaContext, evalCtx *eval.Context,
) string {
	execBld := execbuilder.New(
		context.Background(), nil /* execFactory */, nil, /* optimizer */
		f.Memo(), nil /* catalog */, e, semaCtx, evalCtx,
		false /* allowAutoCommit */, false, /* isANSIDML */
	)
	expr, err := execBld.BuildScalar()
	if err != nil {
		return fmt.Sprintf("error: %v\n", err)
	}
	fmtCtx := tree.NewFmtCtx(
		tree.FmtSimple,
		tree.FmtIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			ctx.WriteString(f.Metadata().ColumnMeta(opt.ColumnID(idx + 1)).Alias)
		}),
	)
	expr.Format(fmtCtx)
	return fmtCtx.String()
}

type testFilterBuilder struct {
	t       *testing.T
	semaCtx *tree.SemaContext
	evalCtx *eval.Context
	f       *norm.Factory
	tbl     opt.TableID
}

func makeFilterBuilder(t *testing.T) testFilterBuilder {
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var f norm.Factory
	f.Init(context.Background(), &evalCtx, nil)
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (i INT PRIMARY KEY, b BOOL)"); err != nil {
		t.Fatal(err)
	}
	tn := tree.NewTableNameWithSchema("t", catconstants.PublicSchemaName, "a")
	tbl := f.Metadata().AddTable(cat.Table(tn), tn)
	return testFilterBuilder{
		t:       t,
		semaCtx: &tree.SemaContext{},
		evalCtx: &evalCtx,
		// o:       &o,
		f:   &f,
		tbl: tbl,
	}
}

func (fb *testFilterBuilder) buildFilter(str string) memo.FiltersItem {
	return testutils.BuildFilters(fb.t, fb.f, fb.semaCtx, fb.evalCtx, str)[0]
}
