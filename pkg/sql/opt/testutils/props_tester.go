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

package testutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"gopkg.in/yaml.v2"
)

// PropsTester is a helper for testing the logical properties and statistics
// of expressions.
//
// The PropsTester is currently used by tests in the memo package.
type PropsTester struct {
	Flags PropsTesterFlags

	catalog opt.Catalog
	input   string
	ctx     context.Context
	semaCtx tree.SemaContext
	evalCtx tree.EvalContext
}

// NewPropsTester constructs a new instance of the PropsTester.
func NewPropsTester(catalog opt.Catalog, input string) *PropsTester {
	return &PropsTester{
		catalog: catalog,
		input:   input,
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(false /* privileged */),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}
}

// PropsTesterFlags are control knobs for tests. Note that specific testcases can
// override these defaults.
type PropsTesterFlags struct {
	// ExprFormat controls the output detail of the props command.
	ExprFormat memo.ExprFmtFlags

	// Operator controls which operator is built with the props command.
	Operator opt.Operator
}

// RunCommand implements the following commands:
//
//  - exec-ddl
//
//    Runs a SQL DDL statement to build the test catalog. Only a small number
//    of DDL statements are supported, and those not fully. This is only
//    available when using a TestCatalog.
//
//  - props [flags]
//
//    Builds logical properties for a relational expression based on child
//    properties.
//
//
// Supported flags:
//
//  - format: controls the formatting of expressions for the props command.
//    Possible values: show-all, hide-all, or any combination of hide-cost,
//    hide-stats, hide-constraints, hide-scalars, hide-qual.
//    For example:
//      props operator=scan format=(hide-cost,hide-stats)
//
//  - operator: used with props; the value is the name of a relational
//    operator.
//    For example:
//      props operator=scan
//
func (pt *PropsTester) RunCommand(tb testing.TB, d *datadriven.TestData) string {
	// Allow testcases to override the flags.
	for _, a := range d.CmdArgs {
		if err := pt.Flags.Set(a); err != nil {
			d.Fatalf(tb, "%s", err)
		}
	}

	switch d.Cmd {
	case "exec-ddl":
		testCatalog, ok := pt.catalog.(*testcat.Catalog)
		if !ok {
			d.Fatalf(tb, "exec-ddl can only be used with TestCatalog")
		}
		s, err := testCatalog.ExecuteDDL(d.Input)
		if err != nil {
			d.Fatalf(tb, "%v", err)
		}
		return s

	case "props":
		switch pt.Flags.Operator {
		case opt.ScanOp:
			scan := pt.buildScan(tb, d)
			return memo.FormatExpr(scan, pt.Flags.ExprFormat)

		case opt.ProjectOp:
			project := pt.buildProject(tb, d)
			return memo.FormatExpr(project, pt.Flags.ExprFormat)

		default:
			d.Fatalf(tb, "unsupported operator: %s", pt.Flags.Operator)
			return ""
		}

	default:
		d.Fatalf(tb, "unsupported command: %s", d.Cmd)
		return ""
	}
}

// Set parses an argument that refers to a flag.
// See PropsTester.RunCommand for supported flags.
func (f *PropsTesterFlags) Set(arg datadriven.CmdArg) error {
	switch arg.Key {
	case "format":
		f.ExprFormat = 0
		if len(arg.Vals) == 0 {
			return fmt.Errorf("format flag requires value(s)")
		}
		for _, v := range arg.Vals {
			m := map[string]memo.ExprFmtFlags{
				"show-all":         memo.ExprFmtShowAll,
				"hide-all":         memo.ExprFmtHideAll,
				"hide-stats":       memo.ExprFmtHideStats,
				"hide-cost":        memo.ExprFmtHideCost,
				"hide-constraints": memo.ExprFmtHideConstraints,
				"hide-ruleprops":   memo.ExprFmtHideRuleProps,
				"hide-scalars":     memo.ExprFmtHideScalars,
				"hide-qual":        memo.ExprFmtHideQualifications,
			}
			if val, ok := m[v]; ok {
				f.ExprFormat |= val
			} else {
				return fmt.Errorf("unknown format value %s", v)
			}
		}

	case "operator":
		if len(arg.Vals) != 1 {
			return fmt.Errorf("operator requires one argument")
		}
		var err error
		f.Operator, err = operatorFromString(arg.Vals[0])
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unknown argument: %s", arg.Key)
	}
	return nil
}

type sharedPropsTestSpec struct {
	OuterCols             []int
	HasSubquery           bool
	HasCorrelatedSubquery bool
	CanHaveSideEffects    bool
	HasPlaceholder        bool
}

func (pt *PropsTester) buildSharedProps(spec *sharedPropsTestSpec) props.Shared {
	return props.Shared{
		OuterCols:             toColSet(spec.OuterCols),
		HasSubquery:           spec.HasSubquery,
		HasCorrelatedSubquery: spec.HasCorrelatedSubquery,
		CanHaveSideEffects:    spec.CanHaveSideEffects,
		HasPlaceholder:        spec.HasPlaceholder,
	}
}

type scalarPropsTestSpec struct {
	Shared           sharedPropsTestSpec
	Constraints      []string
	TightConstraints bool
	// TODO(rytaft): add other properties.
}

func (pt *PropsTester) buildScalarProps(spec *scalarPropsTestSpec) props.Scalar {
	cs := constraint.Unconstrained
	for i := range spec.Constraints {
		c := constraint.ParseConstraint(&pt.evalCtx, spec.Constraints[i])
		cs = cs.Intersect(&pt.evalCtx, constraint.SingleConstraint(&c))
	}
	return props.Scalar{
		Shared:           pt.buildSharedProps(&spec.Shared),
		Populated:        true,
		Constraints:      cs,
		TightConstraints: spec.TightConstraints,
	}
}

type relPropsTestSpec struct {
	Tables      []string
	Shared      sharedPropsTestSpec
	OutputCols  []int
	NotNullCols []int
	Cardinality props.Cardinality
	// TODO(rytaft): add other properties.
}

func (pt *PropsTester) buildFakeRelExpr(
	tb testing.TB, d *datadriven.TestData, mem *memo.Memo, spec *relPropsTestSpec,
) memo.RelExpr {
	// Resolve table names and add to metadata.
	for _, tab := range spec.Tables {
		pt.addTable(tb, d, mem, tab)
	}

	// Build the fake relational expression with the given relational properties.
	return NewFakeRelExpr(props.Relational{
		Shared:      pt.buildSharedProps(&spec.Shared),
		OutputCols:  toColSet(spec.OutputCols),
		NotNullCols: toColSet(spec.NotNullCols),
		Cardinality: spec.Cardinality,
	})
}

type scanPropsTestSpec struct {
	Table      string
	Index      int
	Cols       []int
	Constraint string
	HardLimit  int
	Flags      memo.ScanFlags
}

func (pt *PropsTester) buildScan(tb testing.TB, d *datadriven.TestData) *memo.ScanExpr {
	mem := &memo.Memo{}
	mem.Init(&pt.evalCtx)

	// Unmarshall the YAML input into the spec.
	var spec scanPropsTestSpec
	if err := yaml.UnmarshalStrict([]byte(d.Input), &spec); err != nil {
		d.Fatalf(tb, "%s", err)
	}

	// Resolve table name and add table to metadata.
	table, tabID := pt.addTable(tb, d, mem, spec.Table)

	// Parse constraint.
	var scanConstraint *constraint.Constraint
	if spec.Constraint != "" {
		c := constraint.ParseConstraint(&pt.evalCtx, spec.Constraint)
		scanConstraint = &c
	}

	// Parse columns.
	cols := toColSet(spec.Cols)
	if spec.Cols == nil {
		for i := 0; i < table.ColumnCount(); i++ {
			colID := tabID.ColumnID(i)
			cols.Add(int(colID))
		}
	}

	// Build the scan private.
	private := &memo.ScanPrivate{
		Table:      tabID,
		Index:      spec.Index,
		Cols:       cols,
		Constraint: scanConstraint,
		HardLimit:  memo.ScanLimit(spec.HardLimit),
		Flags:      spec.Flags,
	}

	return mem.MemoizeScan(private)
}

type projectPropsTestSpec struct {
	Input       relPropsTestSpec
	Projections []scalarPropsTestSpec
	Passthrough []int
}

func (pt *PropsTester) buildProject(tb testing.TB, d *datadriven.TestData) *memo.ProjectExpr {
	mem := &memo.Memo{}
	mem.Init(&pt.evalCtx)

	// Unmarshall the YAML input into the spec.
	var spec projectPropsTestSpec
	if err := yaml.UnmarshalStrict([]byte(d.Input), &spec); err != nil {
		d.Fatalf(tb, "%s", err)
	}

	// Populate the input expression with relational properties.
	input := pt.buildFakeRelExpr(tb, d, mem, &spec.Input)

	// Populate the projections expressions with scalar properties.
	projections := make(memo.ProjectionsExpr, len(spec.Projections))
	for i := range spec.Projections {
		projections[i].Element = NewFakeScalarExpr(types.Any)
		projections[i].Col = mem.Metadata().AddColumn("", types.Any)
		scalarProps := projections[i].ScalarProps(mem)
		*scalarProps = pt.buildScalarProps(&spec.Projections[i])
	}

	// Parse passthrough columns.
	passthrough := toColSet(spec.Passthrough)

	return mem.MemoizeProject(input, projections, passthrough)
}

// addTable resolves the given table name and adds it to the metadata in the
// given memo.
func (pt *PropsTester) addTable(
	tb testing.TB, d *datadriven.TestData, mem *memo.Memo, tabName string,
) (opt.Table, opt.TableID) {
	ds, err := pt.catalog.ResolveDataSource(
		pt.ctx, tree.NewUnqualifiedTableName(tree.Name(tabName)),
	)
	if err != nil {
		d.Fatalf(tb, "%v", err)
	}

	table, ok := ds.(opt.Table)
	if !ok {
		d.Fatalf(tb, "%s is not a table", tabName)
	}

	tabID := mem.Metadata().AddTable(table)
	return table, tabID
}

// operatorFromString returns the operator that matches the given string,
// or UnknownOp if there is no such operator.
func operatorFromString(str string) (opt.Operator, error) {
	for i := opt.Operator(1); i < opt.NumOperators; i++ {
		if i.String() == str {
			return i, nil
		}
	}

	return opt.UnknownOp, fmt.Errorf("operator '%s' does not exist", str)
}

func toColSet(input []int) opt.ColSet {
	var res opt.ColSet
	for _, col := range input {
		res.Add(col)
	}
	return res
}
