// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func makeGist(ot *opttester.OptTester, t *testing.T) explain.PlanGist {
	var f explain.PlanGistFactory
	f.Init(exec.StubFactory{})
	expr, err := ot.Optimize()
	if err != nil {
		t.Error(err)
	}
	_, err = ot.ExecBuild(&f, ot.GetMemo(), expr)
	if err != nil {
		t.Error(err)
	}
	return f.PlanGist()
}

func explainGist(gist string, catalog cat.Catalog) string {
	flags := explain.Flags{HideValues: true, Deflake: explain.DeflakeAll}
	ob := explain.NewOutputBuilder(flags)
	explainPlan, err := explain.DecodePlanGistToPlan(gist, catalog)
	if err != nil {
		panic(err)
	}
	err = explain.Emit(context.Background(), &eval.Context{}, explainPlan, ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" }, false /* createPostQueryPlanIfMissing */)
	if err != nil {
		panic(err)
	}
	return ob.BuildString()
}

func plan(ot *opttester.OptTester, t *testing.T) string {
	f := explain.NewFactory(exec.StubFactory{}, &tree.SemaContext{}, &eval.Context{})
	expr, err := ot.Optimize()
	if err != nil {
		t.Error(err)
	}
	if expr == nil {
		t.Error("Optimize failed, use a logictest instead?")
	}
	explainPlan, err := ot.ExecBuild(f, ot.GetMemo(), expr)
	if err != nil {
		t.Fatal(err)
	}
	if explainPlan == nil {
		t.Fatal("Couldn't ExecBuild memo, use a logictest instead?")
	}
	flags := explain.Flags{HideValues: true, Deflake: explain.DeflakeAll, OnlyShape: true, ShowPolicyInfo: true}
	ob := explain.NewOutputBuilder(flags)
	err = explain.Emit(context.Background(), &eval.Context{}, explainPlan.(*explain.Plan), ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" }, false /* createPostQueryPlanIfMissing */)
	if err != nil {
		t.Error(err)
	}
	str := ob.BuildString()
	fmt.Printf("%s\n", str)
	return str
}

func TestExplainBuilder(t *testing.T) {
	catalog := testcat.New()
	testGists := func(t *testing.T, d *datadriven.TestData) string {
		ot := opttester.New(catalog, d.Input)

		for _, a := range d.CmdArgs {
			if err := ot.Flags.Set(a); err != nil {
				d.Fatalf(t, "%+v", err)
			}
		}
		switch d.Cmd {
		case "gist-explain-roundtrip":
			plan := plan(ot, t)
			pg := makeGist(ot, t)
			fmt.Printf("%s\n", d.Input)
			pgplan := explainGist(pg.String(), catalog)
			return fmt.Sprintf("hash: %d\nplan-gist: %s\nexplain(shape):\n%sexplain(gist):\n%s", pg.Hash(), pg.String(), plan, pgplan)
		case "plan-gist":
			return fmt.Sprintf("%s\n", makeGist(ot, t).String())
			// Take gist string and display plan
		case "explain-plan-gist":
			return explainGist(d.Input, catalog)
		case "plan":
			return plan(ot, t)
		case "hash":
			return fmt.Sprintf("%d\n", makeGist(ot, t).Hash())
		default:
			return ot.RunCommand(t, d)
		}
	}
	// RFC: should I move this to opt_tester?
	for _, testfile := range []string{"gists", "gists_tpce", "row_level_security"} {
		t.Run(testfile, func(t *testing.T) {
			datadriven.RunTest(t, datapathutils.TestDataPath(t, testfile), testGists)
			// Reset the catalog for the next test.
			catalog = testcat.New()
		})
	}
}

func TestPlanGistHashEquivalency(t *testing.T) {
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE foo (x int);")
	if err != nil {
		t.Error(err)
	}
	ot := opttester.New(catalog, "SELECT * FROM foo;")
	gist1 := makeGist(ot, t)
	_, err = catalog.ExecuteDDL("DROP TABLE foo;")
	if err != nil {
		t.Error(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE foo (x int);")
	if err != nil {
		t.Error(err)
	}
	gist2 := makeGist(ot, t)

	if gist1.Hash() != gist2.Hash() {
		t.Errorf("gist hashes should be identical! %d != %d", gist1.Hash(), gist2.Hash())
	}
	if gist1.String() == gist2.String() {
		t.Errorf("gists should be different! %s == %s", gist1.String(), gist2.String())
	}
}
