// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func makeGist(ot *opttester.OptTester, t *testing.T) explain.PlanGist {
	f := explain.NewPlanGistFactory(exec.StubFactory{})
	expr, err := ot.Optimize()
	if err != nil {
		t.Error(err)
	}
	var mem *memo.Memo
	if rel, ok := expr.(memo.RelExpr); ok {
		mem = rel.Memo()
	}
	_, err = ot.ExecBuild(f, mem, expr)
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
	err = explain.Emit(explainPlan, ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" })
	if err != nil {
		panic(err)
	}
	return ob.BuildString()
}

func plan(ot *opttester.OptTester, t *testing.T) string {
	f := explain.NewFactory(exec.StubFactory{})
	expr, err := ot.Optimize()
	if err != nil {
		t.Error(err)
	}
	if expr == nil {
		t.Error("Optimize failed, use a logictest instead?")
	}
	var mem *memo.Memo
	if rel, ok := expr.(memo.RelExpr); ok {
		mem = rel.Memo()
	}
	explainPlan, err := ot.ExecBuild(f, mem, expr)
	if err != nil {
		t.Error(err)
	}
	if explainPlan == nil {
		t.Error("Couldn't ExecBuild memo, use a logictest instead?")
	}
	flags := explain.Flags{HideValues: true, Deflake: explain.DeflakeAll, OnlyShape: true}
	ob := explain.NewOutputBuilder(flags)
	err = explain.Emit(explainPlan.(*explain.Plan), ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" })
	if err != nil {
		t.Error(err)
	}
	str := ob.BuildString()
	fmt.Printf("%s\n", str)
	return str
}

func TestPlanGistBuilder(t *testing.T) {
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
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "gists"), testGists)
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "gists_tpce"), testGists)
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
