// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/datadriven"
)

func fingerprint(ot *opttester.OptTester, t *testing.T) string {
	f := NewFingerprintFactory(exec.StubFactory{})
	expr, err := ot.OptBuild()
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
	return f.Fingerprint().String() + "\n"
}

func explainFingerprint(fp string, catalog cat.Catalog) string {
	flags := Flags{HideValues: true, Redact: RedactAll}
	ob := NewOutputBuilder(flags)
	f := NewFingerprintFactory(exec.StubFactory{})
	explainPlan, err := f.DecodeFingerprint(fp, catalog)
	if err != nil {
		panic(err)
	}
	Emit(explainPlan, ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" })
	return ob.BuildString()
}

func plan(ot *opttester.OptTester, t *testing.T) string {
	f := NewFactory(exec.StubFactory{})
	expr, err := ot.OptBuild()
	if err != nil {
		t.Error(err)
	}
	var mem *memo.Memo
	if rel, ok := expr.(memo.RelExpr); ok {
		mem = rel.Memo()
	}
	explainPlan, err := ot.ExecBuild(f, mem, expr)
	if err != nil {
		t.Error(err)
	}
	flags := Flags{HideValues: true, Redact: RedactAll}
	ob := NewOutputBuilder(flags)
	Emit(explainPlan.(*Plan), ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" })
	return ob.BuildString()
}

func TestFingerprintBuilder(t *testing.T) {
	catalog := testcat.New()
	testFingerprints := func(t *testing.T, d *datadriven.TestData) string {
		ot := opttester.New(catalog, d.Input)
		for _, a := range d.CmdArgs {
			if err := ot.Flags.Set(a); err != nil {
				d.Fatalf(t, "%+v", err)
			}
		}
		switch d.Cmd {
		case "import":
			ot.Import(t)
			return ""
		case "fingerprint-explain-roundtrip":
			fp := fingerprint(ot, t)
			plan := plan(ot, t)
			fpplan := explainFingerprint(fp, catalog)
			return fp + plan + fpplan //strings.Join([]string{fp, plan, fpplan}, "\n")
		case "fingerprint":
			return fingerprint(ot, t)
			// Take fingeprint string and display plan
		case "explain-fingerprint":
			return explainFingerprint(d.Input, catalog)
		case "plan":
			return plan(ot, t)
		default:
			panic(fmt.Sprintf("unknown command %s", d.Cmd))
		}
	}
	// RFC: should I move this to opt_tester?
	datadriven.RunTest(t, "testdata/fingerprint", testFingerprints)
	datadriven.RunTest(t, "testdata/explain_fingerprint", testFingerprints)
}
