// Copyright 2017 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestConjToExpr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testData = []struct {
		sql      string
		expected string
	}{
		{`true AND true`, `true`},
		{`true AND (true AND false)`, `true AND false`},
		{`(true AND true) AND false`, `true AND false`},
		{`(true AND true) AND (false AND true)`, `true AND false`},
		{`true OR false AND false`, `true OR (false AND false)`},
	}

	for _, d := range testData {
		expr, err := parser.ParseExprTraditional(d.sql)
		if err != nil {
			t.Fatal(err)
		}
		typedExpr, err := parser.TypeCheck(expr, nil, parser.TypeAny)
		if err != nil {
			t.Fatal(err)
		}
		conj := exprToConj(typedExpr)
		newExpr := conjToExpr(conj)
		exprStr := newExpr.String()
		if exprStr != d.expected {
			t.Errorf("expected: %s, got: %s", d.expected, exprStr)
		}
	}
}

func TestMergeConj(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testData = []struct {
		sql1     string
		sql2     string
		expected string
	}{
		{`true`, `true`, `[true // "true"]`},
		{`true AND false`, `true`, `[true // "true", false // "false"]`},
		{`false`, `true AND false`, `[false // "false", true // "true"]`},
		{`true OR false`, `true AND false`, `[true OR false // "true OR false", true // "true", false // "false"]`},
	}

	parseExpr := func(sql string) parser.TypedExpr {
		expr, err := parser.ParseExprTraditional(sql)
		if err != nil {
			t.Fatal(err)
		}
		typedExpr, err := parser.TypeCheck(expr, nil, parser.TypeAny)
		if err != nil {
			t.Fatal(err)
		}
		return typedExpr
	}

	for _, d := range testData {
		e1 := exprToConj(parseExpr(d.sql1))
		e2 := exprToConj(parseExpr(d.sql2))

		merged := mergeConj(e1, e2)
		mergedStr := merged.String()
		if mergedStr != d.expected {
			t.Errorf("expected: %s, got: %s", d.expected, mergedStr)
		}
	}
}

func TestExprToConj(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testData = []struct {
		sql      string
		expected string
	}{
		{`true AND true`, `[true // "true"]`},
		{`true AND (true AND false)`, `[true // "true", false // "false"]`},
		{`(true AND true) AND false`, `[true // "true", false // "false"]`},
		{`(true AND true) AND (false AND true)`, `[true // "true", false // "false"]`},
		{`true OR false AND false`, `[true OR (false AND false) // "true OR (false AND false)"]`},
	}

	for _, d := range testData {
		expr, err := parser.ParseExprTraditional(d.sql)
		if err != nil {
			t.Fatal(err)
		}
		typedExpr, err := parser.TypeCheck(expr, nil, parser.TypeAny)
		if err != nil {
			t.Fatal(err)
		}
		conj := exprToConj(typedExpr)
		conjStr := conj.String()
		if conjStr != d.expected {
			t.Errorf("expected: %s, got: %s", d.expected, conjStr)
		}
	}
}
