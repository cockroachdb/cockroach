// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logictest

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestLogic runs logic tests that were written by hand to test various
// CockroachDB features. The tests use a similar methodology to the SQLLite
// Sqllogictests. All of these tests should only verify correctness of output,
// and not how that output was derived. Therefore, these tests can be run
// with multiple configs, or even run against Postgres to verify it returns the
// same logical results.
//
// See the comments in logic.go for more details.
func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	RunLogicTest(t, TestServerArgs{}, "testdata/logic_test/[^.]*")
}

// TestSqlLiteLogic runs the supported SqlLite logic tests. See the comments
// for runSQLLiteLogicTest for more detail on these tests.
func TestSqlLiteLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	RunSQLLiteLogicTest(t, "" /* configOverride */)
}

// TestFloatsMatch is a unit test for floatsMatch() and floatsMatchApprox()
// functions.
func TestFloatsMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		f1, f2 string
		match  bool
	}{
		{f1: "NaN", f2: "+Inf", match: false},
		{f1: "+Inf", f2: "+Inf", match: true},
		{f1: "NaN", f2: "NaN", match: true},
		{f1: "+Inf", f2: "-Inf", match: false},
		{f1: "-0.0", f2: "0.0", match: true},
		{f1: "0.0", f2: "NaN", match: false},
		{f1: "123.45", f2: "12.345", match: false},
		{f1: "0.1234567890123456", f2: "0.1234567890123455", match: true},
		{f1: "0.1234567890123456", f2: "0.1234567890123457", match: true},
		{f1: "-0.1234567890123456", f2: "0.1234567890123456", match: false},
		{f1: "-0.1234567890123456", f2: "-0.1234567890123455", match: true},
	} {
		match, err := floatsMatch(tc.f1, tc.f2)
		if err != nil {
			t.Fatal(err)
		}
		if match != tc.match {
			t.Fatalf("floatsMatch: wrong result on %v", tc)
		}

		match, err = floatsMatchApprox(tc.f1, tc.f2)
		if err != nil {
			t.Fatal(err)
		}
		if match != tc.match {
			t.Fatalf("floatsMatchApprox: wrong result on %v", tc)
		}
	}
}
