// Copyright 2015 The Cockroach Authors.
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
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	_ "github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
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
