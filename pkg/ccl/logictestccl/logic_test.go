// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logictestccl

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const logictestGlob = "logic_test/[^.]*"

func TestCCLLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, testutils.TestDataPath(t, logictestGlob))
}

// TestTenantLogic runs all non-CCL logic test files under the 3node-tenant
// configuration, which constructs a secondary tenant and runs the test within
// that secondary tenant's sandbox. Test files that blocklist the 3node-tenant
// configuration (i.e. "# LogicTest: !3node-tenant") are not run.
func TestTenantLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testdataDir := "../../sql/logictest/testdata/"
	if bazel.BuiltWithBazel() {
		runfile, err := bazel.Runfile("pkg/sql/logictest/testdata/")
		if err != nil {
			t.Fatal(err)
		}
		testdataDir = runfile
	}

	logictest.RunLogicTestWithDefaultConfig(
		t, logictest.TestServerArgs{}, "3node-tenant", true, /* runCCLConfigs */
		filepath.Join(testdataDir, logictestGlob))
}

// TestTenantCCLLogic is the same as TestTenantLogic but runs all CCL logic test
// files.
func TestTenantCCLLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logictest.RunLogicTestWithDefaultConfig(
		t, logictest.TestServerArgs{}, "3node-tenant", true, /* runCCLConfigs */
		testutils.TestDataPath(t, logictestGlob))
}

func TestTenantSQLLiteLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logictest.RunSQLLiteLogicTest(t, "3node-tenant")
}
