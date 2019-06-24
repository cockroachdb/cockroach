// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/logictest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestExecBuild runs logic tests that are specific to how the optimizer builds
// queries.
//
// The test files should use combinations of the local-opt, fakedist-opt and
// 5node-dist-opt configs. For tests that only have EXPLAIN (PLAN) statements,
// it's sufficient to run on a single configuration.
func TestExecBuild(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logictest.RunLogicTest(t, "testdata/[^.]*")
}
