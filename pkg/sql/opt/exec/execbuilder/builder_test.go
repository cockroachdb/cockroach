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
