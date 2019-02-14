// Copyright 2019 The Cockroach Authors.
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

package exprgen_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
)

func TestExprGen(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			return tester.RunCommand(t, d)
		})
	})
}
