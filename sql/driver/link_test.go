// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package driver

import (
	"go/build"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)

	context := build.Default
	// We only care about the cgo disabled case. If it's enabled, cgo
	// dependencies are obviously OK.
	context.CgoEnabled = false

	if context.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	imports := make(map[string]struct{})

	var addImports func(string)
	addImports = func(root string) {
		pkg, err := context.Import(root, context.GOPATH, 0)
		if err != nil {
			t.Fatal(err)
		}

		for _, imp := range pkg.Imports {
			// https://github.com/golang/tools/blob/master/refactor/importgraph/graph.go#L115
			if imp == "C" {
				t.Errorf("sql/driver depends on cgo via %s", pkg.ImportPath)
				continue // "C" is fake
			}
			if _, ok := imports[imp]; !ok {
				imports[imp] = struct{}{}
				addImports(imp)
			}
		}
	}

	addImports("github.com/cockroachdb/cockroach/sql/driver")

	for _, forbidden := range []string{
		"testing",
	} {
		if _, ok := imports[forbidden]; ok {
			t.Errorf("sql/driver includes %s, which defines flags", forbidden)
		}
	}
}
