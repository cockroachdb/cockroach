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
// Author: Peter Mattis (peter.mattis@gmail.com)

package cli

import (
	"go/build"
	"testing"
)

func TestNoLinkTesting(t *testing.T) {
	if build.Default.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	imports := make(map[string]struct{})

	var addImports func(string)
	addImports = func(root string) {
		pkg, err := build.Import(root, build.Default.GOPATH, 0)
		if err != nil {
			t.Fatal(err)
		}

		for _, imp := range pkg.Imports {
			// https: //github.com/golang/tools/blob/master/refactor/importgraph/graph.go#L115
			if imp == "C" {
				continue // "C" is fake
			}
			if _, ok := imports[imp]; !ok {
				imports[imp] = struct{}{}
				addImports(imp)
			}
		}
	}

	addImports("github.com/cockroachdb/cockroach")

	for _, forbidden := range []string{
		"testing",
		"github.com/cockroachdb/cockroach/security/securitytest",
	} {
		if _, ok := imports[forbidden]; ok {
			t.Errorf("%s is included in the main cockroach binary!", forbidden)
		}
	}
}
