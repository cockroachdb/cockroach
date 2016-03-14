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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package buildutil

import "go/build"

// TransitiveImports returns a set containing all of importpath's
// transitive dependencies.
func TransitiveImports(importpath string, cgo bool) (map[string]struct{}, error) {
	buildContext := build.Default
	buildContext.CgoEnabled = cgo

	imports := make(map[string]struct{})

	var addImports func(string) error
	addImports = func(root string) error {
		pkg, err := buildContext.Import(root, buildContext.GOPATH, 0)
		if err != nil {
			return err
		}

		for _, imp := range pkg.Imports {
			// https://github.com/golang/tools/blob/master/refactor/importgraph/graph.go#L115
			if imp == "C" {
				continue // "C" is fake
			}
			if _, ok := imports[imp]; !ok {
				imports[imp] = struct{}{}
				if err := addImports(imp); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return imports, addImports(importpath)
}
