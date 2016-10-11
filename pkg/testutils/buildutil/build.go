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

import (
	"go/build"
	"strings"
	"testing"
)

// TransitiveImports returns a set containing all of importPath's transitive
// dependencies.
func TransitiveImports(importPath string, cgo bool) (map[string]struct{}, error) {
	buildContext := build.Default
	buildContext.CgoEnabled = cgo

	imports := make(map[string]struct{})

	var addImports func(string) error
	addImports = func(root string) error {
		pkg, err := buildContext.Import(root, "", 0)
		if err != nil {
			return err
		}

		for _, imp := range pkg.Imports {
			// https://github.com/golang/tools/blob/master/refactor/importgraph/graph.go#L159
			if imp == "C" {
				continue // "C" is fake
			}
			importPkg, err := buildContext.Import(imp, pkg.Dir, build.FindOnly)
			if err != nil {
				return err
			}
			imp = importPkg.ImportPath
			if _, ok := imports[imp]; !ok {
				imports[imp] = struct{}{}
				if err := addImports(imp); err != nil {
					return err
				}
			}
		}
		return nil
	}

	return imports, addImports(importPath)
}

// VerifyNoImports verifies that a package doesn't depend (directly or
// indirectly) on forbidden packages. The forbidden packages are specified as
// either exact matches or prefix matches.
// If GOPATH isn't set, it is an indication that the source is not available and
// the test is skipped.
func VerifyNoImports(
	t testing.TB, pkgPath string, cgo bool, forbiddenPkgs, forbiddenPrefixes []string,
) {
	// Skip test if source is not available.
	if build.Default.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	imports, err := TransitiveImports(pkgPath, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range forbiddenPkgs {
		if _, ok := imports[forbidden]; ok {
			t.Errorf("Package %s includes %s, which is forbidden", pkgPath, forbidden)
		}
	}
	for _, forbiddenPrefix := range forbiddenPrefixes {
		for k := range imports {
			if strings.HasPrefix(k, forbiddenPrefix) {
				t.Errorf("Package %s includes %s, which is forbidden", pkgPath, k)
			}
		}
	}
}
