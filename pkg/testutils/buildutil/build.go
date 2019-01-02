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

package buildutil

import (
	"go/build"
	"runtime"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

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

	buildContext := build.Default
	buildContext.CgoEnabled = cgo

	checked := make(map[string]struct{})

	short := func(in string) string {
		return strings.Replace(in, "github.com/cockroachdb/cockroach/pkg/", "./pkg/", -1)
	}

	var check func(string) error
	check = func(path string) error {
		pkg, err := buildContext.Import(path, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range pkg.Imports {
			for _, forbidden := range forbiddenPkgs {
				if forbidden == imp {
					return errors.Errorf("%s imports %s, which is forbidden", short(path), short(imp))
				}
				if forbidden == "c-deps" && imp == "C" && strings.HasPrefix(path, "github.com/cockroachdb/cockroach/pkg") {
					for _, name := range pkg.CgoFiles {
						if strings.Contains(name, "zcgo_flags") {
							return errors.Errorf("%s imports %s (%s), which is forbidden", short(path), short(imp), name)
						}
					}
				}
			}
			for _, prefix := range forbiddenPrefixes {
				if strings.HasPrefix(imp, prefix) {
					return errors.Errorf("%s imports %s which has prefix %s, which is forbidden", short(path), short(imp), prefix)
				}
			}

			// https://github.com/golang/tools/blob/master/refactor/importgraph/graph.go#L159
			if imp == "C" {
				continue // "C" is fake
			}

			importPkg, err := buildContext.Import(imp, pkg.Dir, build.FindOnly)
			if err != nil {
				// go/build does not know that gccgo's standard packages don't have
				// source, and will report an error saying that it can not find them.
				//
				// See https://github.com/golang/go/issues/16701
				// and https://github.com/golang/go/issues/23607.
				if runtime.Compiler == "gccgo" {
					continue
				}
				t.Fatal(err)
			}
			imp = importPkg.ImportPath
			if _, ok := checked[imp]; ok {
				continue
			}
			if err := check(imp); err != nil {
				return errors.Wrapf(err, "%s depends on", short(path))
			}
			checked[pkg.ImportPath] = struct{}{}
		}
		return nil
	}
	if err := check(pkgPath); err != nil {
		t.Fatal(err)
	}
}
