// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package buildutil

import (
	"go/build"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/errors"
)

func short(in string) string {
	return strings.Replace(in, "github.com/cockroachdb/cockroach/pkg/", "./pkg/", -1)
}

// VerifyNoImports verifies that a package doesn't depend (directly or
// indirectly) on forbidden packages. The forbidden packages are specified as
// either exact matches or prefix matches.
// A match is not reported if the package that includes the forbidden package
// is listed in the allowlist.
// If GOPATH isn't set, it is an indication that the source is not available and
// the test is skipped.
func VerifyNoImports(
	t testing.TB,
	pkgPath string,
	cgo bool,
	forbiddenPkgs, forbiddenPrefixes []string,
	allowlist ...string,
) {

	// Skip test if source is not available.
	if build.Default.GOPATH == "" {
		skip.IgnoreLint(t, "GOPATH isn't set")
	}

	buildContext := build.Default
	buildContext.CgoEnabled = cgo

	checked := make(map[string]struct{})

	var check func(string) error
	check = func(path string) error {
		pkg, err := buildContext.Import(path, "", build.FindOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range pkg.Imports {
			for _, forbidden := range forbiddenPkgs {
				if forbidden == imp {
					allowlisted := false
					for _, w := range allowlist {
						if path == w {
							allowlisted = true
							break
						}
					}
					if !allowlisted {
						return errors.Errorf("%s imports %s, which is forbidden", short(path), short(imp))
					}
				}
				if forbidden == "c-deps" &&
					imp == "C" &&
					strings.HasPrefix(path, "github.com/cockroachdb/cockroach/pkg") &&
					path != "github.com/cockroachdb/cockroach/pkg/geo/geoproj" {
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
