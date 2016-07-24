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

	"github.com/pkg/errors"
	"golang.org/x/tools/refactor/importgraph"
)

// VerifyNoImports verifies that a package doesn't depend (directly or
// indirectly) on forbidden packages. The forbidden packages are specified as
// either exact matches or prefix matches.
// If GOPATH isn't set, it is an indication that the source is not available and
// the test is skipped.
func VerifyNoImports(
	t testing.TB,
	pkgPath string,
	cgo bool,
	forbiddenPkgs, forbiddenPrefixes []string,
) {
	// Skip test if source is not available.
	if build.Default.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	buildContext := build.Default
	buildContext.CgoEnabled = cgo

	forward, _, errs := importgraph.Build(&buildContext)
	for pkg, err := range errs {
		t.Error(errors.Wrapf(err, "error loading package %s", pkg))
	}
	imports := forward.Search(pkgPath)

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
