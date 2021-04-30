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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"
)

// CDeps is a magic
const CDeps = "c-deps"

// VerifyNoImports verifies that a package doesn't depend (directly or
// indirectly) on forbidden packages. The forbidden packages are specified as
// either exact matches or prefix matches.
// A match is not reported if the package that includes the forbidden package
// is listed in the allowlist.
// If GOPATH isn't set, it is an indication that the source is not available and
// the test is skipped.
//
// Note that this test is skipped under stress, race, and short.
func VerifyNoImports(
	t testing.TB, pkgPath string, forbiddenPkgs, forbiddenPrefixes []string, allowlist ...string,
) {

	skip.UnderRace(t)
	skip.UnderStress(t)
	skip.UnderShort(t)

	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps |
			packages.NeedFiles | packages.NeedTypes | packages.NeedTypesInfo,
		Logf: t.Logf,
	}, pkgPath)
	require.NoError(t, err)
	require.Len(t, pkgs, 1)
	rootPkg := pkgs[0]
	v := verifier{
		rootPkg:           rootPkg,
		forbiddenPkgs:     forbiddenPkgs,
		forbiddenPrefixes: forbiddenPrefixes,
		allowedPkgs:       allowlist,
	}
	v.verifyTransitiveImports(t, map[string]struct{}{}, []*packages.Package{rootPkg})
}

// verifier is used to verify that a package has valid imports.
type verifier struct {
	rootPkg           *packages.Package
	forbiddenPkgs     []string
	forbiddenPrefixes []string
	allowedPkgs       []string
}

// verifyTransitiveImports recursively verifies that all imports of the last
// element in the stack are returning early if that element is in the checkedSet,
// and populating into the check set before recurring into transitive imports.
//
// The stack is used to provide helpful advice on determining how a package is
// imported.
func (v *verifier) verifyTransitiveImports(
	t testing.TB, checkedSet map[string]struct{}, stack []*packages.Package,
) {
	cur := stack[len(stack)-1]
	if _, checked := checkedSet[cur.PkgPath]; checked {
		return
	}
	v.verifyImport(t, stack)
	checkedSet[cur.PkgPath] = struct{}{}
	for _, imp := range importsToSortedSlice(cur.Imports) {
		v.verifyTransitiveImports(t, checkedSet, append(stack, imp))
	}
}

func importsToSortedSlice(m map[string]*packages.Package) (s []*packages.Package) {
	s = make([]*packages.Package, 0, len(m))
	for _, p := range m {
		s = append(s, p)
	}
	sort.Slice(s, func(i, j int) bool {
		return s[i].PkgPath < s[j].PkgPath
	})
	return s
}

func (v *verifier) report(
	t testing.TB, stack []*packages.Package, format string, args ...interface{},
) {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s imports %s: ", short(v.rootPkg.PkgPath), short(stack[len(stack)-1].PkgPath))
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString("\nvia:\n")
	for i := len(stack) - 2; i >= 0; i-- {
		fmt.Fprintf(&buf, "\t%s\n", stack[i].PkgPath)
	}
	t.Errorf(buf.String())
}

func (v *verifier) verifyImport(t testing.TB, stack []*packages.Package) {
	imp := stack[len(stack)-1]
	for _, forbidden := range v.forbiddenPkgs {
		v.checkIfImportIsForbidden(t, stack, forbidden, imp)
		v.verifyCgoPackageForbidden(t, stack, forbidden, imp)
	}
	for _, prefix := range v.forbiddenPrefixes {
		if strings.HasPrefix(imp.PkgPath, prefix) {
			v.report(t, stack, "prefix %s forbidden", prefix)
		}
	}
}

// verifyCgoPackageForbidden reports an error if the forbidden string is CDeps and
// the package is in the cockroach tree and it's not some special geoproj package
// and it has some generated zcgo_flags files.
//
// TODO(ajwerner): Reconsider this cgo logic.
func (v *verifier) verifyCgoPackageForbidden(
	t testing.TB, stack []*packages.Package, forbidden string, imp *packages.Package,
) {
	if forbidden != CDeps {
		return
	}

	_, importsCgo := imp.Imports["runtime/cgo"]
	if !importsCgo ||
		!strings.HasPrefix(imp.PkgPath, "github.com/cockroachdb/cockroach/pkg") ||
		imp.PkgPath == "github.com/cockroachdb/cockroach/pkg/geo/geoproj" {
		return
	}
	for _, name := range imp.GoFiles {
		if strings.Contains(name, "zcgo_flags") {
			v.report(t, stack, "cgo forbidden")
		}
	}
}

func (v *verifier) checkIfImportIsForbidden(
	t testing.TB, stack []*packages.Package, forbidden string, imp *packages.Package,
) {
	if forbidden != imp.PkgPath {
		return
	}
	for _, w := range v.allowedPkgs {
		if imp.PkgPath == w {
			return
		}
	}
	v.report(t, stack, "forbidden")
}

func short(in string) string {
	return strings.Replace(in, "github.com/cockroachdb/cockroach/pkg/", "./pkg/", -1)
}
