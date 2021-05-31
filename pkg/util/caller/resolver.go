// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package caller

import (
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type cachedLookup struct {
	file string
	line int
	fun  string
}

var dummyLookup = cachedLookup{file: "???", line: 1, fun: "???"}

// A CallResolver is a helping hand around runtime.Caller() to look up file,
// line and name of the calling function. CallResolver caches the results of
// its lookups and strips the uninteresting prefix from both the caller's
// location and name; see NewCallResolver().
type CallResolver struct {
	mu    syncutil.Mutex
	cache map[uintptr]*cachedLookup
	re    *regexp.Regexp
}

var reStripNothing = regexp.MustCompile(`^$`)

// findFileAndPackageRoot identifies separately the package path to
// the crdb source tree relative to the package build root, and the
// package build root. This information is needed to construct
// defaultRE below.
//
// For example:
//
//     /home/kena/src/go/src/github.com/cockroachdb/cockroach/pkg/util/caller
//                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ crdb package path
//     ^^^^^^^^^^^^^^^^^^^^^^ package build root
//
// Within a Bazel sandbox:
//
//     github.com/cockroachdb/cockroach/pkg/util/caller
//     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ crdb package path
//     (there is no package build root in this case)
//
//
// The first return value is false if the paths could not be
// determined.
func findFileAndPackageRoot() (ok bool, crdbPath string, srcRoot string) {
	pcs := make([]uintptr, 1)
	if runtime.Callers(1, pcs[:]) < 1 {
		return false, "", ""
	}
	frame, _ := runtime.CallersFrames(pcs).Next()

	// frame.Function is the name of the function prefixed by its
	// *symbolic* package path.
	// For example:
	//     github.com/cockroachdb/cockroach/pkg/util/caller.findFileAndPackageRoot
	funcName := frame.Function

	crdbPath = strings.TrimSuffix(funcName, ".findFileAndPackageRoot")

	// frame.File is the name of the file on the filesystem.
	// For example:
	//   /home/kena/src/go/src/github.com/cockroachdb/cockroach/pkg/util/caller/resolver.go
	//
	// (or, in a Bazel sandbox)
	//   github.com/cockroachdb/cockroach/pkg/util/caller/resolver.go
	//
	// root is its immediate parent directory.
	root := path.Dir(frame.File)

	// Coverage tests report back as `[...]/util/caller/_test/_obj_test`;
	// strip back to this package's directory.
	if !strings.HasSuffix(root, "/caller") {
		// This trims the last component.
		root = path.Dir(root)
	}

	if !strings.HasSuffix(root, "/caller") {
		// If we are not finding the current package in the path, this is
		// indicative of a bug in this code; either:
		//
		// - the name of the function was changed without updating the TrimSuffix
		//   call above.
		// - the package was renamed without updating the two HasSuffix calls
		//   above.
		panic(fmt.Sprintf("cannot find self package: expected .../caller, got %q", root))
	}

	if !strings.HasSuffix(root, crdbPath) {
		// We require the logical package name to be included in the
		// physical file name.
		//
		// Conceptually, this requirement could be violated if e.g. the
		// filesystem used different path separators as the logical
		// package paths.
		//
		// However, as of Go 1.16, the stack frame code normalizes
		// paths to use the same delimiters.
		// If this ever changes, we'll need to update this logic.
		panic(fmt.Sprintf("cannot find crdb path (%q) inside file path (%q)", crdbPath, root))
	}

	// The package build root is everything before the package path.
	srcRoot = strings.TrimSuffix(root, crdbPath)

	// For the crdb package root, rewind up to the `pkg` root
	// and one up.
	for {
		if strings.HasSuffix(crdbPath, "/pkg") {
			break
		}
		// Sanity check.
		if crdbPath == "." {
			// There was no "pkg" root.
			panic(fmt.Sprintf("caller package is not located under pkg tree: %q", root))
		}
		crdbPath = path.Dir(crdbPath)
	}
	crdbPath = path.Dir(crdbPath) // Also trim /pkg.

	// At this point we have simplified:
	//
	//     /home/kena/src/go/src/github.com/cockroachdb/cockroach/pkg/util/caller/resolver.go
	//     crdbPath: "github.com/cockroachdb/cockroach"
	//     srcRoot: "/home/kena/src/go/src/"
	//
	// Within a Bazel sandbox:
	//
	//     github.com/cockroachdb/cockroach/pkg/util/caller
	//     crdbPath: "github.com/cockroachdb/cockroach"
	//     srcRoot: ""
	//
	return true, crdbPath, srcRoot
}

// defaultRE strips as follows:
//
// - <fileroot><crdbroot>/(pkg/)?module/submodule/file.go
//   -> module/submodule/file.go
//
// - <fileroot><crdbroot>/vendor/<otherpkg>/path/to/file
//   -> vendor/<otherpkg>/path/to/file
//
// - <fileroot><otherpkg>/path/to/file
//   -> <otherpkg>/path/to/file
//
// It falls back to stripping nothing when it's unable to look up its
// own location via runtime.Caller().
var defaultRE = func() *regexp.Regexp {
	ok, crdbRoot, fileRoot := findFileAndPackageRoot()
	if !ok {
		return reStripNothing
	}

	pkgStrip := regexp.QuoteMeta(fileRoot) + "(?:" + regexp.QuoteMeta(crdbRoot) + "/)?(?:pkg/)?(.*)"
	return regexp.MustCompile(pkgStrip)
}()

var defaultCallResolver = NewCallResolver(defaultRE)

// Lookup returns the (reduced) file, line and function of the caller at the
// requested depth, using a default call resolver which drops the path of
// the project repository.
func Lookup(depth int) (file string, line int, fun string) {
	return defaultCallResolver.Lookup(depth + 1)
}

// NewCallResolver returns a CallResolver. The supplied pattern must specify a
// valid regular expression and is used to format the paths returned by
// Lookup(): If submatches are specified, their concatenation forms the path,
// otherwise the match of the whole expression is used. Paths which do not
// match at all are left unchanged.
// TODO(bdarnell): don't strip paths at lookup time, but at display time;
// need better handling for callers such as x/tools/something.
func NewCallResolver(re *regexp.Regexp) *CallResolver {
	return &CallResolver{
		cache: map[uintptr]*cachedLookup{},
		re:    re,
	}
}

// Lookup returns the (reduced) file, line and function of the caller at the
// requested depth.
func (cr *CallResolver) Lookup(depth int) (file string, line int, fun string) {
	pc, file, line, ok := runtime.Caller(depth + 1)
	if !ok || cr == nil {
		return dummyLookup.file, dummyLookup.line, dummyLookup.fun
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if v, okCache := cr.cache[pc]; okCache {
		return v.file, v.line, v.fun
	}
	if matches := cr.re.FindStringSubmatch(file); matches != nil {
		if len(matches) == 1 {
			file = matches[0]
		} else {
			// NB: "path" is used here (and elsewhere in this file) over
			// "path/filepath" because runtime.Caller always returns unix paths.
			file = path.Join(matches[1:]...)
		}
	}

	cr.cache[pc] = &cachedLookup{file: file, line: line, fun: dummyLookup.fun}
	if f := runtime.FuncForPC(pc); f != nil {
		fun = f.Name()
		if indDot := strings.LastIndexByte(fun, '.'); indDot != -1 {
			fun = fun[indDot+1:]
		}
		cr.cache[pc].fun = fun
	}
	return file, line, fun
}
