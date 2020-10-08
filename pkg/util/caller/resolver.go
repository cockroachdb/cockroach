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

// defaultRE strips src/github.com/org/project/(pkg/)module/submodule/file.go
// down to module/submodule/file.go. It falls back to stripping nothing when
// it's unable to look up its own location via runtime.Caller().
var defaultRE = func() *regexp.Regexp {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return reStripNothing
	}
	const sep = "/"
	root := path.Dir(file)
	// Coverage tests report back as `[...]/util/caller/_test/_obj_test`;
	// strip back to this package's directory.
	for strings.Contains(root, sep) && !strings.HasSuffix(root, "caller") {
		root = path.Dir(root)
	}
	// Strip to $GOPATH/src.
	for i := 0; i < 6; i++ {
		root = path.Dir(root)
	}
	qSep := regexp.QuoteMeta(sep)
	// Part of the regexp that matches `/github.com/username/reponame/(pkg/)`.
	pkgStrip := qSep + strings.Repeat(strings.Join([]string{"[^", "]+", ""}, qSep), 3) + "(?:pkg/)?(.*)"
	if !strings.Contains(root, sep) {
		// This is again the unusual case above. The actual callsites will have
		// a "real" caller, so now we don't exactly know what to strip; going
		// up to the rightmost "src" directory will be correct unless someone
		// creates packages inside of a "src" directory within their GOPATH.
		return regexp.MustCompile(".*" + qSep + "src" + pkgStrip)
	}
	if !strings.HasSuffix(root, sep+"src") && !strings.HasSuffix(root, sep+"vendor") &&
		!strings.HasSuffix(root, sep+"pkg/mod") {
		panic("unable to find base path for default call resolver, got " + root)
	}
	return regexp.MustCompile(regexp.QuoteMeta(root) + pkgStrip)
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
