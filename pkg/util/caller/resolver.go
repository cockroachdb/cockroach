// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package caller

import (
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type cachedLookup struct {
	pkg  string // <logical package name>/<file>.go
	file string // <file>.go
	line int
	fun  string
}

var dummyLookup = cachedLookup{pkg: "???", line: 1, fun: "???"}

// A CallResolver is a helping hand around runtime.Caller() to look up pkg,
// line and name of the calling function. CallResolver caches the results of
// its lookups and strips the uninteresting prefix from both the caller's
// location and name; see NewCallResolver().
type CallResolver struct {
	mu    syncutil.Mutex
	cache map[uintptr]*cachedLookup
}

var defaultCallResolver = NewCallResolver()

// Lookup returns the (reduced) pkg, line and function of the caller at the
// requested depth, using a default call resolver which drops the path of
// the project repository.
func Lookup(depth int) (file string, line int, fun string) {
	return defaultCallResolver.Lookup(depth + 1)
}

// NewCallResolver returns a CallResolver.
func NewCallResolver() *CallResolver {
	return &CallResolver{
		cache: map[uintptr]*cachedLookup{},
	}
}

var uintptrSlPool = sync.Pool{
	New: func() interface{} {
		sl := make([]uintptr, 1)
		return &sl
	},
}

var stripPrefixes = []string{
	"github.com/cockroachdb/cockroach/pkg/",
	"github.com/cockroachdb/",
}

// Lookup returns the "logical" path (i.e. logical import path of the
// surrounding package joined with filename, regardless of the physical location
// of the file), line and function of the caller at the requested depth.
func (cr *CallResolver) Lookup(depth int) (_logicalPath string, _line int, _fun string) {
	sl := uintptrSlPool.Get().(*[]uintptr)
	// NB: +2 for Callers, +1 for Caller (historical reasons)
	ok := runtime.Callers(depth+2, *sl) == 1
	pc := (*sl)[0]
	uintptrSlPool.Put(sl)
	sl = nil // prevent reuse
	if !ok || cr == nil {
		return dummyLookup.pkg, dummyLookup.line, dummyLookup.fun
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()
	if v, okCache := cr.cache[pc]; okCache {
		return v.file, v.line, v.fun
	}
	// Now do the expensive thing which we intend to cache.
	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()

	pkg, fun := parseFQFun(frame.Function)

	cl := &cachedLookup{
		pkg:  pkg,
		file: filepath.Join(pkg, filepath.Base(frame.File)),
		line: frame.Line,
		fun:  fun,
	}
	cr.cache[pc] = cl
	return cl.file, cl.line, cl.fun
}

func parseFQFun(
	fqFun string,
) (
	string, /* pkg */
	string, /* fun */
) {
	pkg := fqFun
	// Get the package name by stripping trailing dots until we see the first
	// slash (or find that there is none), which is when we need to re-add the
	// last component of the package path.
	idx := len(pkg) - 1 // last char can't match '/.', this helps unify break below
	var fun string
	for {
		newIdx := strings.LastIndexAny(pkg[:idx], "/.")
		// Examples to consider (at time of `break`)
		//
		// github.com/foo/bar/baz.(*Something).Else
		//                       ^-- idx
		//                   ^------ newIdx
		// github.com/foo/bar/baz.Else
		//                       ^-- idx
		//                   ^-- newIdx
		// github.com/foo/bar/baz..func1
		//                       ^-- idx
		//                   ^-- newIdx
		// main.(*Bar).Baz
		//     ^-- idx
		//         newIdx == -1
		// main.init
		//     ^-- idx
		//         newIdx == -1
		if newIdx == -1 || pkg[newIdx] == '/' {
			pkg, fun = pkg[:idx], pkg[idx+1:]
			break
		}
		idx = newIdx
	}

	for _, pref := range stripPrefixes {
		if strings.HasPrefix(pkg, pref) {
			pkg = strings.TrimPrefix(pkg, pref)
			break // only strip one prefix
		}
	}
	return pkg, fun
}
