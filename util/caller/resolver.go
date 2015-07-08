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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package caller

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
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
	mu    sync.Mutex
	cache map[uintptr]*cachedLookup
	re    *regexp.Regexp
}

// defaultPattern strips src/github.com/organization/project/module/submodule/file.go
// down to module/submodule/file.go.
var defaultRE = func() *regexp.Regexp {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("unable to look up location")
	}
	const sep = string(os.PathSeparator)
	path := filepath.Dir(file)
	// Strip to $GOPATH/src.
	for i := 0; i < 5; i++ {
		path = filepath.Dir(filepath.Clean(path))
	}
	if !strings.HasSuffix(path, sep+"src") {
		panic("unable to find base path for default call resolver, got " + path)
	}
	qSep := regexp.QuoteMeta(sep)
	return regexp.MustCompile(regexp.QuoteMeta(path) + qSep + strings.Repeat(strings.Join([]string{"[^", "]+", ""}, qSep), 3) + "(.*)")
}()

var defaultCallResolver = NewCallResolver(0, defaultRE)

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
func NewCallResolver(offset int, re *regexp.Regexp) *CallResolver {
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
			file = filepath.Join(matches[1:]...)
		}
	}

	cr.cache[pc] = &cachedLookup{file: file, line: line, fun: dummyLookup.fun}
	if f := runtime.FuncForPC(pc); f != nil {
		fun = f.Name()
		if indSlash := strings.LastIndex(fun, "/"); indSlash != -1 {
			fun = fun[indSlash+1:]
			if indDot := strings.Index(fun, "."); indDot != -1 {
				fun = fun[indDot+1:]
			}
		}
		cr.cache[pc].fun = fun
	}
	return file, line, fun
}
