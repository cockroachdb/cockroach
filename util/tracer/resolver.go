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

package tracer

import (
	"bytes"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// A CallResolver is a helping hand around runtime.Caller() to look up file,
// line and name of the calling function. CallResolver caches the results of
// its lookups and strips the uninteresting prefix from both the caller's
// location and name; see NewCallResolver().
// TODO(tschottdorf): consolidate this object with the Caller code we use in
// logging.
type CallResolver struct {
	mu          sync.Mutex
	cache       map[string]string
	stripPrefix string
}

// NewCallResolver returns a CallResolver. The caller's path (or, if offset >
// 0, the caller up the stack's) is resolved and numStrip components dropped.
// The resulting location is taken as a base path of the resolver; all calls
// from inside that path will resolve relative to it. A value of -1 disables
// this feature.
func NewCallResolver(offset int, numStrip int) *CallResolver {
	cr := &CallResolver{
		cache: map[string]string{},
	}
	if numStrip < 0 {
		return cr
	}
	_, file, _, ok := runtime.Caller(offset + 1)
	if !ok {
		panic("could not look up callsite of NewCallResolver")
	}
	fileParts := strings.Split(file, "/")
	cr.stripPrefix = strings.Join(fileParts[:len(fileParts)-numStrip-1], "/") + "/"
	return cr
}

func (cr *CallResolver) unknown() (string, string) {
	return "???", "???"
}

// Lookup returns the (reduced) file:line and function of the caller at the
// requested depth.
func (cr *CallResolver) Lookup(depth int) (fileline, fun string) {
	pc, file, line, ok := runtime.Caller(depth + 1)
	if !ok || cr == nil {
		return cr.unknown()
	}
	if strings.HasPrefix(file, cr.stripPrefix) {
		file = file[len(cr.stripPrefix):]
	}
	fileline = file + ":" + strconv.Itoa(line)
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if v, ok := cr.cache[fileline]; ok {
		return fileline, v
	}
	if f := runtime.FuncForPC(pc); f != nil {
		fun := f.Name()
		if indSlash := strings.LastIndex(fun, "/"); indSlash != -1 {
			fun = fun[indSlash+1:]
			if indDot := strings.Index(fun, "."); indDot != -1 {
				fun = fun[indDot+1:]
			}
		}
		cr.cache[fileline] = fun
	} else {
		cr.cache[fileline] = "???"
	}
	return fileline, cr.cache[fileline]
}

// String implements fmt.Stringer.
func (cr *CallResolver) String() string {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	var keys []string
	for k := range cr.cache {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		_, _ = buf.WriteString(cr.cache[k] + ":\n\t" + k + "\n")
	}
	return buf.String()
}
