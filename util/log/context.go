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
// Author: Tobias Schottdorf

package log

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"
)

// Context wraps a context.Context into an object suitable for logging.
type Context interface {
	context.Context
	With(kvs ...interface{}) Context

	Infof(string, ...interface{})
	Warningf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
}

type logContext struct {
	context.Context
}

// Background ...
func Background() Context {
	return Wrap(context.Background())
}

// Wrap ...
func Wrap(ctx context.Context) Context {
	return &logContext{ctx}
}

func (lc *logContext) With(kvs ...interface{}) Context {
	ctx := lc.Context
	l := len(kvs)
	if l%2 != 0 {
		panic("Add called with odd number of arguments")
	}
	for i := 1; i < l; i += 2 {
		ctx = context.WithValue(ctx, kvs[i-1], kvs[i])
	}
	return &logContext{ctx}
}

// Infof ...
func (lc *logContext) Infof(pat string, args ...interface{}) {
	Infof(ctxPattern(lc, pat), args...)
}

// Warningf ...
func (lc *logContext) Warningf(pat string, args ...interface{}) {
	Warningf(ctxPattern(lc, pat), args...)
}

// Errorf ...
func (lc *logContext) Errorf(pat string, args ...interface{}) {
	Errorf(ctxPattern(lc, pat), args...)
}

// Fatalf ...
func (lc *logContext) Fatalf(pat string, args ...interface{}) {
	Fatalf(ctxPattern(lc, pat), args...)
}

var _ Context = &logContext{}

func parseContext(ctx context.Context) []string {
	var r []string
	for i := Field(0); i < maxField; i++ {
		if v := ctx.Value(i); v != nil {
			r = append(r, i.String()+"="+fmt.Sprintf("%v", v))
		}
	}
	return r
}

func ctxPattern(ctx context.Context, pat string) string {
	sl := parseContext(ctx)
	if len(sl) == 0 {
		return pat
	}
	return strings.Join(sl, " ") + ": " + pat
}
