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

type level int

const (
	levelInfo level = iota
	levelWarning
	levelError
	levelFatal
)

// Add takes a context and an additional even number of arguments,
// interpreted as key-value pairs. These are added on top of the
// supplied context and the resulting context returned.
func Add(ctx context.Context, kvs ...interface{}) context.Context {
	l := len(kvs)
	if l%2 != 0 {
		panic("Add called with odd number of arguments")
	}
	for i := 1; i < l; i += 2 {
		ctx = context.WithValue(ctx, kvs[i-1], kvs[i])
	}
	return ctx
}

// Infoc ...
func Infoc(ctx context.Context, msg string) {
	glogHandler(levelInfo, contextKV(ctx), msg)
}

// Warningc ...
func Warningc(ctx context.Context, msg string) {
	glogHandler(levelWarning, contextKV(ctx), msg)
}

// Errorc  ...
func Errorc(ctx context.Context, msg string) {
	glogHandler(levelError, contextKV(ctx), msg)
}

type kvSlice []interface{}

func contextKV(ctx context.Context) kvSlice {
	var r []interface{}
	for i := Field(0); i < maxField; i++ {
		if v := ctx.Value(i); v != nil {
			r = append(r, i, v)
		}
	}
	return r
}

func (kvs kvSlice) String() string {
	l := len(kvs)
	r := []string{}
	for i := 1; i < l; i += 2 {
		r = append(r, kvs[i-1].(fmt.Stringer).String()+"="+fmt.Sprintf("%v", kvs[i]))
	}
	return strings.Join(r, " ")
}

func glogHandler(level level, kvs kvSlice, pattern string) {
	switch level {
	case levelInfo:
		Info(kvs.String() + ": " + pattern)
	case levelWarning:
		Warning(kvs.String() + ": " + pattern)
	//case levelError:
	//	Error(kvs.String() + ": " + pattern)
	//case levelFatal:
	//	Fatal(kvs.String() + ": " + pattern)
	default:
		panic(fmt.Sprintf("unknown log level %v", level))
	}
}
