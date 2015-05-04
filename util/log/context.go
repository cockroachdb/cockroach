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

	"github.com/cockroachdb/cockroach/util/log/logfield"
	"golang.org/x/net/context"
)

// RoachContext wraps a context.Context, providing additional sugar.
type RoachContext interface {
	context.Context
	Add(kvs ...interface{}) RoachContext
}

type roachContext struct {
	context.Context
}

// Background ...
func Background() RoachContext {
	return &roachContext{context.Background()}
}

func (rc *roachContext) Add(kvs ...interface{}) RoachContext {
	return &roachContext{Add(rc.Context, kvs...)}
}

var _ RoachContext = &roachContext{}

// Add returns a context with the supplied kvs stored as key-value
// pairs. The even entries constitute the keys.
func Add(ctx context.Context, kvs ...interface{}) context.Context {
	for i := 1; i < len(kvs); i += 2 {
		ctx = context.WithValue(ctx, kvs[i-1], kvs[i])
	}
	return ctx
}

func parseContext(ctx context.Context) []string {
	var r []string
	for i := logfield.Field(0); i < logfield.MaxField; i++ {
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

// CtxInfof ...
func CtxInfof(ctx context.Context, pat string, args ...interface{}) {
	Infof(ctxPattern(ctx, pat), args...)
}

// CtxWarningf ...
func CtxWarningf(ctx context.Context, pat string, args ...interface{}) {
	Warningf(ctxPattern(ctx, pat), args...)
}

// CtxErrorf ...
func CtxErrorf(ctx context.Context, pat string, args ...interface{}) {
	Errorf(ctxPattern(ctx, pat), args...)
}

// CtxFatalf ...
func CtxFatalf(ctx context.Context, pat string, args ...interface{}) {
	Fatalf(ctxPattern(ctx, pat), args...)
}
