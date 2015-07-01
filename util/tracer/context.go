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

import "golang.org/x/net/context"

// contextKeyType is a dummy type to avoid naming collisions with other
// package's context keys.
type contextKeyType int

// contextKey is the key claimed for storing and retrieving a Trace from
// a context.Context.
const contextKey contextKeyType = 0

// FromCtx is a helper to pass a Trace in a context.Context. It returns the
// Trace stored at ContextKey or nil.
func FromCtx(ctx context.Context) *Trace {
	if t, ok := ctx.Value(contextKey).(*Trace); ok {
		return t
	}
	return nil
}

// ToCtx is a helper to store a Trace in a context.Context under ContextKey.
func ToCtx(ctx context.Context, trace *Trace) context.Context {
	return context.WithValue(ctx, contextKey, trace)
}
