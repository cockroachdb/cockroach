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

import "golang.org/x/net/context"

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

func contextToDict(ctx context.Context, dict map[string]interface{}) {
	// TODO(tschottdorf): Could put an LRU cache here if parsing the contexts
	// turns out to be a lot of work - there will be a few static ones, and
	// then one per client.
	if ctx == nil {
		return
	}
	for i := Field(0); i < maxField; i++ {
		if v := ctx.Value(i); v != nil {
			dict[i.String()] = v
		}
	}
}
