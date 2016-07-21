// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Radu Berinde

package log

import "golang.org/x/net/context"

type logTag struct {
	name  string
	value interface{}
	prev  *logTag
}

// Unique handle for the contextTags value (see context.Value).
var contextTagKey interface{} = new(int)

func contextLastTag(ctx context.Context) *logTag {
	val := ctx.Value(contextTagKey)
	if val == nil {
		return nil
	}
	return val.(*logTag)
}

func contextLogTags(ctx context.Context) []logTag {
	var tags []logTag
	for t := contextLastTag(ctx); t != nil; t = t.prev {
		tags = append(tags, *t)
	}
	for i, j := 0, len(tags)-1; i < j; i, j = i+1, j-1 {
		tags[i], tags[j] = tags[j], tags[i]
	}
	return tags
}

// WithLogTag returns a context (derived from the given context) which when used
// with a logging function results in the given name and value being printed in
// the message. If the value is nil, just the name shows up.
func WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	lastLogTag := contextLastTag(ctx)
	newTag := &logTag{name: name, value: value, prev: lastLogTag}
	return context.WithValue(ctx, contextTagKey, newTag)
}
