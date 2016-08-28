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

type valueType int

const (
	genericType valueType = iota
	int64Type
	stringType
)

type logTag struct {
	name string

	valType    valueType
	intVal     int64
	strVal     string
	genericVal interface{}

	prev *logTag
}

func (t *logTag) value() interface{} {
	switch t.valType {
	case int64Type:
		return t.intVal
	case stringType:
		return t.strVal
	default:
		return t.genericVal
	}
}

// contextTagKeyType is an empty type for the handle associated with the
// logTag value (see context.Value).
type contextTagKeyType struct{}

func contextLastTag(ctx context.Context) *logTag {
	val := ctx.Value(contextTagKeyType{})
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

func withLogTag(ctx context.Context, newTag *logTag) context.Context {
	newTag.prev = contextLastTag(ctx)
	return context.WithValue(ctx, contextTagKeyType{}, newTag)
}

// WithLogTag returns a context (derived from the given context) which when used
// with a logging function results in the given name and value being printed in
// the message. If the value is nil, just the name shows up.
func WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	return withLogTag(ctx, &logTag{name: name, valType: genericType, genericVal: value})
}

// WithLogTagInt is a variant of WithLogTag that avoids the allocation
// associated with boxing the value in an interface{}.
func WithLogTagInt(ctx context.Context, name string, value int) context.Context {
	return withLogTag(ctx, &logTag{name: name, valType: int64Type, intVal: int64(value)})
}

// WithLogTagInt64 is a variant of WithLogTag that avoids the allocation
// associated with boxing the value in an interface{}.
func WithLogTagInt64(ctx context.Context, name string, value int64) context.Context {
	return withLogTag(ctx, &logTag{name: name, valType: int64Type, intVal: value})
}

// WithLogTagStr is a variant of WithLogTag that avoids the allocation
// associated with boxing the value in an interface{}.
func WithLogTagStr(ctx context.Context, name string, value string) context.Context {
	return withLogTag(ctx, &logTag{name: name, valType: stringType, strVal: value})
}
