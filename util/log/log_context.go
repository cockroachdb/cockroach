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

import (
	"fmt"
	"math"
	"sync/atomic"

	"golang.org/x/net/context"
)

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

// WithLogTagsFromCtx returns a context based on ctx with fromCtx's log tags
// added on.
func WithLogTagsFromCtx(ctx, fromCtx context.Context) context.Context {
	fromLastTag := contextLastTag(fromCtx)
	if fromLastTag == nil {
		// Nothing to do.
		return ctx
	}

	toLastTag := contextLastTag(ctx)
	if toLastTag == nil {
		// Easy case: we reuse the same log tag list directly.
		return context.WithValue(ctx, contextTagKeyType{}, fromLastTag)
	}

	// Make a copy of the logTag list.

	newTag := &logTag{}
	*newTag = *fromLastTag
	// The prev link will be set below via prevPtr.
	prevPtr := &newTag.prev

	for t := fromLastTag.prev; t != nil; t = t.prev {
		tCopy := &logTag{}
		*tCopy = *t

		*prevPtr = tCopy
		prevPtr = &tCopy.prev
	}
	*prevPtr = toLastTag
	return context.WithValue(ctx, contextTagKeyType{}, newTag)
}

// DynamicIntValue is a helper type that allows using a "dynamic" int32 value
// for a log tag.
type DynamicIntValue struct {
	value int64
}

// This value can be used to have the value show up as "?".
const DynamicIntValueUnknown = math.MinInt64

func (dv *DynamicIntValue) String() string {
	val := atomic.LoadInt64(&dv.value)
	if val == math.MinInt64 {
		return "?"
	}
	return fmt.Sprintf("%d", atomic.LoadInt64(&dv.value))
}

// Set changes the value returned by String().
func (dv *DynamicIntValue) Set(newVal int64) {
	atomic.StoreInt64(&dv.value, newVal)
}
