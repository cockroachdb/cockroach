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
	otlog "github.com/opentracing/opentracing-go/log"

	"golang.org/x/net/context"
)

// logTag contains a tag name and value.
//
// Log tags are associated with Contexts and appear in all log and trace
// messages under that context.
//
// The logTag entries form a linked chain (newer to older), overlaid onto a
// Context chain. A context has an association to the bottom-most logTag (via
// context.Value) and from that we can traverse the entire chain. Different
// contexts can share pieces of the same chain, so once a logTag is associated
// to a context, it is immutable.
type logTag struct {
	otlog.Field

	parent *logTag
}

// contextTagKeyType is an empty type for the handle associated with the
// logTag value (see context.Value).
type contextTagKeyType struct{}

func contextBottomTag(ctx context.Context) *logTag {
	val := ctx.Value(contextTagKeyType{})
	if val == nil {
		return nil
	}
	return val.(*logTag)
}

func contextLogTags(ctx context.Context) []logTag {
	var tags []logTag
	for t := contextBottomTag(ctx); t != nil; t = t.parent {
		tags = append(tags, *t)
	}
	for i, j := 0, len(tags)-1; i < j; i, j = i+1, j-1 {
		tags[i], tags[j] = tags[j], tags[i]
	}
	return tags
}

// addLogTagChain adds a chain of log tags to a context.
func addLogTagChain(ctx context.Context, bottomTag *logTag) context.Context {
	t := bottomTag
	for t.parent != nil {
		t = t.parent
	}
	t.parent = contextBottomTag(ctx)
	return context.WithValue(ctx, contextTagKeyType{}, bottomTag)
}

// WithLogTag returns a context (derived from the given context) which when used
// with a logging function results in the given name and value being printed in
// the message.
//
// The value is stored and passed to fmt.Fprint when the log message is
// constructed. A fmt.Stringer can be passed which allows the value to be
// "dynamic".
//
// If the value is nil, just the name shows up.
func WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	return addLogTagChain(ctx, &logTag{Field: otlog.Object(name, value)})
}

// WithLogTagInt is a variant of WithLogTag that avoids the allocation
// associated with boxing the value in an interface{}.
func WithLogTagInt(ctx context.Context, name string, value int) context.Context {
	return addLogTagChain(ctx, &logTag{Field: otlog.Int(name, value)})
}

// WithLogTagInt64 is a variant of WithLogTag that avoids the allocation
// associated with boxing the value in an interface{}.
func WithLogTagInt64(ctx context.Context, name string, value int64) context.Context {
	return addLogTagChain(ctx, &logTag{Field: otlog.Int64(name, value)})
}

// WithLogTagStr is a variant of WithLogTag that avoids the allocation
// associated with boxing the value in an interface{}.
func WithLogTagStr(ctx context.Context, name string, value string) context.Context {
	return addLogTagChain(ctx, &logTag{Field: otlog.String(name, value)})
}

// copyTagChain appends the tags in a given chain to the tags already in the
// context, skipping any duplicates.
func copyTagChain(ctx context.Context, bottomTag *logTag) context.Context {
	existingChain := contextBottomTag(ctx)
	if existingChain == nil {
		// Special fast path: reuse the same log tag list directly.
		return context.WithValue(ctx, contextTagKeyType{}, bottomTag)
	}

	if bottomTag == existingChain {
		// Special case when both contexts already have the same tags.
		return ctx
	}

	var chainTop, chainBottom *logTag

	// Make a copy of the logTag chain, skipping tags that already exist in the
	// context.
TopLoop:
	for t := bottomTag; t != nil; t = t.parent {
		// Look for the same tag in the existing chain. We expect only a few tags so
		// going through the chain every time should be faster than allocating a map.
		tName := t.Key()
		for e := existingChain; e != nil; e = e.parent {
			if e.Key() == tName {
				continue TopLoop
			}
		}
		tCopy := *t
		tCopy.parent = nil
		if chainTop == nil {
			chainBottom = &tCopy
		} else {
			chainTop.parent = &tCopy
		}
		chainTop = &tCopy
	}

	if chainBottom == nil {
		return ctx
	}

	return addLogTagChain(ctx, chainBottom)
}

// WithLogTagsFromCtx returns a context based on ctx with fromCtx's log tags
// added on.
//
// The result is equivalent to replicating the WithLogTag* calls that were
// used to obtain fromCtx and applying them to ctx in the same order - but
// skipping those for which ctx already has a tag with the same name.
func WithLogTagsFromCtx(ctx, fromCtx context.Context) context.Context {
	if bottomTag := contextBottomTag(fromCtx); bottomTag != nil {
		return copyTagChain(ctx, bottomTag)
	}
	return ctx
}
