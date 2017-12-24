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

package log

import (
	"context"

	otlog "github.com/opentracing/opentracing-go/log"
)

// logTag contains a tag name and value.
//
// Log tags are associated with Contexts and appear in all log and trace
// messages under that context.
//
// The logTag entries form a linked chain - newer (a.k.a bottom, the head of the
// list) to older (a.k.a top, the tail of the list) - overlaid onto a Context
// chain. A context has an association to the bottom-most logTag (via
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

// contextLogTags returns the tags in the context in order. The given tags
// buffer is potentially used to avoid allocations.
func contextLogTags(ctx context.Context, tags []*logTag) []*logTag {
	t := contextBottomTag(ctx)
	if t == nil {
		return nil
	}
	var n int
	for q := t; q != nil; q = q.parent {
		n++
	}
	if cap(tags) < n {
		tags = make([]*logTag, n)
	} else {
		tags = tags[:n]
	}
	for ; t != nil; t = t.parent {
		n--
		tags[n] = t
	}
	return tags
}

// addLogTagChain adds a chain of log tags to a context. The tags will be linked
// to the existing chain referenced by the context, if any.
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

// augmentTagChain appends the tags in a given chain to the tags already in the
// context, deduping elements. The order for duplicate elements will change.
// The chain is copied, not modified in place.
func augmentTagChain(ctx context.Context, bottomTag *logTag) context.Context {
	if bottomTag == nil {
		return ctx
	}
	existingChain := contextBottomTag(ctx)
	if existingChain == nil {
		// Special fast path: reuse the same log tag list directly.
		return context.WithValue(ctx, contextTagKeyType{}, bottomTag)
	}

	if bottomTag == existingChain {
		// Special case when both contexts already have the same tags.
		return ctx
	}

	return context.WithValue(ctx, contextTagKeyType{}, mergeChains(existingChain, bottomTag))
}

// mergeChains takes two chains and returns a chain formed by:
// - removing the elements in c1 that are also present in c2
// - copying c2
// - linking c2's copy in front of what's left of c1
func mergeChains(c1 *logTag, c2 *logTag) *logTag {
	// Check to see if c1 is already included in c2.
	for t := c2; t != nil; t = t.parent {
		if t == c1 {
			return c2
		}
	}

	c1 = subtractChain(c1, c2)
	bottom, top := copyChain(c2, nil)
	top.parent = c1
	return bottom
}

// copyChain takes the bottom of a chain and copies all the nodes in the chain
// before (not including) top. top can be nil to copy the whole chain.
// It returns the bottom and top of the copy.
func copyChain(bottom *logTag, top *logTag) (*logTag, *logTag) {
	if bottom == nil {
		return nil, nil
	}
	var cbottom, ctop *logTag
	for t := bottom; t != top; t = t.parent {
		cpy := *t
		cpy.parent = nil
		if cbottom == nil {
			cbottom = &cpy
		} else {
			ctop.parent = &cpy
		}
		ctop = &cpy
	}
	return cbottom, ctop
}

// subtractChain takes a chain (passed by its bottom tag) and another chain vals,
// and returns a chain containing all the nodes from the first one except the
// nodes that also exist in the second one (by key).
// The chain is not modified in place; instead, all nodes whose links changed
// are copied.
func subtractChain(chain *logTag, vals *logTag) *logTag {
	var copyTop, copyBottom *logTag
	for t := chain; t != nil; {
		// Skip any duplicate tags.
		for t != nil && chainContains(vals, t.Key()) {
			t = t.parent
		}
		if t == nil {
			break
		}
		// Now find a subchain of non-duplicate tags.
		subchainBottom, subchainTop := t, t
		for t = t.parent; t != nil && !chainContains(vals, t.Key()); {
			subchainTop, t = t, t.parent
		}
		// We have a subchain of non-duplicate tags. If it ends at the top, we can
		// use it directly; otherwise we need to make a copy because we will
		// reattach it to another parent.
		if t != nil {
			subchainBottom, subchainTop = copyChain(subchainBottom, t)
		}
		if copyTop != nil {
			copyTop.parent = subchainBottom
		} else {
			copyBottom = subchainBottom
		}
		copyTop = subchainTop
	}
	return copyBottom
}

func chainContains(vals *logTag, key string) bool {
	for e := vals; e != nil; e = e.parent {
		if e.Key() == key {
			return true
		}
	}
	return false
}
