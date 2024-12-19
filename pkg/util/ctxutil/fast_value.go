// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxutil

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/logtags"
)

// FastValueKey is a key that can be used to get and set a fast value. Keys must
// be initialized using RegisterFastValueKey().
type FastValueKey uint8

// MaxFastValues is the number of FastValueKeys that can be registered.
//
// To register new values, this constant will need to be increased (which could
// cause a regression in performance).
const MaxFastValues = 8

// FastValue retrieves the value for the given key.
func FastValue(ctx context.Context, key FastValueKey) any {
	// This case is not necessary, but it should improve the fast path a bit.
	if c, ok := ctx.(*fastValuesCtx); ok {
		return c.values[key]
	}
	if v := ctx.Value(fastValuesAncestorKey{}); v != nil {
		return v.(*fastValuesCtx).values[key]
	}
	return nil
}

// WithFastValue creates a new context with a new value for the given key. It
// overrides any existing value for that same key.
//
// The given value will be returned by FastValue(ctx, key). The key must have
// been generated using RegisterFastValueKey().
//
// This is a more efficient alternative to using context.WithValue() and
// Context.Value().
func WithFastValue(parent context.Context, key FastValueKey, val any) context.Context {
	ancestor, parent := findFastValuesCtxAncestor(parent)
	ctx := newFastValuesCtx(ancestor, parent)
	ctx.values[key] = val
	return ctx
}

// WithFastValues starts the process of creating a new context that sets a
// number of fast values. The returned builder can be used to set values and
// construct the context; it also provides efficient access to the current
// values in the parent context.
//
// Sample usage:
//
//	b := WithFastValues(ctx)
//	b.Set(key1, val1)
//	b.Set(key2, val2)
//	ctx = b.Finish()
func WithFastValues(parent context.Context) FastValuesBuilder {
	ancestor, parent := findFastValuesCtxAncestor(parent)
	return FastValuesBuilder{
		ctx: newFastValuesCtx(ancestor, parent),
	}
}

// WithFastValuesPrealloc is like WithFastValues, but preallocates multiple
// contexts that can later be reused on subsequent WithFastValue(s) calls to
// save on allocations.
func WithFastValuesPrealloc(parent context.Context) FastValuesBuilder {
	ancestor, parent := findFastValuesCtxAncestor(parent)
	container := &fastValuesContainer{}
	container.used.Store(1)
	ctx := &container.buf[0]
	ctx.init(parent, container, ancestor)
	return FastValuesBuilder{
		ctx: ctx,
	}
}

// FastValuesBuilder contains multiple values; used for WithFastValues.
type FastValuesBuilder struct {
	ctx *fastValuesCtx
}

// Get gets the value for the key in the context being built. If this key was
// set before, it returns the last value passed to Set(); otherwise it returns
// the value in the context that was passed to WithFastValues().
func (b *FastValuesBuilder) Get(key FastValueKey) any {
	return b.ctx.values[key]
}

// Set sets the value for the key in the context being built.
func (b *FastValuesBuilder) Set(key FastValueKey, val any) {
	b.ctx.values[key] = val
}

// Finish constructs the context with the values set by Set().
//
// The FastValuesBuilder must not be used again.
func (b *FastValuesBuilder) Finish() context.Context {
	if buildutil.CrdbTestBuild && b.ctx == nil {
		panic("invalid use of FastValuesBuilder")
	}
	ctx := b.ctx
	b.ctx = nil
	return ctx
}

// RegisterFastValueKey creates a key that can be used with WithFastValue(). This
// is intended to be called from global initialization code.
//
// Only MaxFastValues calls to RegisterFastValueKey are allowed for the lifetime
// of the binary; only a handful of very frequent in-context values should use
// this infrastructure.
func RegisterFastValueKey() FastValueKey {
	n := numFastValues.Add(1)
	if n > MaxFastValues {
		panic("too many fast values registered")
	}
	return FastValueKey(n - 1)
}

var numFastValues atomic.Uint32

type fastValuesCtx struct {
	context.Context
	values [MaxFastValues]any
	// container is set if this node was allocated using a fastValuesContainer
	// that had more space. The container can be used to allocate new descendant
	// nodes.
	container *fastValuesContainer
}

func (ctx *fastValuesCtx) init(
	parent context.Context, container *fastValuesContainer, ancestor *fastValuesCtx,
) {
	ctx.Context = parent
	if ancestor != nil {
		// Because we want the new node to be able to produce values for all fast
		// keys, we need to copy all fast values from the parent context (i.e. the
		// closest fastValuesCtx ancestor).
		ctx.values = ancestor.values
	}
	ctx.container = container
}

// fastValuesAncestorKey is used to retrieve the closest fastValuesCtx ancestor.
type fastValuesAncestorKey struct{}

// Value is part of the context.Context interface.
func (ctx *fastValuesCtx) Value(key any) any {
	if _, ok := key.(fastValuesAncestorKey); ok {
		return ctx
	}
	return ctx.Context.Value(key)
}

// newFastValuesCtx returns a new context with the given parent and the values
// from the ancestor.
//
// If the ancestor has an associated container that has free slots, it is used
// to avoid a new allocation.
func newFastValuesCtx(ancestor *fastValuesCtx, parent context.Context) *fastValuesCtx {
	var ctx *fastValuesCtx
	var container *fastValuesContainer
	if ancestor != nil && ancestor.container != nil {
		// Try to use the container.
		var more bool
		ctx, more = ancestor.container.get()
		if more {
			container = ancestor.container
		} else if ctx == nil {
			ctx = &fastValuesCtx{}
		}
	} else {
		ctx = &fastValuesCtx{}
	}
	ctx.init(parent, container, ancestor)
	return ctx
}

// findFastValuesCtxAncestor returns the closest fastValuesCtx ancestor of the
// given parent context.
//
// The returned newParent is the same with parent except when parent is a
// fastValueCtx, in which case newParent is that parent's parent. This is used
// to avoid unnecessarily stacking fastValueCtx nodes.
func findFastValuesCtxAncestor(
	parent context.Context,
) (_ *fastValuesCtx, newParent context.Context) {
	if p, ok := parent.(*fastValuesCtx); ok {
		return p, p.Context
	}
	if v := parent.Value(fastValuesAncestorKey{}); v != nil {
		return v.(*fastValuesCtx), parent
	}
	return nil, parent
}

const fastValuesContainerSize = 3

// fastValuesContainer is used when we expect successive WithFastValue(s) calls.
// We allocate a multi-object container for the first call and then use it as
// needed.
type fastValuesContainer struct {
	used atomic.Int32
	buf  [fastValuesContainerSize]fastValuesCtx
}

// get returns a new fastValueCtx from the container, and whether there are
// still available slots in the container. If the container is used, returns nil
// and false.
func (c *fastValuesContainer) get() (_ *fastValuesCtx, more bool) {
	for {
		u := c.used.Load()
		if u >= fastValuesContainerSize {
			return nil, false
		}
		if c.used.CompareAndSwap(u, u+1) {
			return &c.buf[u], u+1 < fastValuesContainerSize
		}
	}
}

func init() {
	// Set up log tags to use fast values.
	logtags.OverrideContextFuncs(LogTagsFromContext, WithLogTags)
}

var LogTagsKey = RegisterFastValueKey()

// LogTagsFromContext returns the log tags from the context, if any.
func LogTagsFromContext(ctx context.Context) *logtags.Buffer {
	if v := FastValue(ctx, LogTagsKey); v != nil {
		return v.(*logtags.Buffer)
	}
	return nil
}

// WithLogTags returns a context with the given tags. Used as the
// implementation for logtags.WithTags.
func WithLogTags(ctx context.Context, tags *logtags.Buffer) context.Context {
	return WithFastValue(ctx, LogTagsKey, tags)
}
