// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxutil

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/crlib/cralloc"
	"github.com/cockroachdb/logtags"
)

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
	c := fastValuesAlloc.Alloc()
	c.Context = parent
	if p, ok := parent.(*fastValuesCtx); ok {
		// Connect to the grandparent context, since the parent won't do anything.
		c.Context = p.Context
		c.values = p.values
	} else if v := parent.Value(fastValuesAncestorKey{}); v != nil {
		c.values = v.(*fastValuesCtx).values
	}
	c.values[key] = val
	return c
}

// WithFastValues creates a new context that sets multiple fast values.
//
// It returns a FastValuesBuilder which can be used to set keys and then create
// the context.
//
// Sample usage:
//
//	ctx = WithFastValues(ctx).Set(key1, val1).Set(key2, val2).Finish()
func WithFastValues(parent context.Context) FastValuesBuilder {
	c := fastValuesAlloc.Alloc()
	c.Context = parent
	if p, ok := parent.(*fastValuesCtx); ok {
		// Connect to the grandparent context, since the parent won't do anything.
		c.Context = p.Context
		c.values = p.values
	} else if v := parent.Value(fastValuesAncestorKey{}); v != nil {
		c.values = v.(*fastValuesCtx).values
	}
	return FastValuesBuilder{c: c}
}

// FastValuesBuilder is returned by WithFastValues. Set() should be called as
// necessary, followed by a final Finish().
type FastValuesBuilder struct {
	c *fastValuesCtx
}

// Set sets the value for the key in the context being built.
func (b FastValuesBuilder) Set(key FastValueKey, val any) FastValuesBuilder {
	b.c.values[key] = val
	return b
}

// Finish returns the built context. The FastValuesBuilder should not be used again.
func (b FastValuesBuilder) Finish() context.Context {
	c := b.c
	b.c = nil
	return c
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

// FastValueKey is a key that can be used to get and set a fast value.
type FastValueKey uint8

// MaxFastValues is the number of FastValueKeys that can be registered.
const MaxFastValues = 4

var numFastValues atomic.Uint32

type fastValuesCtx struct {
	context.Context
	values [MaxFastValues]any
}

var fastValuesAlloc = cralloc.MakeBatchAllocator[fastValuesCtx]()

// fastValuesAncestorKey is used to retrieve the closest fastValuesCtx ancestor.
type fastValuesAncestorKey struct{}

// Value is part of the context.Context interface.
func (c *fastValuesCtx) Value(key any) any {
	if _, ok := key.(fastValuesAncestorKey); ok {
		return c
	}
	return c.Context.Value(key)
}

func init() {
	// Set up log tags to use fast values.
	logtags.OverrideContextFuncs(logTagsFromContext, withLogTags)
}

var logTagsKey = RegisterFastValueKey()

func logTagsFromContext(ctx context.Context) *logtags.Buffer {
	if v := FastValue(ctx, logTagsKey); v != nil {
		return v.(*logtags.Buffer)
	}
	return nil
}

func withLogTags(ctx context.Context, tags *logtags.Buffer) context.Context {
	return WithFastValue(ctx, logTagsKey, tags)
}
