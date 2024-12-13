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
	c := &fastValuesCtx{Context: parent}
	// Because we want the new node to be able to produce values for all keys, we
	// need to copy all fast values from the parent context (i.e. the closest
	// fastValuesCtx ancestor).
	if p, ok := parent.(*fastValuesCtx); ok {
		// Connect to the grandparent context, since the parent won't do anything.
		// Note that (because of exactly this code), the grandparent context won't
		// be a fastValuesCtx.
		c.Context = p.Context
		c.values = p.values
	} else if v := parent.Value(fastValuesAncestorKey{}); v != nil {
		c.values = v.(*fastValuesCtx).values
	}
	c.values[key] = val
	return c
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
	var bld FastValuesBuilder
	bld.parent = parent
	// Because we want the new node to be able to produce values for all keys, we
	// need to copy all fast values from the parent context (i.e. the closest
	// fastValuesCtx ancestor).
	if p, ok := parent.(*fastValuesCtx); ok {
		// Connect to the grandparent context, since the parent won't do anything.
		// Note that (because of exactly this code), the grandparent context won't
		// be a fastValuesCtx.
		bld.parent = p.Context
		bld.values = p.values
	} else if v := parent.Value(fastValuesAncestorKey{}); v != nil {
		bld.values = v.(*fastValuesCtx).values
	}
	return bld
}

// FastValuesBuilder contains multiple values; used for WithFastValues.
type FastValuesBuilder struct {
	parent context.Context
	values [MaxFastValues]any
}

// Get gets the value for the key in the context being built. If this key was
// set before, it returns the last value passed to Set(); otherwise it returns
// the value in the context that was passed to WithFastValues().
func (b *FastValuesBuilder) Get(key FastValueKey) any {
	return b.values[key]
}

// Set sets the value for the key in the context being built.
func (b *FastValuesBuilder) Set(key FastValueKey, val any) {
	b.values[key] = val
}

// Finish constructs the context with the values set by Set().
//
// The FastValuesBuilder must not be used again.
func (b *FastValuesBuilder) Finish() context.Context {
	if buildutil.CrdbTestBuild && b.parent == nil {
		panic("invalid use of FastValuesBuilder")
	}
	c := &fastValuesCtx{
		Context: b.parent,
		values:  b.values,
	}
	b.parent = nil
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

var numFastValues atomic.Uint32

type fastValuesCtx struct {
	context.Context
	values [MaxFastValues]any
}

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
