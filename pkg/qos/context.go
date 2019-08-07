// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package qos

// TODO(ajwerner): change this store the encoded value so that intermediate
// servers of different versions do not mutate qos.Level values.

import "context"

type levelContextKey struct{}

// LevelFromContext extracts a Level from the passed context.
func LevelFromContext(ctx context.Context) (Level, bool) {
	l, ok := ctx.Value(levelContextKey{}).(Level)
	return l, ok
}

// ContextWithLevel returns a context which is a child of ctx storing l to be
// extracted later with LevelFromContext.
func ContextWithLevel(ctx context.Context, l Level) context.Context {
	return context.WithValue(ctx, levelContextKey{}, l)
}
