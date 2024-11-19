// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvnemesisutil provides basic utilities for kvnemesis.
package kvnemesisutil

import "context"

type seqKey struct{}

// WithSeq wraps the Context with a Seq.
func WithSeq(ctx context.Context, seq Seq) context.Context {
	return context.WithValue(ctx, seqKey{}, seq)
}

// FromContext extracts a Seq from the Context if there is one.
func FromContext(ctx context.Context) (Seq, bool) {
	v, ok := ctx.Value(seqKey{}).(Seq)
	return v, ok
}
