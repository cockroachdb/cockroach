// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build race

package tracing

// finalizers are expensive under race, so this is a no-op.
func (sp *Span) setFinalizer(fn func(sp *Span)) {}
